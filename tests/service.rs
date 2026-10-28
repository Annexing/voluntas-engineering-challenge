use std::{collections::BTreeMap, fs::File, path::Path, process::Stdio, time::Duration};

use engineering_challenge::{
    book::Record,
    collector::{Config, run},
    util::dtos::challenge_orderbook::{AnyMessage, PriceLevelUpdateDomain, SnapshotDomain},
    util::{AlignedSbeReader, OrderbookSchema},
};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
    time::timeout,
};
use tokio_tungstenite::{WebSocketStream, accept_async, tungstenite::Message};

type Socket = WebSocketStream<TcpStream>;

fn snapshot(asset: &str, size: &str, timestamp: &str) -> Value {
    json!({"event_type":"book", "asset_id":asset, "bids":[{"price":"0.4","size":size}],
        "asks":[], "timestamp":timestamp})
}

fn update(asset: &str, size: &str, timestamp: &str) -> Value {
    json!({"event_type":"price_change", "price_changes":[{"asset_id":asset,
        "price":"0.4", "size":size, "side":"BUY"}], "timestamp":timestamp})
}

async fn send(socket: &mut Socket, value: Value) {
    socket
        .send(Message::Text(value.to_string().into()))
        .await
        .unwrap();
}

async fn receive_json(socket: &mut Socket) -> Value {
    serde_json::from_str(socket.next().await.unwrap().unwrap().to_text().unwrap()).unwrap()
}

async fn accept(listener: &TcpListener) -> (String, Socket) {
    let (tcp, _) = listener.accept().await.unwrap();
    let mut socket = accept_async(tcp).await.unwrap();
    let request = receive_json(&mut socket).await;
    assert_eq!(request["type"], "market");
    assert_eq!(request["initial_dump"], true);
    let assets = request["assets_ids"].as_array().unwrap();
    assert_eq!(assets.len(), 1);
    (assets[0].as_str().unwrap().to_owned(), socket)
}

async fn barrier(socket: &mut Socket) {
    // A Pong establishes that the preceding frames reached the collector's queue.
    socket
        .send(Message::Ping(vec![1, 2, 3].into()))
        .await
        .unwrap();
    assert_eq!(
        socket.next().await.unwrap().unwrap(),
        Message::Pong(vec![1, 2, 3].into())
    );
}

fn records(path: &Path) -> Vec<Record> {
    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(path).unwrap();
    let mut result = Vec::new();
    while let Some(message) = reader.read_msg().unwrap() {
        result.push(match message {
            AnyMessage::Snapshot(decoded) => {
                Record::Snapshot(SnapshotDomain::try_from_decoder(decoded).unwrap())
            }
            AnyMessage::PriceLevelUpdate(decoded) => {
                Record::Update(PriceLevelUpdateDomain::try_from_decoder(decoded).unwrap())
            }
            _ => panic!("unexpected SBE message"),
        });
    }
    result
}

fn config(listener: &TcpListener) -> Config {
    Config {
        websocket_url: format!("ws://{}", listener.local_addr().unwrap()),
        assets_per_socket: 1,
        snapshot_interval: Duration::from_secs(3600),
        discovery_interval: Duration::from_secs(3600),
        connection_spacing: Duration::ZERO,
        reconnect_delay: Duration::from_millis(1),
        ..Config::default()
    }
}

#[tokio::test]
async fn reconnect_replaces_the_book_and_timestamp_regression_does_not_reconnect() {
    timeout(Duration::from_secs(10), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("capture.sbe");
        let mut config = config(&listener);
        config.fixed_assets = Some(["1".into()].into());
        let (stop, stopped) = oneshot::channel();
        let task = tokio::spawn(run(config, File::create(&path).unwrap(), async {
            stopped.await?;
            Ok(())
        }));

        let (_, mut first) = accept(&listener).await;
        send(&mut first, snapshot("1", "5", "1000")).await;
        send(&mut first, update("1", "4", "1001")).await;
        barrier(&mut first).await;
        first.close(None).await.unwrap();
        drop(first);

        let (_, mut second) = accept(&listener).await;
        send(&mut second, snapshot("1", "9", "2000")).await;
        send(&mut second, update("1", "7", "1999")).await;
        barrier(&mut second).await;
        stop.send(()).unwrap();
        assert!(matches!(
            second.next().await.unwrap().unwrap(),
            Message::Close(_)
        ));
        task.await.unwrap().unwrap();

        let records = records(&path);
        assert_eq!(records.len(), 4);
        let Record::Snapshot(initial) = &records[0] else {
            panic!("initial snapshot missing")
        };
        let Record::Update(first_update) = &records[1] else {
            panic!("initial update missing")
        };
        let Record::Snapshot(recovered) = &records[2] else {
            panic!("recovery snapshot missing")
        };
        let Record::Update(regressed) = &records[3] else {
            panic!("regressed update was discarded")
        };
        assert_eq!(
            (
                initial.bids[0].quantity,
                first_update.quantity,
                recovered.bids[0].quantity,
                regressed.quantity
            ),
            (5_000_000, 4_000_000, 9_000_000, 7_000_000)
        );
        assert_eq!(regressed.exchange_timestamp.timestamp_millis(), 1999);
    })
    .await
    .expect("reconnect test timed out");
}

#[tokio::test]
async fn discovery_changes_subscriptions_without_restarting_unaffected_books() {
    timeout(Duration::from_secs(10), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = config(&listener);
        config.discovery_url = format!("http://{}/markets/keyset", http.local_addr().unwrap());
        let server = tokio::spawn(async move {
            for ids in [["1", "2"], ["1", "3"]] {
                let (mut socket, _) = http.accept().await.unwrap();
                let mut request = Vec::new();
                while !request.ends_with(b"\r\n\r\n") { request.push(socket.read_u8().await.unwrap()); }
                let body = json!({"markets":ids.map(|id| json!({"id":id,"active":true,"closed":false,
                    "enableOrderBook":true,"acceptingOrders":true,"clobTokenIds":[id]}))}).to_string();
                socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
            }
        });
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("capture.sbe");
        let (stop, stopped) = oneshot::channel();
        let task = tokio::spawn(run(config, File::create(&path).unwrap(), async { stopped.await?; Ok(()) }));
        let mut sockets = BTreeMap::new();
        for _ in 0..2 {
            let (asset, mut socket) = accept(&listener).await;
            send(&mut socket, snapshot(&asset, "5", "1000")).await;
            barrier(&mut socket).await;
            sockets.insert(asset, socket);
        }
        send(sockets.get_mut("1").unwrap(), json!({"event_type":"new_market"})).await;
        let changed = sockets.get_mut("2").unwrap();
        let removed = receive_json(changed).await;
        let added = receive_json(changed).await;
        assert_eq!(removed["operation"], "unsubscribe");
        assert_eq!(removed["assets_ids"], json!(["2"]));
        assert_eq!(added["operation"], "subscribe");
        assert_eq!(added["assets_ids"], json!(["3"]));
        send(changed, update("2", "99", "1001")).await;
        send(changed, snapshot("3", "8", "1001")).await;
        barrier(changed).await;
        send(sockets.get_mut("1").unwrap(), update("1", "0", "1001")).await;
        barrier(sockets.get_mut("1").unwrap()).await;
        stop.send(()).unwrap();
        for socket in sockets.values_mut() { assert!(matches!(socket.next().await.unwrap().unwrap(), Message::Close(_))); }
        task.await.unwrap().unwrap();
        server.await.unwrap();
        let records = records(&path);
        assert_eq!(records.len(), 4);
        let snapshots: Vec<_> = records.iter().filter_map(|r| match r { Record::Snapshot(s) => Some(s.symbol.as_str()), _ => None }).collect();
        assert!(snapshots.contains(&"1") && snapshots.contains(&"2") && snapshots.contains(&"3"));
        let updates: Vec<_> = records.iter().filter_map(|r| match r { Record::Update(u) => Some((u.symbol.as_str(),u.quantity)), _ => None }).collect();
        assert_eq!(updates, [("1",0)]);
    }).await.expect("discovery integration timed out");
}

#[tokio::test]
async fn real_sigint_closes_multiple_sockets_and_flushes_the_file() {
    timeout(Duration::from_secs(15), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("capture.sbe");
        let child = tokio::process::Command::new(env!("CARGO_BIN_EXE_engineering-challenge"))
            .env(
                "WEBSOCKET_URL",
                format!("ws://{}", listener.local_addr().unwrap()),
            )
            .env("ASSET_IDS", "1,2")
            .env("ASSETS_PER_SOCKET", "1")
            .env("OUTPUT_PATH", &path)
            .env("OUTPUT_STDOUT", "0")
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let mut sockets = Vec::new();
        for _ in 0..2 {
            let (asset, mut socket) = accept(&listener).await;
            send(&mut socket, snapshot(&asset, "5", "1000")).await;
            send(&mut socket, update(&asset, "0", "1001")).await;
            barrier(&mut socket).await;
            sockets.push(socket);
        }
        assert!(
            std::process::Command::new("kill")
                .args(["-INT", &child.id().unwrap().to_string()])
                .status()
                .unwrap()
                .success()
        );
        for socket in &mut sockets {
            assert!(matches!(
                socket.next().await.unwrap().unwrap(),
                Message::Close(_)
            ));
        }
        let result = child.wait_with_output().await.unwrap();
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        assert!(String::from_utf8_lossy(&result.stderr).contains("capture closed cleanly"));
        let records = records(&path);
        assert_eq!(records.len(), 4);
        assert_eq!(
            records
                .iter()
                .filter(|r| matches!(r, Record::Snapshot(_)))
                .count(),
            2
        );
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 64 << 20);
    })
    .await
    .expect("SIGINT integration timed out");
}
