use std::{collections::BTreeSet, time::Duration};

use engineering_challenge::discovery::{Shards, discover};
use serde_json::{Value, json};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

fn market(id: &str) -> Value {
    json!({"id": id, "active": true, "closed": false, "archived": false,
        "enableOrderBook": true, "acceptingOrders": true, "version": "v1",
        "clobTokenIds": format!("[\"{id}\"]")})
}

async fn serve(pages: Vec<(u16, Value)>) -> (String, tokio::task::JoinHandle<Vec<String>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/markets/keyset", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let mut requests = Vec::new();
        for (status, page) in pages {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            while !request.ends_with(b"\r\n\r\n") {
                request.push(socket.read_u8().await.unwrap());
                assert!(request.len() < 8192);
            }
            requests.push(String::from_utf8(request).unwrap());
            let body = page.to_string();
            socket.write_all(format!("HTTP/1.1 {status} response\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
        }
        requests
    });
    (endpoint, server)
}

fn client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap()
}

#[tokio::test]
async fn discovery_paginates_both_protocols_and_filters_nontrading_markets() {
    let mut v2 = market("2");
    v2["version"] = "v2".into();
    v2["positionIds"] = json!(["20", "21"]);
    let mut v1 = market("1");
    v1["positionIds"] = json!(["10", "11"]);
    let mut markets = vec![v1, v2];
    for (flag, value) in [
        ("active", false),
        ("closed", true),
        ("archived", true),
        ("enableOrderBook", false),
        ("acceptingOrders", false),
    ] {
        let mut excluded = market("99");
        excluded[flag] = value.into();
        excluded.as_object_mut().unwrap().remove("clobTokenIds");
        markets.push(excluded);
    }
    let mut unconfirmed = market("98");
    unconfirmed
        .as_object_mut()
        .unwrap()
        .remove("acceptingOrders");
    markets.push(unconfirmed);
    let (endpoint, server) = serve(vec![
        (200, json!({"markets": markets, "next_cursor": "next/+"})),
        (200, json!({"markets": [market("3")]})),
    ])
    .await;
    assert_eq!(
        discover(&client(), &endpoint).await.unwrap(),
        BTreeSet::from(["1".into(), "20".into(), "21".into(), "3".into()])
    );
    let requests = server.await.unwrap();
    assert!(requests[0].contains("version=v1&version=v2"));
    assert!(requests[0].contains("limit=100"));
    assert!(requests[1].contains("after_cursor=next%2F%2B"));
}

#[tokio::test]
async fn discovery_rejects_failed_pages_cycles_and_invalid_live_outcome_ids() {
    let first = json!({"markets": [market("1")], "next_cursor": "next"});
    for second in [(503, json!({})), (200, first.clone())] {
        let (endpoint, server) = serve(vec![(200, first.clone()), second]).await;
        assert!(discover(&client(), &endpoint).await.is_err());
        server.await.unwrap();
    }
    let mut invalid = market("1");
    invalid.as_object_mut().unwrap().remove("clobTokenIds");
    let (endpoint, server) = serve(vec![(200, json!({"markets": [invalid]}))]).await;
    assert!(discover(&client(), &endpoint).await.is_err());
    server.await.unwrap();
}

#[test]
fn assignments_are_balanced_bounded_and_stable_across_market_changes() {
    let mut shards = Shards::new(2).unwrap();
    let assets: BTreeSet<String> = (0..7).map(|id| id.to_string()).collect();
    shards.reconcile(&assets);
    assert_eq!(shards.assignments().len(), 4);
    assert!(
        shards
            .assignments()
            .values()
            .all(|group| (1..=2).contains(&group.len()))
    );
    let original = shards.assignments().clone();
    let mut changed = assets;
    changed.remove("0");
    changed.insert("8".into());
    shards.reconcile(&changed);
    assert_eq!(
        shards
            .assignments()
            .values()
            .flatten()
            .cloned()
            .collect::<BTreeSet<_>>(),
        changed
    );
    for (id, group) in shards.assignments() {
        assert!(group.len() <= 2);
        for asset in group.difference(&BTreeSet::from(["8".into()])) {
            assert!(original[id].contains(asset));
        }
    }
    shards.reconcile(&BTreeSet::new());
    assert!(shards.assignments().values().all(BTreeSet::is_empty));
    shards.reconcile(&BTreeSet::from(["1".into(), "2".into()]));
    assert_eq!(
        shards
            .assignments()
            .values()
            .filter(|group| !group.is_empty())
            .count(),
        2
    );
    assert!(Shards::new(0).is_err());
}
