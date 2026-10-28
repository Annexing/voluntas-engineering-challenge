use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    io::Write,
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, ensure};
use chrono::Utc;
use tokio::{
    sync::{Mutex, Notify, mpsc, watch},
    task::JoinSet,
    time::{Instant, MissedTickBehavior, interval_at, sleep, timeout},
};
use tokio_util::sync::CancellationToken;

use crate::{
    book::{Books, Event},
    discovery::{Shards, discover},
    output::{Command, write_output},
    socket::{self, Connections, Feed, Subscription},
};

#[derive(Clone)]
pub struct Config {
    pub discovery_url: String,
    pub websocket_url: String,
    pub assets_per_socket: usize,
    pub discovery_interval: Duration,
    pub snapshot_interval: Duration,
    pub initialization_timeout: Duration,
    pub connection_spacing: Duration,
    pub reconnect_delay: Duration,
    pub fixed_assets: Option<BTreeSet<String>>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            discovery_url: "https://gamma-api.polymarket.com/markets/keyset".into(),
            websocket_url: "wss://ws-subscriptions-clob.polymarket.com/ws/market".into(),
            assets_per_socket: 200,
            discovery_interval: Duration::from_secs(60),
            snapshot_interval: Duration::from_secs(60),
            initialization_timeout: Duration::from_secs(45),
            connection_spacing: Duration::from_millis(250),
            reconnect_delay: Duration::from_secs(1),
            fixed_assets: None,
        }
    }
}

struct Worker {
    desired: watch::Sender<Subscription>,
    generation: Option<u64>,
    pending: BTreeMap<String, Instant>,
}

struct State {
    books: Books,
    shards: Shards,
    workers: BTreeMap<usize, Worker>,
    assets: BTreeSet<String>,
    retired: BTreeSet<String>,
    removed: BTreeSet<String>,
    last_warning: Option<Instant>,
    output: mpsc::Sender<Command>,
}

fn invalidate_worker(books: &mut Books, worker: &mut Worker) {
    books.invalidate(&worker.desired.borrow().assets);
    worker.generation = None;
    worker.pending.clear();
    worker.desired.send_modify(|s| s.reset += 1);
}

impl State {
    fn expire_initializations(&mut self, now: Instant) {
        let expired: Vec<_> = self
            .workers
            .iter()
            .filter(|(_, worker)| worker.pending.values().any(|deadline| *deadline <= now))
            .map(|(&id, _)| id)
            .collect();
        for id in expired {
            let worker = &self.workers[&id];
            eprintln!(
                "socket {id}: {} initial books missing; first asset {}",
                worker.pending.len(),
                worker.pending.keys().next().unwrap()
            );
            self.reset(id, "initial snapshot timed out");
        }
    }

    fn reset(&mut self, id: usize, reason: &str) {
        let worker = self.workers.get_mut(&id).unwrap();
        invalidate_worker(&mut self.books, worker);
        eprintln!("socket {id}: rebuilding books: {reason}");
    }

    async fn feed(
        &mut self,
        feed: Feed,
        config: &Config,
        refresh: &Notify,
        stopping: bool,
    ) -> Result<()> {
        match feed {
            Feed::Connected {
                socket,
                generation,
                reset,
            } => {
                let worker = self.workers.get_mut(&socket).context("unknown socket")?;
                if reset != worker.desired.borrow().reset {
                    return Ok(());
                }
                worker.generation = Some(generation);
                let assets = &worker.desired.borrow().assets;
                self.books.invalidate(assets);
                worker.pending = assets
                    .iter()
                    .map(|id| (id.clone(), Instant::now() + config.initialization_timeout))
                    .collect();
            }
            Feed::Disconnected {
                socket,
                generation,
                reason,
            } => {
                let worker = self.workers.get_mut(&socket).context("unknown socket")?;
                if worker.generation.is_none() || worker.generation == Some(generation) {
                    self.books.invalidate(&worker.desired.borrow().assets);
                    worker.generation = None;
                    worker.pending.clear();
                }
                eprintln!("socket {socket}: {reason}; will reconnect");
            }
            Feed::Message {
                socket,
                generation,
                received_at,
                events,
            } => {
                let worker = self.workers.get(&socket).context("unknown socket")?;
                if worker.generation != Some(generation) {
                    return Ok(());
                }
                for event in events {
                    match event {
                        Event::NewMarket => {
                            if !stopping {
                                refresh.notify_one();
                            }
                        }
                        Event::Resolved { assets } => {
                            self.retired.extend(assets.iter().cloned());
                            let previous_count = self.assets.len();
                            for id in assets {
                                self.assets.remove(&id);
                            }
                            if self.assets.len() == previous_count {
                                continue;
                            }
                            self.books.set_assets(self.assets.clone());
                            for worker in self.workers.values_mut() {
                                worker.desired.send_if_modified(|s| {
                                    let previous_count = s.assets.len();
                                    s.assets.retain(|id| self.assets.contains(id));
                                    s.assets.len() != previous_count
                                });
                                worker.pending.retain(|id, _| self.assets.contains(id));
                            }
                            if !stopping {
                                refresh.notify_one();
                            }
                        }
                        event => {
                            // Some frames contain changes for both outcomes. Keep only
                            // assets assigned to this socket to avoid cross-socket ordering.
                            let event = {
                                let desired = self.workers[&socket].desired.borrow();
                                let owned = &desired.assets;
                                match event {
                                    Event::Snapshot { ref asset_id, .. }
                                        if !owned.contains(asset_id) =>
                                    {
                                        continue;
                                    }
                                    Event::Changes {
                                        mut changes,
                                        timestamp,
                                    } => {
                                        changes.retain(|c| owned.contains(&c.asset_id));
                                        Event::Changes { changes, timestamp }
                                    }
                                    event => event,
                                }
                            };
                            let applied = self.books.apply(event, received_at);
                            match applied {
                                Ok(applied) => {
                                    if self
                                        .last_warning
                                        .is_none_or(|t| t.elapsed() >= Duration::from_secs(30))
                                        && let Some(r) = applied.regressions.first()
                                    {
                                        eprintln!(
                                            "asset {}: exchange timestamp moved backward ({} -> {}); preserving socket order",
                                            r.asset_id, r.previous, r.current
                                        );
                                        self.last_warning = Some(Instant::now());
                                    }
                                    if !applied.records.is_empty() {
                                        self.output.send(Command::Records(applied.records)).await?;
                                    }
                                }
                                Err(error) => {
                                    self.reset(socket, &format!("{error:#}"));
                                    return Ok(());
                                }
                            }
                        }
                    }
                }
                self.workers
                    .get_mut(&socket)
                    .unwrap()
                    .pending
                    .retain(|id, _| !self.books.is_initialized(id));
            }
        }
        Ok(())
    }
}

pub async fn run<W, F>(config: Config, sink: W, shutdown: F) -> Result<()>
where
    W: Write + Send + 'static,
    F: Future<Output = Result<()>>,
{
    ensure!(
        !config.discovery_interval.is_zero()
            && !config.snapshot_interval.is_zero()
            && !config.initialization_timeout.is_zero()
            && !config.reconnect_delay.is_zero(),
        "intervals must be positive"
    );
    let shards = Shards::new(config.assets_per_socket)?;
    let (output, output_rx) = mpsc::channel(128);
    let mut writer = tokio::task::spawn_blocking(move || write_output(sink, output_rx));
    let mut state = State {
        books: Books::default(),
        shards,
        workers: BTreeMap::new(),
        assets: BTreeSet::new(),
        retired: BTreeSet::new(),
        removed: BTreeSet::new(),
        last_warning: None,
        output,
    };
    let stop = CancellationToken::new();
    let refresh = Arc::new(Notify::new());
    let (catalog_tx, mut catalog) = mpsc::channel(1);
    let mut discovery = None;
    if let Some(assets) = &config.fixed_assets {
        catalog_tx.send(assets.clone()).await?;
    } else {
        let cfg = config.clone();
        let cancel = stop.clone();
        let wake = refresh.clone();
        discovery = Some(tokio::spawn(async move {
            let client = reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()?;
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => return Ok::<(), anyhow::Error>(()),
                    result = discover(&client, &cfg.discovery_url) => match result {
                        Ok(assets) => {
                            tokio::select! {
                                _ = cancel.cancelled() => return Ok(()),
                                result = catalog_tx.send(assets) => { result?; }
                            }
                        },
                        Err(error) => eprintln!("discovery failed; keeping subscriptions: {error:#}"),
                    }
                }
                tokio::select! {
                    _ = cancel.cancelled() => return Ok(()),
                    _ = sleep(cfg.discovery_interval) => {},
                    _ = wake.notified() => {}
                }
                tokio::select! {
                    _ = cancel.cancelled() => return Ok(()),
                    _ = sleep(Duration::from_secs(1)) => {}
                }
            }
        }));
    }
    let connections = Arc::new(Connections {
        url: config.websocket_url.clone(),
        spacing: config.connection_spacing,
        retry_delay: config.reconnect_delay,
        next_connect: Mutex::new(Instant::now()),
    });
    let (feed_tx, mut feeds) = mpsc::channel(1024);
    let mut sockets = JoinSet::new();
    let mut snapshots = interval_at(
        Instant::now() + config.snapshot_interval,
        config.snapshot_interval,
    );
    snapshots.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut health = interval_at(
        Instant::now() + Duration::from_secs(1),
        Duration::from_secs(1),
    );
    health.set_missed_tick_behavior(MissedTickBehavior::Skip);
    tokio::pin!(shutdown);
    let mut writer_done = false;
    let mut result = loop {
        tokio::select! {
            result = &mut shutdown => break result,
            result = &mut writer => {
                writer_done = true;
                break result.context("writer panicked").and_then(|r| r)
                    .and_then(|_| anyhow::bail!("writer stopped unexpectedly"));
            },
            Some(result) = sockets.join_next(), if !sockets.is_empty() => {
                break result.context("socket task panicked").and_then(|r| r)
                    .and_then(|_| anyhow::bail!("socket task stopped unexpectedly"));
            },
            Some(mut assets) = catalog.recv() => {
                assets.retain(|id| !state.retired.contains(id));
                let readded: BTreeSet<_> = assets.difference(&state.assets)
                    .filter(|id| state.removed.contains(*id)).cloned().collect();
                state.removed.extend(state.assets.difference(&assets).cloned());
                state.shards.reconcile(&assets);
                state.books.set_assets(assets.clone());
                for (&id, group) in state.shards.assignments() {
                    if let Some(worker) = state.workers.get_mut(&id) {
                        if worker.desired.borrow().assets != *group {
                            let old = worker.desired.borrow().assets.clone();
                            worker.pending.retain(|asset, _| group.contains(asset));
                            if worker.generation.is_some() {
                                worker.pending.extend(group.difference(&old).map(|asset| {
                                    (asset.clone(), Instant::now() + config.initialization_timeout)
                                }));
                            }
                            worker.desired.send_modify(|s| s.assets = group.clone());
                            if !group.is_disjoint(&readded) {
                                invalidate_worker(&mut state.books, worker);
                            }
                        }
                    } else if !group.is_empty() {
                        let (tx, rx) = watch::channel(Subscription { assets: group.clone(), reset: 0 });
                        state.workers.insert(id, Worker {
                            desired: tx, generation: None, pending: BTreeMap::new()
                        });
                        sockets.spawn(socket::run(
                            id, connections.clone(), rx, feed_tx.clone(), stop.clone()
                        ));
                    }
                }
                state.assets = assets;
                eprintln!("tracking {} assets across {} sockets", state.assets.len(),
                    state.shards.assignments().values().filter(|s| !s.is_empty()).count());
            },
            Some(feed) = feeds.recv() => {
                if let Err(error) = state.feed(feed, &config, &refresh, false).await {
                    break Err(error);
                }
            },
            _ = snapshots.tick() => {
                let records = state.books.snapshots(Utc::now());
                eprintln!("snapshot: {}/{} books initialized; feed queue {}",
                    records.len(), state.assets.len(), feeds.len());
                if state.output.send(Command::Records(records)).await.is_err()
                    || state.output.send(Command::Flush).await.is_err()
                {
                    break Err(anyhow::anyhow!("output writer stopped"));
                }
            },
            _ = health.tick() => {
                state.expire_initializations(Instant::now());
                if discovery.as_ref().is_some_and(|d| d.is_finished()) {
                    break match discovery.take().unwrap().await {
                        Ok(Err(e)) => Err(e),
                        Err(e) => Err(e.into()),
                        _ => Err(anyhow::anyhow!("discovery task stopped"))
                    };
                }
            }
        }
    };
    stop.cancel();
    drop(feed_tx);
    if let Some(discovery) = discovery {
        let discovered = discovery
            .await
            .context("discovery task panicked")
            .and_then(|r| r);
        if result.is_ok() {
            result = discovered;
        }
    }
    let cleanup = timeout(Duration::from_secs(10), async {
        while let Some(feed) = feeds.recv().await {
            if result.is_ok() {
                result = state.feed(feed, &config, &refresh, true).await;
            }
        }
        while let Some(task) = sockets.join_next().await {
            let completed = task.context("socket task panicked").and_then(|r| r);
            if result.is_ok() {
                result = completed;
            }
        }
    })
    .await;
    if cleanup.is_err() {
        sockets.abort_all();
        while sockets.join_next().await.is_some() {}
        if result.is_ok() {
            result = Err(anyhow::anyhow!("socket cleanup timed out"));
        }
    }
    drop(state);
    if !writer_done {
        let written = writer.await.context("writer panicked").and_then(|r| r);
        if result.is_ok() {
            result = written;
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        book::parse_message,
        util::dtos::challenge_orderbook::AnyMessage,
        util::{AlignedSbeReader, OrderbookSchema},
    };
    use futures_util::{SinkExt, StreamExt};
    use std::{
        fs::File,
        io,
        sync::atomic::{AtomicUsize, Ordering},
    };
    use tokio::{net::TcpListener, sync::oneshot};
    use tokio_tungstenite::{accept_async, tungstenite::Message};

    fn state() -> (State, mpsc::Receiver<Command>) {
        let assets = BTreeSet::from(["1".into(), "2".into()]);
        let mut books = Books::default();
        books.set_assets(assets.clone());
        let (desired, _) = watch::channel(Subscription {
            assets: assets.clone(),
            reset: 0,
        });
        let (output, rx) = mpsc::channel(16);
        (
            State {
                books,
                shards: Shards::new(200).unwrap(),
                workers: BTreeMap::from([(
                    0,
                    Worker {
                        desired,
                        generation: None,
                        pending: BTreeMap::new(),
                    },
                )]),
                assets,
                retired: BTreeSet::new(),
                removed: BTreeSet::new(),
                last_warning: None,
                output,
            },
            rx,
        )
    }

    fn book_feed(generation: u64, asset: &str) -> Feed {
        Feed::Message {
            socket: 0,
            generation,
            received_at: Utc::now(),
            events: parse_message(
                &serde_json::json!({"event_type":"book", "asset_id":asset,
                "bids":[], "asks":[], "timestamp":"1757908892351"})
                .to_string(),
            )
            .unwrap(),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn initialization_timeout_invalidates_shard_and_rejects_obsolete_generations() {
        let (mut state, mut output) = state();
        let config = Config::default();
        let refresh = Notify::new();
        state
            .feed(
                Feed::Connected {
                    socket: 0,
                    generation: 1,
                    reset: 0,
                },
                &config,
                &refresh,
                false,
            )
            .await
            .unwrap();
        state
            .feed(book_feed(1, "1"), &config, &refresh, false)
            .await
            .unwrap();
        assert!(matches!(output.try_recv().unwrap(), Command::Records(_)));
        assert_eq!(state.workers[&0].pending.len(), 1);
        tokio::time::advance(config.initialization_timeout - Duration::from_secs(1)).await;
        state.expire_initializations(Instant::now());
        assert_eq!(state.workers[&0].generation, Some(1));
        tokio::time::advance(Duration::from_secs(1)).await;
        state.expire_initializations(Instant::now());
        assert!(state.books.snapshots(Utc::now()).is_empty());
        assert_eq!(state.workers[&0].generation, None);
        assert_eq!(state.workers[&0].desired.borrow().reset, 1);

        state
            .feed(
                Feed::Connected {
                    socket: 0,
                    generation: 1,
                    reset: 0,
                },
                &config,
                &refresh,
                false,
            )
            .await
            .unwrap();
        state
            .feed(book_feed(1, "1"), &config, &refresh, false)
            .await
            .unwrap();
        assert!(output.try_recv().is_err());
        state
            .feed(
                Feed::Connected {
                    socket: 0,
                    generation: 2,
                    reset: 1,
                },
                &config,
                &refresh,
                false,
            )
            .await
            .unwrap();
        for asset in ["1", "2"] {
            state
                .feed(book_feed(2, asset), &config, &refresh, false)
                .await
                .unwrap();
        }
        assert!(state.workers[&0].pending.is_empty());
        state
            .feed(
                Feed::Disconnected {
                    socket: 0,
                    generation: 1,
                    reason: "late old connection".into(),
                },
                &config,
                &refresh,
                false,
            )
            .await
            .unwrap();
        state
            .feed(book_feed(1, "1"), &config, &refresh, false)
            .await
            .unwrap();
        tokio::time::advance(config.initialization_timeout).await;
        state.expire_initializations(Instant::now());
        assert_eq!(state.workers[&0].generation, Some(2));
        assert!(state.books.is_initialized("1") && state.books.is_initialized("2"));
        assert_eq!(output.len(), 2);
    }

    struct ObservedFile {
        file: File,
        bytes: Arc<AtomicUsize>,
        flushed: Arc<Notify>,
    }

    impl Write for ObservedFile {
        fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
            let written = self.file.write(buffer)?;
            self.bytes.fetch_add(written, Ordering::SeqCst);
            Ok(written)
        }
        fn flush(&mut self) -> io::Result<()> {
            self.file.flush()?;
            if self.bytes.load(Ordering::SeqCst) > 0 {
                self.flushed.notify_one();
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn periodic_timer_flushes_complete_books_without_waiting_wall_clock_interval() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("books.sbe");
        let bytes = Arc::new(AtomicUsize::new(0));
        let flushed = Arc::new(Notify::new());
        let sink = ObservedFile {
            file: File::create(&path).unwrap(),
            bytes: bytes.clone(),
            flushed: flushed.clone(),
        };
        let config = Config {
            websocket_url: format!("ws://{}", listener.local_addr().unwrap()),
            snapshot_interval: Duration::from_secs(3),
            fixed_assets: Some(BTreeSet::from(["1".into()])),
            ..Config::default()
        };
        let (stop, stopped) = oneshot::channel();
        let collector = tokio::spawn(run(config, sink, async {
            stopped.await?;
            Ok(())
        }));
        let (tcp, _) = timeout(Duration::from_secs(5), listener.accept())
            .await
            .unwrap()
            .unwrap();
        let mut socket = accept_async(tcp).await.unwrap();
        socket.next().await.unwrap().unwrap();
        socket
            .send(Message::Text(
                include_str!("../tests/fixtures/book_snapshot.json").into(),
            ))
            .await
            .unwrap();
        socket.send(Message::Ping(vec![1].into())).await.unwrap();
        assert!(matches!(
            socket.next().await.unwrap().unwrap(),
            Message::Pong(_)
        ));
        tokio::task::yield_now().await;

        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(2)).await;
        tokio::task::yield_now().await;
        assert_eq!(bytes.load(Ordering::SeqCst), 0);
        tokio::time::advance(Duration::from_secs(1)).await;
        tokio::time::resume();
        timeout(Duration::from_secs(5), flushed.notified())
            .await
            .unwrap();
        stop.send(()).unwrap();
        assert!(matches!(
            socket.next().await.unwrap().unwrap(),
            Message::Close(_)
        ));
        timeout(Duration::from_secs(5), collector)
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        let mut reader = AlignedSbeReader::<OrderbookSchema>::open(&path).unwrap();
        let mut snapshots = Vec::new();
        while let Some(message) = reader.read_msg().unwrap() {
            let AnyMessage::Snapshot(message) = message else {
                panic!("unexpected update")
            };
            snapshots.push(
                crate::util::dtos::challenge_orderbook::SnapshotDomain::try_from_decoder(message)
                    .unwrap(),
            );
        }
        assert_eq!(snapshots.len(), 2);
        assert_eq!(snapshots[0].bids, snapshots[1].bids);
        assert_eq!(snapshots[0].asks, snapshots[1].asks);
        assert_eq!(
            snapshots[0].exchange_timestamp,
            snapshots[1].exchange_timestamp
        );
        assert_eq!(bytes.load(Ordering::SeqCst), crate::output::CHUNK_SIZE);
    }
}
