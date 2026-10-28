use std::{collections::BTreeSet, sync::Arc, time::Duration};

use anyhow::{Context, Result, bail, ensure};
use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use tokio::{
    net::TcpStream,
    sync::{Mutex, mpsc, watch},
    time::{Instant, MissedTickBehavior, interval_at, sleep, sleep_until, timeout},
};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};
use tokio_util::sync::CancellationToken;

use crate::book::{Event, parse_message};

#[derive(Clone, Default)]
pub(crate) struct Subscription {
    pub assets: BTreeSet<String>,
    pub reset: u64,
}

pub(crate) enum Feed {
    Connected {
        socket: usize,
        generation: u64,
        reset: u64,
    },
    Message {
        socket: usize,
        generation: u64,
        received_at: DateTime<Utc>,
        events: Vec<Event>,
    },
    Disconnected {
        socket: usize,
        generation: u64,
        reason: String,
    },
}

pub(crate) struct Connections {
    pub url: String,
    pub spacing: Duration,
    pub retry_delay: Duration,
    pub next_connect: Mutex<Instant>,
}

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

async fn send(socket: &mut Socket, message: Message) -> Result<()> {
    timeout(Duration::from_secs(5), socket.send(message)).await??;
    Ok(())
}

pub(crate) async fn run(
    id: usize,
    connections: Arc<Connections>,
    mut desired: watch::Receiver<Subscription>,
    events: mpsc::Sender<Feed>,
    stop: CancellationToken,
) -> Result<()> {
    let mut generation = 0;
    let mut retry = connections.retry_delay;
    loop {
        if stop.is_cancelled() {
            return Ok(());
        }
        let subscription = desired.borrow_and_update().clone();
        if subscription.assets.is_empty() {
            tokio::select! {
                _ = stop.cancelled() => return Ok(()),
                result = desired.changed() => { result.context("subscription owner stopped")?; }
            }
            continue;
        }
        let attempt_at = {
            let mut next = connections.next_connect.lock().await;
            let at = (*next).max(Instant::now());
            *next = at + connections.spacing;
            at
        };
        let connection = tokio::select! {
            biased;
            _ = stop.cancelled() => return Ok(()),
            result = async { sleep_until(attempt_at).await; timeout(Duration::from_secs(15), connect_async(&connections.url)).await } => result,
        };
        generation += 1;
        let started = Instant::now();
        let reason = match connection {
            Ok(Ok((mut socket, _))) => {
                let session = async {
                    events
                        .send(Feed::Connected {
                            socket: id,
                            generation,
                            reset: subscription.reset,
                        })
                        .await?;
                    serve(
                        id,
                        generation,
                        &mut socket,
                        subscription,
                        &mut desired,
                        &events,
                    )
                    .await
                };
                let result = tokio::select! {
                    biased;
                    _ = stop.cancelled() => Ok(()),
                    result = session => result,
                };
                let reason = match result {
                    Ok(()) => "subscriptions changed".into(),
                    Err(error) => format!("{error:#}"),
                };
                if !stop.is_cancelled() {
                    tokio::select! {
                        _ = stop.cancelled() => {},
                        result = events.send(Feed::Disconnected { socket: id, generation, reason }) => { result?; }
                    }
                }
                let _ = timeout(Duration::from_secs(2), socket.close(None)).await;
                if stop.is_cancelled() {
                    return Ok(());
                }
                None
            }
            Ok(Err(error)) => Some(error.to_string()),
            Err(_) => Some("connection timed out".into()),
        };
        if let Some(reason) = reason {
            tokio::select! {
                _ = stop.cancelled() => return Ok(()),
                result = events.send(Feed::Disconnected { socket: id, generation, reason }) => { result?; }
            }
        }
        if started.elapsed() >= Duration::from_secs(30) {
            retry = connections.retry_delay;
        }
        tokio::select! {
            _ = stop.cancelled() => return Ok(()),
            result = desired.changed() => { result.context("subscription owner stopped")?; },
            _ = sleep(retry) => {},
        }
        retry = (retry * 2).min(Duration::from_secs(30));
    }
}

async fn serve(
    id: usize,
    generation: u64,
    socket: &mut Socket,
    mut subscription: Subscription,
    desired: &mut watch::Receiver<Subscription>,
    events: &mpsc::Sender<Feed>,
) -> Result<()> {
    send(
        socket,
        Message::Text(
            serde_json::json!({
                "type":"market", "assets_ids":subscription.assets,
                "initial_dump":true, "custom_feature_enabled":true,
            })
            .to_string()
            .into(),
        ),
    )
    .await?;
    let mut heartbeat = interval_at(
        Instant::now() + Duration::from_secs(10),
        Duration::from_secs(10),
    );
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut last_receive = Instant::now();
    loop {
        tokio::select! {
            changed = desired.changed() => {
                changed.context("subscription owner stopped")?;
                let next = desired.borrow_and_update().clone();
                if next.reset != subscription.reset { return Ok(()); }
                let removed: Vec<_> = subscription.assets.difference(&next.assets).collect();
                let added: Vec<_> = next.assets.difference(&subscription.assets).collect();
                for (operation, assets) in [("unsubscribe", removed), ("subscribe", added)] {
                    if !assets.is_empty() {
                        send(socket, Message::Text(serde_json::json!({
                            "operation":operation, "assets_ids":assets,
                            "initial_dump":true, "custom_feature_enabled":true,
                        }).to_string().into())).await?;
                    }
                }
                subscription = next;
                if subscription.assets.is_empty() { return Ok(()); }
            },
            _ = heartbeat.tick() => {
                ensure!(last_receive.elapsed() < Duration::from_secs(30), "no traffic or heartbeat response for 30 seconds");
                send(socket, Message::Text("PING".into())).await?;
            },
            message = socket.next() => {
                let received_at = Utc::now();
                let message = message.context("socket EOF")??;
                last_receive = Instant::now();
                match message {
                    Message::Text(text) if text == "PONG" => {},
                    Message::Text(text) => {
                        let parsed = parse_message(&text)?;
                        events.send(Feed::Message { socket:id, generation, received_at, events:parsed }).await?;
                    },
                    Message::Ping(payload) => send(socket, Message::Pong(payload)).await?,
                    Message::Pong(_) => {},
                    Message::Close(_) => bail!("server closed socket"),
                    _ => bail!("unexpected binary market message"),
                }
            }
        }
    }
}
