//! Parses exchange messages and maintains books without network or timer state.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result, bail, ensure};
use chrono::{DateTime, Utc};
use serde::Deserialize;

use crate::Side;
use crate::util::dtos::challenge_orderbook::{
    PriceLevelUpdateDomain, SnapshotAsksEntryDomain, SnapshotBidsEntryDomain, SnapshotDomain,
};

#[derive(Debug, Clone, PartialEq)]
pub enum Record {
    Snapshot(SnapshotDomain),
    Update(PriceLevelUpdateDomain),
}

#[derive(Debug, Clone)]
pub enum Event {
    Snapshot {
        asset_id: String,
        bids: BTreeMap<i64, i64>,
        asks: BTreeMap<i64, i64>,
        timestamp: DateTime<Utc>,
    },
    Changes {
        changes: Vec<Change>,
        timestamp: DateTime<Utc>,
    },
    NewMarket,
    Resolved {
        assets: Vec<String>,
    },
    Ignored,
}

#[derive(Debug, Clone)]
pub struct Change {
    pub asset_id: String,
    pub price: i64,
    pub quantity: i64,
    pub side: Side,
}

#[derive(Debug, Clone)]
pub struct Regression {
    pub asset_id: String,
    pub previous: DateTime<Utc>,
    pub current: DateTime<Utc>,
}

#[derive(Default)]
pub struct Applied {
    pub records: Vec<Record>,
    pub regressions: Vec<Regression>,
}

#[derive(Deserialize)]
struct WireLevel {
    price: String,
    size: String,
}

#[derive(Deserialize)]
struct WireChange {
    asset_id: String,
    price: String,
    size: String,
    side: String,
}

#[derive(Deserialize)]
#[serde(tag = "event_type")]
enum WireEvent {
    #[serde(rename = "book")]
    Snapshot {
        asset_id: String,
        bids: Vec<WireLevel>,
        asks: Vec<WireLevel>,
        timestamp: String,
    },
    #[serde(rename = "price_change")]
    Changes {
        price_changes: Vec<WireChange>,
        timestamp: String,
    },
    #[serde(rename = "new_market")]
    NewMarket,
    #[serde(rename = "market_resolved")]
    Resolved { assets_ids: Vec<String> },
    #[serde(rename = "error")]
    Error,
    #[serde(other)]
    Ignored,
}

pub fn parse_message(text: &str) -> Result<Vec<Event>> {
    let value: serde_json::Value = serde_json::from_str(text)?;
    let values = match value {
        serde_json::Value::Array(values) => values,
        value => vec![value],
    };
    values
        .into_iter()
        .map(|value| {
            ensure!(value.get("error").is_none(), "exchange returned an error");
            let wire: WireEvent = serde_json::from_value(value)?;
            Ok(match wire {
                WireEvent::Snapshot {
                    asset_id,
                    bids,
                    asks,
                    timestamp,
                } => Event::Snapshot {
                    asset_id,
                    bids: parse_levels(bids)?,
                    asks: parse_levels(asks)?,
                    timestamp: parse_timestamp(&timestamp)?,
                },
                WireEvent::Changes {
                    price_changes,
                    timestamp,
                } => Event::Changes {
                    changes: price_changes
                        .into_iter()
                        .map(|change| {
                            Ok(Change {
                                asset_id: change.asset_id,
                                price: parse_price(&change.price)?,
                                quantity: millionths(&change.size)?,
                                side: match change.side.as_str() {
                                    "BUY" => Side::Buy,
                                    "SELL" => Side::Sell,
                                    _ => bail!("invalid price-change side"),
                                },
                            })
                        })
                        .collect::<Result<_>>()?,
                    timestamp: parse_timestamp(&timestamp)?,
                },
                WireEvent::NewMarket => Event::NewMarket,
                WireEvent::Resolved { assets_ids } => Event::Resolved { assets: assets_ids },
                WireEvent::Error => bail!("exchange returned an error event"),
                WireEvent::Ignored => Event::Ignored,
            })
        })
        .collect()
}

pub fn millionths(input: &str) -> Result<i64> {
    let (whole, fraction) = input.split_once('.').unwrap_or((input, ""));
    ensure!(
        !whole.is_empty() && whole.bytes().all(|b| b.is_ascii_digit()),
        "invalid decimal: {input}"
    );
    ensure!(
        fraction.bytes().all(|b| b.is_ascii_digit()),
        "invalid decimal: {input}"
    );
    let fraction = fraction.trim_end_matches('0');
    ensure!(
        fraction.len() <= 6,
        "decimal exceeds millionth precision: {input}"
    );
    let units = whole
        .parse::<i64>()?
        .checked_mul(1_000_000)
        .context("decimal overflow")?;
    let remainder = if fraction.is_empty() {
        0
    } else {
        fraction.parse::<i64>()? * 10_i64.pow(6 - fraction.len() as u32)
    };
    units.checked_add(remainder).context("decimal overflow")
}

fn parse_price(input: &str) -> Result<i64> {
    let price = millionths(input)?;
    ensure!(price <= 1_000_000, "price outside [0, 1]");
    Ok(price)
}

fn parse_timestamp(input: &str) -> Result<DateTime<Utc>> {
    let millis: i64 = input.parse()?;
    ensure!(millis >= 0, "exchange timestamp predates the Unix epoch");
    DateTime::from_timestamp_millis(millis).context("exchange timestamp out of range")
}

fn parse_levels(levels: Vec<WireLevel>) -> Result<BTreeMap<i64, i64>> {
    let mut result = BTreeMap::new();
    for level in levels {
        let price = parse_price(&level.price)?;
        let quantity = millionths(&level.size)?;
        ensure!(
            result.insert(price, quantity).is_none(),
            "duplicate snapshot price"
        );
    }
    result.retain(|_, quantity| *quantity != 0);
    Ok(result)
}

struct Book {
    bids: BTreeMap<i64, i64>,
    asks: BTreeMap<i64, i64>,
    timestamp: DateTime<Utc>,
}

impl Book {
    fn snapshot(&self, asset_id: &str, received_at: DateTime<Utc>) -> Record {
        Record::Snapshot(SnapshotDomain {
            received_at,
            exchange_timestamp: self.timestamp,
            bids: self
                .bids
                .iter()
                .rev()
                .map(|(&price_level, &quantity)| SnapshotBidsEntryDomain {
                    price_level,
                    quantity,
                })
                .collect(),
            asks: self
                .asks
                .iter()
                .map(|(&price_level, &quantity)| SnapshotAsksEntryDomain {
                    price_level,
                    quantity,
                })
                .collect(),
            symbol: asset_id.to_owned(),
        })
    }
}

#[derive(Default)]
pub struct Books {
    books: BTreeMap<String, Option<Book>>,
}

impl Books {
    pub fn set_assets(&mut self, assets: BTreeSet<String>) {
        self.books.retain(|asset, _| assets.contains(asset));
        for asset in assets {
            self.books.entry(asset).or_default();
        }
    }

    pub fn invalidate(&mut self, assets: &BTreeSet<String>) {
        for asset in assets {
            if let Some(book) = self.books.get_mut(asset) {
                *book = None;
            }
        }
    }

    pub fn is_initialized(&self, asset: &str) -> bool {
        self.books.get(asset).is_some_and(Option::is_some)
    }

    pub fn snapshots(&self, received_at: DateTime<Utc>) -> Vec<Record> {
        self.books
            .iter()
            .filter_map(|(asset, book)| book.as_ref().map(|book| book.snapshot(asset, received_at)))
            .collect()
    }

    pub fn apply(&mut self, event: Event, received_at: DateTime<Utc>) -> Result<Applied> {
        let mut result = Applied::default();
        match event {
            Event::Snapshot {
                asset_id,
                bids,
                asks,
                timestamp,
            } => {
                if let Some(slot) = self.books.get_mut(&asset_id) {
                    if let Some(book) = slot {
                        check_timestamp(&mut result, &asset_id, book.timestamp, timestamp);
                    }
                    let book = Book {
                        bids,
                        asks,
                        timestamp,
                    };
                    result.records.push(book.snapshot(&asset_id, received_at));
                    *slot = Some(book);
                }
            }
            Event::Changes { changes, timestamp } => {
                // Validate initialization before applying any part of this event.
                for change in &changes {
                    if let Some(book) = self.books.get(&change.asset_id) {
                        ensure!(
                            book.is_some(),
                            "delta before initial snapshot for {}",
                            change.asset_id
                        );
                    }
                }
                for change in changes {
                    let Some(Some(book)) = self.books.get_mut(&change.asset_id) else {
                        continue;
                    };
                    check_timestamp(&mut result, &change.asset_id, book.timestamp, timestamp);
                    let levels = match change.side {
                        Side::Buy => &mut book.bids,
                        Side::Sell => &mut book.asks,
                    };
                    if change.quantity == 0 {
                        levels.remove(&change.price);
                    } else {
                        levels.insert(change.price, change.quantity);
                    }
                    book.timestamp = timestamp;
                    result.records.push(Record::Update(PriceLevelUpdateDomain {
                        backtest_timestamp: received_at,
                        exchange_timestamp: timestamp,
                        price_level: change.price,
                        quantity: change.quantity,
                        side: change.side,
                        symbol: change.asset_id,
                    }));
                }
            }
            Event::NewMarket | Event::Resolved { .. } | Event::Ignored => {}
        }
        Ok(result)
    }
}

fn check_timestamp(
    result: &mut Applied,
    asset_id: &str,
    previous: DateTime<Utc>,
    incoming: DateTime<Utc>,
) {
    if incoming < previous {
        result.regressions.push(Regression {
            asset_id: asset_id.to_owned(),
            previous,
            current: incoming,
        });
    }
}
