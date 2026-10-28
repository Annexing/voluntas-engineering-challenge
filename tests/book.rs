use std::collections::BTreeSet;

use chrono::{DateTime, Utc};
use engineering_challenge::Side;
use engineering_challenge::book::{Applied, Books, Event, Record, millionths, parse_message};
use engineering_challenge::util::dtos::challenge_orderbook::SnapshotDomain;

const SNAPSHOT: &str = include_str!("fixtures/book_snapshot.json");
const CHANGES: &str = include_str!("fixtures/book_changes.json");

fn now() -> DateTime<Utc> {
    DateTime::from_timestamp_micros(1_757_908_893_000_123).unwrap()
}
fn assets(values: &[&str]) -> BTreeSet<String> {
    values.iter().map(|s| s.to_string()).collect()
}
fn apply(books: &mut Books, text: &str) -> Applied {
    let events = parse_message(text).unwrap();
    assert_eq!(events.len(), 1);
    books
        .apply(events.into_iter().next().unwrap(), now())
        .unwrap()
}
fn snapshot(record: &Record) -> &SnapshotDomain {
    let Record::Snapshot(snapshot) = record else {
        panic!("expected snapshot")
    };
    snapshot
}

#[test]
fn snapshot_updates_and_replacement_preserve_exact_book_state() {
    let mut books = Books::default();
    books.set_assets(assets(&["1"]));
    assert!(books.snapshots(now()).is_empty());
    let initial = apply(&mut books, SNAPSHOT);
    let initial = snapshot(&initial.records[0]);
    assert_eq!(initial.received_at, now());
    assert_eq!(
        initial.exchange_timestamp.timestamp_millis(),
        1_757_908_892_351
    );
    assert_eq!(
        initial
            .bids
            .iter()
            .map(|l| l.price_level)
            .collect::<Vec<_>>(),
        [500_000, 480_000]
    );
    assert_eq!(
        initial
            .asks
            .iter()
            .map(|l| l.price_level)
            .collect::<Vec<_>>(),
        [520_000, 540_000]
    );

    let applied = apply(&mut books, CHANGES);
    assert_eq!(applied.records.len(), 2);
    let Record::Update(update) = &applied.records[0] else {
        panic!("expected update")
    };
    assert_eq!(
        (update.quantity, update.side, update.backtest_timestamp),
        (12_250_000, Side::Buy, now())
    );
    let Record::Update(deletion) = &applied.records[1] else {
        panic!("expected deletion")
    };
    assert_eq!((deletion.quantity, deletion.side), (0, Side::Sell));
    let current = books.snapshots(now());
    assert_eq!(snapshot(&current[0]).bids[0].quantity, 12_250_000);
    assert_eq!(snapshot(&current[0]).asks.len(), 1);
    assert_eq!(snapshot(&current[0]).asks[0].price_level, 540_000);
    apply(&mut books, CHANGES);
    assert_eq!(books.snapshots(now()), current);

    let replacement =
        r#"{"event_type":"book","asset_id":"1","bids":[],"asks":[],"timestamp":"1757908892353"}"#;
    let replaced = apply(&mut books, replacement);
    assert!(snapshot(&replaced.records[0]).bids.is_empty());
    assert!(snapshot(&replaced.records[0]).asks.is_empty());
    assert!(books.is_initialized("1"));
}

#[test]
fn membership_and_recovery_require_fresh_snapshots() {
    let mut books = Books::default();
    books.set_assets(assets(&["1", "2"]));
    apply(&mut books, SNAPSHOT);
    let mixed = CHANGES.replace(
        "\"asset_id\":\"1\",\"price\":\"0.52\"",
        "\"asset_id\":\"2\",\"price\":\"0.52\"",
    );
    let initial = books.snapshots(now());
    assert!(
        books
            .apply(parse_message(&mixed).unwrap().remove(0), now())
            .is_err()
    );
    assert_eq!(books.snapshots(now()), initial);

    books.invalidate(&assets(&["1"]));
    assert!(books.snapshots(now()).is_empty());
    assert!(
        books
            .apply(parse_message(CHANGES).unwrap().remove(0), now())
            .is_err()
    );
    apply(&mut books, SNAPSHOT);
    books.set_assets(assets(&["2"]));
    assert!(apply(&mut books, CHANGES).records.is_empty());
    assert!(apply(&mut books, SNAPSHOT).records.is_empty());
    books.set_assets(assets(&["1", "2"]));
    assert!(!books.is_initialized("1"));
}

#[test]
fn timestamps_warn_but_never_reorder_or_discard_valid_updates() {
    let mut books = Books::default();
    books.set_assets(assets(&["1"]));
    apply(&mut books, SNAPSHOT);
    let regressed = apply(
        &mut books,
        &CHANGES.replace("1757908892352", "1757908892350"),
    );
    assert_eq!(regressed.regressions.len(), 1);
    assert_eq!(regressed.regressions[0].asset_id, "1");
    assert_eq!(regressed.records.len(), 2);
    assert_eq!(
        snapshot(&books.snapshots(now())[0])
            .exchange_timestamp
            .timestamp_millis(),
        1_757_908_892_350
    );
    assert!(
        apply(
            &mut books,
            &CHANGES.replace("1757908892352", "1757908892350")
        )
        .regressions
        .is_empty()
    );
}

#[test]
fn parsing_rejects_invalid_book_data_and_handles_lifecycle_events() {
    for (input, expected) in [
        ("0", 0),
        ("12.25000000", 12_250_000),
        ("0.000001", 1),
        ("9223372036854.775807", i64::MAX),
    ] {
        assert_eq!(millionths(input).unwrap(), expected);
    }
    for input in ["", "-1", "NaN", "1e-3", "0.0000001", "9223372036854.775808"] {
        assert!(millionths(input).is_err(), "{input}");
    }
    for malformed in [
        CHANGES.replace("12.25", "NaN"),
        CHANGES.replace("BUY", "wat"),
        CHANGES.replace("0.50", "1.01"),
        SNAPSHOT.replace("0.48", "0.50"),
    ] {
        assert!(parse_message(&malformed).is_err());
    }
    assert!(parse_message(r#"{"event_type":"error"}"#).is_err());
    assert!(parse_message(r#"{"error":"bad subscription"}"#).is_err());
    let events = parse_message(r#"[{"event_type":"new_market"},{"event_type":"market_resolved","assets_ids":["1"]},{"event_type":"last_trade_price"},{"event_type":"future_feature"}]"#).unwrap();
    assert!(matches!(events[0], Event::NewMarket));
    assert!(matches!(&events[1], Event::Resolved { assets } if assets == &["1"]));
    assert!(matches!(events[2], Event::Ignored));
    assert!(matches!(events[3], Event::Ignored));
}
