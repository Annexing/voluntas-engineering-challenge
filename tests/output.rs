use std::io::{self, Write};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use chrono::DateTime;
use engineering_challenge::{
    Side,
    book::Record,
    output::{CHUNK_SIZE, Command, Destination, write_output},
    util::dtos::challenge_orderbook::{
        AnyMessage, PriceLevelUpdateDomain, SnapshotBidsEntryDomain, SnapshotDomain,
    },
    util::{AlignedSbeReader, OrderbookSchema},
};
use tokio::sync::mpsc;

fn snapshot() -> SnapshotDomain {
    let timestamp = DateTime::from_timestamp_micros(1_700_000_000_000_000).unwrap();
    SnapshotDomain {
        received_at: timestamp,
        exchange_timestamp: timestamp,
        bids: vec![SnapshotBidsEntryDomain {
            price_level: 400_000,
            quantity: 2_000_000,
        }],
        asks: vec![],
        symbol: "123".into(),
    }
}

#[test]
fn explicit_and_final_flush_preserve_the_starter_format() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("capture.sbe");
    let sink = Destination::File(path.clone()).open().unwrap();
    assert!(Destination::File(path.clone()).open().is_err());
    let snapshot = snapshot();
    let update = PriceLevelUpdateDomain {
        backtest_timestamp: snapshot.received_at,
        exchange_timestamp: snapshot.exchange_timestamp,
        price_level: 400_000,
        quantity: 0,
        side: Side::Buy,
        symbol: snapshot.symbol.clone(),
    };
    let (tx, rx) = mpsc::channel(3);
    tx.blocking_send(Command::Records(vec![Record::Snapshot(snapshot.clone())]))
        .unwrap();
    tx.blocking_send(Command::Flush).unwrap();
    tx.blocking_send(Command::Records(vec![Record::Update(update.clone())]))
        .unwrap();
    drop(tx);
    write_output(sink, rx).unwrap();

    assert_eq!(
        std::fs::metadata(&path).unwrap().len(),
        (2 * CHUNK_SIZE) as u64
    );
    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(&path).unwrap();
    let Some(AnyMessage::Snapshot(decoded)) = reader.read_msg().unwrap() else {
        panic!("expected snapshot")
    };
    assert_eq!(SnapshotDomain::try_from_decoder(decoded).unwrap(), snapshot);
    let Some(AnyMessage::PriceLevelUpdate(decoded)) = reader.read_msg().unwrap() else {
        panic!("expected update")
    };
    assert_eq!(
        PriceLevelUpdateDomain::try_from_decoder(decoded).unwrap(),
        update
    );
    assert!(reader.read_msg().unwrap().is_none());
}

struct FailingSink(Arc<AtomicUsize>);

impl Write for FailingSink {
    fn write(&mut self, _: &[u8]) -> io::Result<usize> {
        if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
            Ok(8)
        } else {
            Err(io::Error::other("disk full"))
        }
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn a_partial_write_failure_is_returned_without_retrying_the_chunk() {
    let calls = Arc::new(AtomicUsize::new(0));
    let (tx, rx) = mpsc::channel(2);
    tx.blocking_send(Command::Records(vec![Record::Snapshot(snapshot())]))
        .unwrap();
    tx.blocking_send(Command::Flush).unwrap();
    drop(tx);
    let error = write_output(FailingSink(calls.clone()), rx).unwrap_err();
    assert!(error.to_string().contains("disk full"));
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}
