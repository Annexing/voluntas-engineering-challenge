use std::collections::BTreeSet;

use anyhow::{Context, Result};
use engineering_challenge::util::dtos::challenge_orderbook::{
    AnyMessage, PriceLevelUpdateDomain, SnapshotDomain,
};
use engineering_challenge::util::{AlignedSbeReader, OrderbookSchema};

fn main() -> Result<()> {
    let path = std::env::args()
        .nth(1)
        .context("usage: read_capture <path>")?;
    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(path)?;
    let mut assets = BTreeSet::new();
    let (mut snapshots, mut updates, mut deletions) = (0_u64, 0_u64, 0_u64);
    while let Some(message) = reader.read_msg()? {
        match message {
            AnyMessage::Snapshot(decoded) => {
                let snapshot = SnapshotDomain::try_from_decoder(decoded)?;
                assets.insert(snapshot.symbol);
                snapshots += 1;
            }
            AnyMessage::PriceLevelUpdate(decoded) => {
                let update = PriceLevelUpdateDomain::try_from_decoder(decoded)?;
                deletions += u64::from(update.quantity == 0);
                assets.insert(update.symbol);
                updates += 1;
            }
            _ => anyhow::bail!("unexpected SBE message"),
        }
    }
    println!(
        "{} assets, {snapshots} snapshots, {updates} updates ({deletions} deletions)",
        assets.len()
    );
    Ok(())
}
