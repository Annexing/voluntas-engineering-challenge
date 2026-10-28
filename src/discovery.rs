use std::{
    cmp::Reverse,
    collections::{BTreeMap, BTreeSet, BinaryHeap},
    time::Duration,
};

use anyhow::{Context, Result, bail, ensure};
use serde::Deserialize;
use serde_json::Value;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Market {
    id: String,
    active: Option<bool>,
    closed: Option<bool>,
    archived: Option<bool>,
    enable_order_book: Option<bool>,
    accepting_orders: Option<bool>,
    version: Option<String>,
    #[serde(default)]
    clob_token_ids: Value,
    #[serde(default)]
    position_ids: Value,
}

impl Market {
    fn assets(self) -> Result<Vec<String>> {
        if self.active != Some(true)
            || self.closed != Some(false)
            || self.archived == Some(true)
            || self.enable_order_book != Some(true)
            || self.accepting_orders != Some(true)
        {
            return Ok(Vec::new());
        }
        let ids = match self.version.as_deref() {
            Some("v2") => self.position_ids,
            Some("v1") | None => self.clob_token_ids,
            Some(version) => bail!("market {} has unsupported protocol {version}", self.id),
        };
        let assets: Vec<String> = match ids {
            Value::String(ids) => serde_json::from_str(&ids),
            ids => serde_json::from_value(ids),
        }
        .with_context(|| format!("market {} has invalid outcome IDs", self.id))?;
        ensure!(!assets.is_empty(), "market {} has no outcome IDs", self.id);
        ensure!(
            assets.iter().all(|id| !id.is_empty()
                && id.len() <= 100
                && id.bytes().all(|c| c.is_ascii_digit())),
            "market {} has invalid outcome IDs",
            self.id
        );
        Ok(assets)
    }
}

#[derive(Deserialize)]
struct Page {
    markets: Vec<Market>,
    next_cursor: Option<String>,
}

/// Return a complete scan or an error, so a failed page cannot delist markets.
pub async fn discover(client: &reqwest::Client, endpoint: &str) -> Result<BTreeSet<String>> {
    let mut assets = BTreeSet::new();
    let mut cursor: Option<String> = None;
    let mut seen_cursors = BTreeSet::new();
    let mut pages = 0;
    loop {
        let mut request = client.get(endpoint).query(&[
            ("active", "true"),
            ("closed", "false"),
            ("limit", "100"),
            ("version", "v1"),
            ("version", "v2"),
        ]);
        if let Some(cursor) = &cursor {
            request = request.query(&[("after_cursor", cursor)]);
        }
        let page: Page = request
            .send()
            .await?
            .error_for_status()?
            .json()
            .await
            .context("invalid market discovery response")?;
        for market in page.markets {
            assets.extend(market.assets()?);
        }
        pages += 1;
        if pages % 100 == 0 {
            eprintln!(
                "discovery: {pages} pages, {} outcome IDs so far",
                assets.len()
            );
        }
        cursor = page.next_cursor.filter(|value| !value.is_empty());
        let Some(next) = &cursor else {
            eprintln!(
                "discovery: complete after {pages} pages; {} outcome IDs",
                assets.len()
            );
            return Ok(assets);
        };
        ensure!(
            seen_cursors.insert(next.clone()),
            "repeated market discovery cursor"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

pub struct Shards {
    assignments: BTreeMap<usize, BTreeSet<String>>,
    capacity: usize,
}

impl Shards {
    pub fn new(capacity: usize) -> Result<Self> {
        ensure!(capacity > 0, "assets per socket must be positive");
        Ok(Self {
            assignments: BTreeMap::new(),
            capacity,
        })
    }

    pub fn assignments(&self) -> &BTreeMap<usize, BTreeSet<String>> {
        &self.assignments
    }

    pub fn reconcile(&mut self, assets: &BTreeSet<String>) {
        for assigned in self.assignments.values_mut() {
            assigned.retain(|asset| assets.contains(asset));
        }
        let required = assets
            .len()
            .div_ceil(self.capacity)
            .max(assets.len().min(2));
        while self.assignments.len() < required {
            let id = self.assignments.len();
            self.assignments.insert(id, BTreeSet::new());
        }
        let assigned: BTreeSet<_> = self.assignments.values().flatten().cloned().collect();
        let mut available: BinaryHeap<_> = self
            .assignments
            .iter()
            .filter(|(_, group)| group.len() < self.capacity)
            .map(|(&id, group)| Reverse((group.len(), id)))
            .collect();
        for asset in assets.difference(&assigned) {
            let Reverse((size, id)) = available.pop().expect("shard capacity covers all assets");
            self.assignments.get_mut(&id).unwrap().insert(asset.clone());
            if size + 1 < self.capacity {
                available.push(Reverse((size + 1, id)));
            }
        }
    }
}
