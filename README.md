# Voluntas engineering challenge

I enjoyed working on this challenge!

This is a Rust collector for Polymarket's public order books, because I couldn't use Kalshi. It keeps both outcome books for each trading market and records snapshots and price-level updates with the supplied SBE helpers.

## running

Use current stable Rust on Linux or WSL. The starter's reader uses Unix file APIs. No account or API key is needed.

```sh
cargo build --release --locked
OUTPUT_PATH=orderbooks.sbe ./target/release/engineering-challenge
```

Without `OUTPUT_PATH`, binary output goes to stdout and must be redirected:

```sh
./target/release/engineering-challenge > orderbooks.sbe
```

`OUTPUT_PATH` refuses to overwrite an existing file. Shell redirection follows your shell's normal overwrite rules. Set `OUTPUT_STDOUT=1` alongside `OUTPUT_PATH` to explicitly write both. Diagnostics go to stderr. Ctrl-C stops collection, closes sockets, drains accepted records, and flushes output.

| Variable | Default | Purpose |
| --- | --- | --- |
| `OUTPUT_PATH` | unset | Write a new capture file instead of stdout |
| `OUTPUT_STDOUT` | enabled without a path | Set to `1` or `0`; both destinations must make progress in combined mode |
| `ASSETS_PER_SOCKET` | `200` | Maximum outcome subscriptions per socket |
| `DISCOVERY_INTERVAL_SECS` | `60` | Delay between complete market scans |
| `ASSET_IDS` | unset | Comma-separated IDs for a restricted smoke test; disables discovery |

`DISCOVERY_URL` and `WEBSOCKET_URL` can point to local mocks. Normal runs use the public Gamma and CLOB endpoints.

## how it works

```text
Market discovery ---- market lists ----+
                                      |
WebSocket tasks ---- parsed events ---> Book coordinator ---> SBE writer
```

- Discovery reads all pages for both Polymarket protocol versions. It selects markets explicitly marked active, open, order-enabled, and accepting orders. Missing trading status excludes a market; invalid IDs on an eligible market fail the scan. A failed scan leaves subscriptions unchanged.
- Socket tasks receive and parse messages concurrently. Assignments stay stable as markets change. Connection starts are spaced 250 ms apart; failed connections retry with backoff up to 30 seconds.
- One coordinator owns the books. It applies absolute quantities, removes zero-sized levels, and ignores events from obsolete connections. No book is used before its initial snapshot. A missing snapshot after 45 seconds triggers a reconnect.
- One blocking writer uses the starter's encoder and chunk layout. Bounded channels connect the stages; file writes do not block Tokio's network threads.

The asset cap and connection spacing are application defaults, not published exchange limits. Startup waits for a complete scan, and later scans are eventually consistent. Lifecycle messages request an earlier refresh; resolved assets are removed immediately.

## recording

Each outcome asset ID is its own `symbol`. Prices and quantities are exact integer millionths. Updates contain the new absolute quantity, including zero deletions. Snapshots contain all stored levels, with bids descending and asks ascending.

Initial and recovery snapshots precede updates. Every minute, the coordinator records all initialized books and flushes once. The helper also flushes when its 64 MiB chunk fills. Partial chunks are zero-padded, so even a small flushed capture occupies 64 MiB. Output is buffered, not written per update.

Incoming records retain the exchange timestamp and local receive time. A periodic snapshot uses its creation time locally and the last applied exchange timestamp. Timestamp regressions produce rate-limited warnings; messages are still applied in socket order. Independent sockets have no global exchange ordering.

After a disconnect or invalid book message, affected books are discarded and rebuilt from fresh snapshots. The feed provides no documented sequence counter, so this cannot detect every silent gap or recover events missed during an outage. The schema has no market-removal or unavailable-book record; those transitions are logged instead of represented as empty snapshots.

An output error stops collection with a nonzero exit status. A failed capture can have an incomplete final chunk. Shutdown assumes the file or stdout consumer continues accepting writes; an indefinitely stalled pipe can delay shutdown. Flushing is not a power-loss durability guarantee.

The schema and generated types are unchanged. `AlignedSbeWriter` has a small extension to accept a `Write` destination, allowing the same format on stdout and files. Its original file constructor remains available. The original starter explanation is preserved in [docs/STARTER.md](docs/STARTER.md).

## Checks

```sh
cargo fmt --all -- --check
cargo test --all-targets --locked
cargo clippy --all-targets --locked -- -D warnings
cargo run --release --example read_capture -- orderbooks.sbe
```

Tests use local HTTP/WebSocket servers, fixed message fixtures, temporary files, and a controlled clock. They cover book replacement, updates/deletions, market changes, reconnects, output decoding/failure, periodic snapshots, and actual SIGINT cleanup. They do not require live credentials.

The offline suite has 14 tests. Live checks initialized all 200 books, then all 993 currently open IDs in a wider sample across five sockets. The latter ran for 130 seconds, recorded both minute snapshots with an empty input queue, and closed cleanly on SIGINT. The supplied reader decoded 2,984 snapshots and 5,322 updates, including 1,399 deletions.

Full-catalog discovery was checked separately and found over 500,000 outcome IDs. This scale needs thousands of sockets at the default cap; full-catalog connection capacity and sustained throughput remain unverified. Initial discovery can take many minutes before collection starts. A fixed-ID smoke test keeps retrying IDs that close; normal discovery removes them after a successful scan.

## With more time

I didn't have as much time as I would have liked, so I kept the collector's design small (and focused on getting something working). Beyond the challenge's extensions, I would start subscribing during discovery, while waiting for a complete scan before removing markets. I'd run longer replay and live load tests, measure queue delay, and consider splitting books across workers if the coordinator became a bottleneck. I'd also look at capture rotation and disk-space handling for longer runs. Separate output files could support multiple writers if writing became the limiting step.
