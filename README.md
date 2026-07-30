# `stonfi_scylla_client`

[![CI](https://github.com/ston-fi/stonfi-scylla-client/actions/workflows/build.yml/badge.svg)](https://github.com/ston-fi/stonfi-scylla-client/actions/workflows/build.yml)

`stonfi_scylla_client` is an async ScyllaDB client for STON.fi Rust services.
It adds bounded query concurrency, retries, prepared-statement caching,
Prometheus metrics, and simple CQL migration templates on top of the upstream
`scylla` driver.

The initial release is distributed from GitHub and is not published to
crates.io.

## Installation

```toml
[dependencies]
stonfi_scylla_client = { git = "https://github.com/ston-fi/stonfi-scylla-client", tag = "v0.0.1" }
stonfi_metrics = { version = "0.0.1", git = "https://github.com/ston-fi/stonfi-metrics", rev = "v0.0.1" }
```

The crate requires Rust 1.88 or newer and a Tokio runtime.

## Quick start

```rust
use stonfi_scylla_client::client::ScyllaClient;
use stonfi_scylla_client::config::{KeyspaceConfig, ScyllaClientConfig};

# async fn connect() -> anyhow::Result<()> {
stonfi_metrics::init_metrics!()?;

let config = ScyllaClientConfig {
    url: "127.0.0.1:9042".to_owned(),
    max_parallel_queries: 64,
    keyspace: KeyspaceConfig {
        name: "my_service".to_owned(),
        replication_factor: 3,
    },
    request_timeout_ms: 5_000,
    retry_count: 3,
    initial_retry_delay_ms: 50,
    max_retry_delay_ms: 1_000,
};

let client = ScyllaClient::new(&config).await?;
client.use_keyspace().await?;

let rows = client
    .select_row(
        "system.local",
        "SELECT cluster_name FROM system.local",
        (),
        Some("read_cluster_name"),
    )
    .await?;

println!("received {} row(s)", rows.len());
# Ok(())
# }
```

See [`examples/connect.rs`](examples/connect.rs) for a runnable version.

## Behavior

- `select`, `select_one`, `select_row`, `select_single_page`, `insert`, and
  `delete` use prepared statements and the configured retry policy.
- `execute_unprepared` performs one attempt. It is intended for `USE` and schema
  migrations and must never interpolate untrusted input.
- Cloned clients share a session, prepared-statement cache, concurrency limit,
  and process-global metrics.
- `select_one` returns an error when a successful query produces more than one
  row.
- The client uses LZ4 compression and `LocalQuorum` consistency.
- Public operations return
  `errors::ScyllaClientResult<T>` with matchable, non-exhaustive error
  categories and preserved source chains.

## Metrics

The crate registers these metrics in the default Prometheus registry:

- `db_scylla_queries`
- `db_scylla_queries_duration_ms`
- `db_scylla_wait_connection_ms`

Query counters and durations use `table_name`, `query_type`, `status`, and
`query_tag` labels. `ScyllaClient::new` initializes the metrics fallibly, and
normal applications should also initialize `stonfi_metrics` during startup to
serve the global registry.

Do not link this crate and `stonfi-commons-scylla-client` into the same process.
Both own the same metric names, so initialization would fail rather than
silently produce duplicate collectors.

## Simple migrations

`simple_migrator::SimpleMigrator` substitutes `[[KEYSPACE_NAME]]` and
`[[REPLICATION_FACTOR]]`, then executes each CQL statement without preparation.
It does not track migration versions or checksums, so migration statements
should be idempotent.

The splitter supports LF and CRLF, final statements without semicolons,
full-line `--` and `//` comments, and semicolons inside single- or double-quoted
values. It is not a complete CQL parser and does not interpret block comments or
dollar-quoted values.

## Migrating from the commons crate

- Rename dependency `stonfi-commons-scylla-client` to `stonfi_scylla_client`.
- Rename Rust imports from `stonfi_commons_scylla_client` to
  `stonfi_scylla_client`.
- Replace `anyhow::Result` assumptions with
  `errors::ScyllaClientResult`/`ScyllaClientError` where errors are matched.
- Rename raw `execute` calls to `execute_unprepared`.
- Remove `provide_metrics` collector wiring and initialize `stonfi_metrics`
  during application startup.
- Switch atomically; do not run both clients in one process because their
  Prometheus metric names intentionally remain stable.

## Development

Docker is required for the integration test.

```bash
docker info
cargo test --lib --locked
cargo test --doc --locked
cargo test --examples --locked
cargo test --test test_client --locked -- --test-threads=1
cargo +nightly fmt --check
cargo clippy --all-targets --all-features --locked -- -D warnings
RUSTDOCFLAGS="-D warnings -D missing_docs" cargo doc --no-deps --all-features --locked
cargo +1.88.0 check --all-features --locked
cargo package --list --locked
git diff --check
```

Merges to `main` run release-plz after all validation jobs succeed. Release-plz
creates Git tags and GitHub Releases; it does not publish to crates.io.
