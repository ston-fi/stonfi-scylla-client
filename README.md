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
stonfi_metrics = { version = "0.0.1", git = "https://github.com/ston-fi/stonfi-metrics", tag = "v0.0.1" }
anyhow = "1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

The crate requires Rust 1.88 or newer and a Tokio runtime. The quick start uses
`anyhow` and Tokio's macros and multithreaded runtime features; applications may
use their existing error and runtime setup instead.

## Quick start

```rust
use std::time::Duration;

use stonfi_scylla_client::client::ScyllaClient;
use stonfi_scylla_client::config::{KeyspaceConfig, RetryConfig, ScyllaClientConfig};

# async fn connect() -> anyhow::Result<()> {
stonfi_metrics::init_metrics!()?;

let config = ScyllaClientConfig {
    endpoints: "127.0.0.1:9042".to_owned(),
    max_parallel_queries: 64,
    keyspace: KeyspaceConfig {
        name: "my_service".to_owned(),
        replication_factor: 3,
    },
    request_timeout: Duration::from_secs(5),
    retry: RetryConfig {
        max_retries: 3,
        min_delay: Duration::from_millis(50),
        max_delay: Duration::from_secs(1),
    },
};

let client = ScyllaClient::new(&config).await?;
client.use_keyspace().await?;

let rows = client
    .select_row(
        "SELECT cluster_name FROM system.local",
        (),
        "read_cluster_name",
    )
    .await?;

println!("received {} row(s)", rows.len());
# Ok(())
# }
```

See [`examples/connect.rs`](examples/connect.rs) for a runnable version.
The `config::ScyllaClientConfig` rustdoc includes a complete YAML example with
human-readable retry durations. A missing `retry` block, or omitted fields
inside it, use `RetryConfig::default()`.

## Behavior

- `select`, `select_one`, `select_row`, `select_page`, `insert`, and
  `delete` use prepared statements and the configured retry policy.
- Prepared operations infer their physical table from server-provided bind
  metadata; row queries also fall back to result metadata. Operations without
  table metadata use `unknown`.
- Every prepared operation requires a stable `caller` metrics label.
- Prepared statements are marked idempotent. Write queries supplied to
  `insert` and `delete` must therefore be safe to replay.
- The configured exponential retry policy is the sole retry mechanism;
  statement-level driver retry policies are ignored.
- `execute_unprepared` requires a stable `caller` and performs one attempt. It
  is intended for schema operations and must never interpolate untrusted input.
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
`caller` labels. Applications must initialize `stonfi_metrics` before executing
queries; client construction does not own the metrics lifecycle.

Prepared statements normally derive `table_name` from bind-variable metadata.
Parameterless row queries fall back to result metadata; preparation failures
and statements without table metadata use `unknown`. The required `caller`
argument supplies the `caller` label. Use stable, bounded caller names rather
than record identifiers or request IDs. Queries must also target a bounded set
of physical table names.

Do not link this crate and `stonfi-commons-scylla-client` into the same process.
Both own the same metric names, so initialization would fail rather than
silently produce duplicate collectors.

## Simple migrations

`simple_migrator::SimpleMigrator` uses the client's validated keyspace
configuration, substitutes `[[KEYSPACE_NAME]]` and `[[REPLICATION_FACTOR]]`,
then executes each CQL statement without preparation. It does not track
migration versions or checksums, so migration statements should be idempotent.

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
- Rename raw `execute` calls to `execute_unprepared` and supply a stable
  `caller`.
- Rename `select_single_page` calls to `select_page`.
- Remove the explicit table argument from prepared operations and replace the
  optional query tag with a required, stable `caller`.
- Rename the `url` configuration field to `endpoints`; its value remains a
  comma-separated string suitable for environment-variable overrides.
- Rename `request_timeout_ms` to `request_timeout` and use a human-readable
  duration such as `5s`.
- Replace the top-level retry fields with the nested `retry` policy. YAML retry
  delays now use human-readable durations such as `50ms` and `1s`.
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
