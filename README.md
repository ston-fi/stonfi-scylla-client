# `stonfi_scylla_client`

[![CI](https://github.com/ston-fi/stonfi-scylla-client/actions/workflows/build.yml/badge.svg)](https://github.com/ston-fi/stonfi-scylla-client/actions/workflows/build.yml)

`stonfi_scylla_client` is an async ScyllaDB client for STON.fi Rust services.
It adds bounded query concurrency, retries, prepared-statement caching,
Prometheus metrics, and simple CQL migration templates on top of the upstream
`scylla` driver.

The crate is published to crates.io and also released through matching Git tags.

## Installation

```toml
[dependencies]
stonfi_scylla_client = "0.1.0"
stonfi_metrics = "0.1.0"
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

# async fn connect() -> anyhow::Result<()> {
stonfi_metrics::init_metrics!()?;

let client = ScyllaClient::builder("127.0.0.1:9042", "my_service")
    .with_max_parallel_queries(64)
    .with_replication_factor(3)
    .with_request_timeout(Duration::from_secs(5))
    .with_retry_count(3)
    .with_retry_min_delay(Duration::from_millis(50))
    .with_retry_max_delay(Duration::from_secs(1))
    .build()
    .await?;
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
The builder requires comma-separated contact endpoints and an unquoted CQL
keyspace name. It defaults to 64 concurrent queries, replication factor 1, a
5-second request timeout, and three retries with exponential backoff between
50ms and 1s. Applications own configuration deserialization and can pass their
typed settings through the `with_*` methods.

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
- A single configured endpoint becomes the address for every
  server-advertised peer, preferring IPv4 when the endpoint resolves to both
  address families and defaulting to port `9042` when omitted. Multiple
  configured endpoints use the driver's advertised topology unchanged.
- Builder settings are validated before endpoint resolution or connection.
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

## Simple migrations

`simple_migrator::SimpleMigrator` uses the client's validated keyspace
configuration, substitutes `[[KEYSPACE_NAME]]` and `[[REPLICATION_FACTOR]]`,
then executes each CQL statement without preparation. It does not track
migration versions or checksums, so migration statements should be idempotent.

The splitter supports LF and CRLF, final statements without semicolons,
full-line `--` and `//` comments, and semicolons inside single- or double-quoted
values. It is not a complete CQL parser and does not interpret block comments or
dollar-quoted values.

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
cargo publish --dry-run --locked
git diff --check
```

Merges to `main` run release-plz after all validation jobs succeed. Release-plz
publishes an unpublished manifest version to crates.io, creates the matching
`v<version>` Git tag and GitHub Release, checks SemVer compatibility, and opens
or updates the next version and changelog pull request. The workflow reads the
crates.io API token from the `CRATES_IO_REGISTRY_TOKEN` GitHub Actions secret.
