# STON.fi Scylla Client Agent Guide

## Purpose

This repository contains the public `stonfi_scylla_client` Rust library. It is
distributed through Git tags for use by STON.fi services. Use the
`rust-library-review` skill for every non-trivial review, implementation,
refactor, dependency update, API change, or release task.

## Capabilities and boundaries

The crate owns:

- construction of an instrumented upstream Scylla driver session;
- bounded query concurrency and configurable retries;
- prepared-statement caching;
- typed selection, paging, insertion, deletion, and unprepared execution;
- process-global Prometheus query metrics; and
- a simple, stateless CQL migration helper.

It does not own application schema design, durable migration history,
application metrics serving, credential management, load-balancing policy
selection, or a general CQL parser.

Public APIs remain module-qualified under `client`, `config`, `errors`, and
`simple_migrator`. The root `scylla` re-export is intentional: consumers need
the exact driver types and derives that match the client. Do not add parallel
root re-exports, aliases, builders, or compatibility wrappers without a
demonstrated downstream need.

## Construction and runtime

Applications should:

1. call `stonfi_metrics::init_metrics!()` during startup;
2. deserialize or construct `ScyllaClientConfig`;
3. call `ScyllaClient::new(&config).await`; and
4. call `use_keyspace()` only after the keyspace exists.

`ScyllaClient::new` validates configuration and initializes this crate's global
metrics before connecting. Clones share the session, prepared statements,
semaphore, and metrics. The crate requires Tokio but does not spawn or own
background tasks.

The public configuration fields are an intentional serialization and
struct-literal contract. Preserve their names and types unless a versioned
breaking change is explicitly requested. `endpoints` is a comma-separated
string so generic environment configuration loaders can override it directly.
The request timeout and retry delays are `Duration` values deserialized through
`humantime_serde`; missing retry blocks and fields use the documented
`RetryConfig` defaults. Configuration structs reject unknown fields. Add
validation at construction rather than silently normalizing invalid values.

## Queries, errors, and metrics

- Prefer prepared methods for data queries. Use `execute_unprepared` only for
  statements such as `USE` and schema migrations.
- Preserve per-statement configuration through the upstream driver's
  `CachingSession`, except for statement-level retry policies, which the client
  overrides so `RetryConfig` remains the sole retry owner. Do not add a second
  prepared-statement cache.
- Never interpolate untrusted values into unprepared CQL.
- Keep prepared statements marked idempotent only while all supported query
  methods are safe to retry.
- Public operations return `ScyllaClientResult<T>`. Preserve source chains and
  add a new non-exhaustive error category only when callers need to distinguish
  it.
- Keep metric names and labels stable. Dashboards depend on
  `db_scylla_queries`, `db_scylla_queries_duration_ms`, and
  `db_scylla_wait_connection_ms`.
- Do not test Prometheus registration, label names, or increments. Test the
  project behavior that drives metrics instead.
- The old commons client owns the same metric names. Consumers must migrate
  atomically rather than link both crates in one process.

## Simple migrations

`SimpleMigrator` is intentionally stateless, derives the validated keyspace
configuration from its client, and reruns all supplied statements. Keep its
keyspace placeholders stable. Changes to splitting must include deterministic
tests for line endings, comments, delimiters, and quoting. Keep the incomplete
splitter private; do not present it as a public parser or expand it into a
versioned migration system without an explicit design task.

## Public-library changes

Every public API, dependency, MSRV, capability, metric contract, package
surface, or release-policy change must review and update the affected rustdoc,
README, this guide, example, tests, changelog, and Cargo include rules in the
same change. Keep package metadata consistent with the repository and retain
`AGENTS.md`, `README.md`, `CHANGELOG.md`, `LICENSE`, examples, and source in the
package.

The crate is Git-distributed with `publish = false`. Release-plz may create
`v<version>` tags and GitHub Releases only. Do not enable crates.io publishing,
add registry credentials, or manually create releases without explicit
authorization.

## Common mistakes

- Do not use `unwrap`, `expect`, or panic-driven control flow in production
  code.
- Do not bypass `ScyllaClientConfig` validation.
- Do not add an `execute` alias beside `execute_unprepared`.
- Do not make retry behavior implicit for new operations; document whether and
  why they are retried.
- Do not expose internal metric handles, retry state, sessions, semaphores, or
  prepared-statement caches.
- Do not silently skip Docker integration tests when Scylla is unavailable.

## Validation

Fast gate:

```text
cargo test --lib --locked
cargo test --doc --locked
cargo test --examples --locked
cargo +nightly fmt --check
cargo clippy --all-targets --all-features --locked -- -D warnings
```

Full gate:

```text
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

Run tests before formatting and Clippy. The Docker test uses
`scylladb/scylla:6.0` and may take time on the first image pull. Treat Docker,
container readiness, and connection failures as blockers rather than skipping
the suite.
