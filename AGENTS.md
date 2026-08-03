# STON.fi Scylla Client Agent Guide

## Purpose

This repository contains the public `stonfi_scylla_client` Rust library. It is
published to crates.io and released through matching Git tags. Use the
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

Public APIs remain module-qualified under `client`, `errors`, and
`simple_migrator`. Construct clients only through `ScyllaClient::builder`; do
not add parallel constructors, configuration types, root re-exports, aliases,
or compatibility wrappers without a demonstrated downstream need. The root
`scylla` re-export is intentional: consumers need the exact driver types and
derives that match the client.

Internal address translation belongs in `address_translator.rs`. Keep it
private and configure it through `ScyllaClient`; do not expose driver sessions
or parallel connection constructors.

## Construction and runtime

Applications should:

1. call `stonfi_metrics::init_metrics!()` during startup;
2. deserialize application-owned settings;
3. pass those settings through `ScyllaClient::builder(endpoints, keyspace)` and
   call `build().await`; and
4. call `use_keyspace()` only after the keyspace exists.

The builder validates settings before connecting. Clones share the session,
prepared statements, and semaphore. The crate requires Tokio but does not spawn
or own background tasks.

The builder requires comma-separated endpoints and an unquoted CQL keyspace
name. It defaults to 64 concurrent queries, replication factor 1, a 5-second
request timeout, and three retries with exponential backoff between 50ms and
1s. Applications own configuration deserialization; do not add a parallel
library configuration type or serde contract. Add validation in `build()`
rather than silently normalizing invalid values.
When exactly one endpoint is configured, the client resolves it and translates
all server-advertised peer addresses to that endpoint, preferring IPv4 when
available and defaulting to port `9042` when omitted. Multiple endpoints use the
driver's advertised topology unchanged.

## Queries, errors, and metrics

- Prefer prepared methods for data queries. Use `execute_unprepared` only for
  statements such as `USE` and schema migrations.
- Prepared methods infer their physical table from server-provided bind
  metadata; row queries also fall back to result metadata. Use `unknown` when
  preparation fails or no table metadata exists; do not parse CQL to recover
  it. Queries must target a bounded set of physical table names because the
  inferred name is a metric label.
- Every prepared method and public `execute_unprepared` call requires a stable,
  bounded `caller` metric label. Internal callers are fixed names.
- Preserve per-statement configuration through the upstream driver's
  `CachingSession`, except for statement-level retry policies, which the client
  overrides so the builder-configured retry policy remains the sole retry
  owner. Do not add a second
  prepared-statement cache.
- Never interpolate untrusted values into unprepared CQL.
- Keep prepared statements marked idempotent only while all supported query
  methods are safe to retry.
- Public operations return `ScyllaClientResult<T>`. Preserve source chains and
  add a new non-exhaustive error category only when callers need to distinguish
  it.
- Keep metric names and labels stable. Dashboards depend on
  `db_scylla_queries`, `db_scylla_queries_duration_ms`,
  `db_scylla_wait_connection_ms`, and the `table_name`, `query_type`, `status`,
  and `caller` query labels.
- Do not test Prometheus registration, label names, or increments. Test the
  project behavior that drives metrics instead.

## Simple migrations

`SimpleMigrator` is stateless, derives the validated keyspace configuration
from its client, and reruns all supplied statements. Keep its keyspace
placeholders stable. The private splitter supports only the syntax documented
in README and rustdoc; changes require deterministic tests for line endings,
comments, delimiters, and quoting.

## Public-library changes

Every public API, dependency, MSRV, capability, metric contract, package
surface, or release-policy change must review and update the affected rustdoc,
README, this guide, example, tests, changelog, and Cargo include rules in the
same change. Keep package metadata consistent with the repository and retain
`AGENTS.md`, `README.md`, `CHANGELOG.md`, `LICENSE`, examples, and source in the
package.

Release-plz owns crates.io publication, SemVer analysis, Git tags and GitHub
Releases, plus future version and changelog pull requests. Store the crates.io
API token only in the protected `CRATES_IO_REGISTRY_TOKEN` GitHub Actions
secret, and map it to Cargo's standard `CARGO_REGISTRY_TOKEN` environment
variable only inside the release job. The initial publication requires a token
with `publish-new`; never commit or print registry credentials. Do not publish
manually or create release tags outside the validated release workflow.

## Common mistakes

- Do not use `unwrap`, `expect`, or panic-driven control flow in production
  code.
- Do not bypass builder validation or add another connection constructor.
- Do not add an `execute` alias beside `execute_unprepared`.
- Do not make retry behavior implicit for new operations; document whether and
  why they are retried.
- Do not reintroduce caller-supplied table labels or optional prepared-query
  callers.
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
cargo publish --dry-run --locked
git diff --check
```

Run tests before formatting and Clippy. The Docker test uses
`scylladb/scylla:6.0` and may take time on the first image pull. Treat Docker,
container readiness, and connection failures as blockers rather than skipping
the suite.
