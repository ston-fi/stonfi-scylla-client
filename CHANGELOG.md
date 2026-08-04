# Changelog

All notable changes to this project will be documented in this file.

## Unreleased

### Changed

- Run CI and release automation without a tracked `Cargo.lock`, using the
  dependency versions currently allowed by the manifest.

## [0.2.0](https://github.com/ston-fi/stonfi-scylla-client/compare/v0.1.0...v0.2.0) - 2026-08-03

### Other

- replace configuration with builder

### Changed

- Replace the deserializable public configuration structs and
  `ScyllaClient::new` with an asynchronous `ScyllaClient::builder` construction
  API. Applications now own configuration deserialization.

## [0.1.0](https://github.com/ston-fi/stonfi-scylla-client/compare/v0.0.2...v0.1.0) - 2026-07-31

### Changed

- Use the crates.io release of `stonfi_metrics` 0.1.0.
- Publish releases to crates.io through the validated GitHub Actions workflow.

## [0.0.2](https://github.com/ston-fi/stonfi-scylla-client/compare/v0.0.1...v0.0.2) - 2026-07-31

### Other

- fix git-only release flow
- single-endpoint translation

- Support single-endpoint deployments whose server-advertised RPC addresses are
  not directly reachable.
- Fix Git-only release automation for the unpublished metrics dependency while
  preserving release-plz SemVer analysis and crates.io opt-out.

## 0.0.1

- Add validated configuration, typed errors, bounded query concurrency, and
  configurable retries.
- Add prepared-statement caching and table-aware Prometheus query metrics.
- Add typed prepared queries, unprepared execution, paging, and stateless CQL
  migrations.
- Add public documentation, Docker integration tests, and GitHub release
  automation.
