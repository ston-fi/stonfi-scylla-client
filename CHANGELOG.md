# Changelog

All notable changes to this project will be documented in this file.

## Unreleased

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
