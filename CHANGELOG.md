# Changelog

All notable changes to this project will be documented in this file.

## 0.0.1

- Extract the ScyllaDB client from the STON.fi commons workspace.
- Add typed errors, validated configuration, global Prometheus metrics, and
  hardened simple CQL migration splitting.
- Group retry settings under `RetryConfig`, with human-readable `Duration`
  values and defaults for missing YAML fields.
- Use a human-readable `Duration` for the request timeout, reject unknown
  configuration fields, and make the client retry policy the sole retry owner.
- Rename `url` to the comma-separated `endpoints` field and rename
  `select_single_page` to `select_page`.
- Use the upstream prepared-statement cache, derive migrator configuration from
  the validated client, and report concurrency wait time for every query type.
- Add public documentation, Docker-backed integration tests, and GitHub release
  automation.
