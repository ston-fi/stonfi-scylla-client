//! A small, instrumented ScyllaDB client for STON.fi Rust services.
//!
//! The crate wraps the upstream [`scylla`] driver with bounded concurrency,
//! retry policy, prepared-statement caching, Prometheus metrics, and a
//! deliberately small CQL migration helper.
//!
//! Applications should initialize [`stonfi_metrics`] during startup, then
//! construct [`client::ScyllaClient`] from a [`config::ScyllaClientConfig`].
//! Client construction also initializes this crate's metrics, so construction
//! remains safe when application-level metrics initialization happens later.
//!
//! # Example
//!
//! ```no_run
//! use stonfi_scylla_client::client::ScyllaClient;
//! use stonfi_scylla_client::config::{KeyspaceConfig, ScyllaClientConfig};
//!
//! # async fn connect() -> anyhow::Result<()> {
//! stonfi_metrics::init_metrics!()?;
//!
//! let config = ScyllaClientConfig {
//!     url: "127.0.0.1:9042".to_owned(),
//!     max_parallel_queries: 64,
//!     keyspace: KeyspaceConfig {
//!         name: "my_service".to_owned(),
//!         replication_factor: 3,
//!     },
//!     request_timeout_ms: 5_000,
//!     retry_count: 3,
//!     initial_retry_delay_ms: 50,
//!     max_retry_delay_ms: 1_000,
//! };
//!
//! let client = ScyllaClient::new(&config).await?;
//! client.use_keyspace().await?;
//! # Ok(())
//! # }
//! ```

/// Instrumented ScyllaDB client and row deserialization bound.
pub mod client;
/// Deserializable client and keyspace configuration.
pub mod config;
/// Errors returned by client and migration operations.
pub mod errors;
/// Minimal CQL migration helper.
pub mod simple_migrator;

mod metrics;
mod types;

/// Re-export of the exact upstream driver version used by this crate.
///
/// Consumers can use this path for row derives, statements, paging state, and
/// CQL value types without risking a driver-version mismatch.
pub use scylla;
