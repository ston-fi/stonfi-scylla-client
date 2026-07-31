//! A small, instrumented ScyllaDB client for STON.fi Rust services.
//!
//! The crate wraps the upstream [`scylla`] driver with bounded concurrency,
//! retry policy, prepared-statement caching, Prometheus metrics, and a
//! deliberately small CQL migration helper.
//!
//! Construct [`client::ScyllaClient`] from a
//! [`config::ScyllaClientConfig`].
//!
//! # Example
//!
//! ```no_run
//! use std::time::Duration;
//!
//! use stonfi_scylla_client::client::ScyllaClient;
//! use stonfi_scylla_client::config::{KeyspaceConfig, RetryConfig, ScyllaClientConfig};
//!
//! # async fn connect() -> anyhow::Result<()> {
//! stonfi_metrics::init_metrics!()?;
//!
//! let config = ScyllaClientConfig {
//!     endpoints: "127.0.0.1:9042".to_owned(),
//!     max_parallel_queries: 64,
//!     keyspace: KeyspaceConfig {
//!         name: "my_service".to_owned(),
//!         replication_factor: 3,
//!     },
//!     request_timeout: Duration::from_secs(5),
//!     retry: RetryConfig {
//!         max_retries: 3,
//!         min_delay: Duration::from_millis(50),
//!         max_delay: Duration::from_secs(1),
//!     },
//! };
//!
//! let client = ScyllaClient::new(&config).await?;
//! client.use_keyspace().await?;
//! # Ok(())
//! # }
//! ```

mod address_translator;
/// Instrumented ScyllaDB client and row deserialization bound.
pub mod client;
/// Deserializable client, keyspace, and retry configuration.
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
