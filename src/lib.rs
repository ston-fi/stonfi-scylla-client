//! A small, instrumented ScyllaDB client for STON.fi Rust services.
//!
//! The crate wraps the upstream [`scylla`] driver with bounded concurrency,
//! retry policy, prepared-statement caching, Prometheus metrics, and a
//! deliberately small CQL migration helper.
//!
//! Construct [`client::ScyllaClient`] through its builder.
//!
//! # Example
//!
//! ```no_run
//! use std::time::Duration;
//!
//! use stonfi_scylla_client::client::ScyllaClient;
//!
//! # async fn connect() -> anyhow::Result<()> {
//! stonfi_metrics::init_metrics!()?;
//!
//! let client = ScyllaClient::builder("127.0.0.1:9042", "my_service")
//!     .with_max_parallel_queries(64)
//!     .with_replication_factor(3)
//!     .with_request_timeout(Duration::from_secs(5))
//!     .with_retry_count(3)
//!     .with_retry_min_delay(Duration::from_millis(50))
//!     .with_retry_max_delay(Duration::from_secs(1))
//!     .build()
//!     .await?;
//! client.use_keyspace().await?;
//! # Ok(())
//! # }
//! ```

mod address_translator;
/// Instrumented ScyllaDB client and row deserialization bound.
pub mod client;
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
