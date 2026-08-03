use std::sync::Arc;
use std::time::Duration;

use derive_setters::Setters;
use scylla::client::caching_session::CachingSessionBuilder;
use scylla::client::execution_profile::ExecutionProfile;
use scylla::client::session_builder::SessionBuilder;
use scylla::frame::Compression;
use scylla::policies::retry::FallthroughRetryPolicy;
use tokio::sync::Semaphore;

use super::{Inner, ScyllaClient};
use crate::address_translator::configure_known_nodes;
use crate::errors::{ScyllaClientError, ScyllaClientResult};

/// Builder for [`ScyllaClient`].
#[derive(Debug, Setters)]
#[setters(prefix = "with_")]
#[must_use]
pub struct Builder {
    #[setters(skip)]
    endpoints: String,
    #[setters(skip)]
    keyspace: String,
    max_parallel_queries: usize,
    replication_factor: u64,
    request_timeout: Duration,
    retry_count: u32,
    retry_min_delay: Duration,
    retry_max_delay: Duration,
}

impl Builder {
    pub(super) fn new(endpoints: String, keyspace: String) -> Self {
        Self {
            endpoints,
            keyspace,
            max_parallel_queries: 64,
            replication_factor: 1,
            request_timeout: Duration::from_secs(5),
            retry_count: 3,
            retry_min_delay: Duration::from_millis(50),
            retry_max_delay: Duration::from_secs(1),
        }
    }

    /// Validate the configured settings and connect to ScyllaDB.
    ///
    /// # Errors
    ///
    /// Returns [`ScyllaClientError::InvalidConfig`] for invalid settings,
    /// [`ScyllaClientError::ResolveEndpoint`] when the sole configured endpoint
    /// cannot be resolved, or [`ScyllaClientError::Connect`] when the driver
    /// session cannot be established.
    pub async fn build(self) -> ScyllaClientResult<ScyllaClient> {
        let endpoints = self.validate()?;
        let profile = ExecutionProfile::builder()
            .request_timeout(Some(self.request_timeout))
            .consistency(scylla::statement::Consistency::LocalQuorum)
            .retry_policy(Arc::new(FallthroughRetryPolicy::new()))
            .build();
        let session = configure_known_nodes(SessionBuilder::new(), &endpoints)
            .await?
            .compression(Some(Compression::Lz4))
            .default_execution_profile_handle(profile.into_handle())
            .build()
            .await
            .map_err(ScyllaClientError::connect)?;
        drop(endpoints);

        Ok(ScyllaClient {
            inner: Arc::new(Inner {
                session: CachingSessionBuilder::new(session).build(),
                keyspace_name: self.keyspace,
                replication_factor: self.replication_factor,
                active_queries: Semaphore::new(self.max_parallel_queries),
            }),
            retry_count: self.retry_count,
            retry_min_delay: self.retry_min_delay,
            retry_max_delay: self.retry_max_delay,
        })
    }

    fn validate(&self) -> ScyllaClientResult<Vec<&str>> {
        let endpoints = self.endpoints.split(',').map(str::trim).collect::<Vec<_>>();
        if endpoints.is_empty() || endpoints.iter().any(|endpoint| endpoint.is_empty()) {
            return Err(ScyllaClientError::invalid_config(
                "endpoints",
                "must contain one or more nonempty comma-separated endpoints",
            ));
        }

        if self.max_parallel_queries == 0 {
            return Err(ScyllaClientError::invalid_config(
                "max_parallel_queries",
                "must be greater than zero",
            ));
        }

        validate_keyspace_name(&self.keyspace)?;

        if self.replication_factor == 0 {
            return Err(ScyllaClientError::invalid_config(
                "keyspace.replication_factor",
                "must be greater than zero",
            ));
        }

        if self.request_timeout.is_zero() {
            return Err(ScyllaClientError::invalid_config(
                "request_timeout",
                "must be greater than zero",
            ));
        }

        if self.retry_max_delay < self.retry_min_delay {
            return Err(ScyllaClientError::invalid_config(
                "retry.max_delay",
                "must be greater than or equal to retry.min_delay",
            ));
        }

        Ok(endpoints)
    }
}

fn validate_keyspace_name(name: &str) -> ScyllaClientResult<()> {
    let mut chars = name.chars();
    let valid_first = chars
        .next()
        .is_some_and(|character| character == '_' || character.is_ascii_alphabetic());
    let valid_rest = chars.all(|character| character == '_' || character.is_ascii_alphanumeric());

    if !valid_first || !valid_rest {
        return Err(ScyllaClientError::invalid_config(
            "keyspace.name",
            "must be an unquoted CQL identifier matching [A-Za-z_][A-Za-z0-9_]*",
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::Builder;
    use crate::errors::ScyllaClientError;

    #[test]
    fn test_validate_splits_multiple_endpoints() -> anyhow::Result<()> {
        let builder = Builder::new(
            "127.0.0.1:9042, scylla.internal:9042".to_owned(),
            "test_keyspace".to_owned(),
        );

        let endpoints = builder.validate()?;

        assert_eq!(endpoints, ["127.0.0.1:9042", "scylla.internal:9042"]);
        Ok(())
    }

    #[test]
    fn test_validate_rejects_unsafe_keyspace_names() {
        for keyspace in ["", "1keyspace", "key-space", "key space", "keyspace;drop"] {
            let builder = Builder::new("127.0.0.1:9042".to_owned(), keyspace.to_owned());
            let result = builder.validate();
            assert!(matches!(
                result,
                Err(ScyllaClientError::InvalidConfig {
                    field: "keyspace.name",
                    ..
                })
            ));
        }
    }
}
