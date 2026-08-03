use std::sync::Arc;
use std::time::Duration;

use derive_setters::Setters;

use super::{ClientSettings, Inner, KeyspaceSettings, RetrySettings, ScyllaClient};
use crate::errors::{ScyllaClientError, ScyllaClientResult};

const DEFAULT_MAX_PARALLEL_QUERIES: usize = 64;
const DEFAULT_REPLICATION_FACTOR: u64 = 1;
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_RETRY_COUNT: u32 = 3;
const DEFAULT_RETRY_MIN_DELAY: Duration = Duration::from_millis(50);
const DEFAULT_RETRY_MAX_DELAY: Duration = Duration::from_secs(1);

/// Builder for [`ScyllaClient`].
#[derive(Debug, Setters)]
#[setters(prefix = "with_")]
#[non_exhaustive]
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
            max_parallel_queries: DEFAULT_MAX_PARALLEL_QUERIES,
            replication_factor: DEFAULT_REPLICATION_FACTOR,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            retry_count: DEFAULT_RETRY_COUNT,
            retry_min_delay: DEFAULT_RETRY_MIN_DELAY,
            retry_max_delay: DEFAULT_RETRY_MAX_DELAY,
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
        let settings = ClientSettings {
            endpoints: self.endpoints,
            max_parallel_queries: self.max_parallel_queries,
            keyspace: KeyspaceSettings {
                name: self.keyspace,
                replication_factor: self.replication_factor,
            },
            request_timeout: self.request_timeout,
            retry: RetrySettings {
                max_retries: self.retry_count,
                min_delay: self.retry_min_delay,
                max_delay: self.retry_max_delay,
            },
        };
        let endpoints = validate_settings(&settings)?;
        let inner = Inner::new(&settings, &endpoints).await?;
        Ok(ScyllaClient {
            inner: Arc::new(inner),
            retry_settings: settings.retry,
        })
    }
}

fn validate_settings(settings: &ClientSettings) -> ScyllaClientResult<Vec<&str>> {
    let endpoints = settings
        .endpoints
        .split(',')
        .map(str::trim)
        .collect::<Vec<_>>();
    if endpoints.is_empty() || endpoints.iter().any(|endpoint| endpoint.is_empty()) {
        return Err(ScyllaClientError::invalid_config(
            "endpoints",
            "must contain one or more nonempty comma-separated endpoints",
        ));
    }

    if settings.max_parallel_queries == 0 {
        return Err(ScyllaClientError::invalid_config(
            "max_parallel_queries",
            "must be greater than zero",
        ));
    }

    validate_keyspace_name(&settings.keyspace.name)?;

    if settings.keyspace.replication_factor == 0 {
        return Err(ScyllaClientError::invalid_config(
            "keyspace.replication_factor",
            "must be greater than zero",
        ));
    }

    if settings.request_timeout.is_zero() {
        return Err(ScyllaClientError::invalid_config(
            "request_timeout",
            "must be greater than zero",
        ));
    }

    if settings.retry.max_delay < settings.retry.min_delay {
        return Err(ScyllaClientError::invalid_config(
            "retry.max_delay",
            "must be greater than or equal to retry.min_delay",
        ));
    }

    Ok(endpoints)
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
    use super::validate_settings;
    use crate::client::{ClientSettings, KeyspaceSettings, RetrySettings};
    use crate::errors::ScyllaClientError;
    use std::time::Duration;

    fn valid_settings() -> ClientSettings {
        ClientSettings {
            endpoints: "127.0.0.1:9042".to_owned(),
            max_parallel_queries: 1,
            keyspace: KeyspaceSettings {
                name: "test_keyspace".to_owned(),
                replication_factor: 1,
            },
            request_timeout: Duration::from_millis(500),
            retry: RetrySettings {
                max_retries: 0,
                min_delay: Duration::from_millis(10),
                max_delay: Duration::from_millis(100),
            },
        }
    }

    fn invalid_field(settings: &ClientSettings) -> &'static str {
        match validate_settings(settings) {
            Err(ScyllaClientError::InvalidConfig { field, .. }) => field,
            Err(_) => "unexpected error",
            Ok(_) => "configuration unexpectedly valid",
        }
    }

    #[test]
    fn test_validate_settings_accepts_multiple_endpoints_and_zero_retries() -> anyhow::Result<()> {
        let mut settings = valid_settings();
        settings.endpoints = "127.0.0.1:9042, scylla.internal:9042".to_owned();

        let endpoints = validate_settings(&settings)?;

        assert_eq!(endpoints, ["127.0.0.1:9042", "scylla.internal:9042"]);
        Ok(())
    }

    #[test]
    fn test_validate_settings_rejects_empty_endpoint() {
        let mut settings = valid_settings();
        settings.endpoints = "127.0.0.1:9042, ".to_owned();
        assert_eq!(invalid_field(&settings), "endpoints");
    }

    #[test]
    fn test_validate_settings_rejects_zero_parallel_queries() {
        let mut settings = valid_settings();
        settings.max_parallel_queries = 0;
        assert_eq!(invalid_field(&settings), "max_parallel_queries");
    }

    #[test]
    fn test_validate_settings_rejects_invalid_keyspace_name() {
        for invalid_name in ["", "1keyspace", "key-space", "key space", "keyspace;drop"] {
            let mut settings = valid_settings();
            settings.keyspace.name = invalid_name.to_owned();
            assert_eq!(invalid_field(&settings), "keyspace.name");
        }
    }

    #[test]
    fn test_validate_settings_rejects_zero_replication_factor() {
        let mut settings = valid_settings();
        settings.keyspace.replication_factor = 0;
        assert_eq!(invalid_field(&settings), "keyspace.replication_factor");
    }

    #[test]
    fn test_validate_settings_rejects_zero_request_timeout() {
        let mut settings = valid_settings();
        settings.request_timeout = Duration::ZERO;
        assert_eq!(invalid_field(&settings), "request_timeout");
    }

    #[test]
    fn test_validate_settings_accepts_zero_min_retry_delay() -> anyhow::Result<()> {
        let mut settings = valid_settings();
        settings.retry.min_delay = Duration::ZERO;
        validate_settings(&settings)?;
        Ok(())
    }

    #[test]
    fn test_validate_settings_rejects_inverted_retry_delays() {
        let mut settings = valid_settings();
        settings.retry.min_delay = Duration::from_millis(100);
        settings.retry.max_delay = Duration::from_millis(99);
        assert_eq!(invalid_field(&settings), "retry.max_delay");
    }
}
