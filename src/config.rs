use std::time::Duration;

use serde::Deserialize;

use crate::errors::{ScyllaClientError, ScyllaClientResult};

/// Runtime configuration for [`crate::client::ScyllaClient`].
///
/// The public fields intentionally form a stable deserialization and direct
/// construction contract.
///
/// # YAML example
///
/// ```yaml
/// endpoints: "scylla-1:9042,scylla-2:9042"
/// max_parallel_queries: 64
/// keyspace:
///   name: my_service
///   replication_factor: 3
/// request_timeout: 5s
/// retry:
///   max_retries: 3
///   min_delay: 50ms
///   max_delay: 1s
/// ```
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScyllaClientConfig {
    /// Comma-separated ScyllaDB contact endpoints.
    pub endpoints: String,
    /// Maximum number of queries that may be active through this client.
    pub max_parallel_queries: usize,
    /// Keyspace selected by [`crate::client::ScyllaClient::use_keyspace`].
    pub keyspace: KeyspaceConfig,
    /// Per-request driver timeout.
    #[serde(with = "humantime_serde")]
    pub request_timeout: Duration,
    /// Retry policy. Missing blocks and fields use [`RetryConfig::default`].
    #[serde(default)]
    pub retry: RetryConfig,
}

/// Keyspace name and replication settings used by the simple migrator.
///
/// The fields are intentionally public for configuration deserialization and
/// struct-literal construction.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KeyspaceConfig {
    /// Unquoted CQL identifier used as the keyspace name.
    pub name: String,
    /// Replication factor substituted into migration templates.
    pub replication_factor: u64,
}

/// Exponential retry policy for prepared client operations.
///
/// Missing fields use the default policy of three retries, a 50ms minimum
/// delay, and a 1s maximum delay. Human-readable values such as `50ms`, `1s`,
/// and `2m` are accepted during deserialization.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct RetryConfig {
    /// Number of retries after the initial attempt.
    pub max_retries: u32,
    /// Minimum exponential retry delay.
    #[serde(with = "humantime_serde")]
    pub min_delay: Duration,
    /// Maximum exponential retry delay.
    #[serde(with = "humantime_serde")]
    pub max_delay: Duration,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            min_delay: Duration::from_millis(50),
            max_delay: Duration::from_secs(1),
        }
    }
}

pub(crate) fn validate_config(config: &ScyllaClientConfig) -> ScyllaClientResult<Vec<&str>> {
    let endpoints = config
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

    if config.max_parallel_queries == 0 {
        return Err(ScyllaClientError::invalid_config(
            "max_parallel_queries",
            "must be greater than zero",
        ));
    }

    validate_keyspace_name(&config.keyspace.name)?;

    if config.keyspace.replication_factor == 0 {
        return Err(ScyllaClientError::invalid_config(
            "keyspace.replication_factor",
            "must be greater than zero",
        ));
    }

    if config.request_timeout.is_zero() {
        return Err(ScyllaClientError::invalid_config(
            "request_timeout",
            "must be greater than zero",
        ));
    }

    if config.retry.max_delay < config.retry.min_delay {
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
    use std::time::Duration;

    use super::{KeyspaceConfig, RetryConfig, ScyllaClientConfig, validate_config};
    use crate::errors::ScyllaClientError;

    fn valid_config() -> ScyllaClientConfig {
        ScyllaClientConfig {
            endpoints: "127.0.0.1:9042".to_owned(),
            max_parallel_queries: 1,
            keyspace: KeyspaceConfig {
                name: "test_keyspace".to_owned(),
                replication_factor: 1,
            },
            request_timeout: Duration::from_millis(500),
            retry: RetryConfig {
                max_retries: 0,
                min_delay: Duration::from_millis(10),
                max_delay: Duration::from_millis(100),
            },
        }
    }

    fn invalid_field(config: &ScyllaClientConfig) -> &'static str {
        match validate_config(config) {
            Err(ScyllaClientError::InvalidConfig { field, .. }) => field,
            Err(_) => "unexpected error",
            Ok(_) => "configuration unexpectedly valid",
        }
    }

    #[test]
    fn test_validate_config_accepts_multiple_endpoints_and_zero_retries() -> anyhow::Result<()> {
        let mut config = valid_config();
        config.endpoints = "127.0.0.1:9042, scylla.internal:9042".to_owned();

        let endpoints = validate_config(&config)?;

        assert_eq!(endpoints, ["127.0.0.1:9042", "scylla.internal:9042"]);
        Ok(())
    }

    #[test]
    fn test_validate_config_rejects_empty_endpoint() {
        let mut config = valid_config();
        config.endpoints = "127.0.0.1:9042, ".to_owned();
        assert_eq!(invalid_field(&config), "endpoints");
    }

    #[test]
    fn test_validate_config_rejects_zero_parallel_queries() {
        let mut config = valid_config();
        config.max_parallel_queries = 0;
        assert_eq!(invalid_field(&config), "max_parallel_queries");
    }

    #[test]
    fn test_validate_config_rejects_invalid_keyspace_name() {
        for invalid_name in ["", "1keyspace", "key-space", "key space", "keyspace;drop"] {
            let mut config = valid_config();
            config.keyspace.name = invalid_name.to_owned();
            assert_eq!(invalid_field(&config), "keyspace.name");
        }
    }

    #[test]
    fn test_validate_config_rejects_zero_replication_factor() {
        let mut config = valid_config();
        config.keyspace.replication_factor = 0;
        assert_eq!(invalid_field(&config), "keyspace.replication_factor");
    }

    #[test]
    fn test_validate_config_rejects_zero_request_timeout() {
        let mut config = valid_config();
        config.request_timeout = Duration::ZERO;
        assert_eq!(invalid_field(&config), "request_timeout");
    }

    #[test]
    fn test_validate_config_accepts_zero_min_retry_delay() -> anyhow::Result<()> {
        let mut config = valid_config();
        config.retry.min_delay = Duration::ZERO;
        validate_config(&config)?;
        Ok(())
    }

    #[test]
    fn test_validate_config_rejects_inverted_retry_delays() {
        let mut config = valid_config();
        config.retry.min_delay = Duration::from_millis(100);
        config.retry.max_delay = Duration::from_millis(99);
        assert_eq!(invalid_field(&config), "retry.max_delay");
    }

    #[test]
    fn test_deserialize_yaml_uses_retry_defaults_when_block_is_missing() -> anyhow::Result<()> {
        let config: ScyllaClientConfig = serde_yaml_ng::from_str(
            r#"
endpoints: "scylla-1:9042,scylla-2:9042"
max_parallel_queries: 64
keyspace:
  name: my_service
  replication_factor: 3
request_timeout: 5s
"#,
        )?;

        assert_eq!(config.request_timeout, Duration::from_secs(5));
        assert_eq!(config.retry, RetryConfig::default());
        assert_eq!(
            validate_config(&config)?,
            ["scylla-1:9042", "scylla-2:9042"]
        );

        let unknown_retry_field = serde_yaml_ng::from_str::<ScyllaClientConfig>(
            r#"
endpoints: "scylla-1:9042"
max_parallel_queries: 64
keyspace:
  name: my_service
  replication_factor: 3
request_timeout: 5s
retry:
  max_retrise: 3
"#,
        );
        assert!(unknown_retry_field.is_err());
        Ok(())
    }

    #[test]
    fn test_deserialize_yaml_parses_human_retry_durations_and_partial_defaults()
    -> anyhow::Result<()> {
        let config_with_min_delay: ScyllaClientConfig = serde_yaml_ng::from_str(
            r#"
endpoints: "127.0.0.1:9042"
max_parallel_queries: 64
keyspace:
  name: my_service
  replication_factor: 3
request_timeout: 5s
retry:
  min_delay: 250ms
"#,
        )?;

        assert_eq!(
            config_with_min_delay.retry,
            RetryConfig {
                min_delay: Duration::from_millis(250),
                ..RetryConfig::default()
            }
        );

        Ok(())
    }
}
