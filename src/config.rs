use serde::Deserialize;

use crate::errors::{ScyllaClientError, ScyllaClientResult};

/// Runtime configuration for [`crate::client::ScyllaClient`].
///
/// The public fields intentionally form a stable deserialization and direct
/// construction contract. Millisecond fields use wall-clock durations.
#[derive(Clone, Debug, Deserialize)]
pub struct ScyllaClientConfig {
    /// One endpoint or a comma-separated list of cluster endpoints.
    pub url: String,
    /// Maximum number of queries that may be active through this client.
    pub max_parallel_queries: usize,
    /// Keyspace selected by [`crate::client::ScyllaClient::use_keyspace`].
    pub keyspace: KeyspaceConfig,
    /// Per-request driver timeout in milliseconds.
    pub request_timeout_ms: u64,
    /// Number of retries after the initial attempt.
    pub retry_count: u32,
    /// Initial exponential retry delay in milliseconds.
    pub initial_retry_delay_ms: u64,
    /// Maximum exponential retry delay in milliseconds.
    pub max_retry_delay_ms: u64,
}

/// Keyspace name and replication settings used by the simple migrator.
///
/// The fields are intentionally public for configuration deserialization and
/// struct-literal construction.
#[derive(Clone, Debug, Deserialize)]
pub struct KeyspaceConfig {
    /// Unquoted CQL identifier used as the keyspace name.
    pub name: String,
    /// Replication factor substituted into migration templates.
    pub replication_factor: u64,
}

pub(crate) fn validate_config(config: &ScyllaClientConfig) -> ScyllaClientResult<Vec<&str>> {
    let endpoints = config.url.split(',').map(str::trim).collect::<Vec<_>>();
    if endpoints.is_empty() || endpoints.iter().any(|endpoint| endpoint.is_empty()) {
        return Err(ScyllaClientError::invalid_config(
            "url",
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

    if config.request_timeout_ms == 0 {
        return Err(ScyllaClientError::invalid_config(
            "request_timeout_ms",
            "must be greater than zero",
        ));
    }

    if config.initial_retry_delay_ms == 0 {
        return Err(ScyllaClientError::invalid_config(
            "initial_retry_delay_ms",
            "must be greater than zero",
        ));
    }

    if config.max_retry_delay_ms < config.initial_retry_delay_ms {
        return Err(ScyllaClientError::invalid_config(
            "max_retry_delay_ms",
            "must be greater than or equal to initial_retry_delay_ms",
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
    use super::{KeyspaceConfig, ScyllaClientConfig, validate_config};
    use crate::errors::ScyllaClientError;

    fn valid_config() -> ScyllaClientConfig {
        ScyllaClientConfig {
            url: "127.0.0.1:9042".to_owned(),
            max_parallel_queries: 1,
            keyspace: KeyspaceConfig {
                name: "test_keyspace".to_owned(),
                replication_factor: 1,
            },
            request_timeout_ms: 500,
            retry_count: 0,
            initial_retry_delay_ms: 10,
            max_retry_delay_ms: 100,
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
        config.url = "127.0.0.1:9042, scylla.internal:9042".to_owned();

        let endpoints = validate_config(&config)?;

        assert_eq!(endpoints, ["127.0.0.1:9042", "scylla.internal:9042"]);
        Ok(())
    }

    #[test]
    fn test_validate_config_rejects_empty_endpoint() {
        let mut config = valid_config();
        config.url = "127.0.0.1:9042, ".to_owned();
        assert_eq!(invalid_field(&config), "url");
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
        config.request_timeout_ms = 0;
        assert_eq!(invalid_field(&config), "request_timeout_ms");
    }

    #[test]
    fn test_validate_config_rejects_zero_initial_retry_delay() {
        let mut config = valid_config();
        config.initial_retry_delay_ms = 0;
        assert_eq!(invalid_field(&config), "initial_retry_delay_ms");
    }

    #[test]
    fn test_validate_config_rejects_inverted_retry_delays() {
        let mut config = valid_config();
        config.initial_retry_delay_ms = 100;
        config.max_retry_delay_ms = 99;
        assert_eq!(invalid_field(&config), "max_retry_delay_ms");
    }
}
