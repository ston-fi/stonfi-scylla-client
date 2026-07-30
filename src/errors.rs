use std::error::Error;

use thiserror::Error;

/// Result type returned by Scylla client and migration operations.
pub type ScyllaClientResult<T> = Result<T, ScyllaClientError>;

/// Errors produced while configuring or using the Scylla client.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ScyllaClientError {
    /// A configuration field violates a client invariant.
    #[error("invalid configuration field `{field}`: {reason}")]
    InvalidConfig {
        /// Name of the invalid field.
        field: &'static str,
        /// Human-readable invariant that was violated.
        reason: String,
    },
    /// Prometheus metric registration failed.
    #[error("failed to initialize Scylla client metrics")]
    Metrics {
        /// Underlying registration failure.
        #[source]
        source: Box<dyn Error + Send + Sync + 'static>,
    },
    /// Establishing the driver session failed.
    #[error("failed to connect to ScyllaDB")]
    Connect {
        /// Underlying driver failure.
        #[source]
        source: Box<dyn Error + Send + Sync + 'static>,
    },
    /// Preparing a CQL statement failed.
    #[error("failed to prepare {operation} query for table `{table}` (tag `{query_tag}`)")]
    Prepare {
        /// High-level query operation.
        operation: &'static str,
        /// Logical table label supplied for metrics and diagnostics.
        table: String,
        /// Query tag supplied for metrics and diagnostics.
        query_tag: String,
        /// Underlying driver failure.
        #[source]
        source: Box<dyn Error + Send + Sync + 'static>,
    },
    /// Executing or decoding a CQL query failed.
    #[error("{operation} query failed for table `{table}` (tag `{query_tag}`)")]
    Query {
        /// High-level query operation.
        operation: &'static str,
        /// Logical table label supplied for metrics and diagnostics.
        table: String,
        /// Query tag supplied for metrics and diagnostics.
        query_tag: String,
        /// Underlying driver or decoding failure.
        #[source]
        source: Box<dyn Error + Send + Sync + 'static>,
    },
    /// Applying one statement from a migration input failed.
    #[error("migration {migration_index}, statement {statement_index} failed")]
    Migration {
        /// Zero-based migration input index.
        migration_index: usize,
        /// Zero-based statement index within that migration input.
        statement_index: usize,
        /// Client failure returned while applying the statement.
        #[source]
        source: Box<ScyllaClientError>,
    },
}

impl ScyllaClientError {
    pub(crate) fn invalid_config(field: &'static str, reason: impl Into<String>) -> Self {
        Self::InvalidConfig {
            field,
            reason: reason.into(),
        }
    }

    pub(crate) fn metrics(source: anyhow::Error) -> Self {
        Self::Metrics {
            source: source.into_boxed_dyn_error(),
        }
    }

    pub(crate) fn connect(source: impl Error + Send + Sync + 'static) -> Self {
        Self::Connect {
            source: Box::new(source),
        }
    }

    pub(crate) fn prepare(
        operation: &'static str,
        table: &str,
        query_tag: &str,
        source: impl Error + Send + Sync + 'static,
    ) -> Self {
        Self::Prepare {
            operation,
            table: table.to_owned(),
            query_tag: query_tag.to_owned(),
            source: Box::new(source),
        }
    }

    pub(crate) fn query(
        operation: &'static str,
        table: &str,
        query_tag: &str,
        source: impl Error + Send + Sync + 'static,
    ) -> Self {
        Self::Query {
            operation,
            table: table.to_owned(),
            query_tag: query_tag.to_owned(),
            source: Box::new(source),
        }
    }

    pub(crate) fn migration(
        migration_index: usize,
        statement_index: usize,
        source: ScyllaClientError,
    ) -> Self {
        Self::Migration {
            migration_index,
            statement_index,
            source: Box::new(source),
        }
    }
}
