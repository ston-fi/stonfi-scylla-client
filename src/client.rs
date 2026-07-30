use std::error::Error;
use std::future::Future;
use std::ops::ControlFlow;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::StreamExt;
use scylla::client::caching_session::{CachingSession, CachingSessionBuilder};
use scylla::client::execution_profile::ExecutionProfile;
use scylla::client::session_builder::SessionBuilder;
use scylla::frame::Compression;
use scylla::policies::retry::FallthroughRetryPolicy;
use scylla::response::PagingState;
use scylla::serialize::row::SerializeRow;
use scylla::statement::Statement;
use scylla::statement::prepared::PreparedStatement;
use scylla::value::Row;
use tokio::sync::{Semaphore, SemaphorePermit};

use crate::config::{KeyspaceConfig, RetryConfig, ScyllaClientConfig, validate_config};
use crate::errors::{ScyllaClientError, ScyllaClientResult};
use crate::metrics::ScyllaClientMetrics;
use crate::types::QueryType;
use crate::types::QueryType::{Delete, Execute, Insert, Select};

const QUERY_TAG_DEFAULT_SELECT: &str = "select_default";
const QUERY_TAG_DEFAULT_SELECT_ONE: &str = "select_one_default";
const QUERY_TAG_DEFAULT_INSERT: &str = "insert_default";
const QUERY_TAG_DEFAULT_SELECT_ROW: &str = "select_row_default";
const QUERY_TAG_DEFAULT_SELECT_PAGE: &str = "select_page_default";
const QUERY_TAG_DEFAULT_EXECUTE_SUCCESS: &str = "update_success_default";
const QUERY_TAG_DEFAULT_QUERY_ERROR: &str = "query_error_default";
const QUERY_TAG_DEFAULT_DELETE: &str = "delete_default";

/// Row types that can be deserialized by the upstream Scylla driver.
///
/// This trait is implemented automatically for every compatible driver row
/// type. Consumers should derive [`scylla::DeserializeRow`] rather than
/// implementing this trait directly.
pub trait DeserRow: for<'a, 'b> scylla::deserialize::row::DeserializeRow<'a, 'b> + 'static {}

impl<T> DeserRow for T where
    T: for<'a, 'b> scylla::deserialize::row::DeserializeRow<'a, 'b> + 'static
{
}

/// Cloneable ScyllaDB client with bounded concurrency and retry behavior.
///
/// Clones share the driver session, prepared-statement cache, concurrency
/// semaphore, and process-global metrics.
///
/// The `table` and `query_tag` arguments become Prometheus labels. Use stable,
/// bounded values rather than record identifiers or other unbounded input.
#[derive(Clone)]
pub struct ScyllaClient {
    inner: Arc<Inner>,
    retry_config: RetryConfig,
}

impl ScyllaClient {
    /// Validate configuration, initialize metrics, and connect to ScyllaDB.
    ///
    /// The driver uses LZ4 compression, `LocalQuorum` consistency, and the
    /// configured request timeout.
    ///
    /// # Errors
    ///
    /// Returns [`ScyllaClientError::InvalidConfig`] for invalid configuration,
    /// [`ScyllaClientError::Metrics`] when metric registration fails,
    /// or [`ScyllaClientError::Connect`] when the driver session cannot be
    /// established.
    pub async fn new(config: &ScyllaClientConfig) -> ScyllaClientResult<Self> {
        let endpoints = validate_config(config)?;
        let metrics = ScyllaClientMetrics::initialize()?;
        let inner = Inner::new(config, &endpoints, metrics).await?;
        Ok(Self {
            inner: Arc::new(inner),
            retry_config: config.retry,
        })
    }

    /// Execute a prepared query and deserialize all returned rows.
    ///
    /// Failed preparation, execution, or row decoding is retried according to
    /// the client configuration.
    ///
    /// # Errors
    ///
    /// Returns [`ScyllaClientError::Prepare`] or
    /// [`ScyllaClientError::Query`] after retry attempts are exhausted.
    pub async fn select<R: DeserRow>(
        &self,
        table: &str,
        query: impl Into<Statement> + Clone,
        values: impl SerializeRow + Clone,
        query_tag: Option<&str>,
    ) -> ScyllaClientResult<Vec<R>> {
        let query_tag = query_tag.unwrap_or(QUERY_TAG_DEFAULT_SELECT);
        execute_with_retry(&self.retry_config, || {
            self.inner
                .select(table, query.clone(), values.clone(), query_tag)
        })
        .await
    }

    /// Execute a prepared query that must return zero or one row.
    ///
    /// Failed database work is retried. A successful query that returns more
    /// than one row produces a non-retried [`ScyllaClientError::Query`].
    ///
    /// # Errors
    ///
    /// Returns [`ScyllaClientError::Prepare`] or
    /// [`ScyllaClientError::Query`] for database, decoding, or cardinality
    /// failures.
    pub async fn select_one<R: DeserRow>(
        &self,
        table: &str,
        query: impl Into<Statement> + Clone,
        values: impl SerializeRow + Clone,
        query_tag: Option<&str>,
    ) -> ScyllaClientResult<Option<R>> {
        let query_tag = query_tag.unwrap_or(QUERY_TAG_DEFAULT_SELECT_ONE);
        let rows = execute_with_retry(&self.retry_config, || {
            self.inner
                .select::<R>(table, query.clone(), values.clone(), query_tag)
        })
        .await?;
        into_optional_single(table, query_tag, rows)
    }

    /// Execute a prepared query and return dynamically typed driver rows.
    ///
    /// # Errors
    ///
    /// Returns [`ScyllaClientError::Prepare`] or
    /// [`ScyllaClientError::Query`] after retry attempts are exhausted.
    pub async fn select_row(
        &self,
        table: &str,
        query: impl Into<Statement> + Clone,
        values: impl SerializeRow + Clone,
        query_tag: Option<&str>,
    ) -> ScyllaClientResult<Vec<Row>> {
        let query_tag = query_tag.unwrap_or(QUERY_TAG_DEFAULT_SELECT_ROW);
        execute_with_retry(&self.retry_config, || {
            self.inner
                .select(table, query.clone(), values.clone(), query_tag)
        })
        .await
    }

    /// Execute one page of a prepared query.
    ///
    /// The returned [`ControlFlow`] breaks when paging is complete and
    /// continues with the state required for the next page.
    ///
    /// # Errors
    ///
    /// Returns [`ScyllaClientError::Prepare`] or
    /// [`ScyllaClientError::Query`] after retry attempts are exhausted.
    pub async fn select_page<R: DeserRow>(
        &self,
        table: &str,
        query: impl Into<Statement> + Clone,
        values: impl SerializeRow + Clone,
        paging_state: PagingState,
        query_tag: Option<&str>,
    ) -> ScyllaClientResult<(Vec<R>, ControlFlow<(), PagingState>)> {
        let query_tag = query_tag.unwrap_or(QUERY_TAG_DEFAULT_SELECT_PAGE);
        execute_with_retry(&self.retry_config, || {
            self.inner.select_page(
                table,
                query.clone(),
                values.clone(),
                paging_state.clone(),
                query_tag,
            )
        })
        .await
    }

    /// Execute a prepared insert statement.
    ///
    /// The statement is marked idempotent and may be attempted more than once.
    /// Only use this method for writes that are safe to replay.
    ///
    /// # Errors
    ///
    /// Returns [`ScyllaClientError::Prepare`] or
    /// [`ScyllaClientError::Query`] after retry attempts are exhausted.
    pub async fn insert(
        &self,
        table: &str,
        query: impl Into<Statement> + Clone,
        values: impl SerializeRow + Clone,
        query_tag: Option<&str>,
    ) -> ScyllaClientResult<()> {
        let query_tag = query_tag.unwrap_or(QUERY_TAG_DEFAULT_INSERT);
        execute_with_retry(&self.retry_config, || {
            self.inner
                .query(table, query.clone(), values.clone(), Insert, query_tag)
        })
        .await
    }

    /// Execute a prepared delete statement.
    ///
    /// The statement is marked idempotent and may be attempted more than once.
    /// Only use this method for writes that are safe to replay.
    ///
    /// # Errors
    ///
    /// Returns [`ScyllaClientError::Prepare`] or
    /// [`ScyllaClientError::Query`] after retry attempts are exhausted.
    pub async fn delete(
        &self,
        table: &str,
        query: impl Into<Statement> + Clone,
        values: impl SerializeRow + Clone,
        query_tag: Option<&str>,
    ) -> ScyllaClientResult<()> {
        let query_tag = query_tag.unwrap_or(QUERY_TAG_DEFAULT_DELETE);
        execute_with_retry(&self.retry_config, || {
            self.inner
                .query(table, query.clone(), values.clone(), Delete, query_tag)
        })
        .await
    }

    /// Select the configured keyspace for this driver session.
    ///
    /// The generated unprepared statement is safe because construction
    /// validates the keyspace as an unquoted CQL identifier.
    ///
    /// # Errors
    ///
    /// Returns [`ScyllaClientError::Query`] after retry attempts are
    /// exhausted.
    pub async fn use_keyspace(&self) -> ScyllaClientResult<()> {
        let query = format!("USE {}", self.inner.keyspace.name);
        execute_with_retry(&self.retry_config, || self.execute_unprepared(&query)).await
    }

    /// Execute raw CQL without preparing it.
    ///
    /// This method performs one attempt and is intended for statements that
    /// the driver cannot prepare, such as `USE` and schema migrations. It does
    /// not validate or quote CQL. Never interpolate untrusted input.
    ///
    /// # Errors
    ///
    /// Returns [`ScyllaClientError::Query`] when the concurrency permit cannot
    /// be acquired or the driver rejects the statement.
    pub async fn execute_unprepared(&self, query: &str) -> ScyllaClientResult<()> {
        self.inner
            .execute_unprepared("execute(no_table)", query)
            .await
    }

    pub(crate) fn keyspace_config(&self) -> KeyspaceConfig {
        self.inner.keyspace.clone()
    }
}

async fn execute_with_retry<F, Fut, T, E>(config: &RetryConfig, operation: F) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    tryhard::retry_fn(operation)
        .retries(config.max_retries)
        .exponential_backoff(config.min_delay)
        .max_delay(config.max_delay)
        .await
}

fn into_optional_single<T>(
    table: &str,
    query_tag: &str,
    rows: Vec<T>,
) -> ScyllaClientResult<Option<T>> {
    match rows.len() {
        0 => Ok(None),
        1 => Ok(rows.into_iter().next()),
        count => Err(ScyllaClientError::query(
            "select",
            table,
            query_tag,
            std::io::Error::other(format!("expected at most one row, received {count}")),
        )),
    }
}

struct Inner {
    session: CachingSession,
    keyspace: KeyspaceConfig,
    active_queries: Semaphore,
    metrics: &'static ScyllaClientMetrics,
}

impl Inner {
    async fn new(
        config: &ScyllaClientConfig,
        endpoints: &[&str],
        metrics: &'static ScyllaClientMetrics,
    ) -> ScyllaClientResult<Self> {
        let profile = ExecutionProfile::builder()
            .request_timeout(Some(config.request_timeout))
            .consistency(scylla::statement::Consistency::LocalQuorum)
            .retry_policy(Arc::new(FallthroughRetryPolicy::new()))
            .build();

        let mut session_builder = SessionBuilder::new();
        for endpoint in endpoints {
            session_builder = session_builder.known_node(endpoint);
        }

        let session = session_builder
            .compression(Some(Compression::Lz4))
            .default_execution_profile_handle(profile.into_handle())
            .build()
            .await
            .map_err(ScyllaClientError::connect)?;

        Ok(Self {
            session: CachingSessionBuilder::new(session).build(),
            keyspace: config.keyspace.clone(),
            active_queries: Semaphore::new(config.max_parallel_queries),
            metrics,
        })
    }

    async fn execute_unprepared(&self, table: &str, query: &str) -> ScyllaClientResult<()> {
        let start_time = Instant::now();
        let _active_query = self
            .acquire_query_permit(table, Execute, QUERY_TAG_DEFAULT_QUERY_ERROR, start_time)
            .await?;

        log::trace!("Executing unprepared query: {query:?}");
        match self.session.get_session().query_unpaged(query, &()).await {
            Ok(_) => {
                self.metrics.update_success(
                    table,
                    Execute,
                    start_time.elapsed(),
                    QUERY_TAG_DEFAULT_EXECUTE_SUCCESS,
                );
                Ok(())
            }
            Err(error) => Err(self.query_error(
                table,
                Execute,
                error,
                start_time.elapsed(),
                QUERY_TAG_DEFAULT_QUERY_ERROR,
            )),
        }
    }

    async fn query(
        &self,
        table: &str,
        query: impl Into<Statement>,
        values: impl SerializeRow,
        query_type: QueryType,
        query_tag: &str,
    ) -> ScyllaClientResult<()> {
        let start_time = Instant::now();
        let _active_query = self
            .acquire_query_permit(table, query_type, query_tag, start_time)
            .await?;
        let prepared_query = self
            .prepare_query(table, query, query_type, query_tag)
            .await?;

        log::trace!("Executing prepared query: {prepared_query:?}");
        match self
            .session
            .get_session()
            .execute_unpaged(&prepared_query, values)
            .await
        {
            Ok(_) => {
                self.metrics
                    .update_success(table, query_type, start_time.elapsed(), query_tag);
                Ok(())
            }
            Err(error) => {
                Err(self.query_error(table, query_type, error, start_time.elapsed(), query_tag))
            }
        }
    }

    async fn select<R: DeserRow>(
        &self,
        table: &str,
        query: impl Into<Statement>,
        values: impl SerializeRow,
        query_tag: &str,
    ) -> ScyllaClientResult<Vec<R>> {
        let start_time = Instant::now();
        let _active_query = self
            .acquire_query_permit(table, Select, query_tag, start_time)
            .await?;

        let prepared_query = self.prepare_query(table, query, Select, query_tag).await?;
        log::trace!("Executing prepared query: {prepared_query:?}");

        let query_result = self
            .session
            .get_session()
            .execute_iter(prepared_query, values)
            .await
            .map_err(|error| {
                self.query_error(table, Select, error, start_time.elapsed(), query_tag)
            })?;

        let mut result = Vec::new();
        let mut rows_stream = query_result.rows_stream().map_err(|error| {
            self.query_error(table, Select, error, start_time.elapsed(), query_tag)
        })?;
        while let Some(row) = rows_stream.next().await {
            result.push(row.map_err(|error| {
                self.query_error(table, Select, error, start_time.elapsed(), query_tag)
            })?);
        }

        self.metrics
            .update_success(table, Select, start_time.elapsed(), query_tag);
        Ok(result)
    }

    async fn select_page<R: DeserRow>(
        &self,
        table: &str,
        query: impl Into<Statement>,
        values: impl SerializeRow,
        paging_state: PagingState,
        query_tag: &str,
    ) -> ScyllaClientResult<(Vec<R>, ControlFlow<(), PagingState>)> {
        let start_time = Instant::now();
        let _active_query = self
            .acquire_query_permit(table, Select, query_tag, start_time)
            .await?;

        let prepared_query = self.prepare_query(table, query, Select, query_tag).await?;
        log::trace!("Executing prepared query: {prepared_query:?}");

        let (query_result, paging_state_response) = self
            .session
            .get_session()
            .execute_single_page(&prepared_query, values, paging_state)
            .await
            .map_err(|error| {
                self.query_error(table, Select, error, start_time.elapsed(), query_tag)
            })?;

        let elapsed = start_time.elapsed();
        let rows = query_result
            .into_rows_result()
            .map_err(|error| self.query_error(table, Select, error, elapsed, query_tag))?
            .rows::<R>()
            .map_err(|error| self.query_error(table, Select, error, elapsed, query_tag))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| self.query_error(table, Select, error, elapsed, query_tag))?;

        self.metrics
            .update_success(table, Select, elapsed, query_tag);
        Ok((rows, paging_state_response.into_paging_control_flow()))
    }

    async fn prepare_query(
        &self,
        table: &str,
        query: impl Into<Statement>,
        query_type: QueryType,
        query_tag: &str,
    ) -> ScyllaClientResult<PreparedStatement> {
        let query = query.into();
        log::trace!("Preparing query: {}", query.contents);
        let prepared_statement =
            self.session
                .add_prepared_statement(&query)
                .await
                .map_err(|error| {
                    let operation: &str = query_type.into();
                    ScyllaClientError::prepare(operation, table, query_tag, error)
                })?;
        let mut prepared_statement = prepared_statement;
        prepared_statement.set_retry_policy(Some(Arc::new(FallthroughRetryPolicy::new())));
        prepared_statement.set_is_idempotent(true);
        Ok(prepared_statement)
    }

    async fn acquire_query_permit(
        &self,
        table: &str,
        query_type: QueryType,
        query_tag: &str,
        start_time: Instant,
    ) -> ScyllaClientResult<SemaphorePermit<'_>> {
        let permit = self.active_queries.acquire().await.map_err(|error| {
            self.query_error(table, query_type, error, start_time.elapsed(), query_tag)
        })?;
        self.metrics.update_wait_connection(start_time.elapsed());
        Ok(permit)
    }

    fn query_error(
        &self,
        table: &str,
        query_type: QueryType,
        source: impl Error + Send + Sync + 'static,
        duration: Duration,
        query_tag: &str,
    ) -> ScyllaClientError {
        let operation: &str = query_type.into();
        log::warn!(
            "Scylla query error: table={table}, operation={operation}, query_tag={query_tag}, error={source}"
        );
        self.metrics
            .update_error(table, query_type, duration, query_tag);
        ScyllaClientError::query(operation, table, query_tag, source)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::Duration;

    use super::{execute_with_retry, into_optional_single};
    use crate::config::RetryConfig;
    use crate::errors::ScyllaClientError;

    #[tokio::test]
    async fn test_retry_config_stops_after_configured_retries() {
        let attempts = Arc::new(AtomicU32::new(0));
        let retry_config = RetryConfig {
            min_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(1),
            max_retries: 3,
        };

        let result = execute_with_retry(&retry_config, || {
            let attempts = Arc::clone(&attempts);
            async move {
                attempts.fetch_add(1, Ordering::Relaxed);
                Err::<(), _>("retry")
            }
        })
        .await;

        assert_eq!(result, Err("retry"));
        assert_eq!(attempts.load(Ordering::Relaxed), 4);
    }

    #[test]
    fn test_into_optional_single_enforces_cardinality() -> anyhow::Result<()> {
        assert_eq!(
            into_optional_single::<u8>("table", "tag", Vec::new())?,
            None
        );
        assert_eq!(into_optional_single("table", "tag", vec![7])?, Some(7));

        let error = into_optional_single("table", "tag", vec![7, 8]);
        assert!(matches!(error, Err(ScyllaClientError::Query { .. })));
        Ok(())
    }
}
