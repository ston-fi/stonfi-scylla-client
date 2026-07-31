use std::borrow::Cow;
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

const TABLE_UNKNOWN: &str = "unknown";

/// Row types that can be deserialized by the upstream Scylla driver.
///
/// Implemented automatically for types compatible with
/// [`scylla::DeserializeRow`].
pub trait DeserRow: for<'a, 'b> scylla::deserialize::row::DeserializeRow<'a, 'b> + 'static {}

impl<T> DeserRow for T where
    T: for<'a, 'b> scylla::deserialize::row::DeserializeRow<'a, 'b> + 'static
{
}

/// ScyllaDB client with bounded concurrency, retries, caching, and metrics.
///
/// Prepared methods infer table labels from driver metadata. Query callers must
/// be stable and bounded because they are exported as Prometheus labels.
#[derive(Clone)]
pub struct ScyllaClient {
    inner: Arc<Inner>,
    retry_config: RetryConfig,
}

impl ScyllaClient {
    /// Validate configuration and connect to ScyllaDB.
    ///
    /// # Errors
    ///
    /// Returns [`ScyllaClientError::InvalidConfig`] for invalid configuration,
    /// or [`ScyllaClientError::Connect`] when the driver session cannot be
    /// established.
    pub async fn new(config: &ScyllaClientConfig) -> ScyllaClientResult<Self> {
        let endpoints = validate_config(config)?;
        let inner = Inner::new(config, &endpoints).await?;
        Ok(Self {
            inner: Arc::new(inner),
            retry_config: config.retry,
        })
    }

    /// Execute a retried prepared query and deserialize all rows.
    ///
    /// `caller` is a stable, bounded metrics label.
    ///
    /// # Errors
    ///
    /// Returns [`ScyllaClientError::Prepare`] or
    /// [`ScyllaClientError::Query`] after retry attempts are exhausted.
    pub async fn select<R: DeserRow>(
        &self,
        query: impl Into<Statement> + Clone,
        values: impl SerializeRow + Clone,
        caller: &'static str,
    ) -> ScyllaClientResult<Vec<R>> {
        execute_with_retry(&self.retry_config, || {
            self.inner.select(query.clone(), values.clone(), caller)
        })
        .await
        .map(|selected| selected.rows)
    }

    /// Execute a retried prepared query returning at most one row.
    ///
    /// Cardinality errors are not retried. `caller` is a stable, bounded metrics
    /// label.
    ///
    /// # Errors
    ///
    /// Returns [`ScyllaClientError::Prepare`] or
    /// [`ScyllaClientError::Query`] for database, decoding, or cardinality
    /// failures.
    pub async fn select_one<R: DeserRow>(
        &self,
        query: impl Into<Statement> + Clone,
        values: impl SerializeRow + Clone,
        caller: &'static str,
    ) -> ScyllaClientResult<Option<R>> {
        let selected = execute_with_retry(&self.retry_config, || {
            self.inner
                .select::<R>(query.clone(), values.clone(), caller)
        })
        .await?;
        let table = prepared_row_table(&selected.prepared);
        into_optional_single(&table, caller, selected.rows)
    }

    /// Execute a retried prepared query and return dynamically typed rows.
    ///
    /// `caller` is a stable, bounded metrics label.
    ///
    /// # Errors
    ///
    /// Returns [`ScyllaClientError::Prepare`] or
    /// [`ScyllaClientError::Query`] after retry attempts are exhausted.
    pub async fn select_row(
        &self,
        query: impl Into<Statement> + Clone,
        values: impl SerializeRow + Clone,
        caller: &'static str,
    ) -> ScyllaClientResult<Vec<Row>> {
        execute_with_retry(&self.retry_config, || {
            self.inner.select(query.clone(), values.clone(), caller)
        })
        .await
        .map(|selected| selected.rows)
    }

    /// Execute one page of a prepared query.
    ///
    /// [`ControlFlow`] breaks at completion or continues with the next paging
    /// state. `caller` is a stable, bounded metrics label.
    ///
    /// # Errors
    ///
    /// Returns [`ScyllaClientError::Prepare`] or
    /// [`ScyllaClientError::Query`] after retry attempts are exhausted.
    pub async fn select_page<R: DeserRow>(
        &self,
        query: impl Into<Statement> + Clone,
        values: impl SerializeRow + Clone,
        paging_state: PagingState,
        caller: &'static str,
    ) -> ScyllaClientResult<(Vec<R>, ControlFlow<(), PagingState>)> {
        execute_with_retry(&self.retry_config, || {
            self.inner
                .select_page(query.clone(), values.clone(), paging_state.clone(), caller)
        })
        .await
    }

    /// Execute a retried prepared insert.
    ///
    /// The statement must be safe to replay. `caller` is a stable, bounded
    /// metrics label.
    ///
    /// # Errors
    ///
    /// Returns [`ScyllaClientError::Prepare`] or
    /// [`ScyllaClientError::Query`] after retry attempts are exhausted.
    pub async fn insert(
        &self,
        query: impl Into<Statement> + Clone,
        values: impl SerializeRow + Clone,
        caller: &'static str,
    ) -> ScyllaClientResult<()> {
        execute_with_retry(&self.retry_config, || {
            self.inner
                .query(query.clone(), values.clone(), Insert, caller)
        })
        .await
    }

    /// Execute a retried prepared delete.
    ///
    /// The statement must be safe to replay. `caller` is a stable, bounded
    /// metrics label.
    ///
    /// # Errors
    ///
    /// Returns [`ScyllaClientError::Prepare`] or
    /// [`ScyllaClientError::Query`] after retry attempts are exhausted.
    pub async fn delete(
        &self,
        query: impl Into<Statement> + Clone,
        values: impl SerializeRow + Clone,
        caller: &'static str,
    ) -> ScyllaClientResult<()> {
        execute_with_retry(&self.retry_config, || {
            self.inner
                .query(query.clone(), values.clone(), Delete, caller)
        })
        .await
    }

    /// Select the configured keyspace.
    ///
    /// Construction validates the interpolated keyspace as a CQL identifier.
    ///
    /// # Errors
    ///
    /// Returns [`ScyllaClientError::Query`] after retry attempts are
    /// exhausted.
    pub async fn use_keyspace(&self) -> ScyllaClientResult<()> {
        let query = format!("USE {}", self.inner.keyspace.name);
        execute_with_retry(&self.retry_config, || {
            self.execute_unprepared(&query, "use_keyspace")
        })
        .await
    }

    /// Execute raw CQL without preparing it.
    ///
    /// Performs one attempt without validating or quoting CQL. Never
    /// interpolate untrusted input. `caller` is a stable, bounded metrics label.
    ///
    /// # Errors
    ///
    /// Returns [`ScyllaClientError::Query`] when the concurrency permit cannot
    /// be acquired or the driver rejects the statement.
    pub async fn execute_unprepared(
        &self,
        query: &str,
        caller: &'static str,
    ) -> ScyllaClientResult<()> {
        self.inner
            .execute_unprepared("execute_no_table", query, caller)
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
    caller: &str,
    rows: Vec<T>,
) -> ScyllaClientResult<Option<T>> {
    match rows.len() {
        0 => Ok(None),
        1 => Ok(rows.into_iter().next()),
        count => Err(ScyllaClientError::query(
            "select",
            table,
            caller,
            std::io::Error::other(format!("expected at most one row, received {count}")),
        )),
    }
}

struct SelectedRows<T> {
    rows: Vec<T>,
    prepared: PreparedStatement,
}

struct Inner {
    session: CachingSession,
    keyspace: KeyspaceConfig,
    active_queries: Semaphore,
}

impl Inner {
    async fn new(config: &ScyllaClientConfig, endpoints: &[&str]) -> ScyllaClientResult<Self> {
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
        })
    }

    async fn execute_unprepared(
        &self,
        table: &str,
        query: &str,
        caller: &str,
    ) -> ScyllaClientResult<()> {
        let start_time = Instant::now();
        let _active_query = self
            .acquire_query_permit(Execute, caller, start_time)
            .await?;

        log::trace!("Executing unprepared query: {query:?}");
        match self.session.get_session().query_unpaged(query, &()).await {
            Ok(_) => {
                ScyllaClientMetrics::update_success(table, Execute, start_time.elapsed(), caller);
                Ok(())
            }
            Err(error) => {
                Err(self.query_error(table, Execute, error, start_time.elapsed(), caller))
            }
        }
    }

    async fn query(
        &self,
        query: impl Into<Statement>,
        values: impl SerializeRow,
        query_type: QueryType,
        caller: &str,
    ) -> ScyllaClientResult<()> {
        let start_time = Instant::now();
        let _active_query = self
            .acquire_query_permit(query_type, caller, start_time)
            .await?;
        let prepared_query = self.prepare_query(query, query_type, caller).await?;
        let table = prepared_table(&prepared_query);

        log::trace!("Executing prepared query: {prepared_query:?}");
        match self
            .session
            .get_session()
            .execute_unpaged(&prepared_query, values)
            .await
        {
            Ok(_) => {
                ScyllaClientMetrics::update_success(
                    &table,
                    query_type,
                    start_time.elapsed(),
                    caller,
                );
                Ok(())
            }
            Err(error) => {
                Err(self.query_error(&table, query_type, error, start_time.elapsed(), caller))
            }
        }
    }

    async fn select<R: DeserRow>(
        &self,
        query: impl Into<Statement>,
        values: impl SerializeRow,
        caller: &str,
    ) -> ScyllaClientResult<SelectedRows<R>> {
        let start_time = Instant::now();
        let _active_query = self
            .acquire_query_permit(Select, caller, start_time)
            .await?;

        let prepared_query = self.prepare_query(query, Select, caller).await?;
        let table = prepared_row_table(&prepared_query);
        log::trace!("Executing prepared query: {prepared_query:?}");

        let query_result = self
            .session
            .get_session()
            .execute_iter(prepared_query.clone(), values)
            .await
            .map_err(|error| {
                self.query_error(&table, Select, error, start_time.elapsed(), caller)
            })?;

        let mut result = Vec::new();
        let mut rows_stream = query_result.rows_stream().map_err(|error| {
            self.query_error(&table, Select, error, start_time.elapsed(), caller)
        })?;
        while let Some(row) = rows_stream.next().await {
            result.push(row.map_err(|error| {
                self.query_error(&table, Select, error, start_time.elapsed(), caller)
            })?);
        }

        ScyllaClientMetrics::update_success(&table, Select, start_time.elapsed(), caller);
        drop(table);
        Ok(SelectedRows {
            rows: result,
            prepared: prepared_query,
        })
    }

    async fn select_page<R: DeserRow>(
        &self,
        query: impl Into<Statement>,
        values: impl SerializeRow,
        paging_state: PagingState,
        caller: &str,
    ) -> ScyllaClientResult<(Vec<R>, ControlFlow<(), PagingState>)> {
        let start_time = Instant::now();
        let _active_query = self
            .acquire_query_permit(Select, caller, start_time)
            .await?;

        let prepared_query = self.prepare_query(query, Select, caller).await?;
        let table = prepared_row_table(&prepared_query);
        log::trace!("Executing prepared query: {prepared_query:?}");

        let (query_result, paging_state_response) = self
            .session
            .get_session()
            .execute_single_page(&prepared_query, values, paging_state)
            .await
            .map_err(|error| {
                self.query_error(&table, Select, error, start_time.elapsed(), caller)
            })?;

        let elapsed = start_time.elapsed();
        let rows = query_result
            .into_rows_result()
            .map_err(|error| self.query_error(&table, Select, error, elapsed, caller))?
            .rows::<R>()
            .map_err(|error| self.query_error(&table, Select, error, elapsed, caller))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| self.query_error(&table, Select, error, elapsed, caller))?;

        ScyllaClientMetrics::update_success(&table, Select, elapsed, caller);
        Ok((rows, paging_state_response.into_paging_control_flow()))
    }

    async fn prepare_query(
        &self,
        query: impl Into<Statement>,
        query_type: QueryType,
        caller: &str,
    ) -> ScyllaClientResult<PreparedStatement> {
        let query = query.into();
        log::trace!("Preparing query: {}", query.contents);
        let prepared_statement =
            self.session
                .add_prepared_statement(&query)
                .await
                .map_err(|error| {
                    let operation: &str = query_type.into();
                    ScyllaClientError::prepare(operation, TABLE_UNKNOWN, caller, error)
                })?;
        let mut prepared_statement = prepared_statement;
        prepared_statement.set_retry_policy(Some(Arc::new(FallthroughRetryPolicy::new())));
        prepared_statement.set_is_idempotent(true);
        Ok(prepared_statement)
    }

    async fn acquire_query_permit(
        &self,
        query_type: QueryType,
        caller: &str,
        start_time: Instant,
    ) -> ScyllaClientResult<SemaphorePermit<'_>> {
        let permit = self.active_queries.acquire().await.map_err(|error| {
            self.query_error(
                TABLE_UNKNOWN,
                query_type,
                error,
                start_time.elapsed(),
                caller,
            )
        })?;
        ScyllaClientMetrics::update_wait_connection(start_time.elapsed());
        Ok(permit)
    }

    fn query_error(
        &self,
        table: &str,
        query_type: QueryType,
        source: impl Error + Send + Sync + 'static,
        duration: Duration,
        caller: &str,
    ) -> ScyllaClientError {
        let operation: &str = query_type.into();
        log::warn!(
            "Scylla query error: table={table}, operation={operation}, caller={caller}, error={source}"
        );
        ScyllaClientMetrics::update_error(table, query_type, duration, caller);
        ScyllaClientError::query(operation, table, caller, source)
    }
}

fn prepared_table(prepared: &PreparedStatement) -> Cow<'_, str> {
    if let Some(table) = prepared.get_table_name().filter(|table| !table.is_empty()) {
        return Cow::Borrowed(table);
    }

    Cow::Borrowed(TABLE_UNKNOWN)
}

fn prepared_row_table(prepared: &PreparedStatement) -> Cow<'_, str> {
    if let Some(table) = prepared.get_table_name().filter(|table| !table.is_empty()) {
        return Cow::Borrowed(table);
    }

    prepared
        .get_current_result_set_col_specs()
        .get()
        .get_by_index(0)
        .map(|column| column.table_spec().table_name())
        .filter(|table| !table.is_empty())
        .map(|table| Cow::Owned(table.to_owned()))
        .unwrap_or(Cow::Borrowed(TABLE_UNKNOWN))
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
            into_optional_single::<u8>("table", "caller", Vec::new())?,
            None
        );
        assert_eq!(into_optional_single("table", "caller", vec![7])?, Some(7));

        let error = into_optional_single("table", "caller", vec![7, 8]);
        assert!(matches!(error, Err(ScyllaClientError::Query { .. })));
        Ok(())
    }
}
