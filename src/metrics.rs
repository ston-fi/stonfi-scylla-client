use std::time::Duration;

use stonfi_metrics::MetricsCell;
use stonfi_metrics::constants::DURATION_BUCKETS_01MS_20S;
use stonfi_metrics::prometheus::{self, Histogram, HistogramVec, IntCounterVec};
use stonfi_metrics::utils::format_duration_ms;

use crate::errors::{ScyllaClientError, ScyllaClientResult};
use crate::types::QueryType;

static METRICS: MetricsCell<ScyllaClientMetrics> = MetricsCell::new();

stonfi_metrics::register_metrics!(ScyllaClientMetrics, METRICS);

pub(crate) struct ScyllaClientMetrics {
    db_scylla_queries: IntCounterVec,
    db_scylla_queries_duration_ms: HistogramVec,
    db_scylla_wait_connection_ms: Histogram,
}

impl ScyllaClientMetrics {
    pub(crate) fn initialize() -> ScyllaClientResult<&'static Self> {
        METRICS
            .init("stonfi_scylla_client::ScyllaClientMetrics", Self::new)
            .map_err(ScyllaClientError::metrics)?;
        METRICS.get().ok_or_else(|| {
            ScyllaClientError::metrics(
                std::io::Error::other("Scylla metrics initialization completed without metrics")
                    .into(),
            )
        })
    }

    fn new() -> anyhow::Result<Self> {
        let labels = &["table_name", "query_type", "status", "query_tag"];

        Ok(Self {
            db_scylla_queries: prometheus::register_int_counter_vec!(
                "db_scylla_queries",
                "Number of Scylla queries",
                labels,
            )?,
            db_scylla_queries_duration_ms: prometheus::register_histogram_vec!(
                "db_scylla_queries_duration_ms",
                "Scylla query duration in milliseconds",
                labels,
                DURATION_BUCKETS_01MS_20S.clone(),
            )?,
            db_scylla_wait_connection_ms: prometheus::register_histogram!(
                "db_scylla_wait_connection_ms",
                "Time spent waiting for a Scylla client concurrency permit in milliseconds",
                DURATION_BUCKETS_01MS_20S.clone(),
            )?,
        })
    }

    pub(crate) fn update_success(
        &self,
        table: &str,
        query_type: QueryType,
        duration: Duration,
        query_tag: &str,
    ) {
        self.update(table, query_type, duration, true, query_tag);
    }

    pub(crate) fn update_error(
        &self,
        table: &str,
        query_type: QueryType,
        duration: Duration,
        query_tag: &str,
    ) {
        self.update(table, query_type, duration, false, query_tag);
    }

    pub(crate) fn update_wait_connection(&self, duration: Duration) {
        self.db_scylla_wait_connection_ms
            .observe(format_duration_ms(duration));
    }

    fn update(
        &self,
        table_name: &str,
        query_type: QueryType,
        duration: Duration,
        is_ok: bool,
        query_tag: &str,
    ) {
        let status = if is_ok { "ok" } else { "error" };
        let query_type: &str = query_type.into();
        let label_values = &[table_name, query_type, status, query_tag];

        self.db_scylla_queries.with_label_values(label_values).inc();
        self.db_scylla_queries_duration_ms
            .with_label_values(label_values)
            .observe(format_duration_ms(duration));
    }
}
