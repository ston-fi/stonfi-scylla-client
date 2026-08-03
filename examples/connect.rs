use std::time::Duration;

use stonfi_scylla_client::client::ScyllaClient;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    stonfi_metrics::init_metrics!()?;

    let endpoints =
        std::env::var("SCYLLA_ENDPOINTS").unwrap_or_else(|_| "127.0.0.1:9042".to_owned());
    let keyspace = std::env::var("SCYLLA_KEYSPACE").unwrap_or_else(|_| "example".to_owned());
    let client = ScyllaClient::builder(endpoints, keyspace)
        .with_max_parallel_queries(64)
        .with_replication_factor(1)
        .with_request_timeout(Duration::from_secs(5))
        .with_retry_count(3)
        .with_retry_min_delay(Duration::from_millis(50))
        .with_retry_max_delay(Duration::from_secs(1))
        .build()
        .await?;
    client.use_keyspace().await?;
    let rows = client
        .select_row(
            "SELECT cluster_name FROM system.local",
            (),
            "example_cluster_name",
        )
        .await?;

    println!("received {} row(s)", rows.len());
    Ok(())
}
