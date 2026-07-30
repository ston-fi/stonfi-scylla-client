use std::time::Duration;

use stonfi_scylla_client::client::ScyllaClient;
use stonfi_scylla_client::config::{KeyspaceConfig, RetryConfig, ScyllaClientConfig};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    stonfi_metrics::init_metrics!()?;

    let config = ScyllaClientConfig {
        endpoints: std::env::var("SCYLLA_ENDPOINTS")
            .unwrap_or_else(|_| "127.0.0.1:9042".to_owned()),
        max_parallel_queries: 64,
        keyspace: KeyspaceConfig {
            name: std::env::var("SCYLLA_KEYSPACE").unwrap_or_else(|_| "example".to_owned()),
            replication_factor: 1,
        },
        request_timeout: Duration::from_secs(5),
        retry: RetryConfig {
            max_retries: 3,
            min_delay: Duration::from_millis(50),
            max_delay: Duration::from_secs(1),
        },
    };

    let client = ScyllaClient::new(&config).await?;
    client.use_keyspace().await?;
    let rows = client
        .select_row(
            "system.local",
            "SELECT cluster_name FROM system.local",
            (),
            Some("example_cluster_name"),
        )
        .await?;

    println!("received {} row(s)", rows.len());
    Ok(())
}
