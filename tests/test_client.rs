use std::collections::HashSet;
use std::net::TcpListener;
use std::ops::ControlFlow;
use std::time::Duration;

use scylla::response::PagingState;
use scylla::statement::Statement;
use scylla::{DeserializeRow, SerializeRow};
use stonfi_scylla_client::client::ScyllaClient;
use stonfi_scylla_client::config::{KeyspaceConfig, RetryConfig, ScyllaClientConfig};
use stonfi_scylla_client::errors::ScyllaClientError;
use stonfi_scylla_client::simple_migrator::SimpleMigrator;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

const CREATE_KEYSPACE_QUERY: &str = "
// Create keyspace if it does not exist
CREATE KEYSPACE IF NOT EXISTS [[KEYSPACE_NAME]]
WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'replication_factor' : [[REPLICATION_FACTOR]]}
AND durable_writes = true;
";

const CREATE_TABLE_QUERY: &str = "
CREATE TABLE IF NOT EXISTS test_object (
    field1 int,
    field2 text,
    PRIMARY KEY (field1)
);
";

#[derive(Debug, Clone, PartialEq, Eq, DeserializeRow, SerializeRow, Hash)]
struct TestObject {
    field1: i32,
    field2: String,
}

#[tokio::test]
async fn test_client_end_to_end() -> anyhow::Result<()> {
    stonfi_metrics::init_metrics!()?;

    let port_reservation = TcpListener::bind(("127.0.0.1", 0))?;
    let listening_port = port_reservation.local_addr()?.port();
    drop(port_reservation);
    let listening_port_arg = listening_port.to_string();

    let container = GenericImage::new("scylladb/scylla", "6.0")
        .with_wait_for(WaitFor::message_on_stderr("initialization completed"))
        .with_mapped_port(listening_port, listening_port.tcp())
        .with_cmd([
            "--smp",
            "1",
            "--memory",
            "512M",
            "--skip-wait-for-gossip-to-settle",
            "0",
            "--reactor-backend",
            "epoll",
            "--minimum-replication-factor-warn-threshold",
            "1",
            "--enable-tablets",
            "false",
            "--developer-mode",
            "1",
            "--rpc-address",
            "0.0.0.0",
            "--broadcast-rpc-address",
            "127.0.0.1",
            "--native-transport-port",
            listening_port_arg.as_str(),
        ])
        .start()
        .await?;

    let config = ScyllaClientConfig {
        endpoints: format!("127.0.0.1:{listening_port}"),
        max_parallel_queries: 4,
        keyspace: KeyspaceConfig {
            name: format!("scylla_client_test_{}", std::process::id()),
            replication_factor: 1,
        },
        request_timeout: Duration::from_secs(2),
        retry: RetryConfig {
            max_retries: 3,
            min_delay: Duration::from_millis(25),
            max_delay: Duration::from_millis(250),
        },
    };

    let client = ScyllaClient::new(&config).await?;
    wait_for_scylla(&client, &container).await?;
    let migrator = SimpleMigrator::new(client.clone());
    migrator
        .apply_all(&[CREATE_KEYSPACE_QUERY.to_owned()])
        .await?;
    client.use_keyspace().await?;
    client
        .execute_unprepared(CREATE_TABLE_QUERY, "create_test_object")
        .await?;

    let keyspaces = client
        .select_row("DESCRIBE KEYSPACES", (), "describe_keyspaces")
        .await?;
    assert!(keyspaces.iter().any(|row| {
        row.columns[0]
            .as_ref()
            .and_then(|value| value.clone().into_string())
            .is_some_and(|name| name == config.keyspace.name)
    }));

    let empty = client
        .select::<TestObject>("SELECT * FROM test_object", (), "select_empty_test_objects")
        .await?;
    assert!(empty.is_empty());

    let first = TestObject {
        field1: 1,
        field2: "first".to_owned(),
    };
    let second = TestObject {
        field1: 2,
        field2: "second".to_owned(),
    };
    for object in [&first, &second] {
        client
            .insert(
                "INSERT INTO test_object (field1, field2) VALUES (?, ?)",
                object.clone(),
                "insert_test_object",
            )
            .await?;
    }

    let invalid_insert = client
        .insert(
            "INSERT INTO test_object (field1, field2) VALUES (?, ?)",
            ("invalid-int", "invalid"),
            "insert_invalid_test_object",
        )
        .await;
    assert!(matches!(
        invalid_insert,
        Err(ScyllaClientError::Query {
            table,
            caller,
            ..
        }) if table == "test_object" && caller == "insert_invalid_test_object"
    ));

    let selected = client
        .select_one::<TestObject>(
            "SELECT * FROM test_object WHERE field1 = ?",
            (1,),
            "select_test_object",
        )
        .await?;
    assert_eq!(selected, Some(first.clone()));

    let cardinality_error = client
        .select_one::<TestObject>("SELECT * FROM test_object", (), "select_many_as_one")
        .await;
    assert!(matches!(
        cardinality_error,
        Err(ScyllaClientError::Query {
            table,
            caller,
            ..
        }) if table == "test_object" && caller == "select_many_as_one"
    ));

    client
        .delete(
            "DELETE FROM test_object WHERE field1 = ?",
            (1,),
            "delete_test_object",
        )
        .await?;
    let deleted = client
        .select_one::<TestObject>(
            "SELECT * FROM test_object WHERE field1 = ?",
            (1,),
            "select_deleted_test_object",
        )
        .await?;
    assert_eq!(deleted, None);

    for field1 in 3..=50 {
        client
            .insert(
                "INSERT INTO test_object (field1, field2) VALUES (?, ?)",
                TestObject {
                    field1,
                    field2: format!("value-{field1}"),
                },
                "insert_paged_test_object",
            )
            .await?;
    }

    let query_with_small_page: Statement =
        Into::<Statement>::into("SELECT * FROM test_object WHERE field1 <= ? ALLOW FILTERING")
            .with_page_size(7);
    let (small_page, _) = client
        .select_page::<TestObject>(
            query_with_small_page,
            (50,),
            PagingState::start(),
            "small_page_test_objects",
        )
        .await?;
    assert_eq!(small_page.len(), 7);

    let query: Statement =
        Into::<Statement>::into("SELECT * FROM test_object WHERE field1 <= ? ALLOW FILTERING")
            .with_page_size(10);
    let mut paging_state = PagingState::start();
    let mut paged_rows = HashSet::new();
    loop {
        let (rows, control_flow) = client
            .select_page::<TestObject>(
                query.clone(),
                (50,),
                paging_state.clone(),
                "page_test_objects",
            )
            .await?;
        paged_rows.extend(rows);

        match control_flow {
            ControlFlow::Continue(next_state) => paging_state = next_state,
            ControlFlow::Break(()) => break,
        }
    }
    assert_eq!(paged_rows.len(), 49);
    assert!(paged_rows.contains(&second));

    let query_error = client
        .select_row("SELECT * FROM missing_table", (), "select_missing_table")
        .await;
    assert!(matches!(
        query_error,
        Err(
            ScyllaClientError::Prepare {
                table,
                caller,
                ..
            } | ScyllaClientError::Query {
                table,
                caller,
                ..
            }
        ) if table == "unknown" && caller == "select_missing_table"
    ));

    Ok(())
}

async fn wait_for_scylla(
    client: &ScyllaClient,
    container: &ContainerAsync<GenericImage>,
) -> anyhow::Result<()> {
    let mut last_error = None;
    for _ in 0..30 {
        match client
            .execute_unprepared("SELECT now() FROM system.local", "wait_for_scylla")
            .await
        {
            Ok(()) => return Ok(()),
            Err(error) => {
                last_error = Some(error);
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }

    let stderr = container.stderr_to_vec().await?;
    let stderr = String::from_utf8_lossy(&stderr);
    let stderr_tail = stderr
        .chars()
        .rev()
        .take(8_000)
        .collect::<String>()
        .chars()
        .rev()
        .collect::<String>();
    match last_error {
        Some(error) => Err(anyhow::Error::new(error).context(format!(
            "Scylla CQL endpoint was not ready within 30 seconds; container stderr tail:\n{stderr_tail}"
        ))),
        None => Err(anyhow::anyhow!("Scylla readiness check did not run")),
    }
}
