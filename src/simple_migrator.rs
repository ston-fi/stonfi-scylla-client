use crate::client::ScyllaClient;
use crate::config::KeyspaceConfig;
use crate::errors::{ScyllaClientError, ScyllaClientResult};

/// Minimal CQL migration runner with keyspace template substitution.
///
/// The migrator executes every statement on every invocation; it does not
/// track versions, checksums, or applied migrations. Statements must therefore
/// be idempotent when repeated execution is possible.
pub struct SimpleMigrator {
    client: ScyllaClient,
    config: KeyspaceConfig,
}

impl SimpleMigrator {
    /// Create a migrator using the client's validated keyspace configuration.
    #[must_use]
    pub fn new(client: ScyllaClient) -> Self {
        let config = client.keyspace_config();
        Self { client, config }
    }

    /// Split and apply each migration input in order.
    ///
    /// # Errors
    ///
    /// Returns [`ScyllaClientError::Migration`] with zero-based migration and
    /// statement indexes when a statement fails.
    pub async fn apply_all(&self, migrations: &[String]) -> ScyllaClientResult<()> {
        for (migration_index, migration) in migrations.iter().enumerate() {
            for (statement_index, statement) in Self::split_statements(migration).iter().enumerate()
            {
                self.apply(statement).await.map_err(|error| {
                    ScyllaClientError::migration(migration_index, statement_index, error)
                })?;
            }
        }
        Ok(())
    }

    /// Substitute keyspace placeholders and execute one unprepared statement.
    ///
    /// Supported placeholders are `[[KEYSPACE_NAME]]` and
    /// `[[REPLICATION_FACTOR]]`.
    ///
    /// # Errors
    ///
    /// Returns [`ScyllaClientError::Query`] when ScyllaDB rejects the rendered
    /// statement.
    pub async fn apply(&self, query_template: &str) -> ScyllaClientResult<()> {
        let query = render_statement(query_template, &self.config);
        log::trace!("Executing migration statement: {query}");
        self.client.execute_unprepared(&query).await
    }

    /// Split CQL source into statements.
    ///
    /// Semicolons inside single- or double-quoted CQL values are preserved.
    /// Blank lines and full-line `--` or `//` comments are discarded. Both LF
    /// and CRLF input and a final statement without a semicolon are supported.
    ///
    /// This is intentionally not a full CQL parser. Block comments and
    /// dollar-quoted values are not interpreted.
    #[must_use]
    fn split_statements(source: &str) -> Vec<String> {
        let characters = source.chars().collect::<Vec<_>>();
        let mut statements = Vec::new();
        let mut statement = String::new();
        let mut index = 0;
        let mut in_single_quote = false;
        let mut in_double_quote = false;
        let mut at_line_start = true;

        while index < characters.len() {
            let character = characters[index];
            let next = characters.get(index + 1).copied();

            if !in_single_quote && !in_double_quote && at_line_start {
                if matches!(character, ' ' | '\t') {
                    statement.push(character);
                    index += 1;
                    continue;
                }
                if (character == '-' && next == Some('-'))
                    || (character == '/' && next == Some('/'))
                {
                    while index < characters.len() && !matches!(characters[index], '\n' | '\r') {
                        index += 1;
                    }
                    continue;
                }
            }

            if character == '\r' {
                if next == Some('\n') {
                    index += 1;
                }
                statement.push('\n');
                at_line_start = true;
                index += 1;
                continue;
            }

            if character == '\n' {
                statement.push('\n');
                at_line_start = true;
                index += 1;
                continue;
            }

            if in_single_quote {
                statement.push(character);
                if character == '\'' {
                    if next == Some('\'') {
                        statement.push('\'');
                        index += 1;
                    } else {
                        in_single_quote = false;
                    }
                }
                at_line_start = false;
                index += 1;
                continue;
            }

            if in_double_quote {
                statement.push(character);
                if character == '"' {
                    if next == Some('"') {
                        statement.push('"');
                        index += 1;
                    } else {
                        in_double_quote = false;
                    }
                }
                at_line_start = false;
                index += 1;
                continue;
            }

            match character {
                '\'' => {
                    in_single_quote = true;
                    statement.push(character);
                    at_line_start = false;
                }
                '"' => {
                    in_double_quote = true;
                    statement.push(character);
                    at_line_start = false;
                }
                ';' => push_statement(&mut statements, &mut statement),
                _ => {
                    statement.push(character);
                    if !character.is_whitespace() {
                        at_line_start = false;
                    }
                }
            }

            index += 1;
        }

        push_statement(&mut statements, &mut statement);
        statements
    }
}

fn render_statement(query_template: &str, config: &KeyspaceConfig) -> String {
    query_template
        .replace("[[KEYSPACE_NAME]]", &config.name)
        .replace(
            "[[REPLICATION_FACTOR]]",
            &config.replication_factor.to_string(),
        )
}

fn push_statement(statements: &mut Vec<String>, statement: &mut String) {
    let trimmed = statement.trim();
    if !trimmed.is_empty() {
        statements.push(trimmed.to_owned());
    }
    statement.clear();
}

#[cfg(test)]
mod tests {
    use super::{SimpleMigrator, render_statement};
    use crate::config::KeyspaceConfig;

    #[test]
    fn test_split_statements_handles_comments_line_endings_and_trailing_statement() {
        let source = "\r\n\
            // first comment\r\n\
            CREATE TABLE first (id int);\r\n\
              -- second comment\r\n\
            CREATE TABLE second (id int)\r\n";

        assert_eq!(
            SimpleMigrator::split_statements(source),
            [
                "CREATE TABLE first (id int)",
                "CREATE TABLE second (id int)",
            ]
        );
    }

    #[test]
    fn test_split_statements_preserves_quoted_semicolons_and_escaped_quotes() {
        let source = "INSERT INTO test (a, b) VALUES ('a;''b', \"c;\"\"d\");\nSELECT * FROM test;";

        assert_eq!(
            SimpleMigrator::split_statements(source),
            [
                "INSERT INTO test (a, b) VALUES ('a;''b', \"c;\"\"d\")",
                "SELECT * FROM test",
            ]
        );
    }

    #[test]
    fn test_render_statement_substitutes_keyspace_values() {
        let config = KeyspaceConfig {
            name: "example".to_owned(),
            replication_factor: 3,
        };

        let rendered = render_statement(
            "CREATE KEYSPACE [[KEYSPACE_NAME]] WITH replication_factor = [[REPLICATION_FACTOR]]",
            &config,
        );

        assert_eq!(
            rendered,
            "CREATE KEYSPACE example WITH replication_factor = 3"
        );
    }
}
