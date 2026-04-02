//! SQL query builder for the PyroSQL Diesel backend.

use crate::PyroSqlBackend;
use diesel::query_builder::QueryBuilder;
use diesel::result::QueryResult;

/// Query builder that produces PyroSQL-compatible SQL.
///
/// PyroSQL uses a PostgreSQL-like SQL dialect with double-quoted
/// identifiers and `$N` positional parameter placeholders.
#[derive(Debug, Default)]
pub struct PyroSqlQueryBuilder {
    sql: String,
    bind_idx: usize,
}

impl PyroSqlQueryBuilder {
    /// Create a new empty query builder.
    pub fn new() -> Self {
        Self {
            sql: String::new(),
            bind_idx: 0,
        }
    }

    /// Consume the builder and return the generated SQL string.
    pub fn finish(self) -> String {
        self.sql
    }
}

impl QueryBuilder<PyroSqlBackend> for PyroSqlQueryBuilder {
    fn push_sql(&mut self, sql: &str) {
        self.sql.push_str(sql);
    }

    fn push_identifier(&mut self, identifier: &str) -> QueryResult<()> {
        self.sql.push('"');
        self.sql.push_str(&identifier.replace('"', "\"\""));
        self.sql.push('"');
        Ok(())
    }

    fn push_bind_param(&mut self) {
        self.bind_idx += 1;
        self.sql.push('$');
        self.sql.push_str(&self.bind_idx.to_string());
    }

    fn finish(self) -> String {
        self.sql
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_push_sql() {
        let mut qb = PyroSqlQueryBuilder::new();
        qb.push_sql("SELECT ");
        qb.push_sql("* FROM users");
        assert_eq!(qb.finish(), "SELECT * FROM users");
    }

    #[test]
    fn test_push_identifier() {
        let mut qb = PyroSqlQueryBuilder::new();
        qb.push_sql("SELECT ");
        qb.push_identifier("user name").unwrap();
        qb.push_sql(" FROM ");
        qb.push_identifier("my_table").unwrap();
        assert_eq!(qb.finish(), "SELECT \"user name\" FROM \"my_table\"");
    }

    #[test]
    fn test_push_identifier_escaping() {
        let mut qb = PyroSqlQueryBuilder::new();
        qb.push_identifier("col\"with\"quotes").unwrap();
        assert_eq!(qb.finish(), "\"col\"\"with\"\"quotes\"");
    }

    #[test]
    fn test_push_bind_param() {
        let mut qb = PyroSqlQueryBuilder::new();
        qb.push_sql("SELECT * FROM users WHERE id = ");
        qb.push_bind_param();
        qb.push_sql(" AND name = ");
        qb.push_bind_param();
        assert_eq!(
            qb.finish(),
            "SELECT * FROM users WHERE id = $1 AND name = $2"
        );
    }

    #[test]
    fn test_empty_builder() {
        let qb = PyroSqlQueryBuilder::new();
        assert_eq!(qb.finish(), "");
    }

    #[test]
    fn test_complex_query() {
        let mut qb = PyroSqlQueryBuilder::new();
        qb.push_sql("INSERT INTO ");
        qb.push_identifier("orders").unwrap();
        qb.push_sql(" (");
        qb.push_identifier("product_id").unwrap();
        qb.push_sql(", ");
        qb.push_identifier("quantity").unwrap();
        qb.push_sql(") VALUES (");
        qb.push_bind_param();
        qb.push_sql(", ");
        qb.push_bind_param();
        qb.push_sql(")");
        assert_eq!(
            qb.finish(),
            "INSERT INTO \"orders\" (\"product_id\", \"quantity\") VALUES ($1, $2)"
        );
    }
}
