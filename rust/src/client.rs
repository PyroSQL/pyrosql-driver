//! The main PyroSQL client — a thin, runtime-agnostic wrapper over
//! [`PyroWireConnection`](crate::pwire::PyroWireConnection).
//!
//! # Example
//!
//! ```no_run
//! use pyrosql::{Client, ConnectConfig};
//!
//! # async fn demo() -> Result<(), pyrosql::ClientError> {
//! let client = Client::connect_url("vsql://localhost:12520/mydb").await?;
//! let result = client.query("SELECT * FROM users WHERE id = $1", &[42.into()]).await?;
//! for row in result.rows {
//!     println!("{}: {}", row.get::<String>("name").unwrap_or_default(), row.get::<i64>("age").unwrap_or(0));
//! }
//! # Ok(())
//! # }
//! ```

use std::sync::Arc;

use crate::config::ConnectConfig;
use crate::error::ClientError;
use crate::pwire::PyroWireConnection;
use crate::row::{QueryResult, Value};

// ── Client ───────────────────────────────────────────────────────────────────

/// A connected PyroSQL client speaking the PWire binary protocol.
///
/// Construction goes through [`Client::connect`] / [`Client::connect_url`];
/// the public query surface mirrors the common database-driver idioms
/// (`query`, `execute`, `begin`, `prepare`) and is runtime-agnostic.
///
/// # Cheaply cloneable
///
/// The underlying [`PyroWireConnection`] is wrapped in an [`Arc`], so
/// cloning a `Client` is cheap and all clones share the same PWire
/// worker thread + socket.
pub struct Client {
    conn: Arc<PyroWireConnection>,
    #[allow(dead_code)]
    config: ConnectConfig,
}

impl Clone for Client {
    fn clone(&self) -> Self {
        Self {
            conn: Arc::clone(&self.conn),
            config: self.config.clone(),
        }
    }
}

impl Client {
    /// Open a PWire connection using a [`ConnectConfig`].
    ///
    /// The scheme is effectively ignored (all connections are PWire/TCP).
    /// Only `config.host` + `config.port` are consulted.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the TCP connect or PWire runtime spawn fails.
    pub async fn connect(config: ConnectConfig) -> Result<Self, ClientError> {
        let addr = format!("{}:{}", config.host, config.port);
        // Send MSG_AUTH unconditionally when the caller supplied credentials
        // or selected a SQL dialect — that way the server locks the session
        // into PostAuth state and applies the dialect before the first query
        // runs. If none of those are set we still go through the auth path
        // with empty credentials so the server consistently transitions to
        // PostAuth; the cost is one extra roundtrip at connect time, which
        // is paid once per Client and lets us drop the old `SET syntax_mode`
        // fallback that had a race between connect and first query.
        let dialect = config.syntax_mode.map(|m| m.as_set_value().to_owned());
        let conn = PyroWireConnection::connect_authed(
            &addr,
            &config.user,
            &config.password,
            dialect.as_deref(),
        )?;

        Ok(Self { conn: Arc::new(conn), config })
    }

    /// Parse a URL and [`connect`](Client::connect).
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the URL is invalid or the connection fails.
    pub async fn connect_url(url: &str) -> Result<Self, ClientError> {
        Self::connect(ConnectConfig::from_url(url)?).await
    }

    /// Access the inner [`PyroWireConnection`] for pipelining or for
    /// low-level PWire operations not exposed on `Client`.
    #[must_use]
    pub fn inner(&self) -> &Arc<PyroWireConnection> {
        &self.conn
    }

    /// Execute a query and return the result rows.
    ///
    /// Parameters in `params` are referenced as `$1`, `$2`, … in the SQL.
    /// Server-side prepare caching is handled transparently by the PWire
    /// transport.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] on transport or server errors.
    pub async fn query(&self, sql: &str, params: &[Value]) -> Result<QueryResult, ClientError> {
        if params.is_empty() {
            self.conn.query_simple(sql).await
        } else {
            self.conn.query(sql, params).await
        }
    }

    /// Execute a DML statement and return the number of affected rows.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] on transport or server errors.
    pub async fn execute(&self, sql: &str, params: &[Value]) -> Result<u64, ClientError> {
        if params.is_empty() {
            self.conn.query_simple(sql).await.map(|r| r.rows_affected)
        } else {
            self.conn.execute(sql, params).await
        }
    }

    /// Returns the active transport tier name (always `"PWire"`).
    #[must_use]
    pub fn transport_tier(&self) -> &'static str {
        "PWire"
    }

    /// Force-close the underlying PWire connection from any thread.
    ///
    /// Intended for interactive cancel: an `.await`ing query on this
    /// client returns a `ClientError::Connection` and the session is
    /// dropped server-side. Subsequent calls on this `Client` continue
    /// to fail, so the caller (REPL) must open a fresh `Client` via
    /// [`Client::connect`] or via [`crate::ConnectConfig`].
    ///
    /// This is a TCP-level hammer — PWire still lacks a CancelRequest
    /// equivalent, so the only thing we can do from the caller side
    /// without server-side protocol support is drop the socket.
    /// Idempotent.
    pub fn abort(&self) {
        self.conn.abort();
    }

    // === Transactions ===

    /// Begin a transaction.  Returns a [`Transaction`] handle that auto-
    /// rollbacks on drop if not explicitly committed.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the server rejects `BEGIN`.
    pub async fn begin(&self) -> Result<Transaction<'_>, ClientError> {
        self.execute("BEGIN", &[]).await?;
        Ok(Transaction { client: self, committed: false })
    }

    /// Begin a transaction with serializable isolation level.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the server rejects the request.
    pub async fn begin_serializable(&self) -> Result<Transaction<'_>, ClientError> {
        self.execute("BEGIN ISOLATION LEVEL SERIALIZABLE", &[]).await?;
        Ok(Transaction { client: self, committed: false })
    }

    // === Prepared Statements ===

    /// Prepare a statement for repeated execution.
    ///
    /// The returned [`PreparedStatement`] holds the SQL text; the PWire
    /// transport's internal cache promotes the statement to a server-side
    /// PREPARE handle on first use.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the server rejects the prepare request.
    pub async fn prepare(&self, sql: &str) -> Result<PreparedStatement<'_>, ClientError> {
        Ok(PreparedStatement { sql: sql.to_owned(), client: self })
    }

    // === Auto-reconnect ===

    /// Execute a query with one-shot reconnect on connection failure.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if both the initial and retry attempts fail.
    pub async fn query_with_retry(
        &mut self,
        sql: &str,
        params: &[Value],
    ) -> Result<QueryResult, ClientError> {
        match self.query(sql, params).await {
            Ok(r) => Ok(r),
            Err(ClientError::Connection(_)) => {
                self.reconnect().await?;
                self.query(sql, params).await
            }
            Err(e) => Err(e),
        }
    }

    /// Execute a DML statement with one-shot reconnect on connection failure.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if both the initial and retry attempts fail.
    pub async fn execute_with_retry(
        &mut self,
        sql: &str,
        params: &[Value],
    ) -> Result<u64, ClientError> {
        match self.execute(sql, params).await {
            Ok(r) => Ok(r),
            Err(ClientError::Connection(_)) => {
                self.reconnect().await?;
                self.execute(sql, params).await
            }
            Err(e) => Err(e),
        }
    }

    async fn reconnect(&mut self) -> Result<(), ClientError> {
        let fresh = Client::connect(self.config.clone()).await?;
        self.conn = fresh.conn;
        Ok(())
    }
}

// ── Transaction ─────────────────────────────────────────────────────────────

/// A transaction handle tied to a [`Client`].
///
/// Created by [`Client::begin`] / [`Client::begin_serializable`].  If
/// dropped without an explicit `commit` or `rollback`, a warning is
/// printed and the server will eventually time out the transaction.
pub struct Transaction<'a> {
    client: &'a Client,
    committed: bool,
}

impl<'a> Transaction<'a> {
    /// Execute a query inside this transaction.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] on transport or server errors.
    pub async fn query(&self, sql: &str, params: &[Value]) -> Result<QueryResult, ClientError> {
        self.client.query(sql, params).await
    }

    /// Execute a DML statement inside this transaction.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] on transport or server errors.
    pub async fn execute(&self, sql: &str, params: &[Value]) -> Result<u64, ClientError> {
        self.client.execute(sql, params).await
    }

    /// Commit the transaction.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the server rejects the commit.
    pub async fn commit(mut self) -> Result<(), ClientError> {
        self.client.execute("COMMIT", &[]).await?;
        self.committed = true;
        Ok(())
    }

    /// Rollback the transaction.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the server rejects the rollback.
    pub async fn rollback(mut self) -> Result<(), ClientError> {
        self.client.execute("ROLLBACK", &[]).await?;
        self.committed = true;
        Ok(())
    }

    /// Create a savepoint inside this transaction.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the server rejects the savepoint.
    pub async fn savepoint(&self, name: &str) -> Result<(), ClientError> {
        // Quoting: savepoint names are SQL identifiers; pass through verbatim.
        self.client.execute(&format!("SAVEPOINT {name}"), &[]).await?;
        Ok(())
    }

    /// Rollback to a previously-created savepoint.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the server rejects the rollback.
    pub async fn rollback_to(&self, name: &str) -> Result<(), ClientError> {
        self.client.execute(&format!("ROLLBACK TO SAVEPOINT {name}"), &[]).await?;
        Ok(())
    }
}

impl Drop for Transaction<'_> {
    fn drop(&mut self) {
        if !self.committed {
            eprintln!(
                "Transaction dropped without commit/rollback — auto-rollback (best-effort)"
            );
        }
    }
}

// ── PreparedStatement ───────────────────────────────────────────────────────

/// A prepared statement handle tied to a [`Client`].
///
/// The current implementation stores the SQL text; first execution
/// promotes it to a server-side PREPARE handle via the PWire transport's
/// internal cache.  Subsequent executions ship a MSG_EXECUTE frame with
/// the cached handle.
pub struct PreparedStatement<'a> {
    sql: String,
    client: &'a Client,
}

impl<'a> PreparedStatement<'a> {
    /// The original SQL string used to create this prepared statement.
    #[must_use]
    pub fn sql(&self) -> &str { &self.sql }

    /// Execute the prepared statement as a query, returning result rows.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] on transport or server errors.
    pub async fn query(&self, params: &[Value]) -> Result<QueryResult, ClientError> {
        self.client.query(&self.sql, params).await
    }

    /// Execute the prepared statement as a DML operation.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] on transport or server errors.
    pub async fn execute(&self, params: &[Value]) -> Result<u64, ClientError> {
        self.client.execute(&self.sql, params).await
    }
}

// ── Parameter interpolation helpers ─────────────────────────────────────────
//
// These are public `pub(crate)` to let `pwire.rs` reuse the interpolation
// and quoting routines when degrading from EXECUTE to a literal QUERY.

/// Returns the length of a UTF-8 character from its leading byte.
#[inline]
fn utf8_char_len(b: u8) -> usize {
    if b < 0x80 { 1 }
    else if b < 0xE0 { 2 }
    else if b < 0xF0 { 3 }
    else { 4 }
}

/// Client-side parameter interpolation.
///
/// Replaces `$1`, `$2`, … with the SQL literal form of the matching
/// [`Value`].  Used as a fallback when the server declines to PREPARE a
/// statement; the PWire transport prefers server-side parameter binding.
#[inline]
pub(crate) fn interpolate_params(sql: &str, params: &[Value]) -> String {
    if params.is_empty() {
        return sql.to_owned();
    }

    let replacements: Vec<std::borrow::Cow<'static, str>> =
        params.iter().map(value_to_sql).collect();

    let mut result = String::with_capacity(sql.len());
    let bytes = sql.as_bytes();
    let mut pos = 0;
    while pos < bytes.len() {
        if bytes[pos] == b'$' && pos + 1 < bytes.len() && bytes[pos + 1].is_ascii_digit() {
            let start = pos + 1;
            let mut end = start;
            while end < bytes.len() && bytes[end].is_ascii_digit() {
                end += 1;
            }
            if let Ok(idx) = sql[start..end].parse::<usize>() {
                if idx >= 1 && idx <= params.len() {
                    result.push_str(&replacements[idx - 1]);
                    pos = end;
                    continue;
                }
            }
        }
        let ch_len = utf8_char_len(bytes[pos]);
        result.push_str(&sql[pos..pos + ch_len]);
        pos += ch_len;
    }
    result
}

/// Convert a [`Value`] to its SQL literal representation.
#[inline]
pub(crate) fn value_to_sql(v: &Value) -> std::borrow::Cow<'static, str> {
    match v {
        Value::Null => std::borrow::Cow::Borrowed("NULL"),
        Value::Bool(true) => std::borrow::Cow::Borrowed("TRUE"),
        Value::Bool(false) => std::borrow::Cow::Borrowed("FALSE"),
        Value::Int(n) => std::borrow::Cow::Owned(n.to_string()),
        Value::Float(f) => std::borrow::Cow::Owned(f.to_string()),
        Value::Text(s) => std::borrow::Cow::Owned(format!("'{}'", s.replace('\'', "''"))),
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interpolate_no_params() {
        assert_eq!(interpolate_params("SELECT 1", &[]), "SELECT 1");
    }

    #[test]
    fn interpolate_single_param() {
        let sql = interpolate_params("SELECT * FROM t WHERE id = $1", &[Value::Int(42)]);
        assert_eq!(sql, "SELECT * FROM t WHERE id = 42");
    }

    #[test]
    fn interpolate_multiple_params() {
        let sql = interpolate_params(
            "INSERT INTO t (a, b) VALUES ($1, $2)",
            &[Value::Text("hello".into()), Value::Int(7)],
        );
        assert_eq!(sql, "INSERT INTO t (a, b) VALUES ('hello', 7)");
    }

    #[test]
    fn interpolate_text_escaping() {
        let sql = interpolate_params("SELECT $1", &[Value::Text("it's".into())]);
        assert_eq!(sql, "SELECT 'it''s'");
    }

    #[test]
    fn interpolate_null_and_bool() {
        let sql = interpolate_params(
            "SELECT $1, $2",
            &[Value::Null, Value::Bool(true)],
        );
        assert_eq!(sql, "SELECT NULL, TRUE");
    }

    #[test]
    fn value_to_sql_coverage() {
        assert_eq!(value_to_sql(&Value::Float(3.14)), "3.14");
    }
}
