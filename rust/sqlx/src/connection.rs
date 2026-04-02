//! PyroSQL connection for sqlx.

use crate::pwire;
use crate::{PyroSql, PyroSqlArguments, PyroSqlQueryResult};
use crate::row::{PyroSqlColumn, PyroSqlRow, PyroSqlValueOwned};
use crate::type_info::PyroSqlTypeInfo;
use futures_core::future::BoxFuture;
use futures_core::stream::BoxStream;
use log;
use sqlx_core::connection::Connection;
use sqlx_core::database::Database;
use sqlx_core::error::Error as SqlxError;
use sqlx_core::transaction::TransactionManager;
use std::fmt;
use tokio::io::{AsyncWriteExt, BufStream};
use tokio::net::TcpStream;

/// A connection to a PyroSQL database via the PWire binary protocol.
pub struct PyroSqlConnection {
    stream: BufStream<TcpStream>,
    is_closed: bool,
}

impl fmt::Debug for PyroSqlConnection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PyroSqlConnection")
            .field("closed", &self.is_closed)
            .finish()
    }
}

impl PyroSqlConnection {
    /// Connect to a PyroSQL server and authenticate.
    ///
    /// URL format: `pyrosql://user:password@host:port/database`
    pub async fn connect(url: &str) -> Result<Self, SqlxError> {
        let url_str = url
            .strip_prefix("pyrosql://")
            .ok_or_else(|| SqlxError::Configuration(
                format!("Invalid URL scheme, expected pyrosql://: {}", url).into()
            ))?;

        let (auth_part, host_part) = url_str
            .split_once('@')
            .ok_or_else(|| SqlxError::Configuration(
                format!("Missing @ in URL: {}", url).into()
            ))?;

        let (user, password) = auth_part.split_once(':').unwrap_or((auth_part, ""));

        let host_and_rest = host_part.split_once('/').map(|(h, _)| h).unwrap_or(host_part);
        let (host, port_str) = host_and_rest.split_once(':').unwrap_or((host_and_rest, "12520"));

        let port: u16 = port_str.parse().map_err(|_| {
            SqlxError::Configuration(format!("Invalid port: {}", port_str).into())
        })?;

        let tcp = TcpStream::connect((host, port)).await.map_err(|e| {
            SqlxError::Io(e)
        })?;
        tcp.set_nodelay(true).ok();

        let mut stream = BufStream::new(tcp);

        // Authenticate
        let auth_frame = pwire::encode_auth(user, password);
        stream.write_all(&auth_frame).await.map_err(SqlxError::Io)?;
        stream.flush().await.map_err(SqlxError::Io)?;

        let (resp_type, payload) = pwire::async_read_frame(&mut stream).await.map_err(SqlxError::Io)?;

        if resp_type == pwire::RESP_ERROR {
            let err = pwire::decode_error(&payload).unwrap_or_else(|_| pwire::ErrorResponse {
                sql_state: "?????".into(),
                message: "Unknown error".into(),
            });
            return Err(SqlxError::Protocol(format!(
                "Auth failed [{}]: {}",
                err.sql_state, err.message
            )));
        }

        if resp_type != pwire::RESP_READY {
            return Err(SqlxError::Protocol(format!(
                "Expected READY after auth, got 0x{:02X}",
                resp_type
            )));
        }

        Ok(PyroSqlConnection {
            stream,
            is_closed: false,
        })
    }

    /// Execute a simple query and return (response_type, payload).
    async fn raw_execute(&mut self, sql: &str) -> Result<(u8, Vec<u8>), SqlxError> {
        let frame = pwire::encode_query(sql);
        self.stream.write_all(&frame).await.map_err(SqlxError::Io)?;
        self.stream.flush().await.map_err(SqlxError::Io)?;
        let (resp_type, payload) = pwire::async_read_frame(&mut self.stream).await.map_err(SqlxError::Io)?;
        Ok((resp_type, payload))
    }

    /// Execute a prepared statement with parameters.
    async fn raw_execute_prepared(
        &mut self,
        sql: &str,
        params: &[String],
    ) -> Result<(u8, Vec<u8>), SqlxError> {
        // PREPARE
        let prep_frame = pwire::encode_prepare(sql);
        self.stream.write_all(&prep_frame).await.map_err(SqlxError::Io)?;
        self.stream.flush().await.map_err(SqlxError::Io)?;

        let (prep_type, prep_payload) = pwire::async_read_frame(&mut self.stream).await.map_err(SqlxError::Io)?;

        if prep_type == pwire::RESP_ERROR {
            let err = pwire::decode_error(&prep_payload).unwrap_or_else(|_| pwire::ErrorResponse {
                sql_state: "?????".into(),
                message: "Prepare failed".into(),
            });
            return Err(SqlxError::Protocol(format!("[{}]: {}", err.sql_state, err.message)));
        }

        if prep_type != pwire::RESP_OK {
            return Err(SqlxError::Protocol(format!(
                "Expected OK from PREPARE, got 0x{:02X}",
                prep_type
            )));
        }

        let ok = pwire::decode_ok(&prep_payload)
            .map_err(|e| SqlxError::Protocol(e))?;
        let handle = ok.rows_affected as u32;

        // EXECUTE
        let exec_frame = pwire::encode_execute(handle, params);
        self.stream.write_all(&exec_frame).await.map_err(SqlxError::Io)?;
        self.stream.flush().await.map_err(SqlxError::Io)?;

        let result = pwire::async_read_frame(&mut self.stream).await.map_err(SqlxError::Io)?;

        // CLOSE (best effort)
        let close_frame = pwire::encode_close(handle);
        let _ = self.stream.write_all(&close_frame).await;
        let _ = self.stream.flush().await;
        let _ = pwire::async_read_frame(&mut self.stream).await;

        Ok(result)
    }

    /// Execute a query and return the query result (rows affected).
    pub async fn execute_sql(&mut self, sql: &str) -> Result<PyroSqlQueryResult, SqlxError> {
        let (resp_type, payload) = self.raw_execute(sql).await?;
        match resp_type {
            pwire::RESP_OK => {
                let ok = pwire::decode_ok(&payload).map_err(|e| SqlxError::Protocol(e))?;
                Ok(PyroSqlQueryResult::new(ok.rows_affected as u64))
            }
            pwire::RESP_RESULT_SET => Ok(PyroSqlQueryResult::new(0)),
            pwire::RESP_ERROR => {
                let err = pwire::decode_error(&payload).map_err(|e| SqlxError::Protocol(e))?;
                Err(SqlxError::Protocol(format!("[{}]: {}", err.sql_state, err.message)))
            }
            _ => Err(SqlxError::Protocol(format!(
                "Unexpected response type 0x{:02X}",
                resp_type
            ))),
        }
    }

    /// Execute a query and return rows.
    pub async fn fetch_all_sql(&mut self, sql: &str) -> Result<Vec<PyroSqlRow>, SqlxError> {
        let (resp_type, payload) = self.raw_execute(sql).await?;
        match resp_type {
            pwire::RESP_RESULT_SET => {
                let rs = pwire::decode_result_set(&payload)
                    .map_err(|e| SqlxError::Protocol(e))?;
                let columns: Vec<PyroSqlColumn> = rs.columns.iter().enumerate().map(|(i, c)| {
                    PyroSqlColumn {
                        ordinal: i,
                        name: c.name.clone(),
                        type_info: PyroSqlTypeInfo::from_tag(c.type_tag),
                    }
                }).collect();
                let columns = std::sync::Arc::new(columns);

                let rows = rs.rows.into_iter().map(|values| {
                    let owned_values: Vec<PyroSqlValueOwned> = values.into_iter().map(|v| {
                        PyroSqlValueOwned { inner: v }
                    }).collect();
                    PyroSqlRow {
                        columns: columns.clone(),
                        values: owned_values,
                    }
                }).collect();

                Ok(rows)
            }
            pwire::RESP_OK => Ok(Vec::new()),
            pwire::RESP_ERROR => {
                let err = pwire::decode_error(&payload).map_err(|e| SqlxError::Protocol(e))?;
                Err(SqlxError::Protocol(format!("[{}]: {}", err.sql_state, err.message)))
            }
            _ => Err(SqlxError::Protocol(format!(
                "Unexpected response type 0x{:02X}",
                resp_type
            ))),
        }
    }

    /// Ping the server to check connection health.
    pub async fn ping_server(&mut self) -> Result<bool, SqlxError> {
        let frame = pwire::encode_ping();
        self.stream.write_all(&frame).await.map_err(SqlxError::Io)?;
        self.stream.flush().await.map_err(SqlxError::Io)?;
        let (resp_type, _) = pwire::async_read_frame(&mut self.stream).await.map_err(SqlxError::Io)?;
        Ok(resp_type == pwire::RESP_PONG)
    }

    /// Close the connection gracefully.
    pub async fn close_connection(&mut self) -> Result<(), SqlxError> {
        if !self.is_closed {
            let quit_frame = pwire::encode_quit();
            let _ = self.stream.write_all(&quit_frame).await;
            let _ = self.stream.flush().await;
            self.is_closed = true;
        }
        Ok(())
    }
}

impl Connection for PyroSqlConnection {
    type Database = PyroSql;
    type Options = PyroSqlConnectOptions;

    fn close(mut self) -> BoxFuture<'static, Result<(), SqlxError>> {
        Box::pin(async move {
            self.close_connection().await
        })
    }

    fn close_hard(mut self) -> BoxFuture<'static, Result<(), SqlxError>> {
        Box::pin(async move {
            self.is_closed = true;
            Ok(())
        })
    }

    fn ping(&mut self) -> BoxFuture<'_, Result<(), SqlxError>> {
        Box::pin(async move {
            let ok = self.ping_server().await?;
            if ok {
                Ok(())
            } else {
                Err(SqlxError::Protocol("Ping failed".into()))
            }
        })
    }

    fn begin(
        &mut self,
    ) -> BoxFuture<'_, Result<sqlx_core::transaction::Transaction<'_, Self::Database>, SqlxError>>
    where
        Self: Sized,
    {
        sqlx_core::transaction::Transaction::begin(self)
    }

    fn shrink_buffers(&mut self) {
        // No buffer pool to shrink
    }

    fn flush(&mut self) -> BoxFuture<'_, Result<(), SqlxError>> {
        Box::pin(async move {
            self.stream.flush().await.map_err(SqlxError::Io)
        })
    }

    fn should_flush(&self) -> bool {
        false
    }
}

/// Transaction manager for PyroSQL.
pub struct PyroSqlTransactionManager;

impl TransactionManager for PyroSqlTransactionManager {
    type Database = PyroSql;

    fn begin(conn: &mut PyroSqlConnection) -> BoxFuture<'_, Result<(), SqlxError>> {
        Box::pin(async move {
            conn.execute_sql("BEGIN").await?;
            Ok(())
        })
    }

    fn commit(conn: &mut PyroSqlConnection) -> BoxFuture<'_, Result<(), SqlxError>> {
        Box::pin(async move {
            conn.execute_sql("COMMIT").await?;
            Ok(())
        })
    }

    fn rollback(conn: &mut PyroSqlConnection) -> BoxFuture<'_, Result<(), SqlxError>> {
        Box::pin(async move {
            conn.execute_sql("ROLLBACK").await?;
            Ok(())
        })
    }

    fn start_rollback(conn: &mut PyroSqlConnection) {
        // Best-effort rollback; fire and forget
        let frame = pwire::encode_query("ROLLBACK");
        let _ = conn.stream.write_all(&frame);
    }
}

/// Connection options for PyroSQL.
#[derive(Debug, Clone)]
pub struct PyroSqlConnectOptions {
    /// Server hostname.
    pub host: String,
    /// Server port.
    pub port: u16,
    /// Username for authentication.
    pub username: String,
    /// Password for authentication.
    pub password: String,
    /// Database name.
    pub database: String,
}

impl Default for PyroSqlConnectOptions {
    fn default() -> Self {
        Self {
            host: "localhost".into(),
            port: 12520,
            username: String::new(),
            password: String::new(),
            database: String::new(),
        }
    }
}

impl PyroSqlConnectOptions {
    /// Create new connection options.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the hostname.
    pub fn host(mut self, host: &str) -> Self {
        self.host = host.into();
        self
    }

    /// Set the port.
    pub fn port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    /// Set the username.
    pub fn username(mut self, user: &str) -> Self {
        self.username = user.into();
        self
    }

    /// Set the password.
    pub fn password(mut self, pass: &str) -> Self {
        self.password = pass.into();
        self
    }

    /// Set the database name.
    pub fn database(mut self, db: &str) -> Self {
        self.database = db.into();
        self
    }

    /// Build a URL from these options.
    pub fn to_url(&self) -> String {
        format!(
            "pyrosql://{}:{}@{}:{}/{}",
            self.username, self.password, self.host, self.port, self.database
        )
    }
}

impl std::str::FromStr for PyroSqlConnectOptions {
    type Err = SqlxError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let url = s.strip_prefix("pyrosql://").ok_or_else(|| {
            SqlxError::Configuration(format!("Invalid scheme: {}", s).into())
        })?;

        let (auth_part, host_part) = url.split_once('@').ok_or_else(|| {
            SqlxError::Configuration("Missing @ in URL".to_string().into())
        })?;

        let (user, pass) = auth_part.split_once(':').unwrap_or((auth_part, ""));
        let (host_and_port, database) = host_part.split_once('/').unwrap_or((host_part, ""));
        let (host, port_str) = host_and_port.split_once(':').unwrap_or((host_and_port, "12520"));
        let port: u16 = port_str.parse().map_err(|_| {
            SqlxError::Configuration(format!("Invalid port: {}", port_str).into())
        })?;

        Ok(Self {
            host: host.into(),
            port,
            username: user.into(),
            password: pass.into(),
            database: database.into(),
        })
    }
}

impl sqlx_core::connection::ConnectOptions for PyroSqlConnectOptions {
    type Connection = PyroSqlConnection;

    fn from_url(url: &sqlx_core::url::Url) -> Result<Self, SqlxError> {
        let s = url.as_str();
        s.parse()
    }

    fn connect(
        &self,
    ) -> BoxFuture<'_, Result<Self::Connection, SqlxError>>
    where
        Self::Connection: Sized,
    {
        let url = self.to_url();
        Box::pin(async move { PyroSqlConnection::connect(&url).await })
    }

    fn log_statements(mut self, _level: log::LevelFilter) -> Self {
        self
    }

    fn log_slow_statements(mut self, _level: log::LevelFilter, _duration: std::time::Duration) -> Self {
        self
    }
}
