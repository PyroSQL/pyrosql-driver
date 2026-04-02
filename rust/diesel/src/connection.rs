//! PyroSQL connection implementation for Diesel.

use crate::pwire;
use crate::query_builder::PyroSqlQueryBuilder;
use crate::{PyroSqlBackend, PyroSqlBindCollector, PyroSqlTypeMetadata, PyroSqlValue};
use diesel::connection::{
    AnsiTransactionManager, ConnectionSealed, Instrumentation, SimpleConnection,
    TransactionManager,
};
use diesel::expression::QueryMetadata;
use diesel::query_builder::{AsQuery, QueryFragment, QueryId};
use diesel::result::{ConnectionError, ConnectionResult, QueryResult};
use diesel::{Connection, QueryableByName};
use std::io::{Read, Write};
use std::net::TcpStream;

/// A connection to a PyroSQL database via the PWire binary protocol.
///
/// This implements Diesel's [`Connection`] trait, providing full query
/// execution, prepared statement, and transaction support.
pub struct PyroSqlConnection {
    stream: TcpStream,
    transaction_manager: AnsiTransactionManager,
    instrumentation: Box<dyn Instrumentation>,
}

impl ConnectionSealed for PyroSqlConnection {}

impl SimpleConnection for PyroSqlConnection {
    fn batch_execute(&mut self, query: &str) -> QueryResult<()> {
        for statement in query.split(';') {
            let trimmed = statement.trim();
            if trimmed.is_empty() {
                continue;
            }
            self.raw_execute(trimmed)?;
        }
        Ok(())
    }
}

impl Connection for PyroSqlConnection {
    type Backend = PyroSqlBackend;
    type TransactionManager = AnsiTransactionManager;

    fn establish(database_url: &str) -> ConnectionResult<Self> {
        // Parse URL: pyrosql://user:pass@host:port/database
        let url = database_url
            .strip_prefix("pyrosql://")
            .ok_or_else(|| ConnectionError::InvalidConnectionUrl(database_url.to_string()))?;

        let (auth_part, host_part) = url
            .split_once('@')
            .ok_or_else(|| ConnectionError::InvalidConnectionUrl(database_url.to_string()))?;

        let (user, password) = auth_part
            .split_once(':')
            .unwrap_or((auth_part, ""));

        // host_part is "host:port/database" or "host:port"
        let host_and_rest = host_part.split_once('/').map(|(h, _db)| h).unwrap_or(host_part);

        let (host, port_str) = host_and_rest
            .split_once(':')
            .unwrap_or((host_and_rest, "12520"));

        let port: u16 = port_str.parse().map_err(|_| {
            ConnectionError::InvalidConnectionUrl(database_url.to_string())
        })?;

        let mut stream = TcpStream::connect((host, port)).map_err(|e| {
            ConnectionError::BadConnection(format!("Failed to connect to {}:{}: {}", host, port, e))
        })?;
        stream.set_nodelay(true).ok();

        // Authenticate
        let auth_frame = pwire::encode_auth(user, password);
        stream.write_all(&auth_frame).map_err(|e| {
            ConnectionError::BadConnection(format!("Failed to send auth: {}", e))
        })?;
        stream.flush().map_err(|e| {
            ConnectionError::BadConnection(format!("Failed to flush: {}", e))
        })?;

        let (resp_type, payload) = pwire::read_frame(&mut stream).map_err(|e| {
            ConnectionError::BadConnection(format!("Failed to read auth response: {}", e))
        })?;

        if resp_type == pwire::RESP_ERROR {
            let err = pwire::decode_error(&payload)
                .unwrap_or_else(|_| pwire::ErrorResponse {
                    sql_state: "?????".into(),
                    message: "Unknown error".into(),
                });
            return Err(ConnectionError::BadConnection(format!(
                "Auth failed [{}]: {}",
                err.sql_state, err.message
            )));
        }

        if resp_type != pwire::RESP_READY {
            return Err(ConnectionError::BadConnection(format!(
                "Expected READY after auth, got 0x{:02X}",
                resp_type
            )));
        }

        Ok(PyroSqlConnection {
            stream,
            transaction_manager: AnsiTransactionManager::default(),
            instrumentation: Box::new(()),
        })
    }

    fn execute_returning_count<T>(&mut self, source: &T) -> QueryResult<usize>
    where
        T: QueryFragment<Self::Backend> + QueryId,
    {
        let sql = build_sql(source)?;
        let (resp_type, payload) = self.raw_execute(&sql)?;

        match resp_type {
            pwire::RESP_OK => {
                let ok = pwire::decode_ok(&payload)
                    .map_err(|e| diesel::result::Error::DeserializationError(e.into()))?;
                Ok(ok.rows_affected as usize)
            }
            pwire::RESP_RESULT_SET => Ok(0),
            pwire::RESP_ERROR => {
                let err = pwire::decode_error(&payload)
                    .map_err(|e| diesel::result::Error::DeserializationError(e.into()))?;
                Err(diesel::result::Error::DatabaseError(
                    diesel::result::DatabaseErrorKind::Unknown,
                    Box::new(PyroSqlDbError(err.sql_state, err.message)),
                ))
            }
            _ => Err(diesel::result::Error::DeserializationError(
                format!("Unexpected response type 0x{:02X}", resp_type).into(),
            )),
        }
    }

    fn transaction_state(
        &mut self,
    ) -> &mut <Self::TransactionManager as TransactionManager<Self>>::TransactionStateData {
        self.transaction_manager.transaction_state_mut()
    }

    fn instrumentation(&mut self) -> &mut dyn Instrumentation {
        &mut *self.instrumentation
    }

    fn set_instrumentation(&mut self, instrumentation: impl Instrumentation) {
        self.instrumentation = Box::new(instrumentation);
    }
}

impl PyroSqlConnection {
    /// Execute a raw SQL query and return the response type and payload.
    fn raw_execute(&mut self, sql: &str) -> QueryResult<(u8, Vec<u8>)> {
        let frame = pwire::encode_query(sql);
        self.stream.write_all(&frame).map_err(|e| {
            diesel::result::Error::DatabaseError(
                diesel::result::DatabaseErrorKind::Unknown,
                Box::new(PyroSqlDbError("08006".into(), format!("Write failed: {}", e))),
            )
        })?;
        self.stream.flush().map_err(|e| {
            diesel::result::Error::DatabaseError(
                diesel::result::DatabaseErrorKind::Unknown,
                Box::new(PyroSqlDbError("08006".into(), format!("Flush failed: {}", e))),
            )
        })?;

        pwire::read_frame(&mut self.stream).map_err(|e| {
            diesel::result::Error::DatabaseError(
                diesel::result::DatabaseErrorKind::Unknown,
                Box::new(PyroSqlDbError("08006".into(), format!("Read failed: {}", e))),
            )
        })
    }

    /// Execute a query and load rows, suitable for `SELECT` statements.
    pub fn load_rows(&mut self, sql: &str) -> QueryResult<pwire::ResultSet> {
        let (resp_type, payload) = self.raw_execute(sql)?;

        match resp_type {
            pwire::RESP_RESULT_SET => {
                pwire::decode_result_set(&payload)
                    .map_err(|e| diesel::result::Error::DeserializationError(e.into()))
            }
            pwire::RESP_ERROR => {
                let err = pwire::decode_error(&payload)
                    .map_err(|e| diesel::result::Error::DeserializationError(e.into()))?;
                Err(diesel::result::Error::DatabaseError(
                    diesel::result::DatabaseErrorKind::Unknown,
                    Box::new(PyroSqlDbError(err.sql_state, err.message)),
                ))
            }
            _ => Err(diesel::result::Error::DeserializationError(
                format!("Expected RESULT_SET, got 0x{:02X}", resp_type).into(),
            )),
        }
    }
}

/// Builds SQL text from a Diesel query fragment.
fn build_sql<T: QueryFragment<PyroSqlBackend>>(source: &T) -> QueryResult<String> {
    let mut qb = PyroSqlQueryBuilder::new();
    source.walk_ast(diesel::query_builder::AstPass::to_sql(
        &mut qb,
        &mut (),
        &PyroSqlBackend,
    ))?;
    Ok(qb.finish())
}

/// Database error wrapper for Diesel's error trait.
#[derive(Debug)]
struct PyroSqlDbError(String, String);

impl diesel::result::DatabaseErrorInformation for PyroSqlDbError {
    fn message(&self) -> &str {
        &self.1
    }

    fn details(&self) -> Option<&str> {
        None
    }

    fn hint(&self) -> Option<&str> {
        None
    }

    fn table_name(&self) -> Option<&str> {
        None
    }

    fn column_name(&self) -> Option<&str> {
        None
    }

    fn constraint_name(&self) -> Option<&str> {
        None
    }

    fn statement_position(&self) -> Option<i32> {
        None
    }
}
