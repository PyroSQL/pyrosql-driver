//! Connection pool for PyroLink.
//!
//! Provides a pool of reusable [`Client`] connections with a configurable
//! maximum size.  Connections are lazily created and returned to the pool
//! on drop for reuse by subsequent callers.
//!
//! # Example
//!
//! ```no_run
//! use pyrosql::{ConnectConfig, Pool};
//!
//! #[tokio::main]
//! async fn main() {
//!     let config = ConnectConfig::new("localhost", 12520)
//!         .database("mydb")
//!         .tls_skip_verify(true);
//!     let pool = Pool::new(config, 10);
//!
//!     let conn = pool.get().await.unwrap();
//!     let result = conn.query("SELECT 1", &[]).await.unwrap();
//!     // conn is returned to the pool when dropped
//! }
//! ```

use std::sync::Arc;

use tokio::sync::{Mutex, Semaphore};

use crate::client::Client;
use crate::config::ConnectConfig;
use crate::error::ClientError;
use crate::row::{QueryResult, Value};

/// Shared inner state of the pool, wrapped in `Arc` so that `PooledClient`
/// can return connections on drop without lifetime issues.
struct PoolInner {
    config: ConnectConfig,
    connections: Mutex<Vec<Client>>,
    semaphore: Arc<Semaphore>,
    max_size: usize,
}

/// A connection pool for PyroLink.
///
/// Maintains a set of reusable [`Client`] connections up to a configurable
/// maximum size.  When all connections are in use, callers of [`get`](Pool::get)
/// will wait until one becomes available.
pub struct Pool {
    inner: Arc<PoolInner>,
}

impl Pool {
    /// Create a new connection pool.
    ///
    /// `max_size` controls the maximum number of concurrent connections.
    /// Connections are created lazily on first use.
    pub fn new(config: ConnectConfig, max_size: usize) -> Self {
        Self {
            inner: Arc::new(PoolInner {
                config,
                connections: Mutex::new(Vec::with_capacity(max_size)),
                semaphore: Arc::new(Semaphore::new(max_size)),
                max_size,
            }),
        }
    }

    /// The maximum number of connections this pool will maintain.
    #[must_use]
    pub fn max_size(&self) -> usize {
        self.inner.max_size
    }

    /// Get a connection from the pool (or create a new one).
    ///
    /// If a previously returned connection is available it will be reused;
    /// otherwise a new connection is established.  If the pool is at capacity,
    /// this method will wait until a connection is returned.
    ///
    /// The returned [`PooledClient`] implements query/execute methods and
    /// returns itself to the pool when dropped.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the pool is closed or connecting fails.
    pub async fn get(&self) -> Result<PooledClient, ClientError> {
        let permit = self
            .inner
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| ClientError::Connection("pool closed".into()))?;

        // Try to reuse an existing idle connection
        {
            let mut conns = self.inner.connections.lock().await;
            if let Some(client) = conns.pop() {
                return Ok(PooledClient {
                    client: Some(client),
                    inner: Arc::clone(&self.inner),
                    _permit: permit,
                });
            }
        }

        // Create a new connection
        let client = Client::connect(self.inner.config.clone()).await?;
        Ok(PooledClient {
            client: Some(client),
            inner: Arc::clone(&self.inner),
            _permit: permit,
        })
    }
}

/// A connection borrowed from a [`Pool`].
///
/// Delegates query and execute operations to the underlying [`Client`].
/// When dropped, the connection is returned to the pool automatically.
pub struct PooledClient {
    client: Option<Client>,
    inner: Arc<PoolInner>,
    /// Holds the semaphore permit — released on drop.
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl PooledClient {
    /// Execute a query and return the result rows.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] on stream, protocol, or server errors.
    pub async fn query(&self, sql: &str, params: &[Value]) -> Result<QueryResult, ClientError> {
        self.client
            .as_ref()
            .expect("PooledClient used after drop")
            .query(sql, params)
            .await
    }

    /// Execute a DML statement and return affected rows.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] on stream, protocol, or server errors.
    pub async fn execute(&self, sql: &str, params: &[Value]) -> Result<u64, ClientError> {
        self.client
            .as_ref()
            .expect("PooledClient used after drop")
            .execute(sql, params)
            .await
    }

    /// Begin a transaction on this pooled connection.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the server rejects the begin request.
    pub async fn begin(
        &self,
    ) -> Result<crate::client::Transaction<'_>, ClientError> {
        self.client
            .as_ref()
            .expect("PooledClient used after drop")
            .begin()
            .await
    }

    /// Prepare a statement on this pooled connection.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the server rejects the prepare request.
    pub async fn prepare(
        &self,
        sql: &str,
    ) -> Result<crate::client::PreparedStatement<'_>, ClientError> {
        self.client
            .as_ref()
            .expect("PooledClient used after drop")
            .prepare(sql)
            .await
    }

    /// Bulk insert rows into a table.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] on stream, protocol, or server errors.
    pub async fn bulk_insert(
        &self,
        table: &str,
        columns: &[&str],
        rows: &[Vec<Value>],
    ) -> Result<u64, ClientError> {
        self.client
            .as_ref()
            .expect("PooledClient used after drop")
            .bulk_insert(table, columns, rows)
            .await
    }

    /// Returns the active transport tier name.
    #[must_use]
    pub fn transport_tier(&self) -> &str {
        self.client
            .as_ref()
            .expect("PooledClient used after drop")
            .transport_tier()
    }
}

impl Drop for PooledClient {
    fn drop(&mut self) {
        if let Some(client) = self.client.take() {
            // We cannot await in Drop, so spawn a task to return the
            // connection to the pool.  The semaphore permit is released
            // by the `_permit` field's own Drop impl.
            let inner = Arc::clone(&self.inner);
            tokio::spawn(async move {
                let mut conns = inner.connections.lock().await;
                conns.push(client);
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pool_creation() {
        let config = ConnectConfig::new("localhost", 12520);
        let pool = Pool::new(config, 5);
        assert_eq!(pool.max_size(), 5);
    }
}
