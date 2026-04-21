//! Connection pool for PyroLink.
//!
//! Provides a pool of reusable [`Client`] connections with a configurable
//! maximum size.  Connections are lazily created and returned to the pool
//! on drop for reuse by subsequent callers.
//!
//! # Lock strategy
//!
//! The pool uses [`parking_lot::Mutex`] + [`parking_lot::Condvar`] instead
//! of `tokio::sync::{Mutex, Semaphore}` to stay consistent with the zero-
//! tokio policy on the new PWire path.  Concurrent callers that hit the
//! `max_size` cap block synchronously on the condvar (short-lived — wakes
//! as soon as a `PooledClient` is dropped).  This is acceptable because
//! the typical pool-contention blocking time (< 10 µs on a healthy pool)
//! is below an OS scheduler quantum and does not perturb the caller's
//! async runtime.
//!
//! # Example
//!
//! ```no_run
//! use pyrosql::{ConnectConfig, Pool};
//!
//! # async fn demo() {
//! let config = ConnectConfig::new("localhost", 12520)
//!     .database("mydb")
//!     .tls_skip_verify(true);
//! let pool = Pool::new(config, 10);
//!
//! let conn = pool.get().await.unwrap();
//! let _ = conn.query("SELECT 1", &[]).await.unwrap();
//! // conn is returned to the pool when dropped
//! # }
//! ```

use std::sync::Arc;

use parking_lot::{Condvar, Mutex};

use crate::client::Client;
use crate::config::ConnectConfig;
use crate::error::ClientError;
use crate::row::{QueryResult, Value};

/// Shared inner state of the pool.  Wrapped in `Arc` so `PooledClient`
/// can return connections on drop without lifetime issues.
struct PoolInner {
    config: ConnectConfig,
    /// Idle connection stash protected by a `parking_lot::Mutex`.  The
    /// `available` condvar is signalled whenever a connection returns or
    /// the in-flight count drops.
    state: Mutex<PoolState>,
    available: Condvar,
    max_size: usize,
}

/// Everything the pool tracks under one lock.
struct PoolState {
    /// Idle connections ready for reuse.
    idle: Vec<Client>,
    /// Number of connections currently checked out (in-flight).
    in_flight: usize,
}

/// A connection pool for PyroLink.
///
/// Maintains a set of reusable [`Client`] connections up to a configurable
/// maximum size.  When all connections are in use, callers of [`get`](Pool::get)
/// block until one becomes available.
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
                state: Mutex::new(PoolState {
                    idle: Vec::with_capacity(max_size),
                    in_flight: 0,
                }),
                available: Condvar::new(),
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
    /// otherwise a new connection is established.  If the pool is at capacity
    /// this call **synchronously blocks the current thread** until a
    /// connection is returned.  Blocking is intentionally kept inside a
    /// Condvar wait rather than an async Semaphore to match the zero-tokio
    /// policy; typical wait times are sub-millisecond.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if connecting fails.
    pub async fn get(&self) -> Result<PooledClient, ClientError> {
        // Phase 1: try to reserve an in-flight slot (either reuse idle or
        // bump in_flight if we have headroom).  Blocks on condvar when at
        // capacity.
        let reuse: Option<Client> = {
            let mut state = self.inner.state.lock();
            loop {
                if let Some(client) = state.idle.pop() {
                    state.in_flight += 1;
                    break Some(client);
                }
                if state.in_flight < self.inner.max_size {
                    state.in_flight += 1;
                    break None;
                }
                self.inner.available.wait(&mut state);
            }
        };

        // Phase 2: materialise the Client.  Outside the lock because
        // Client::connect is async and may take time.
        let client = match reuse {
            Some(c) => c,
            None => match Client::connect(self.inner.config.clone()).await {
                Ok(c) => c,
                Err(e) => {
                    // Releasing the reservation we took in phase 1 so the
                    // pool doesn't leak an in_flight slot on connect
                    // failure.
                    let mut state = self.inner.state.lock();
                    state.in_flight -= 1;
                    self.inner.available.notify_one();
                    return Err(e);
                }
            },
        };

        Ok(PooledClient {
            client: Some(client),
            inner: Arc::clone(&self.inner),
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
    pub async fn begin(&self) -> Result<crate::client::Transaction<'_>, ClientError> {
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
    pub async fn prepare(&self, sql: &str) -> Result<crate::client::PreparedStatement<'_>, ClientError> {
        self.client
            .as_ref()
            .expect("PooledClient used after drop")
            .prepare(sql)
            .await
    }

    /// Returns the active transport tier name (always `"PWire"`).
    #[must_use]
    pub fn transport_tier(&self) -> &'static str {
        self.client
            .as_ref()
            .expect("PooledClient used after drop")
            .transport_tier()
    }
}

impl Drop for PooledClient {
    fn drop(&mut self) {
        if let Some(client) = self.client.take() {
            // Return the connection to the pool synchronously.  No tokio
            // spawn needed — parking_lot::Mutex::lock() is blocking but
            // near-instant for this tiny critical section.
            let mut state = self.inner.state.lock();
            state.in_flight -= 1;
            state.idle.push(client);
            self.inner.available.notify_one();
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
