use crate::stats::PoolStats;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, Semaphore};
use tokio::time::timeout;
use tracing::{debug, info};

/// PWire protocol constants.
const HEADER_SIZE: usize = 5;
const MSG_PING: u8 = 0x05;
const RESP_PONG: u8 = 0x04;

/// A single upstream connection to PyroSQL.
pub struct UpstreamConn {
    pub stream: TcpStream,
}

impl UpstreamConn {
    /// Connect to the upstream PyroSQL server.
    pub async fn connect(addr: &str) -> std::io::Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        Ok(Self { stream })
    }

    /// Send a PING and expect a PONG. Returns true if healthy.
    pub async fn health_check(&mut self) -> bool {
        // Build PING frame: [0x05][0x00 0x00 0x00 0x00]
        let ping_frame: [u8; 5] = [MSG_PING, 0x00, 0x00, 0x00, 0x00];
        if let Err(e) = self.stream.write_all(&ping_frame).await {
            debug!("health check write failed: {}", e);
            return false;
        }
        if let Err(e) = self.stream.flush().await {
            debug!("health check flush failed: {}", e);
            return false;
        }

        // Read response header with a 3-second timeout.
        let mut hdr = [0u8; HEADER_SIZE];
        match timeout(Duration::from_secs(3), self.stream.read_exact(&mut hdr)).await {
            Ok(Ok(_)) => hdr[0] == RESP_PONG,
            Ok(Err(e)) => {
                debug!("health check read failed: {}", e);
                false
            }
            Err(_) => {
                debug!("health check timed out");
                false
            }
        }
    }
}

/// Connection pool managing persistent connections to the upstream PyroSQL.
pub struct ConnectionPool {
    upstream_addr: String,
    max_size: usize,
    idle_conns: Mutex<Vec<UpstreamConn>>,
    semaphore: Semaphore,
    max_wait: Duration,
    stats: Arc<PoolStats>,
}

impl ConnectionPool {
    /// Create a new connection pool. Does not pre-fill connections.
    pub fn new(
        upstream_addr: String,
        max_size: usize,
        max_wait: Duration,
        stats: Arc<PoolStats>,
    ) -> Arc<Self> {
        Arc::new(Self {
            upstream_addr,
            max_size,
            idle_conns: Mutex::new(Vec::with_capacity(max_size)),
            semaphore: Semaphore::new(max_size),
            max_wait,
            stats,
        })
    }

    /// Acquire a connection from the pool (or create a new one).
    /// Returns a permit guard and the connection. The permit must be held
    /// until the connection is returned or dropped.
    pub async fn acquire(self: &Arc<Self>) -> Result<PooledConnection, PoolError> {
        self.stats.waiting_clients.fetch_add(1, Ordering::Relaxed);

        let permit = match timeout(self.max_wait, self.semaphore.acquire()).await {
            Ok(Ok(permit)) => {
                self.stats.waiting_clients.fetch_sub(1, Ordering::Relaxed);
                permit
            }
            Ok(Err(_)) => {
                self.stats.waiting_clients.fetch_sub(1, Ordering::Relaxed);
                return Err(PoolError::SemaphoreClosed);
            }
            Err(_) => {
                self.stats.waiting_clients.fetch_sub(1, Ordering::Relaxed);
                return Err(PoolError::Timeout);
            }
        };

        // Try to take an idle connection.
        let conn = {
            let mut idle = self.idle_conns.lock().await;
            idle.pop()
        };

        let conn = match conn {
            Some(c) => {
                self.stats.idle_connections.fetch_sub(1, Ordering::Relaxed);
                c
            }
            None => {
                // Create a new upstream connection.
                match UpstreamConn::connect(&self.upstream_addr).await {
                    Ok(c) => {
                        self.stats
                            .connections_created
                            .fetch_add(1, Ordering::Relaxed);
                        self.stats
                            .total_connections
                            .fetch_add(1, Ordering::Relaxed);
                        c
                    }
                    Err(e) => {
                        // Release the permit since we failed to connect.
                        drop(permit);
                        return Err(PoolError::Connect(e));
                    }
                }
            }
        };

        self.stats.active_connections.fetch_add(1, Ordering::Relaxed);

        // Forget the permit -- we manage it manually via return/discard.
        permit.forget();

        Ok(PooledConnection {
            conn: Some(conn),
            pool: Arc::clone(self),
        })
    }

    /// Return a connection to the pool.
    async fn return_conn(&self, conn: UpstreamConn) {
        self.stats.active_connections.fetch_sub(1, Ordering::Relaxed);
        self.stats.idle_connections.fetch_add(1, Ordering::Relaxed);
        {
            let mut idle = self.idle_conns.lock().await;
            idle.push(conn);
        }
        self.semaphore.add_permits(1);
    }

    /// Discard a connection (do not return it to the pool).
    fn discard_conn(&self) {
        self.stats.active_connections.fetch_sub(1, Ordering::Relaxed);
        self.stats
            .total_connections
            .fetch_sub(1, Ordering::Relaxed);
        self.stats.connections_closed.fetch_add(1, Ordering::Relaxed);
        self.semaphore.add_permits(1);
    }

    /// Run health checks on all idle connections, removing dead ones.
    pub async fn health_check_idle(&self, stats: &Arc<PoolStats>) {
        let mut conns = {
            let mut idle = self.idle_conns.lock().await;
            std::mem::take(&mut *idle)
        };

        let count_before = conns.len();
        let mut healthy = Vec::with_capacity(conns.len());

        for mut conn in conns.drain(..) {
            if conn.health_check().await {
                stats.health_checks_ok.fetch_add(1, Ordering::Relaxed);
                healthy.push(conn);
            } else {
                stats
                    .health_checks_failed
                    .fetch_add(1, Ordering::Relaxed);
                stats
                    .total_connections
                    .fetch_sub(1, Ordering::Relaxed);
                stats.connections_closed.fetch_add(1, Ordering::Relaxed);
                stats.idle_connections.fetch_sub(1, Ordering::Relaxed);
                // Release a permit since we removed a dead connection.
                self.semaphore.add_permits(1);
            }
        }

        let removed = count_before - healthy.len();
        if removed > 0 {
            info!(
                "health check: removed {} dead connections, {} healthy remain",
                removed,
                healthy.len()
            );
        } else {
            debug!(
                "health check: all {} idle connections healthy",
                healthy.len()
            );
        }

        {
            let mut idle = self.idle_conns.lock().await;
            *idle = healthy;
        }
    }

    /// Drain all idle connections (for shutdown).
    pub async fn drain(&self) {
        let mut idle = self.idle_conns.lock().await;
        let count = idle.len();
        idle.clear();
        if count > 0 {
            info!("drained {} idle connections", count);
        }
    }

    pub fn stats(&self) -> &Arc<PoolStats> {
        &self.stats
    }

    pub fn max_size(&self) -> usize {
        self.max_size
    }
}

/// A connection checked out from the pool.
/// When dropped, the connection is discarded.
/// Call `release()` to return it to the pool, or `take()` to take ownership.
pub struct PooledConnection {
    conn: Option<UpstreamConn>,
    pool: Arc<ConnectionPool>,
}

impl PooledConnection {
    /// Get a mutable reference to the underlying stream.
    pub fn stream(&mut self) -> &mut TcpStream {
        &mut self.conn.as_mut().expect("connection taken").stream
    }

    /// Return this connection to the pool for reuse.
    pub async fn release(mut self) {
        if let Some(conn) = self.conn.take() {
            self.pool.return_conn(conn).await;
        }
    }

    /// Discard this connection (don't return to pool).
    pub fn discard(mut self) {
        self.conn.take();
        self.pool.discard_conn();
    }
}

impl Drop for PooledConnection {
    fn drop(&mut self) {
        if self.conn.is_some() {
            // Connection was not explicitly returned or discarded -- discard it.
            self.pool.discard_conn();
        }
    }
}

/// Errors from the connection pool.
#[derive(Debug)]
pub enum PoolError {
    /// Timed out waiting for a connection.
    Timeout,
    /// The semaphore was closed (pool shutting down).
    SemaphoreClosed,
    /// Failed to connect to upstream.
    Connect(std::io::Error),
}

impl std::fmt::Display for PoolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PoolError::Timeout => write!(f, "timed out waiting for pooled connection"),
            PoolError::SemaphoreClosed => write!(f, "connection pool is closed"),
            PoolError::Connect(e) => write!(f, "failed to connect to upstream: {}", e),
        }
    }
}

impl std::error::Error for PoolError {}
