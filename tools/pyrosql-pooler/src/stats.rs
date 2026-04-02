use serde::Serialize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Shared pool statistics, updated atomically.
#[derive(Debug)]
pub struct PoolStats {
    pub total_connections: AtomicU64,
    pub active_connections: AtomicU64,
    pub idle_connections: AtomicU64,
    pub waiting_clients: AtomicU64,
    pub total_queries: AtomicU64,
    pub total_transactions: AtomicU64,
    pub total_errors: AtomicU64,
    pub connections_created: AtomicU64,
    pub connections_closed: AtomicU64,
    pub health_checks_ok: AtomicU64,
    pub health_checks_failed: AtomicU64,
}

impl PoolStats {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            total_connections: AtomicU64::new(0),
            active_connections: AtomicU64::new(0),
            idle_connections: AtomicU64::new(0),
            waiting_clients: AtomicU64::new(0),
            total_queries: AtomicU64::new(0),
            total_transactions: AtomicU64::new(0),
            total_errors: AtomicU64::new(0),
            connections_created: AtomicU64::new(0),
            connections_closed: AtomicU64::new(0),
            health_checks_ok: AtomicU64::new(0),
            health_checks_failed: AtomicU64::new(0),
        })
    }

    pub fn snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            total_connections: self.total_connections.load(Ordering::Relaxed),
            active_connections: self.active_connections.load(Ordering::Relaxed),
            idle_connections: self.idle_connections.load(Ordering::Relaxed),
            waiting_clients: self.waiting_clients.load(Ordering::Relaxed),
            total_queries: self.total_queries.load(Ordering::Relaxed),
            total_transactions: self.total_transactions.load(Ordering::Relaxed),
            total_errors: self.total_errors.load(Ordering::Relaxed),
            connections_created: self.connections_created.load(Ordering::Relaxed),
            connections_closed: self.connections_closed.load(Ordering::Relaxed),
            health_checks_ok: self.health_checks_ok.load(Ordering::Relaxed),
            health_checks_failed: self.health_checks_failed.load(Ordering::Relaxed),
        }
    }
}

/// Serializable snapshot of pool statistics.
#[derive(Debug, Serialize)]
pub struct StatsSnapshot {
    pub total_connections: u64,
    pub active_connections: u64,
    pub idle_connections: u64,
    pub waiting_clients: u64,
    pub total_queries: u64,
    pub total_transactions: u64,
    pub total_errors: u64,
    pub connections_created: u64,
    pub connections_closed: u64,
    pub health_checks_ok: u64,
    pub health_checks_failed: u64,
}
