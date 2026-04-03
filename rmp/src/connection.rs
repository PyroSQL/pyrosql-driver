//! Connection manager for RMP subscriptions.
//!
//! [`PyroConnection`] manages active subscriptions and their associated
//! [`TableMirror`] instances. The network layer is currently mocked —
//! the important piece is the local mirror management and delta application.

use crate::mirror::TableMirror;
use crate::protocol::{
    ColumnInfo, ColumnType, DeltaOp, Predicate, Snapshot,
};
use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Manages RMP subscriptions and their local mirrors.
///
/// In production, this would hold a TCP/QUIC connection to the server
/// and run a background task to receive deltas. For now, the network
/// layer is mocked — `subscribe` creates a local mirror with a synthetic
/// snapshot, and `mutate` applies changes directly to the mirror.
pub struct PyroConnection {
    /// Active mirrors, keyed by subscription ID.
    mirrors: DashMap<u64, Arc<TableMirror>>,
    /// Next subscription ID counter.
    next_sub_id: AtomicU64,
    /// Table metadata for mock mode (table_name -> columns).
    table_columns: DashMap<String, Vec<ColumnInfo>>,
}

impl PyroConnection {
    /// Create a new connection (mock mode — no actual server).
    pub fn new() -> Self {
        Self {
            mirrors: DashMap::new(),
            next_sub_id: AtomicU64::new(1),
            table_columns: DashMap::new(),
        }
    }

    /// Register table metadata for mock mode.
    ///
    /// In production, this would be fetched from the server during subscription.
    pub fn register_table(&self, table: &str, columns: Vec<ColumnInfo>) {
        self.table_columns.insert(table.to_string(), columns);
    }

    /// Subscribe to a table. Returns a mirror that stays in sync.
    ///
    /// In mock mode, returns an empty mirror. Use `load_snapshot_for` to
    /// populate it, or apply deltas directly via the mirror's `apply_delta`.
    pub async fn subscribe(&self, table: &str, _predicate: Predicate) -> Arc<TableMirror> {
        let sub_id = self.next_sub_id.fetch_add(1, Ordering::Relaxed);
        let mirror = Arc::new(TableMirror::new(sub_id));

        // Load column metadata if registered
        let columns = self
            .table_columns
            .get(table)
            .map(|c| c.value().clone())
            .unwrap_or_else(|| {
                vec![
                    ColumnInfo {
                        name: "id".into(),
                        type_tag: ColumnType::Int64,
                    },
                    ColumnInfo {
                        name: "data".into(),
                        type_tag: ColumnType::Bytes,
                    },
                ]
            });

        // Load empty snapshot with column metadata
        mirror.load_snapshot(Snapshot {
            sub_id,
            version: 0,
            columns,
            rows: vec![],
        });

        self.mirrors.insert(sub_id, Arc::clone(&mirror));
        mirror
    }

    /// Send a mutation to the server (mock: applies directly to local mirror).
    ///
    /// In production, this would send a `Mutate` message over the wire and the
    /// server would echo back a `Delta` on the subscription stream.
    pub async fn mutate(
        &self,
        _table: &str,
        op: DeltaOp,
        pk: &[u8],
        row: Option<&[u8]>,
    ) {
        // In mock mode, apply to all mirrors that contain (or should contain) this row.
        // In production, the server sends deltas back on subscription streams.
        for entry in self.mirrors.iter() {
            let mirror = entry.value();
            let delta = crate::protocol::Delta {
                sub_id: mirror.sub_id(),
                version: mirror.version() + 1,
                changes: vec![crate::protocol::RowChange {
                    op,
                    pk: pk.to_vec(),
                    row: row.map(|r| r.to_vec()),
                }],
            };
            mirror.apply_delta(&delta);
        }
    }

    /// Unsubscribe from a mirror by subscription ID.
    pub fn unsubscribe(&self, sub_id: u64) {
        self.mirrors.remove(&sub_id);
    }

    /// Get a mirror by subscription ID.
    pub fn get_mirror(&self, sub_id: u64) -> Option<Arc<TableMirror>> {
        self.mirrors.get(&sub_id).map(|entry| Arc::clone(entry.value()))
    }

    /// Number of active subscriptions.
    pub fn active_subscriptions(&self) -> usize {
        self.mirrors.len()
    }
}

impl Default for PyroConnection {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn subscribe_creates_mirror() {
        let conn = PyroConnection::new();
        let mirror = conn.subscribe("users", Predicate::All).await;
        assert_eq!(mirror.len(), 0);
        assert_eq!(conn.active_subscriptions(), 1);
    }

    #[tokio::test]
    async fn unsubscribe_removes_mirror() {
        let conn = PyroConnection::new();
        let mirror = conn.subscribe("users", Predicate::All).await;
        let sub_id = mirror.sub_id();
        assert_eq!(conn.active_subscriptions(), 1);

        conn.unsubscribe(sub_id);
        assert_eq!(conn.active_subscriptions(), 0);
        assert!(conn.get_mirror(sub_id).is_none());
    }

    #[tokio::test]
    async fn mutate_insert_updates_mirror() {
        let conn = PyroConnection::new();
        let mirror = conn.subscribe("users", Predicate::All).await;

        conn.mutate("users", DeltaOp::Insert, b"pk1", Some(b"data1")).await;

        assert_eq!(mirror.len(), 1);
        assert_eq!(mirror.get(b"pk1").unwrap().as_slice(), b"data1");
    }

    #[tokio::test]
    async fn mutate_update_changes_row() {
        let conn = PyroConnection::new();
        let mirror = conn.subscribe("users", Predicate::All).await;

        conn.mutate("users", DeltaOp::Insert, b"pk1", Some(b"original")).await;
        conn.mutate("users", DeltaOp::Update, b"pk1", Some(b"updated")).await;

        assert_eq!(mirror.len(), 1);
        assert_eq!(mirror.get(b"pk1").unwrap().as_slice(), b"updated");
    }

    #[tokio::test]
    async fn mutate_delete_removes_row() {
        let conn = PyroConnection::new();
        let mirror = conn.subscribe("users", Predicate::All).await;

        conn.mutate("users", DeltaOp::Insert, b"pk1", Some(b"data1")).await;
        assert_eq!(mirror.len(), 1);

        conn.mutate("users", DeltaOp::Delete, b"pk1", None).await;
        assert_eq!(mirror.len(), 0);
        assert!(mirror.get(b"pk1").is_none());
    }

    #[tokio::test]
    async fn multiple_subscriptions_independent() {
        let conn = PyroConnection::new();
        let m1 = conn.subscribe("users", Predicate::All).await;
        let m2 = conn.subscribe("orders", Predicate::All).await;

        assert_ne!(m1.sub_id(), m2.sub_id());
        assert_eq!(conn.active_subscriptions(), 2);

        // Mutate affects all mirrors in mock mode
        conn.mutate("users", DeltaOp::Insert, b"pk1", Some(b"data")).await;
        assert_eq!(m1.len(), 1);
        assert_eq!(m2.len(), 1); // mock mode broadcasts to all

        conn.unsubscribe(m1.sub_id());
        assert_eq!(conn.active_subscriptions(), 1);
    }

    #[tokio::test]
    async fn register_table_columns() {
        let conn = PyroConnection::new();
        conn.register_table(
            "metrics",
            vec![
                ColumnInfo {
                    name: "ts".into(),
                    type_tag: ColumnType::Int64,
                },
                ColumnInfo {
                    name: "value".into(),
                    type_tag: ColumnType::Float64,
                },
            ],
        );

        let mirror = conn.subscribe("metrics", Predicate::All).await;
        let cols = mirror.columns();
        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0].name, "ts");
        assert_eq!(cols[1].name, "value");
    }
}
