//! Connection manager for RMP subscriptions.
//!
//! [`PyroConnection`] manages active subscriptions and their associated
//! [`TableMirror`] instances, with memory budget enforcement and LRU eviction.

use crate::budget::{BudgetExceeded, MemoryBudget};
use crate::fk_walker::walk_fk_depth1;
use crate::limits::SubscriptionLimits;
use crate::live_graph::LiveGraph;
use crate::local_query::{self, LocalResult};
use crate::mirror::TableMirror;
use crate::protocol::{
    ColumnInfo, ColumnType, DeltaOp, Predicate, Snapshot,
};
use crate::row::Row;
use crate::schema::SchemaGraph;
use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Error returned by subscribe when the memory budget cannot accommodate the new mirror.
#[derive(Debug)]
pub enum SubscribeError {
    /// Memory budget exceeded even after evicting all unpinned mirrors.
    BudgetExceeded(BudgetExceeded),
}

impl std::fmt::Display for SubscribeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SubscribeError::BudgetExceeded(e) => write!(f, "subscribe failed: {e}"),
        }
    }
}

impl std::error::Error for SubscribeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            SubscribeError::BudgetExceeded(e) => Some(e),
        }
    }
}

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
    /// Memory budget manager.
    budget: Arc<MemoryBudget>,
    /// Subscription limits.
    limits: SubscriptionLimits,
    /// Tracks which table each mirror belongs to (sub_id -> table_name).
    mirror_tables: DashMap<u64, String>,
    /// Tracks the predicate for each mirror (sub_id -> predicate).
    mirror_predicates: DashMap<u64, Predicate>,
    /// Named mirrors for local query engine (table_name -> mirror).
    named_mirrors: DashMap<String, Arc<TableMirror>>,
    /// Column schemas for local query decoding (table_name -> columns).
    schemas: DashMap<String, Vec<ColumnInfo>>,
}

impl PyroConnection {
    /// Create a new connection with default limits (256 MB budget).
    pub fn new() -> Self {
        let limits = SubscriptionLimits::default();
        Self {
            mirrors: DashMap::new(),
            next_sub_id: AtomicU64::new(1),
            table_columns: DashMap::new(),
            budget: Arc::new(MemoryBudget::new(limits.max_mirror_bytes)),
            limits,
            mirror_tables: DashMap::new(),
            mirror_predicates: DashMap::new(),
            named_mirrors: DashMap::new(),
            schemas: DashMap::new(),
        }
    }

    /// Create a new connection with custom subscription limits.
    pub fn with_limits(limits: SubscriptionLimits) -> Self {
        Self {
            mirrors: DashMap::new(),
            next_sub_id: AtomicU64::new(1),
            table_columns: DashMap::new(),
            budget: Arc::new(MemoryBudget::new(limits.max_mirror_bytes)),
            limits,
            mirror_tables: DashMap::new(),
            mirror_predicates: DashMap::new(),
            named_mirrors: DashMap::new(),
            schemas: DashMap::new(),
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
    ///
    /// If the memory budget is exceeded, unpinned LRU mirrors are evicted first.
    /// If still over budget after evicting all unpinned mirrors, returns an error.
    pub async fn subscribe(
        &self,
        table: &str,
        predicate: Predicate,
    ) -> Result<Arc<TableMirror>, SubscribeError> {
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

        // Empty snapshot costs 0 bytes, so budget check is trivially ok.
        // The real budget check happens when data is loaded via load_snapshot_for.
        self.budget.touch(sub_id);
        self.mirror_tables.insert(sub_id, table.to_string());
        self.mirror_predicates.insert(sub_id, predicate);
        self.mirrors.insert(sub_id, Arc::clone(&mirror));

        // Track named mirror and schema for local query engine
        self.named_mirrors.insert(table.to_string(), Arc::clone(&mirror));
        self.schemas.insert(table.to_string(), mirror.columns());

        Ok(mirror)
    }

    /// Load a snapshot into an existing mirror, enforcing memory budget.
    ///
    /// If the budget would be exceeded, tries LRU eviction of unpinned mirrors.
    /// Returns error if still over budget after eviction.
    pub fn load_snapshot_for(
        &self,
        sub_id: u64,
        snapshot: Snapshot,
    ) -> Result<(), SubscribeError> {
        // Calculate memory cost of the incoming snapshot
        let snapshot_bytes: u64 = snapshot
            .rows
            .iter()
            .map(|(pk, row)| pk.len() as u64 + row.len() as u64 + 64)
            .sum();

        // Get current mirror memory to account for replacement
        let current_mirror_bytes = self
            .mirrors
            .get(&sub_id)
            .map(|m| m.memory_bytes())
            .unwrap_or(0);

        // Net new bytes needed
        let net_bytes = snapshot_bytes.saturating_sub(current_mirror_bytes);

        if net_bytes > 0 {
            // Try to allocate within budget
            if self.budget.try_allocate(net_bytes).is_err() {
                // Evict unpinned LRU mirrors
                let candidates = self.budget.eviction_candidates(&self.mirrors, net_bytes);
                for evict_id in &candidates {
                    if let Some((_, evicted)) = self.mirrors.remove(evict_id) {
                        let freed = evicted.memory_bytes();
                        self.budget.release(freed);
                        self.budget.remove_tracking(*evict_id);
                    }
                }
                // Retry allocation
                self.budget
                    .try_allocate(net_bytes)
                    .map_err(SubscribeError::BudgetExceeded)?;
            }
        } else if current_mirror_bytes > snapshot_bytes {
            // New snapshot is smaller — release the difference
            self.budget.release(current_mirror_bytes - snapshot_bytes);
        }

        // Apply snapshot to mirror and update schema
        if let Some(mirror) = self.mirrors.get(&sub_id) {
            // Update schema from snapshot columns
            if let Some(table_name) = self.mirror_tables.get(&sub_id) {
                self.schemas.insert(table_name.value().clone(), snapshot.columns.clone());
            }
            mirror.load_snapshot(snapshot);
            self.budget.touch(sub_id);
        }

        Ok(())
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
            let old_mem = mirror.memory_bytes();
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
            let new_mem = mirror.memory_bytes();

            // Update budget tracking
            if new_mem > old_mem {
                // Best-effort: if budget exceeded during delta, we still apply
                // (server already accepted the mutation)
                let _ = self.budget.try_allocate(new_mem - old_mem);
            } else if old_mem > new_mem {
                self.budget.release(old_mem - new_mem);
            }
        }
    }

    /// Unsubscribe from a mirror by subscription ID.
    pub fn unsubscribe(&self, sub_id: u64) {
        if let Some((_, mirror)) = self.mirrors.remove(&sub_id) {
            let freed = mirror.memory_bytes();
            self.budget.release(freed);
            self.budget.remove_tracking(sub_id);
            if let Some((_, table_name)) = self.mirror_tables.remove(&sub_id) {
                self.named_mirrors.remove(&table_name);
                self.schemas.remove(&table_name);
            }
            self.mirror_predicates.remove(&sub_id);
        }
    }

    /// Get a mirror by subscription ID.
    ///
    /// Also updates the LRU access time for eviction purposes.
    pub fn get_mirror(&self, sub_id: u64) -> Option<Arc<TableMirror>> {
        self.mirrors.get(&sub_id).map(|entry| {
            self.budget.touch(sub_id);
            Arc::clone(entry.value())
        })
    }

    /// Number of active subscriptions.
    pub fn active_subscriptions(&self) -> usize {
        self.mirrors.len()
    }

    /// Memory usage stats: (used_bytes, max_bytes).
    pub fn memory_stats(&self) -> (u64, u64) {
        (self.budget.used(), self.budget.max_bytes())
    }

    /// Get a reference to the subscription limits.
    pub fn limits(&self) -> &SubscriptionLimits {
        &self.limits
    }
}

/// Result from a transparent query that auto-upgrades to LiveSync.
pub struct QueryResult {
    /// The mirror backing this query result.
    mirror: Arc<TableMirror>,
    /// The predicate used for filtering (if the mirror covers more than requested).
    predicate: Predicate,
}

impl QueryResult {
    /// Create a QueryResult from a mirror and predicate.
    pub fn from_mirror(mirror: Arc<TableMirror>, predicate: &Predicate) -> Self {
        Self {
            mirror,
            predicate: predicate.clone(),
        }
    }

    /// Number of rows matching the predicate in the mirror.
    pub fn len(&self) -> usize {
        match &self.predicate {
            Predicate::All => self.mirror.len(),
            Predicate::Eq { value, .. } => {
                if self.mirror.get(value).is_some() {
                    1
                } else {
                    0
                }
            }
            Predicate::Range { start, end } => {
                // Must iterate to count matching rows.
                self.mirror
                    .iter()
                    .filter(|(pk, _)| pk.as_slice() >= start.as_slice() && pk.as_slice() <= end.as_slice())
                    .count()
            }
        }
    }

    /// Whether the result is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get a specific row by primary key.
    pub fn get(&self, pk: &[u8]) -> Option<dashmap::mapref::one::Ref<'_, Vec<u8>, Vec<u8>>> {
        self.mirror.get(pk)
    }

    /// Iterate all rows matching the predicate.
    pub fn iter(&self) -> impl Iterator<Item = (Vec<u8>, Vec<u8>)> + '_ {
        let pred = self.predicate.clone();
        self.mirror.iter().filter(move |(pk, _)| match &pred {
            Predicate::All => true,
            Predicate::Eq { value, .. } => pk.as_slice() == value.as_slice(),
            Predicate::Range { start, end } => {
                pk.as_slice() >= start.as_slice() && pk.as_slice() <= end.as_slice()
            }
        })
    }

    /// The subscription ID of the underlying mirror.
    pub fn sub_id(&self) -> u64 {
        self.mirror.sub_id()
    }

    /// The current version of the underlying mirror.
    pub fn version(&self) -> u64 {
        self.mirror.version()
    }
}

impl PyroConnection {
    /// Subscribe to a table and all FK-related tables up to `depth`.
    ///
    /// Creates a [`LiveGraph`] with the root subscription and mirrors for all
    /// tables reachable through foreign keys up to the specified depth.
    ///
    /// In mock mode, the root mirror starts empty (like `subscribe`). The
    /// FK walker uses the root PKs from the predicate to generate depth-1
    /// subscriptions immediately. Deeper levels require snapshots from
    /// intermediate tables — use [`walk_fk_next`](crate::fk_walker::walk_fk_next)
    /// after loading those snapshots.
    ///
    /// # Parameters
    ///
    /// - `table`: the root table to subscribe to
    /// - `predicate`: filter for the root subscription
    /// - `schema`: FK graph for walking related tables
    /// - `depth`: how many FK levels to walk (-1 = unlimited, 0 = root only)
    pub async fn live(
        &self,
        table: &str,
        predicate: Predicate,
        schema: &SchemaGraph,
        depth: i32,
    ) -> Result<LiveGraph, SubscribeError> {
        // Subscribe to root table
        let root_mirror = self.subscribe(table, predicate.clone()).await?;

        let graph = LiveGraph::new(
            table.to_string(),
            Arc::clone(&root_mirror),
            schema.clone(),
            depth,
        );

        // If depth is 0, no FK walking needed
        if depth == 0 {
            return Ok(graph);
        }

        // Extract root PKs from the predicate for depth-1 walking
        let root_pks = match &predicate {
            Predicate::Eq { value, .. } => vec![value.clone()],
            Predicate::All | Predicate::Range { .. } => {
                // For All/Range predicates, we can't determine specific PKs
                // until the snapshot arrives. Return the graph as-is and the
                // caller uses walk_fk_next after loading the root snapshot.
                return Ok(graph);
            }
        };

        // Walk depth 1: create subscriptions for directly related tables
        let depth1_subs = walk_fk_depth1(schema, table, &root_pks);

        for fk_sub in &depth1_subs {
            let mirror = self.subscribe(&fk_sub.table, fk_sub.predicate.clone()).await?;
            graph.add_related(fk_sub.table.clone(), mirror);
        }

        Ok(graph)
    }
}

/// Result from a SQL query executed against local mirrors.
pub struct SqlQueryResult {
    /// Column metadata for the result set.
    pub columns: Vec<ColumnInfo>,
    /// Decoded rows. Stored as `Arc<Row>` to allow zero-copy sharing
    /// of pre-decoded rows from the index cache.
    pub rows: Vec<Arc<Row>>,
}

impl SqlQueryResult {
    /// Create from a [`LocalResult`].
    pub fn from_local(local: LocalResult) -> Self {
        Self {
            columns: local.columns,
            rows: local.rows,
        }
    }

    /// Number of rows in the result.
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Whether the result is empty.
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}

impl PyroConnection {
    /// Execute a SQL or FIND query against local mirrors.
    ///
    /// Tries to resolve the query locally first using the local query engine.
    /// Returns `Some(SqlQueryResult)` if resolved locally, `None` if the query
    /// is too complex and should be delegated to the server.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// // SQL syntax
    /// let result = conn.query_sql("SELECT * FROM products WHERE id = 42").unwrap();
    ///
    /// // PyroSQL native syntax
    /// let result = conn.query_sql("FIND products WHERE id = 42").unwrap();
    /// ```
    pub fn query_sql(&self, sql: &str) -> Option<SqlQueryResult> {
        local_query::try_execute_local(sql, &self.named_mirrors, &self.schemas)
            .map(SqlQueryResult::from_local)
    }

    /// Get a reference to the named mirrors map (for advanced/direct use).
    pub fn named_mirrors(&self) -> &DashMap<String, Arc<TableMirror>> {
        &self.named_mirrors
    }

    /// Get a reference to the schemas map.
    pub fn schemas(&self) -> &DashMap<String, Vec<ColumnInfo>> {
        &self.schemas
    }
}

impl PyroConnection {
    /// Query that auto-upgrades to LiveSync for repeated patterns.
    ///
    /// First call: sends SUBSCRIBE, returns from mirror after snapshot.
    /// Subsequent calls: reads directly from mirror (~28ns).
    pub async fn query(
        &self,
        table: &str,
        predicate: Predicate,
    ) -> Result<QueryResult, SubscribeError> {
        // Check if we already have a mirror that covers this query.
        if let Some(mirror) = self.find_covering_mirror(table, &predicate) {
            return Ok(QueryResult::from_mirror(mirror, &predicate));
        }
        // First time -- subscribe and return from mirror.
        let mirror = self.subscribe(table, predicate.clone()).await?;
        Ok(QueryResult::from_mirror(mirror, &predicate))
    }

    /// Check if any existing mirror covers this query.
    ///
    /// A mirror covers a query if:
    /// - It is subscribed to the same table (tracked via table_columns key)
    /// - Its predicate is a superset of the query predicate
    ///
    /// In mock mode, we use a simple heuristic: a mirror with Predicate::All
    /// covers any query on the same table. Specific predicates only cover
    /// themselves.
    fn find_covering_mirror(
        &self,
        table: &str,
        predicate: &Predicate,
    ) -> Option<Arc<TableMirror>> {
        // Track (table, sub_id) associations for covering queries.
        // In mock mode, we check all mirrors. In production, the server would
        // handle hierarchy and return a "covered" response.
        for entry in self.mirrors.iter() {
            let mirror = entry.value();
            let sub_id = *entry.key();

            // Check if this mirror belongs to the same table.
            // We use the mirror_tables map to track table associations.
            if let Some(mirror_table) = self.mirror_tables.get(&sub_id) {
                if mirror_table.value() == table {
                    // Check predicate coverage: All covers everything.
                    if let Some(mirror_pred) = self.mirror_predicates.get(&sub_id) {
                        match mirror_pred.value() {
                            Predicate::All => return Some(Arc::clone(mirror)),
                            _ if mirror_pred.value() == predicate => {
                                return Some(Arc::clone(mirror))
                            }
                            _ => continue,
                        }
                    }
                }
            }
        }
        None
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
    use crate::protocol::ColumnType;

    fn make_snapshot(sub_id: u64, version: u64, num_rows: usize) -> Snapshot {
        let rows: Vec<(Vec<u8>, Vec<u8>)> = (0..num_rows)
            .map(|i| {
                let pk = (i as u64).to_le_bytes().to_vec();
                let data = format!("row_{i}").into_bytes();
                (pk, data)
            })
            .collect();

        Snapshot {
            sub_id,
            version,
            columns: vec![
                ColumnInfo {
                    name: "id".into(),
                    type_tag: ColumnType::Int64,
                },
                ColumnInfo {
                    name: "data".into(),
                    type_tag: ColumnType::Text,
                },
            ],
            rows,
        }
    }

    #[tokio::test]
    async fn subscribe_creates_mirror() {
        let conn = PyroConnection::new();
        let mirror = conn.subscribe("users", Predicate::All).await.unwrap();
        assert_eq!(mirror.len(), 0);
        assert_eq!(conn.active_subscriptions(), 1);
    }

    #[tokio::test]
    async fn unsubscribe_removes_mirror() {
        let conn = PyroConnection::new();
        let mirror = conn.subscribe("users", Predicate::All).await.unwrap();
        let sub_id = mirror.sub_id();
        assert_eq!(conn.active_subscriptions(), 1);

        conn.unsubscribe(sub_id);
        assert_eq!(conn.active_subscriptions(), 0);
        assert!(conn.get_mirror(sub_id).is_none());
    }

    #[tokio::test]
    async fn mutate_insert_updates_mirror() {
        let conn = PyroConnection::new();
        let mirror = conn.subscribe("users", Predicate::All).await.unwrap();

        conn.mutate("users", DeltaOp::Insert, b"pk1", Some(b"data1")).await;

        assert_eq!(mirror.len(), 1);
        assert_eq!(mirror.get(b"pk1").unwrap().as_slice(), b"data1");
    }

    #[tokio::test]
    async fn mutate_update_changes_row() {
        let conn = PyroConnection::new();
        let mirror = conn.subscribe("users", Predicate::All).await.unwrap();

        conn.mutate("users", DeltaOp::Insert, b"pk1", Some(b"original")).await;
        conn.mutate("users", DeltaOp::Update, b"pk1", Some(b"updated")).await;

        assert_eq!(mirror.len(), 1);
        assert_eq!(mirror.get(b"pk1").unwrap().as_slice(), b"updated");
    }

    #[tokio::test]
    async fn mutate_delete_removes_row() {
        let conn = PyroConnection::new();
        let mirror = conn.subscribe("users", Predicate::All).await.unwrap();

        conn.mutate("users", DeltaOp::Insert, b"pk1", Some(b"data1")).await;
        assert_eq!(mirror.len(), 1);

        conn.mutate("users", DeltaOp::Delete, b"pk1", None).await;
        assert_eq!(mirror.len(), 0);
        assert!(mirror.get(b"pk1").is_none());
    }

    #[tokio::test]
    async fn multiple_subscriptions_independent() {
        let conn = PyroConnection::new();
        let m1 = conn.subscribe("users", Predicate::All).await.unwrap();
        let m2 = conn.subscribe("orders", Predicate::All).await.unwrap();

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

        let mirror = conn.subscribe("metrics", Predicate::All).await.unwrap();
        let cols = mirror.columns();
        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0].name, "ts");
        assert_eq!(cols[1].name, "value");
    }

    #[tokio::test]
    async fn budget_tracks_memory() {
        let limits = SubscriptionLimits {
            max_rows_per_subscription: 100_000,
            max_mirror_bytes: 1024 * 1024,
        };
        let conn = PyroConnection::with_limits(limits);
        let mirror = conn.subscribe("users", Predicate::All).await.unwrap();
        let sub_id = mirror.sub_id();

        // Load 10 rows
        let snapshot = make_snapshot(sub_id, 1, 10);
        // Pre-calculate expected bytes
        let expected_bytes: u64 = snapshot
            .rows
            .iter()
            .map(|(pk, row)| pk.len() as u64 + row.len() as u64 + 64)
            .sum();

        conn.load_snapshot_for(sub_id, snapshot).unwrap();

        let (used, max) = conn.memory_stats();
        assert_eq!(used, expected_bytes);
        assert_eq!(max, 1024 * 1024);
        assert_eq!(mirror.memory_bytes(), expected_bytes);
    }

    #[tokio::test]
    async fn budget_rejects_over_limit() {
        // Tiny budget: 100 bytes
        let limits = SubscriptionLimits {
            max_rows_per_subscription: 100_000,
            max_mirror_bytes: 100,
        };
        let conn = PyroConnection::with_limits(limits);
        let mirror = conn.subscribe("users", Predicate::All).await.unwrap();
        let sub_id = mirror.sub_id();

        // Try to load a snapshot that exceeds 100 bytes
        // Each row: 8 (pk) + 5 (data "row_X") + 64 (overhead) = 77 bytes
        // 2 rows = 154 bytes > 100 byte budget
        let snapshot = make_snapshot(sub_id, 1, 2);
        let result = conn.load_snapshot_for(sub_id, snapshot);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn budget_evicts_lru() {
        // Budget fits ~2 mirrors but not 3
        // Each mirror with 1 row: 8 + 5 + 64 = 77 bytes
        // Budget = 200 bytes (fits 2, not 3)
        let limits = SubscriptionLimits {
            max_rows_per_subscription: 100_000,
            max_mirror_bytes: 200,
        };
        let conn = PyroConnection::with_limits(limits);

        // Subscribe mirror 1
        let m1 = conn.subscribe("t1", Predicate::All).await.unwrap();
        let s1_id = m1.sub_id();
        conn.load_snapshot_for(s1_id, make_snapshot(s1_id, 1, 1)).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(2));

        // Subscribe mirror 2
        let m2 = conn.subscribe("t2", Predicate::All).await.unwrap();
        let s2_id = m2.sub_id();
        conn.load_snapshot_for(s2_id, make_snapshot(s2_id, 1, 1)).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(2));

        assert_eq!(conn.active_subscriptions(), 2);

        // Subscribe mirror 3 — should evict mirror 1 (oldest)
        let m3 = conn.subscribe("t3", Predicate::All).await.unwrap();
        let s3_id = m3.sub_id();
        conn.load_snapshot_for(s3_id, make_snapshot(s3_id, 1, 1)).unwrap();

        // Mirror 1 should have been evicted
        assert!(conn.get_mirror(s1_id).is_none());
        // Mirrors 2 and 3 should still exist
        assert!(conn.get_mirror(s2_id).is_some());
        assert!(conn.get_mirror(s3_id).is_some());
    }

    #[tokio::test]
    async fn budget_respects_pin() {
        // Budget fits ~2 mirrors
        let limits = SubscriptionLimits {
            max_rows_per_subscription: 100_000,
            max_mirror_bytes: 200,
        };
        let conn = PyroConnection::with_limits(limits);

        // Mirror 1: pinned, oldest
        let m1 = conn.subscribe("t1", Predicate::All).await.unwrap();
        let s1_id = m1.sub_id();
        m1.pin();
        conn.load_snapshot_for(s1_id, make_snapshot(s1_id, 1, 1)).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(2));

        // Mirror 2: unpinned
        let m2 = conn.subscribe("t2", Predicate::All).await.unwrap();
        let s2_id = m2.sub_id();
        conn.load_snapshot_for(s2_id, make_snapshot(s2_id, 1, 1)).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(2));

        // Mirror 3: should evict mirror 2 (unpinned), NOT mirror 1 (pinned)
        let m3 = conn.subscribe("t3", Predicate::All).await.unwrap();
        let s3_id = m3.sub_id();
        conn.load_snapshot_for(s3_id, make_snapshot(s3_id, 1, 1)).unwrap();

        // Pinned mirror 1 must survive
        assert!(conn.get_mirror(s1_id).is_some());
        // Unpinned mirror 2 should have been evicted
        assert!(conn.get_mirror(s2_id).is_none());
        // New mirror 3 should exist
        assert!(conn.get_mirror(s3_id).is_some());
    }

    #[tokio::test]
    async fn pin_unpin_works() {
        let limits = SubscriptionLimits {
            max_rows_per_subscription: 100_000,
            max_mirror_bytes: 200,
        };
        let conn = PyroConnection::with_limits(limits);

        // Create and pin mirror 1
        let m1 = conn.subscribe("t1", Predicate::All).await.unwrap();
        let s1_id = m1.sub_id();
        m1.pin();
        conn.load_snapshot_for(s1_id, make_snapshot(s1_id, 1, 1)).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(2));

        // Fill with mirror 2
        let m2 = conn.subscribe("t2", Predicate::All).await.unwrap();
        let s2_id = m2.sub_id();
        conn.load_snapshot_for(s2_id, make_snapshot(s2_id, 1, 1)).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(2));

        // Mirror 3: budget full, but m1 is pinned. Should evict m2.
        let m3 = conn.subscribe("t3", Predicate::All).await.unwrap();
        let s3_id = m3.sub_id();
        conn.load_snapshot_for(s3_id, make_snapshot(s3_id, 1, 1)).unwrap();

        assert!(conn.get_mirror(s1_id).is_some(), "pinned mirror must survive");
        assert!(conn.get_mirror(s2_id).is_none(), "unpinned mirror should be evicted");

        // Now unpin m1 and fill again — m1 should become evictable
        m1.unpin();
        assert!(!m1.is_pinned());

        // Touch m3 so it's more recent than m1
        let _ = conn.get_mirror(s3_id);
        std::thread::sleep(std::time::Duration::from_millis(2));

        let m4 = conn.subscribe("t4", Predicate::All).await.unwrap();
        let s4_id = m4.sub_id();
        conn.load_snapshot_for(s4_id, make_snapshot(s4_id, 1, 1)).unwrap();

        // m1 is now unpinned and oldest — should be evicted
        assert!(conn.get_mirror(s1_id).is_none(), "unpinned m1 should now be evictable");
    }

    #[tokio::test]
    async fn memory_tracking_accurate() {
        let conn = PyroConnection::new();
        let mirror = conn.subscribe("users", Predicate::All).await.unwrap();
        let sub_id = mirror.sub_id();

        // Start with empty
        assert_eq!(mirror.memory_bytes(), 0);

        // Insert via delta
        let pk = b"pk_test";
        let data = b"some_data_here";
        let expected_after_insert = pk.len() as u64 + data.len() as u64 + 64;

        conn.mutate("users", DeltaOp::Insert, pk, Some(data)).await;
        assert_eq!(mirror.memory_bytes(), expected_after_insert);

        // Update to larger value
        let bigger = b"some_much_bigger_data_value_here!!";
        let expected_after_update = pk.len() as u64 + bigger.len() as u64 + 64;

        conn.mutate("users", DeltaOp::Update, pk, Some(bigger)).await;
        assert_eq!(mirror.memory_bytes(), expected_after_update);

        // Delete
        conn.mutate("users", DeltaOp::Delete, pk, None).await;
        assert_eq!(mirror.memory_bytes(), 0);

        // Memory stats on connection should also reflect
        let (used, _) = conn.memory_stats();
        assert_eq!(used, 0);

        let _ = sub_id; // suppress unused warning
    }

    #[tokio::test]
    async fn memory_stats_returns_used_and_max() {
        let limits = SubscriptionLimits {
            max_rows_per_subscription: 100_000,
            max_mirror_bytes: 5000,
        };
        let conn = PyroConnection::with_limits(limits);
        let (used, max) = conn.memory_stats();
        assert_eq!(used, 0);
        assert_eq!(max, 5000);
    }

    #[tokio::test]
    async fn unsubscribe_releases_budget() {
        let limits = SubscriptionLimits {
            max_rows_per_subscription: 100_000,
            max_mirror_bytes: 10_000,
        };
        let conn = PyroConnection::with_limits(limits);
        let mirror = conn.subscribe("t1", Predicate::All).await.unwrap();
        let sub_id = mirror.sub_id();

        conn.load_snapshot_for(sub_id, make_snapshot(sub_id, 1, 10)).unwrap();
        let (used_before, _) = conn.memory_stats();
        assert!(used_before > 0);

        conn.unsubscribe(sub_id);
        let (used_after, _) = conn.memory_stats();
        assert_eq!(used_after, 0);
    }

    // ── Transparent query upgrade tests ────────────────────────────────

    #[tokio::test]
    async fn query_auto_subscribes() {
        let conn = PyroConnection::new();
        assert_eq!(conn.active_subscriptions(), 0);

        let result = conn.query("users", Predicate::All).await.unwrap();
        assert_eq!(conn.active_subscriptions(), 1);
        // Empty mirror initially in mock mode.
        assert_eq!(result.len(), 0);
    }

    #[tokio::test]
    async fn query_reuses_mirror() {
        let conn = PyroConnection::new();

        // First query creates a subscription with Predicate::All.
        let r1 = conn.query("users", Predicate::All).await.unwrap();
        let sub_id_1 = r1.sub_id();
        assert_eq!(conn.active_subscriptions(), 1);

        // Second query on the same table with same predicate reuses the mirror.
        let r2 = conn.query("users", Predicate::All).await.unwrap();
        let sub_id_2 = r2.sub_id();
        assert_eq!(conn.active_subscriptions(), 1);
        assert_eq!(sub_id_1, sub_id_2);
    }

    #[tokio::test]
    async fn query_covered_by_table_sub() {
        let conn = PyroConnection::new();

        // First: subscribe to ALL rows on "users".
        let _all_mirror = conn.query("users", Predicate::All).await.unwrap();
        assert_eq!(conn.active_subscriptions(), 1);

        // Second: query a specific row -- should be covered by the ALL subscription.
        let r2 = conn
            .query(
                "users",
                Predicate::Eq {
                    column: "id".into(),
                    value: vec![0, 0, 0, 42],
                },
            )
            .await
            .unwrap();
        // Should NOT create a new subscription -- the ALL mirror covers it.
        assert_eq!(conn.active_subscriptions(), 1);
        // The result should use the same mirror.
        assert_eq!(r2.sub_id(), _all_mirror.sub_id());
    }

    #[tokio::test]
    async fn query_different_table_creates_new_sub() {
        let conn = PyroConnection::new();

        let _r1 = conn.query("users", Predicate::All).await.unwrap();
        let _r2 = conn.query("orders", Predicate::All).await.unwrap();
        assert_eq!(conn.active_subscriptions(), 2);
    }

    #[tokio::test]
    async fn query_result_iterates_filtered() {
        let conn = PyroConnection::new();

        // Subscribe to ALL, then insert some data.
        let r = conn.query("users", Predicate::All).await.unwrap();
        conn.mutate("users", DeltaOp::Insert, b"pk1", Some(b"data1")).await;
        conn.mutate("users", DeltaOp::Insert, b"pk2", Some(b"data2")).await;

        // Query with All should see all rows.
        let all: Vec<_> = r.iter().collect();
        assert_eq!(all.len(), 2);
    }
}
