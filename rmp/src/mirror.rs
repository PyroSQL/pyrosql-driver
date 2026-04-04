//! Local mirror of subscribed data.
//!
//! [`TableMirror`] maintains an in-memory copy of server-side rows so that
//! client reads are direct memory access (~50ns) with zero network overhead.

use crate::protocol::{ColumnInfo, Delta, DeltaOp, Snapshot};
use dashmap::DashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

/// Estimated DashMap per-entry overhead in bytes (shard metadata, hash, pointers).
const DASHMAP_ENTRY_OVERHEAD: u64 = 64;

/// Global counter for unique mirror instance IDs.
static MIRROR_INSTANCE_COUNTER: AtomicU64 = AtomicU64::new(1);

/// A local mirror of subscribed data. Reads are direct memory access.
pub struct TableMirror {
    /// Rows indexed by primary key bytes.
    rows: DashMap<Vec<u8>, Vec<u8>>,
    /// Column metadata.
    columns: parking_lot::RwLock<Vec<ColumnInfo>>,
    /// Last version received from server.
    version: AtomicU64,
    /// Subscription ID this mirror belongs to.
    sub_id: u64,
    /// Approximate memory usage in bytes for all rows in the mirror.
    memory_bytes: AtomicU64,
    /// Whether this mirror is pinned (exempt from LRU eviction).
    pinned: AtomicBool,
    /// Unique instance ID, monotonically increasing, to distinguish mirrors.
    instance_id: u64,
}

/// Calculate the memory cost of a single row entry.
#[inline]
fn row_memory_cost(pk: &[u8], row: &[u8]) -> u64 {
    pk.len() as u64 + row.len() as u64 + DASHMAP_ENTRY_OVERHEAD
}

impl TableMirror {
    /// Create a new empty mirror for the given subscription.
    pub fn new(sub_id: u64) -> Self {
        Self {
            rows: DashMap::new(),
            columns: parking_lot::RwLock::new(Vec::new()),
            version: AtomicU64::new(0),
            sub_id,
            memory_bytes: AtomicU64::new(0),
            pinned: AtomicBool::new(false),
            instance_id: MIRROR_INSTANCE_COUNTER.fetch_add(1, Ordering::Relaxed),
        }
    }

    /// Unique instance ID for this mirror (monotonically increasing).
    pub fn instance_id(&self) -> u64 {
        self.instance_id
    }

    /// Read a row by primary key. Zero network, ~50ns.
    pub fn get(&self, pk: &[u8]) -> Option<dashmap::mapref::one::Ref<'_, Vec<u8>, Vec<u8>>> {
        self.rows.get(pk)
    }

    /// Apply an incremental delta from the server.
    ///
    /// Each [`RowChange`](crate::protocol::RowChange) in the delta is applied
    /// atomically per-row via `DashMap` (lock-free concurrent map).
    /// Memory tracking is updated for each change.
    pub fn apply_delta(&self, delta: &Delta) {
        debug_assert_eq!(delta.sub_id, self.sub_id, "delta sub_id mismatch");

        for change in &delta.changes {
            match change.op {
                DeltaOp::Insert => {
                    if let Some(ref data) = change.row {
                        let cost = row_memory_cost(&change.pk, data);
                        self.rows.insert(change.pk.clone(), data.clone());
                        self.memory_bytes.fetch_add(cost, Ordering::Relaxed);
                    }
                }
                DeltaOp::Update => {
                    if let Some(ref data) = change.row {
                        let new_cost = row_memory_cost(&change.pk, data);
                        // Get old row size before replacing
                        let old_cost = self
                            .rows
                            .get(&change.pk)
                            .map(|old| row_memory_cost(&change.pk, old.value()))
                            .unwrap_or(0);
                        self.rows.insert(change.pk.clone(), data.clone());
                        // Adjust memory: add new, subtract old
                        if new_cost >= old_cost {
                            self.memory_bytes
                                .fetch_add(new_cost - old_cost, Ordering::Relaxed);
                        } else {
                            self.memory_bytes
                                .fetch_sub(old_cost - new_cost, Ordering::Relaxed);
                        }
                    }
                }
                DeltaOp::Delete => {
                    if let Some((_, old_row)) = self.rows.remove(&change.pk) {
                        let cost = row_memory_cost(&change.pk, &old_row);
                        self.memory_bytes.fetch_sub(cost, Ordering::Relaxed);
                    }
                }
            }
        }

        self.version.store(delta.version, Ordering::Release);
    }

    /// Load an initial snapshot from the server, replacing all existing data.
    pub fn load_snapshot(&self, snapshot: Snapshot) {
        debug_assert_eq!(snapshot.sub_id, self.sub_id, "snapshot sub_id mismatch");

        // Replace columns
        {
            let mut cols = self.columns.write();
            *cols = snapshot.columns;
        }

        // Clear existing rows and load new ones
        self.rows.clear();

        let mut total_bytes: u64 = 0;
        for (pk, row) in snapshot.rows {
            total_bytes += row_memory_cost(&pk, &row);
            self.rows.insert(pk, row);
        }

        self.memory_bytes.store(total_bytes, Ordering::Release);
        self.version.store(snapshot.version, Ordering::Release);
    }

    /// Get the current server version this mirror is at.
    pub fn version(&self) -> u64 {
        self.version.load(Ordering::Acquire)
    }

    /// Number of rows currently in the mirror.
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Whether the mirror is empty.
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// The subscription ID this mirror belongs to.
    pub fn sub_id(&self) -> u64 {
        self.sub_id
    }

    /// Get a copy of the column metadata.
    pub fn columns(&self) -> Vec<ColumnInfo> {
        self.columns.read().clone()
    }

    /// Approximate memory usage in bytes for all rows in this mirror.
    pub fn memory_bytes(&self) -> u64 {
        self.memory_bytes.load(Ordering::Relaxed)
    }

    /// Pin this mirror so it is never evicted by the LRU budget manager.
    pub fn pin(&self) {
        self.pinned.store(true, Ordering::Release);
    }

    /// Unpin this mirror, making it eligible for LRU eviction.
    pub fn unpin(&self) {
        self.pinned.store(false, Ordering::Release);
    }

    /// Whether this mirror is pinned (exempt from eviction).
    pub fn is_pinned(&self) -> bool {
        self.pinned.load(Ordering::Acquire)
    }

    /// Iterate all rows, returning cloned (pk, row) pairs.
    ///
    /// The iterator is a snapshot of current state; concurrent mutations
    /// may or may not be visible depending on timing.
    pub fn iter(&self) -> impl Iterator<Item = (Vec<u8>, Vec<u8>)> + '_ {
        self.rows
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{ColumnType, RowChange};

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

    #[test]
    fn load_snapshot_1000_rows() {
        let mirror = TableMirror::new(1);
        let snapshot = make_snapshot(1, 100, 1000);
        mirror.load_snapshot(snapshot);

        assert_eq!(mirror.len(), 1000);
        assert_eq!(mirror.version(), 100);

        // Verify all rows accessible
        for i in 0u64..1000 {
            let pk = i.to_le_bytes().to_vec();
            let row = mirror.get(&pk).expect("row must exist");
            let expected = format!("row_{i}");
            assert_eq!(row.as_slice(), expected.as_bytes());
        }
    }

    #[test]
    fn apply_insert_delta() {
        let mirror = TableMirror::new(1);
        mirror.load_snapshot(make_snapshot(1, 100, 10));
        assert_eq!(mirror.len(), 10);

        let delta = Delta {
            sub_id: 1,
            version: 101,
            changes: vec![RowChange {
                op: DeltaOp::Insert,
                pk: 999u64.to_le_bytes().to_vec(),
                row: Some(b"new_row".to_vec()),
            }],
        };
        mirror.apply_delta(&delta);

        assert_eq!(mirror.len(), 11);
        assert_eq!(mirror.version(), 101);
        let row = mirror.get(&999u64.to_le_bytes().to_vec()).unwrap();
        assert_eq!(row.as_slice(), b"new_row");
    }

    #[test]
    fn apply_update_delta() {
        let mirror = TableMirror::new(1);
        mirror.load_snapshot(make_snapshot(1, 100, 10));

        let pk = 5u64.to_le_bytes().to_vec();
        // Verify original
        assert_eq!(
            mirror.get(&pk).unwrap().as_slice(),
            b"row_5"
        );

        let delta = Delta {
            sub_id: 1,
            version: 102,
            changes: vec![RowChange {
                op: DeltaOp::Update,
                pk: pk.clone(),
                row: Some(b"updated_row_5".to_vec()),
            }],
        };
        mirror.apply_delta(&delta);

        assert_eq!(mirror.len(), 10); // same count
        assert_eq!(mirror.version(), 102);
        assert_eq!(
            mirror.get(&pk).unwrap().as_slice(),
            b"updated_row_5"
        );
    }

    #[test]
    fn apply_delete_delta() {
        let mirror = TableMirror::new(1);
        mirror.load_snapshot(make_snapshot(1, 100, 10));

        let pk = 3u64.to_le_bytes().to_vec();
        assert!(mirror.get(&pk).is_some());

        let delta = Delta {
            sub_id: 1,
            version: 103,
            changes: vec![RowChange {
                op: DeltaOp::Delete,
                pk: pk.clone(),
                row: None,
            }],
        };
        mirror.apply_delta(&delta);

        assert_eq!(mirror.len(), 9);
        assert_eq!(mirror.version(), 103);
        assert!(mirror.get(&pk).is_none());
    }

    #[test]
    fn concurrent_reads_during_delta() {
        use std::sync::Arc;
        use std::thread;

        let mirror = Arc::new(TableMirror::new(1));
        mirror.load_snapshot(make_snapshot(1, 100, 1000));

        let done = Arc::new(std::sync::atomic::AtomicBool::new(false));

        // Spawn reader threads
        let mut handles = Vec::new();
        for _ in 0..4 {
            let m = Arc::clone(&mirror);
            let d = Arc::clone(&done);
            handles.push(thread::spawn(move || {
                let mut reads = 0u64;
                while !d.load(Ordering::Relaxed) {
                    for i in 0u64..100 {
                        let pk = i.to_le_bytes().to_vec();
                        let _ = m.get(&pk);
                        reads += 1;
                    }
                }
                reads
            }));
        }

        // Apply deltas concurrently
        for v in 101..201 {
            let delta = Delta {
                sub_id: 1,
                version: v,
                changes: vec![
                    RowChange {
                        op: DeltaOp::Update,
                        pk: (v % 1000).to_le_bytes().to_vec(),
                        row: Some(format!("v{v}").into_bytes()),
                    },
                    RowChange {
                        op: DeltaOp::Insert,
                        pk: (1000 + v).to_le_bytes().to_vec(),
                        row: Some(format!("new_{v}").into_bytes()),
                    },
                ],
            };
            mirror.apply_delta(&delta);
        }

        done.store(true, Ordering::Relaxed);

        let total_reads: u64 = handles.into_iter().map(|h| h.join().unwrap()).sum();
        // Just verify no crashes and some reads happened
        assert!(total_reads > 0, "readers must have completed some reads");
        assert_eq!(mirror.version(), 200);
    }

    #[test]
    fn load_snapshot_replaces_existing_data() {
        let mirror = TableMirror::new(1);
        mirror.load_snapshot(make_snapshot(1, 100, 500));
        assert_eq!(mirror.len(), 500);

        // Load a smaller snapshot — old rows must be gone
        mirror.load_snapshot(make_snapshot(1, 200, 10));
        assert_eq!(mirror.len(), 10);
        assert_eq!(mirror.version(), 200);
    }

    #[test]
    fn iter_returns_all_rows() {
        let mirror = TableMirror::new(1);
        mirror.load_snapshot(make_snapshot(1, 1, 50));

        let all: Vec<_> = mirror.iter().collect();
        assert_eq!(all.len(), 50);
    }

    #[test]
    fn empty_mirror() {
        let mirror = TableMirror::new(1);
        assert!(mirror.is_empty());
        assert_eq!(mirror.len(), 0);
        assert_eq!(mirror.version(), 0);
        assert!(mirror.get(b"anything").is_none());
    }

    #[test]
    fn columns_metadata_preserved() {
        let mirror = TableMirror::new(1);
        mirror.load_snapshot(make_snapshot(1, 1, 0));
        let cols = mirror.columns();
        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0].name, "id");
        assert_eq!(cols[0].type_tag, ColumnType::Int64);
        assert_eq!(cols[1].name, "data");
        assert_eq!(cols[1].type_tag, ColumnType::Text);
    }

    #[test]
    fn memory_tracking_on_snapshot() {
        let mirror = TableMirror::new(1);
        assert_eq!(mirror.memory_bytes(), 0);

        // Each row: pk=8 bytes (u64 le), data="row_X" (5 bytes for 0..10), overhead=64
        // row_0..row_9: pk=8, data=5, overhead=64 => 77 each => 770 total
        mirror.load_snapshot(make_snapshot(1, 1, 10));
        assert!(mirror.memory_bytes() > 0);

        // Compute expected
        let mut expected: u64 = 0;
        for i in 0..10u64 {
            let pk_len = 8u64; // u64 le bytes
            let data = format!("row_{i}");
            let data_len = data.len() as u64;
            expected += pk_len + data_len + DASHMAP_ENTRY_OVERHEAD;
        }
        assert_eq!(mirror.memory_bytes(), expected);
    }

    #[test]
    fn memory_tracking_on_delta() {
        let mirror = TableMirror::new(1);
        mirror.load_snapshot(make_snapshot(1, 1, 0));
        assert_eq!(mirror.memory_bytes(), 0);

        // Insert
        let pk = 1u64.to_le_bytes().to_vec();
        let data = b"hello".to_vec();
        let insert_cost = pk.len() as u64 + data.len() as u64 + DASHMAP_ENTRY_OVERHEAD;
        mirror.apply_delta(&Delta {
            sub_id: 1,
            version: 2,
            changes: vec![RowChange {
                op: DeltaOp::Insert,
                pk: pk.clone(),
                row: Some(data),
            }],
        });
        assert_eq!(mirror.memory_bytes(), insert_cost);

        // Update to longer value
        let new_data = b"hello_world_extended".to_vec();
        let new_cost = pk.len() as u64 + new_data.len() as u64 + DASHMAP_ENTRY_OVERHEAD;
        mirror.apply_delta(&Delta {
            sub_id: 1,
            version: 3,
            changes: vec![RowChange {
                op: DeltaOp::Update,
                pk: pk.clone(),
                row: Some(new_data),
            }],
        });
        assert_eq!(mirror.memory_bytes(), new_cost);

        // Delete
        mirror.apply_delta(&Delta {
            sub_id: 1,
            version: 4,
            changes: vec![RowChange {
                op: DeltaOp::Delete,
                pk: pk.clone(),
                row: None,
            }],
        });
        assert_eq!(mirror.memory_bytes(), 0);
    }

    #[test]
    fn pin_unpin_defaults() {
        let mirror = TableMirror::new(1);
        assert!(!mirror.is_pinned());

        mirror.pin();
        assert!(mirror.is_pinned());

        mirror.unpin();
        assert!(!mirror.is_pinned());
    }

    #[test]
    fn snapshot_resets_memory_tracking() {
        let mirror = TableMirror::new(1);
        mirror.load_snapshot(make_snapshot(1, 1, 100));
        let mem1 = mirror.memory_bytes();
        assert!(mem1 > 0);

        // Load smaller snapshot — memory must reflect new state only
        mirror.load_snapshot(make_snapshot(1, 2, 5));
        let mem2 = mirror.memory_bytes();
        assert!(mem2 < mem1);
        assert!(mem2 > 0);
    }
}
