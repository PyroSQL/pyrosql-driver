//! Memory budget manager for RMP mirrors.
//!
//! [`MemoryBudget`] enforces a global memory limit across all [`TableMirror`]
//! instances and implements LRU-based eviction of unpinned mirrors.

use crate::mirror::TableMirror;
use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

/// Error returned when a memory allocation would exceed the budget.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BudgetExceeded {
    /// Bytes requested.
    pub requested: u64,
    /// Bytes currently available.
    pub available: u64,
    /// Maximum budget in bytes.
    pub max_bytes: u64,
}

impl std::fmt::Display for BudgetExceeded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "memory budget exceeded: requested {} bytes, available {} of {} max",
            self.requested, self.available, self.max_bytes
        )
    }
}

impl std::error::Error for BudgetExceeded {}

/// Monotonic epoch used to convert `Instant` to a comparable `u64`.
///
/// All access times are stored as nanoseconds since this epoch.
static EPOCH: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();

fn epoch() -> Instant {
    *EPOCH.get_or_init(Instant::now)
}

fn now_nanos() -> u64 {
    epoch().elapsed().as_nanos() as u64
}

/// Manages a global memory budget for all mirrors in a connection.
pub struct MemoryBudget {
    /// Maximum bytes allowed for all mirrors combined.
    max_bytes: u64,
    /// Current total usage across all tracked mirrors.
    used_bytes: AtomicU64,
    /// LRU tracking: sub_id -> last access time (nanos since epoch).
    access_times: DashMap<u64, u64>,
}

impl MemoryBudget {
    /// Create a new budget with the given maximum byte limit.
    pub fn new(max_bytes: u64) -> Self {
        // Ensure epoch is initialized.
        let _ = epoch();
        Self {
            max_bytes,
            used_bytes: AtomicU64::new(0),
            access_times: DashMap::new(),
        }
    }

    /// Record an access to a mirror (updates LRU timestamp).
    pub fn touch(&self, sub_id: u64) {
        self.access_times.insert(sub_id, now_nanos());
    }

    /// Try to allocate `bytes` within the budget.
    ///
    /// Returns `Ok(())` if the allocation fits, or `Err(BudgetExceeded)` if not.
    pub fn try_allocate(&self, bytes: u64) -> Result<(), BudgetExceeded> {
        loop {
            let current = self.used_bytes.load(Ordering::Relaxed);
            let new_total = current.saturating_add(bytes);
            if new_total > self.max_bytes {
                return Err(BudgetExceeded {
                    requested: bytes,
                    available: self.max_bytes.saturating_sub(current),
                    max_bytes: self.max_bytes,
                });
            }
            if self
                .used_bytes
                .compare_exchange_weak(current, new_total, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                return Ok(());
            }
        }
    }

    /// Release `bytes` back to the budget (e.g. when a mirror is evicted).
    pub fn release(&self, bytes: u64) {
        self.used_bytes.fetch_sub(bytes, Ordering::Relaxed);
    }

    /// Remove LRU tracking for a subscription (called on eviction/unsubscribe).
    pub fn remove_tracking(&self, sub_id: u64) {
        self.access_times.remove(&sub_id);
    }

    /// Get a list of unpinned subscription IDs sorted by LRU (oldest first),
    /// enough to free at least `needed_bytes`.
    ///
    /// Returns the sub_ids that should be evicted. Does not actually evict them.
    pub fn eviction_candidates(
        &self,
        mirrors: &DashMap<u64, Arc<TableMirror>>,
        needed_bytes: u64,
    ) -> Vec<u64> {
        // Collect unpinned mirrors with their access times
        let mut candidates: Vec<(u64, u64, u64)> = Vec::new(); // (sub_id, access_time, mem_bytes)
        for entry in mirrors.iter() {
            let mirror = entry.value();
            if !mirror.is_pinned() {
                let sub_id = *entry.key();
                let access_time = self
                    .access_times
                    .get(&sub_id)
                    .map(|v| *v.value())
                    .unwrap_or(0);
                let mem = mirror.memory_bytes();
                candidates.push((sub_id, access_time, mem));
            }
        }

        // Sort by access time ascending (oldest / least recently used first)
        candidates.sort_by_key(|&(_, access_time, _)| access_time);

        let mut freed: u64 = 0;
        let mut result = Vec::new();
        for (sub_id, _, mem) in candidates {
            if freed >= needed_bytes {
                break;
            }
            result.push(sub_id);
            freed += mem;
        }

        result
    }

    /// Current memory usage in bytes.
    pub fn used(&self) -> u64 {
        self.used_bytes.load(Ordering::Relaxed)
    }

    /// Available bytes before hitting the limit.
    pub fn available(&self) -> u64 {
        self.max_bytes.saturating_sub(self.used_bytes.load(Ordering::Relaxed))
    }

    /// Maximum budget in bytes.
    pub fn max_bytes(&self) -> u64 {
        self.max_bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allocate_within_budget() {
        let budget = MemoryBudget::new(1000);
        assert!(budget.try_allocate(500).is_ok());
        assert_eq!(budget.used(), 500);
        assert_eq!(budget.available(), 500);
    }

    #[test]
    fn allocate_exceeds_budget() {
        let budget = MemoryBudget::new(100);
        assert!(budget.try_allocate(50).is_ok());
        let err = budget.try_allocate(60).unwrap_err();
        assert_eq!(err.requested, 60);
        assert_eq!(err.available, 50);
        assert_eq!(err.max_bytes, 100);
    }

    #[test]
    fn release_frees_space() {
        let budget = MemoryBudget::new(100);
        budget.try_allocate(80).unwrap();
        assert_eq!(budget.available(), 20);
        budget.release(50);
        assert_eq!(budget.available(), 70);
        assert_eq!(budget.used(), 30);
    }

    #[test]
    fn touch_updates_access_time() {
        let budget = MemoryBudget::new(1000);
        budget.touch(1);
        let t1 = *budget.access_times.get(&1).unwrap().value();
        // Touch again — time should be >= previous
        std::thread::sleep(std::time::Duration::from_millis(1));
        budget.touch(1);
        let t2 = *budget.access_times.get(&1).unwrap().value();
        assert!(t2 >= t1);
    }

    #[test]
    fn eviction_candidates_sorted_by_lru() {
        let budget = MemoryBudget::new(10_000);
        let mirrors: DashMap<u64, Arc<TableMirror>> = DashMap::new();

        // Create 3 mirrors: sub 1 (oldest), sub 2, sub 3 (newest)
        for id in 1..=3 {
            let m = Arc::new(TableMirror::new(id));
            // Load some data so memory > 0
            m.load_snapshot(crate::protocol::Snapshot {
                sub_id: id,
                version: 1,
                columns: vec![],
                rows: vec![(vec![0; 8], vec![0; 100])],
            });
            mirrors.insert(id, m);
            budget.touch(id);
            std::thread::sleep(std::time::Duration::from_millis(2));
        }

        // Need to evict enough for 200 bytes — should pick oldest first
        let candidates = budget.eviction_candidates(&mirrors, 200);
        assert!(!candidates.is_empty());
        // First candidate should be sub_id 1 (oldest)
        assert_eq!(candidates[0], 1);
    }

    #[test]
    fn eviction_skips_pinned() {
        let budget = MemoryBudget::new(10_000);
        let mirrors: DashMap<u64, Arc<TableMirror>> = DashMap::new();

        for id in 1..=3 {
            let m = Arc::new(TableMirror::new(id));
            m.load_snapshot(crate::protocol::Snapshot {
                sub_id: id,
                version: 1,
                columns: vec![],
                rows: vec![(vec![0; 8], vec![0; 100])],
            });
            if id == 1 {
                m.pin();
            }
            mirrors.insert(id, m);
            budget.touch(id);
            std::thread::sleep(std::time::Duration::from_millis(2));
        }

        // Even though sub 1 is oldest, it's pinned — should not appear
        let candidates = budget.eviction_candidates(&mirrors, 500);
        assert!(!candidates.iter().any(|&id| id == 1));
    }
}
