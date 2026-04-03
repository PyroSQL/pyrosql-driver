//! Reactive Memtable Projection (RMP) — client-side local mirror for PyroSQL.
//!
//! RMP subscribes to server data, receives binary deltas, and maintains a local
//! in-memory mirror for zero-latency reads. Client reads hit local memory
//! (~50ns) instead of going over the network.
//!
//! # Architecture
//!
//! ```text
//! ┌──────────────┐     subscribe      ┌──────────────┐
//! │  Application  │ ──────────────────▶│  PyroSQL     │
//! │               │                    │  Server      │
//! │  TableMirror  │◀── snapshot ──────│              │
//! │  (DashMap)    │◀── delta ─────────│              │
//! │               │                    │              │
//! │  get(pk) ~50ns│    mutate ────────▶│              │
//! └──────────────┘                    └──────────────┘
//! ```
//!
//! # Quick start
//!
//! ```rust
//! use pyrosql_rmp::protocol::{Snapshot, ColumnInfo, ColumnType};
//! use pyrosql_rmp::mirror::TableMirror;
//!
//! let mirror = TableMirror::new(1);
//! mirror.load_snapshot(Snapshot {
//!     sub_id: 1,
//!     version: 1,
//!     columns: vec![ColumnInfo { name: "id".into(), type_tag: ColumnType::Int64 }],
//!     rows: vec![(vec![0, 0, 0, 1], b"Alice".to_vec())],
//! });
//!
//! let row = mirror.get(&[0, 0, 0, 1]).unwrap();
//! assert_eq!(row.as_slice(), b"Alice");
//! ```

#![deny(unsafe_code)]
#![warn(missing_docs)]

pub mod budget;
pub mod connection;
pub mod fk_walker;
pub mod limits;
pub mod live_graph;
pub mod mirror;
pub mod protocol;
pub mod schema;

pub use budget::{BudgetExceeded, MemoryBudget};
pub use connection::PyroConnection;
pub use fk_walker::{walk_fk_depth1, walk_fk_next, FkSubscription};
pub use limits::SubscriptionLimits;
pub use live_graph::LiveGraph;
pub use mirror::TableMirror;
pub use protocol::{
    ColumnInfo, ColumnType, Delta, DeltaOp, Message, Mutate, Predicate, RowChange, Snapshot,
    Subscribe, Unsubscribe,
};
pub use schema::{ForeignKey, SchemaGraph};
