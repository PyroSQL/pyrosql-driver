//! FK graph walker for reactive subscriptions.
//!
//! Implements a two-phase BFS walk of the foreign key graph:
//!
//! - **Phase 1** ([`walk_fk_depth1`]): given root PKs, returns immediate FK
//!   subscriptions (depth 1) — tables that reference or are referenced by the
//!   root table.
//!
//! - **Phase 2** ([`walk_fk_next`]): given a snapshot of a table at depth N,
//!   extracts FK column values from rows and returns subscriptions for depth N+1.
//!
//! The two-phase design is necessary because deeper levels need actual data
//! (snapshots) from intermediate levels to know which PKs to filter on.

use crate::protocol::Predicate;
use crate::schema::SchemaGraph;
use std::collections::HashSet;

/// A subscription derived from walking the FK graph.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FkSubscription {
    /// Table to subscribe to.
    pub table: String,
    /// How to filter rows (derived from parent's data).
    pub predicate: Predicate,
    /// Which FK column links this to its parent.
    pub fk_column: String,
    /// The parent table in the graph.
    pub parent_table: String,
    /// Depth level from root (0 = root).
    pub depth: u32,
}

/// Phase 1: Get immediate FK subscriptions (depth 1) from root PKs.
///
/// Given a root table and its primary key values, walks one level of the FK
/// graph in both directions:
///
/// - **Incoming FKs** (other tables referencing root): creates subscriptions
///   filtered by `fk_column = root_pk`. For example, if `orders.user_id -> users.id`
///   and we are rooted on `users` with PK 42, produces a subscription for
///   `orders WHERE user_id = 42`.
///
/// - **Outgoing FKs** (root references other tables): these need FK column
///   values from the root table's rows, which are not available until the root
///   snapshot arrives. Use [`walk_fk_next`] for those once the snapshot is loaded.
///
/// Returns an empty vec if `root_pks` is empty.
pub fn walk_fk_depth1(
    schema: &SchemaGraph,
    root_table: &str,
    root_pks: &[Vec<u8>],
) -> Vec<FkSubscription> {
    if root_pks.is_empty() {
        return Vec::new();
    }

    let mut subs = Vec::new();

    // Incoming FKs: other tables that reference the root table.
    // e.g., orders.user_id -> users.id  =>  subscribe to orders WHERE user_id IN (root_pks)
    for fk in schema.incoming(root_table) {
        // For each root PK, create a subscription on the referencing table.
        // We use the first PK value for the Eq predicate (single-PK case).
        // For multi-PK roots, the caller should issue one subscription per PK
        // or use a Range predicate. Here we create one per PK to keep it simple.
        for pk in root_pks {
            subs.push(FkSubscription {
                table: fk.from_table.clone(),
                predicate: Predicate::Eq {
                    column: fk.from_column.clone(),
                    value: pk.clone(),
                },
                fk_column: fk.from_column.clone(),
                parent_table: root_table.to_string(),
                depth: 1,
            });
        }
    }

    // Outgoing FKs from root are NOT resolved here because we need the actual
    // row data from the root table to extract FK column values. The caller
    // should use walk_fk_next once the root snapshot is loaded.

    subs
}

/// Phase 2: Given a snapshot of a table at depth N, get subscriptions for depth N+1.
///
/// Walks the FK graph from `table` and generates subscriptions for related tables
/// using actual FK column values extracted from the snapshot rows.
///
/// # Parameters
///
/// - `schema`: the FK graph
/// - `table`: the table whose snapshot we just received
/// - `snapshot_rows`: `(pk, row_bytes)` pairs from the snapshot
/// - `column_offsets`: mapping of `(column_name, byte_offset)` — how to extract
///   a fixed-width value from `row_bytes`. Each FK column value is extracted as
///   `row_bytes[offset..offset+8]` (8 bytes, i64 LE).
/// - `current_depth`: the depth of `table` in the graph
/// - `max_depth`: depth limit (-1 = unlimited, 0 = root only)
/// - `visited`: set of already-visited table names (to avoid cycles)
///
/// Returns subscriptions for depth `current_depth + 1`.
pub fn walk_fk_next(
    schema: &SchemaGraph,
    table: &str,
    snapshot_rows: &[(Vec<u8>, Vec<u8>)],
    column_offsets: &[(String, usize)],
    current_depth: u32,
    max_depth: i32,
    visited: &mut HashSet<String>,
) -> Vec<FkSubscription> {
    let next_depth = current_depth + 1;

    // Check depth limit: 0 = root only (no walking), -1 = unlimited
    if max_depth >= 0 && next_depth > max_depth as u32 {
        return Vec::new();
    }

    // Mark current table as visited
    visited.insert(table.to_string());

    let mut subs = Vec::new();

    // Incoming FKs: other tables referencing this table.
    // e.g., order_items.order_id -> orders.id
    // We subscribe to order_items WHERE order_id IN (pk values from orders snapshot)
    for fk in schema.incoming(table) {
        if visited.contains(&fk.from_table) {
            continue;
        }

        // Use the PKs from the snapshot as the filter values
        for (pk, _row) in snapshot_rows {
            subs.push(FkSubscription {
                table: fk.from_table.clone(),
                predicate: Predicate::Eq {
                    column: fk.from_column.clone(),
                    value: pk.clone(),
                },
                fk_column: fk.from_column.clone(),
                parent_table: table.to_string(),
                depth: next_depth,
            });
        }
    }

    // Outgoing FKs: this table references other tables.
    // e.g., order_items.product_id -> products.id
    // We need to extract product_id values from order_items rows and subscribe
    // to products WHERE id IN (those values).
    for fk in schema.outgoing(table) {
        if visited.contains(&fk.to_table) {
            continue;
        }

        // Find the column offset for this FK column
        let offset = column_offsets
            .iter()
            .find(|(name, _)| name == &fk.from_column)
            .map(|(_, off)| *off);

        let Some(offset) = offset else {
            // No offset mapping for this FK column — skip it.
            // This can happen if the caller didn't provide metadata for all columns.
            continue;
        };

        // Extract unique FK values from all rows
        let mut seen_values: HashSet<Vec<u8>> = HashSet::new();
        for (_pk, row) in snapshot_rows {
            if offset + 8 <= row.len() {
                let value = row[offset..offset + 8].to_vec();
                seen_values.insert(value);
            }
        }

        for value in seen_values {
            subs.push(FkSubscription {
                table: fk.to_table.clone(),
                predicate: Predicate::Eq {
                    column: fk.to_column.clone(),
                    value,
                },
                fk_column: fk.from_column.clone(),
                parent_table: table.to_string(),
                depth: next_depth,
            });
        }
    }

    subs
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{ForeignKey, SchemaGraph};

    fn test_schema() -> SchemaGraph {
        // users(id PK)
        // orders(id PK, user_id FK -> users.id)
        // order_items(id PK, order_id FK -> orders.id, product_id FK -> products.id)
        // products(id PK)
        SchemaGraph::new(vec![
            ForeignKey {
                from_table: "orders".into(),
                from_column: "user_id".into(),
                to_table: "users".into(),
                to_column: "id".into(),
            },
            ForeignKey {
                from_table: "order_items".into(),
                from_column: "order_id".into(),
                to_table: "orders".into(),
                to_column: "id".into(),
            },
            ForeignKey {
                from_table: "order_items".into(),
                from_column: "product_id".into(),
                to_table: "products".into(),
                to_column: "id".into(),
            },
        ])
    }

    #[test]
    fn walk_depth0_returns_empty() {
        // depth 0 = root only, no FK walking at all
        let schema = test_schema();
        let root_pks = vec![42i64.to_le_bytes().to_vec()];
        // walk_fk_depth1 returns depth-1 subscriptions, but if depth limit is 0
        // the caller wouldn't call it. Verify walk_fk_next with max_depth=0.
        let mut visited = HashSet::new();
        let subs = walk_fk_next(
            &schema,
            "users",
            &[(42i64.to_le_bytes().to_vec(), vec![])],
            &[],
            0, // current depth
            0, // max depth = 0, so next_depth=1 > 0 => empty
            &mut visited,
        );
        assert!(subs.is_empty());
        let _ = root_pks; // suppress unused
    }

    #[test]
    fn walk_depth1_returns_direct_fks() {
        let schema = test_schema();
        let root_pks = vec![42i64.to_le_bytes().to_vec()];

        let subs = walk_fk_depth1(&schema, "users", &root_pks);

        // users has one incoming FK: orders.user_id -> users.id
        assert_eq!(subs.len(), 1);
        assert_eq!(subs[0].table, "orders");
        assert_eq!(subs[0].fk_column, "user_id");
        assert_eq!(subs[0].parent_table, "users");
        assert_eq!(subs[0].depth, 1);
        assert_eq!(
            subs[0].predicate,
            Predicate::Eq {
                column: "user_id".into(),
                value: 42i64.to_le_bytes().to_vec(),
            }
        );
    }

    #[test]
    fn walk_depth1_incoming_fks() {
        // Root on users with pk=42 => orders WHERE user_id=42
        let schema = test_schema();
        let pk42 = 42i64.to_le_bytes().to_vec();
        let subs = walk_fk_depth1(&schema, "users", &[pk42.clone()]);

        assert_eq!(subs.len(), 1);
        assert_eq!(subs[0].table, "orders");
        assert_eq!(
            subs[0].predicate,
            Predicate::Eq {
                column: "user_id".into(),
                value: pk42,
            }
        );
    }

    #[test]
    fn walk_depth1_multiple_pks() {
        let schema = test_schema();
        let pk1 = 1i64.to_le_bytes().to_vec();
        let pk2 = 2i64.to_le_bytes().to_vec();

        let subs = walk_fk_depth1(&schema, "users", &[pk1.clone(), pk2.clone()]);

        // One subscription per PK for the incoming FK
        assert_eq!(subs.len(), 2);
        assert_eq!(subs[0].predicate, Predicate::Eq { column: "user_id".into(), value: pk1 });
        assert_eq!(subs[1].predicate, Predicate::Eq { column: "user_id".into(), value: pk2 });
    }

    #[test]
    fn walk_depth1_empty_pks() {
        let schema = test_schema();
        let subs = walk_fk_depth1(&schema, "users", &[]);
        assert!(subs.is_empty());
    }

    #[test]
    fn walk_depth1_no_incoming_fks() {
        // order_items has no incoming FKs
        let schema = test_schema();
        let pk = 1i64.to_le_bytes().to_vec();
        let subs = walk_fk_depth1(&schema, "order_items", &[pk]);
        assert!(subs.is_empty());
    }

    #[test]
    fn walk_fk_next_depth2_from_orders() {
        let schema = test_schema();

        // Simulate: we have orders snapshot with PKs [100, 200]
        let order_rows: Vec<(Vec<u8>, Vec<u8>)> = vec![
            (100i64.to_le_bytes().to_vec(), vec![0; 16]), // row data placeholder
            (200i64.to_le_bytes().to_vec(), vec![0; 16]),
        ];

        let mut visited = HashSet::new();
        visited.insert("users".to_string()); // already visited root

        let subs = walk_fk_next(
            &schema,
            "orders",
            &order_rows,
            &[], // no outgoing FK offsets needed for this case
            1,   // current depth
            2,   // max depth
            &mut visited,
        );

        // orders has incoming FK: order_items.order_id -> orders.id
        // So we expect subscriptions for order_items WHERE order_id IN (100, 200)
        assert_eq!(subs.len(), 2);
        assert!(subs.iter().all(|s| s.table == "order_items"));
        assert!(subs.iter().all(|s| s.fk_column == "order_id"));
        assert!(subs.iter().all(|s| s.depth == 2));

        let values: HashSet<Vec<u8>> = subs.iter().map(|s| {
            if let Predicate::Eq { value, .. } = &s.predicate { value.clone() } else { panic!() }
        }).collect();
        assert!(values.contains(&100i64.to_le_bytes().to_vec()));
        assert!(values.contains(&200i64.to_le_bytes().to_vec()));
    }

    #[test]
    fn walk_fk_next_outgoing_extracts_values() {
        let schema = test_schema();

        // Simulate: order_items snapshot with rows that have product_id at offset 8
        // Row layout: [order_id: 8 bytes][product_id: 8 bytes]
        let product_id_a = 50i64.to_le_bytes();
        let product_id_b = 60i64.to_le_bytes();

        let mut row_a = vec![0u8; 16];
        row_a[8..16].copy_from_slice(&product_id_a);

        let mut row_b = vec![0u8; 16];
        row_b[8..16].copy_from_slice(&product_id_b);

        let rows: Vec<(Vec<u8>, Vec<u8>)> = vec![
            (1i64.to_le_bytes().to_vec(), row_a),
            (2i64.to_le_bytes().to_vec(), row_b),
        ];

        let column_offsets = vec![
            ("order_id".to_string(), 0usize),
            ("product_id".to_string(), 8usize),
        ];

        let mut visited = HashSet::new();
        visited.insert("users".to_string());
        visited.insert("orders".to_string());

        let subs = walk_fk_next(
            &schema,
            "order_items",
            &rows,
            &column_offsets,
            2,  // current depth
            -1, // unlimited
            &mut visited,
        );

        // order_items has outgoing FK product_id -> products.id
        // Should produce subscriptions for products WHERE id IN (50, 60)
        let product_subs: Vec<_> = subs.iter().filter(|s| s.table == "products").collect();
        assert_eq!(product_subs.len(), 2);

        let values: HashSet<Vec<u8>> = product_subs.iter().map(|s| {
            if let Predicate::Eq { value, .. } = &s.predicate { value.clone() } else { panic!() }
        }).collect();
        assert!(values.contains(&50i64.to_le_bytes().to_vec()));
        assert!(values.contains(&60i64.to_le_bytes().to_vec()));
    }

    #[test]
    fn walk_avoids_cycles() {
        // Create a circular schema: A.b_id -> B.id, B.a_id -> A.id
        let schema = SchemaGraph::new(vec![
            ForeignKey {
                from_table: "a".into(),
                from_column: "b_id".into(),
                to_table: "b".into(),
                to_column: "id".into(),
            },
            ForeignKey {
                from_table: "b".into(),
                from_column: "a_id".into(),
                to_table: "a".into(),
                to_column: "id".into(),
            },
        ]);

        // Phase 1: root on "a" with pk=1
        let pk = 1i64.to_le_bytes().to_vec();
        let depth1 = walk_fk_depth1(&schema, "a", &[pk.clone()]);

        // "a" has incoming FK from "b" (b.a_id -> a.id)
        assert_eq!(depth1.len(), 1);
        assert_eq!(depth1[0].table, "b");

        // Phase 2: from "b" snapshot, try to walk further
        let b_rows = vec![(10i64.to_le_bytes().to_vec(), vec![0; 16])];
        let mut visited = HashSet::new();
        visited.insert("a".to_string()); // already visited root

        let depth2 = walk_fk_next(
            &schema,
            "b",
            &b_rows,
            &[("a_id".to_string(), 0)],
            1,  // current depth
            -1, // unlimited depth
            &mut visited,
        );

        // "b" has incoming FK from "a" (a.b_id -> b.id) — but "a" is already visited
        // "b" has outgoing FK b.a_id -> a.id — but "a" is already visited
        // So no new subscriptions should be generated
        assert!(
            depth2.is_empty(),
            "cycle should be detected: got {:?}",
            depth2
        );
    }

    #[test]
    fn walk_respects_max_depth() {
        let schema = test_schema();

        // At depth 1 with max_depth=1, walk_fk_next should return empty
        let order_rows = vec![(100i64.to_le_bytes().to_vec(), vec![0; 16])];
        let mut visited = HashSet::new();
        visited.insert("users".to_string());

        let subs = walk_fk_next(
            &schema,
            "orders",
            &order_rows,
            &[],
            1, // current depth
            1, // max depth = 1, so next_depth=2 > 1 => empty
            &mut visited,
        );

        assert!(subs.is_empty());
    }

    #[test]
    fn walk_fk_next_deduplicates_outgoing_values() {
        let schema = test_schema();

        // Two order_items rows with the SAME product_id
        let product_id = 99i64.to_le_bytes();
        let mut row = vec![0u8; 16];
        row[8..16].copy_from_slice(&product_id);

        let rows: Vec<(Vec<u8>, Vec<u8>)> = vec![
            (1i64.to_le_bytes().to_vec(), row.clone()),
            (2i64.to_le_bytes().to_vec(), row),
        ];

        let column_offsets = vec![
            ("order_id".to_string(), 0),
            ("product_id".to_string(), 8),
        ];

        let mut visited = HashSet::new();
        visited.insert("users".to_string());
        visited.insert("orders".to_string());

        let subs = walk_fk_next(
            &schema,
            "order_items",
            &rows,
            &column_offsets,
            2,
            -1,
            &mut visited,
        );

        // Should produce only ONE subscription for products with value 99
        let product_subs: Vec<_> = subs.iter().filter(|s| s.table == "products").collect();
        assert_eq!(product_subs.len(), 1);
    }
}
