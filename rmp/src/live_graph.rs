//! Live reactive graph of FK-related tables.
//!
//! [`LiveGraph`] combines a root [`TableMirror`] with mirrors for all
//! FK-related tables discovered by the graph walker.  It provides a single
//! view over the full object graph with memory tracking.

use crate::mirror::TableMirror;
use crate::schema::SchemaGraph;
use dashmap::DashMap;
use std::sync::Arc;

/// A live, reactive view of a table and all its FK-related data.
///
/// Created by [`PyroConnection::live`](crate::connection::PyroConnection::live).
/// The root mirror holds the primary subscription data; related mirrors hold
/// FK-linked tables discovered by the graph walker.
pub struct LiveGraph {
    /// Name of the root table.
    root_table: String,
    /// Root mirror.
    root: Arc<TableMirror>,
    /// Related mirrors, keyed by table name.
    related: DashMap<String, Arc<TableMirror>>,
    /// Schema graph for FK walking.
    schema: SchemaGraph,
    /// Depth limit (-1 = unlimited, 0 = root only).
    depth: i32,
}

impl LiveGraph {
    /// Create a new LiveGraph with the given root mirror and schema.
    pub fn new(
        root_table: String,
        root: Arc<TableMirror>,
        schema: SchemaGraph,
        depth: i32,
    ) -> Self {
        Self {
            root_table,
            root,
            related: DashMap::new(),
            schema,
            depth,
        }
    }

    /// Get the root table mirror.
    pub fn root(&self) -> &TableMirror {
        &self.root
    }

    /// Get the root table mirror as an Arc.
    pub fn root_arc(&self) -> Arc<TableMirror> {
        Arc::clone(&self.root)
    }

    /// Get the root table name.
    pub fn root_table(&self) -> &str {
        &self.root_table
    }

    /// Get a related table mirror by name.
    pub fn table(&self, name: &str) -> Option<Arc<TableMirror>> {
        self.related.get(name).map(|entry| Arc::clone(entry.value()))
    }

    /// Add a related table mirror.
    pub fn add_related(&self, name: String, mirror: Arc<TableMirror>) {
        self.related.insert(name, mirror);
    }

    /// Get all table names in the graph (root + related).
    pub fn tables(&self) -> Vec<String> {
        let mut names = vec![self.root_table.clone()];
        for entry in self.related.iter() {
            names.push(entry.key().clone());
        }
        names.sort();
        names
    }

    /// Total memory across all mirrors (root + related).
    pub fn total_memory(&self) -> u64 {
        let mut total = self.root.memory_bytes();
        for entry in self.related.iter() {
            total += entry.value().memory_bytes();
        }
        total
    }

    /// Number of related tables (excluding root).
    pub fn related_count(&self) -> usize {
        self.related.len()
    }

    /// Get the schema graph.
    pub fn schema(&self) -> &SchemaGraph {
        &self.schema
    }

    /// Get the depth limit.
    pub fn depth_limit(&self) -> i32 {
        self.depth
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{ColumnInfo, ColumnType, Snapshot};
    use crate::schema::ForeignKey;

    fn test_schema() -> SchemaGraph {
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
        ])
    }

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
    fn live_graph_creates_all_mirrors() {
        let root_mirror = Arc::new(TableMirror::new(1));
        root_mirror.load_snapshot(make_snapshot(1, 1, 5));

        let graph = LiveGraph::new(
            "users".to_string(),
            Arc::clone(&root_mirror),
            test_schema(),
            2,
        );

        // Add related mirrors
        let orders_mirror = Arc::new(TableMirror::new(2));
        orders_mirror.load_snapshot(make_snapshot(2, 1, 3));
        graph.add_related("orders".to_string(), Arc::clone(&orders_mirror));

        let items_mirror = Arc::new(TableMirror::new(3));
        items_mirror.load_snapshot(make_snapshot(3, 1, 2));
        graph.add_related("order_items".to_string(), Arc::clone(&items_mirror));

        // Verify root
        assert_eq!(graph.root().len(), 5);
        assert_eq!(graph.root_table(), "users");

        // Verify related
        assert_eq!(graph.related_count(), 2);
        let orders = graph.table("orders").expect("orders mirror must exist");
        assert_eq!(orders.len(), 3);
        let items = graph.table("order_items").expect("order_items mirror must exist");
        assert_eq!(items.len(), 2);

        // Verify nonexistent table
        assert!(graph.table("nonexistent").is_none());

        // Verify all table names
        let tables = graph.tables();
        assert_eq!(tables.len(), 3);
        assert!(tables.contains(&"users".to_string()));
        assert!(tables.contains(&"orders".to_string()));
        assert!(tables.contains(&"order_items".to_string()));
    }

    #[test]
    fn live_graph_total_memory() {
        let root_mirror = Arc::new(TableMirror::new(1));
        root_mirror.load_snapshot(make_snapshot(1, 1, 10));
        let root_mem = root_mirror.memory_bytes();

        let graph = LiveGraph::new(
            "users".to_string(),
            Arc::clone(&root_mirror),
            test_schema(),
            1,
        );

        let orders_mirror = Arc::new(TableMirror::new(2));
        orders_mirror.load_snapshot(make_snapshot(2, 1, 5));
        let orders_mem = orders_mirror.memory_bytes();
        graph.add_related("orders".to_string(), orders_mirror);

        assert_eq!(graph.total_memory(), root_mem + orders_mem);
        assert!(graph.total_memory() > 0);
    }

    #[test]
    fn live_graph_empty_related() {
        let root_mirror = Arc::new(TableMirror::new(1));
        let graph = LiveGraph::new(
            "users".to_string(),
            root_mirror,
            SchemaGraph::new(vec![]),
            0,
        );

        assert_eq!(graph.related_count(), 0);
        assert_eq!(graph.tables(), vec!["users".to_string()]);
        assert_eq!(graph.total_memory(), 0);
        assert_eq!(graph.depth_limit(), 0);
    }

    #[test]
    fn live_graph_root_arc() {
        let root_mirror = Arc::new(TableMirror::new(1));
        root_mirror.load_snapshot(make_snapshot(1, 1, 3));

        let graph = LiveGraph::new(
            "test".to_string(),
            Arc::clone(&root_mirror),
            SchemaGraph::new(vec![]),
            0,
        );

        let arc = graph.root_arc();
        assert_eq!(arc.len(), 3);
        assert_eq!(arc.sub_id(), root_mirror.sub_id());
    }
}
