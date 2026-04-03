//! FK schema types for reactive graph walking.
//!
//! [`SchemaGraph`] stores foreign key metadata and provides efficient lookups
//! for outgoing (this table references another) and incoming (another table
//! references this one) foreign keys.

/// Foreign key constraint from the database schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForeignKey {
    /// Source table (the table that HAS the FK column).
    pub from_table: String,
    /// Source column (the FK column).
    pub from_column: String,
    /// Target table (the table being referenced).
    pub to_table: String,
    /// Target column (usually the PK).
    pub to_column: String,
}

/// Schema information for FK graph walking.
///
/// Stores all foreign keys and provides O(n) lookups for outgoing and incoming
/// references from/to a given table.  For the typical schema sizes in LiveSync
/// (tens of tables), linear scans are perfectly adequate.
#[derive(Debug, Clone)]
pub struct SchemaGraph {
    /// All foreign keys in the database.
    fks: Vec<ForeignKey>,
}

impl SchemaGraph {
    /// Create a new schema graph from a list of foreign keys.
    pub fn new(fks: Vec<ForeignKey>) -> Self {
        Self { fks }
    }

    /// Get all FKs where `table` is the source (outgoing references).
    ///
    /// Example: if `orders` has FK `user_id -> users.id`, calling
    /// `outgoing("orders")` returns that FK.
    pub fn outgoing(&self, table: &str) -> Vec<&ForeignKey> {
        self.fks.iter().filter(|fk| fk.from_table == table).collect()
    }

    /// Get all FKs where `table` is the target (incoming references).
    ///
    /// Example: if `orders.user_id -> users.id`, calling
    /// `incoming("users")` returns that FK.
    pub fn incoming(&self, table: &str) -> Vec<&ForeignKey> {
        self.fks.iter().filter(|fk| fk.to_table == table).collect()
    }

    /// Get all foreign keys in the schema.
    pub fn all_fks(&self) -> &[ForeignKey] {
        &self.fks
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
            ForeignKey {
                from_table: "order_items".into(),
                from_column: "product_id".into(),
                to_table: "products".into(),
                to_column: "id".into(),
            },
        ])
    }

    #[test]
    fn schema_graph_outgoing() {
        let schema = test_schema();

        let out_orders = schema.outgoing("orders");
        assert_eq!(out_orders.len(), 1);
        assert_eq!(out_orders[0].to_table, "users");
        assert_eq!(out_orders[0].from_column, "user_id");

        let out_items = schema.outgoing("order_items");
        assert_eq!(out_items.len(), 2);
        let target_tables: Vec<&str> = out_items.iter().map(|fk| fk.to_table.as_str()).collect();
        assert!(target_tables.contains(&"orders"));
        assert!(target_tables.contains(&"products"));

        // users has no outgoing FKs
        assert!(schema.outgoing("users").is_empty());
        // products has no outgoing FKs
        assert!(schema.outgoing("products").is_empty());
    }

    #[test]
    fn schema_graph_incoming() {
        let schema = test_schema();

        // users is referenced by orders.user_id
        let inc_users = schema.incoming("users");
        assert_eq!(inc_users.len(), 1);
        assert_eq!(inc_users[0].from_table, "orders");

        // orders is referenced by order_items.order_id
        let inc_orders = schema.incoming("orders");
        assert_eq!(inc_orders.len(), 1);
        assert_eq!(inc_orders[0].from_table, "order_items");

        // products is referenced by order_items.product_id
        let inc_products = schema.incoming("products");
        assert_eq!(inc_products.len(), 1);
        assert_eq!(inc_products[0].from_table, "order_items");

        // order_items has no incoming FKs
        assert!(schema.incoming("order_items").is_empty());
    }

    #[test]
    fn schema_graph_nonexistent_table() {
        let schema = test_schema();
        assert!(schema.outgoing("nonexistent").is_empty());
        assert!(schema.incoming("nonexistent").is_empty());
    }

    #[test]
    fn schema_graph_empty() {
        let schema = SchemaGraph::new(vec![]);
        assert!(schema.outgoing("anything").is_empty());
        assert!(schema.incoming("anything").is_empty());
        assert!(schema.all_fks().is_empty());
    }
}
