//! Query result types: [`Row`], [`Value`], and [`QueryResult`].

use std::collections::HashMap;
use std::sync::Arc;
use serde::{Deserialize, Serialize};

// ── Value ────────────────────────────────────────────────────────────────────

/// A dynamically-typed SQL value.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Value {
    /// SQL `NULL`.
    Null,
    /// A boolean value.
    Bool(bool),
    /// A 64-bit signed integer.
    Int(i64),
    /// A 64-bit floating-point number.
    Float(f64),
    /// A UTF-8 text string.
    Text(String),
    // NOTE: `Bytes` is intentionally omitted from serde untagged because
    // JSON has no native binary type.  Binary columns are base64-encoded
    // as `Text` on the wire.
}

impl From<i32> for Value {
    fn from(v: i32) -> Self {
        Self::Int(i64::from(v))
    }
}

impl From<i64> for Value {
    fn from(v: i64) -> Self {
        Self::Int(v)
    }
}

impl From<f64> for Value {
    fn from(v: f64) -> Self {
        Self::Float(v)
    }
}

impl From<bool> for Value {
    fn from(v: bool) -> Self {
        Self::Bool(v)
    }
}

impl From<String> for Value {
    fn from(v: String) -> Self {
        Self::Text(v)
    }
}

impl From<&str> for Value {
    fn from(v: &str) -> Self {
        Self::Text(v.to_owned())
    }
}

// ── FromValue trait ──────────────────────────────────────────────────────────

/// Trait for extracting a typed value from a [`Value`].
pub trait FromValue: Sized {
    /// Try to convert a [`Value`] into `Self`.
    fn from_value(v: &Value) -> Option<Self>;
}

impl FromValue for i64 {
    #[inline]
    fn from_value(v: &Value) -> Option<Self> {
        match v {
            Value::Int(n) => Some(*n),
            _ => None,
        }
    }
}

impl FromValue for f64 {
    #[inline]
    fn from_value(v: &Value) -> Option<Self> {
        match v {
            Value::Float(f) => Some(*f),
            Value::Int(n) => Some(*n as f64),
            _ => None,
        }
    }
}

impl FromValue for bool {
    #[inline]
    fn from_value(v: &Value) -> Option<Self> {
        match v {
            Value::Bool(b) => Some(*b),
            _ => None,
        }
    }
}

impl FromValue for String {
    #[inline]
    fn from_value(v: &Value) -> Option<Self> {
        match v {
            Value::Text(s) => Some(s.clone()),
            _ => None,
        }
    }
}

// ── Row ──────────────────────────────────────────────────────────────────────

/// Shared column metadata — allocated once per QueryResult, shared across all rows.
#[derive(Debug, Clone)]
pub struct ColumnMeta {
    /// Column names in order.
    pub names: Vec<String>,
    /// Name → index lookup for O(1) `get()` by name.
    pub index: HashMap<String, usize>,
}

impl ColumnMeta {
    /// Build column metadata with precomputed index map.
    pub fn new(names: Vec<String>) -> Arc<Self> {
        let index = names.iter().enumerate().map(|(i, n)| (n.clone(), i)).collect();
        Arc::new(Self { names, index })
    }
}

/// A single result row with named columns.
///
/// Columns are shared via `Arc<ColumnMeta>` across all rows in a result set —
/// zero per-row allocation for column metadata.
#[derive(Debug, Clone)]
pub struct Row {
    meta: Arc<ColumnMeta>,
    values: Vec<Value>,
}

impl Row {
    /// Create a new row from shared column metadata and values.
    ///
    /// # Panics
    ///
    /// Panics if `meta.names.len() != values.len()`.
    pub fn new(meta: Arc<ColumnMeta>, values: Vec<Value>) -> Self {
        assert_eq!(meta.names.len(), values.len(), "column/value count mismatch");
        Self { meta, values }
    }

    /// Backward-compatible constructor (clones column names into new ColumnMeta).
    pub fn from_columns(columns: Vec<String>, values: Vec<Value>) -> Self {
        Self::new(ColumnMeta::new(columns), values)
    }

    /// Get a typed value by column name (O(1) HashMap lookup).
    #[inline]
    pub fn get<T: FromValue>(&self, column: &str) -> Option<T> {
        let idx = *self.meta.index.get(column)?;
        T::from_value(&self.values[idx])
    }

    /// Get a typed value by column index.
    #[inline]
    pub fn get_idx<T: FromValue>(&self, idx: usize) -> Option<T> {
        self.values.get(idx).and_then(T::from_value)
    }

    /// The column names for this row.
    pub fn columns(&self) -> &[String] {
        &self.meta.names
    }

    /// The raw values in column order.
    pub fn values(&self) -> &[Value] {
        &self.values
    }
}

// ── QueryResult ──────────────────────────────────────────────────────────────

/// The result of a query execution.
#[derive(Debug, Clone)]
pub struct QueryResult {
    /// Column names in order.
    pub columns: Vec<String>,
    /// Result rows.
    pub rows: Vec<Row>,
    /// Number of rows affected (for DML statements).
    pub rows_affected: u64,
}
