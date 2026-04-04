//! Local SQL query engine for mirror data.
//!
//! Executes simple `SELECT` and `FIND` (PyroSQL native syntax) queries directly
//! against [`TableMirror`] data, avoiding network round-trips for queries that
//! can be resolved locally.
//!
//! Supported SQL subset:
//! - `SELECT cols FROM table [WHERE cond] [ORDER BY col [ASC|DESC]] [LIMIT n]`
//! - `SELECT cols FROM t1 [alias] JOIN t2 [alias] ON cond [WHERE cond]`
//!
//! Supported PyroSQL native syntax:
//! - `FIND table` (all rows)
//! - `FIND table WHERE cond`
//! - `FIND table.col1, table.col2 [WHERE cond]`
//! - `FIND TOP N table [WHERE cond] [SORT BY col [ASC|DESC]]`
//! - `FIND table WITH table2 ON cond [WHERE cond]`
//!
//! Anything more complex (GROUP BY, aggregates, subqueries, UNION, window
//! functions, 3+ table joins, FIND UNIQUE/COUNT/SUM) returns `None` so the
//! caller falls back to the server.

use crate::mirror::TableMirror;
use crate::protocol::{ColumnInfo, ColumnType};
use crate::row::{encode_row, raw_field_decode, raw_field_eq, raw_field_offset, Row, Value};
use dashmap::DashMap;
use parking_lot::RwLock;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

// ── OrderableValue: Value wrapper with total Ord for BTreeMap keys ───────────

/// A wrapper around `Value` that implements `Eq + Ord` for use as BTreeMap keys.
/// NaN floats are ordered after all other floats (consistent with total_cmp).
#[derive(Debug, Clone)]
struct OrderableValue(Value);

impl PartialEq for OrderableValue {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == std::cmp::Ordering::Equal
    }
}
impl Eq for OrderableValue {}

impl PartialOrd for OrderableValue {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrderableValue {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (&self.0, &other.0) {
            (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
            (Value::Null, _) => std::cmp::Ordering::Less,
            (_, Value::Null) => std::cmp::Ordering::Greater,
            (Value::Int64(a), Value::Int64(b)) => a.cmp(b),
            (Value::Float64(a), Value::Float64(b)) => a.total_cmp(b),
            (Value::Text(a), Value::Text(b)) => a.cmp(b),
            (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
            (Value::Bytes(a), Value::Bytes(b)) => a.cmp(b),
            // Cross-type: Int64 vs Float64
            (Value::Int64(a), Value::Float64(b)) => (*a as f64).total_cmp(b),
            (Value::Float64(a), Value::Int64(b)) => a.total_cmp(&(*b as f64)),
            // Different types: order by a type tag number for deterministic ordering
            _ => type_tag_ord(&self.0).cmp(&type_tag_ord(&other.0)),
        }
    }
}

/// Assign a stable numeric tag to each Value variant for cross-type Ord.
fn type_tag_ord(v: &Value) -> u8 {
    match v {
        Value::Null => 0,
        Value::Bool(_) => 1,
        Value::Int64(_) => 2,
        Value::Float64(_) => 3,
        Value::Text(_) => 4,
        Value::Bytes(_) => 5,
    }
}

/// Global query pattern cache: SQL string → parsed SELECT.
/// Avoids re-parsing the same SQL pattern on every call.
fn query_cache() -> &'static RwLock<HashMap<String, Option<ParsedSelect>>> {
    static CACHE: std::sync::OnceLock<RwLock<HashMap<String, Option<ParsedSelect>>>> =
        std::sync::OnceLock::new();
    CACHE.get_or_init(|| RwLock::new(HashMap::with_capacity(256)))
}

/// Secondary index for a single table mirror.
///
/// Maps `(column_index, stringified_value)` to a list of PK bytes, enabling
/// O(1) equality lookups on any column without scanning the full mirror.
///
/// Also maintains lazy range indexes (BTreeMap per column) for range predicates
/// and sorted indexes (Vec of PKs sorted by column value) for ORDER BY + LIMIT.
pub struct MirrorIndex {
    /// Maps (col_idx, value_key) → Vec<pk_bytes>.
    by_column: HashMap<(usize, Vec<u8>), Vec<Vec<u8>>>,
    /// Range indexes: col_idx → BTreeMap<OrderableValue, Vec<pk_bytes>>.
    /// Built lazily on first range query against a column.
    range_indexes: HashMap<usize, BTreeMap<OrderableValue, Vec<Vec<u8>>>>,
    /// Sorted indexes: col_idx → Vec<(OrderableValue, pk_bytes)> sorted by value.
    /// Built lazily on first ORDER BY query against a column.
    sorted_indexes: HashMap<usize, Vec<(OrderableValue, Vec<u8>)>>,
    /// The mirror version at the time this index was built.
    version: u64,
    /// The mirror row count at the time this index was built.
    row_count: usize,
}

impl MirrorIndex {
    /// Build a secondary index from all rows in a mirror.
    fn build(mirror: &TableMirror, columns: &[ColumnInfo]) -> Self {
        let mut by_column: HashMap<(usize, Vec<u8>), Vec<Vec<u8>>> =
            HashMap::with_capacity(columns.len() * mirror.len());
        let version = mirror.version();
        let row_count = mirror.len();

        for (pk, raw) in mirror.iter() {
            for (i, col) in columns.iter().enumerate() {
                let key_bytes = Self::extract_field_key(&raw, i, col.type_tag);
                by_column
                    .entry((i, key_bytes))
                    .or_default()
                    .push(pk.clone());
            }
        }

        Self {
            by_column,
            range_indexes: HashMap::new(),
            sorted_indexes: HashMap::new(),
            version,
            row_count,
        }
    }

    /// Extract the raw key bytes for a field value (used as index key).
    /// For Int64/Float64/Bool, use the raw encoded bytes.
    /// For Text/Bytes, use the raw bytes directly.
    fn extract_field_key(raw: &[u8], col_idx: usize, _type_tag: ColumnType) -> Vec<u8> {
        match crate::row::raw_field_offset(raw, col_idx) {
            Some((offset, len)) => raw[offset..offset + len].to_vec(),
            None => vec![0xFF, 0xFF, 0xFF, 0xFF], // NULL sentinel key
        }
    }

    /// Look up all PK bytes matching a column equality condition.
    fn lookup(&self, col_idx: usize, literal: &Value) -> Option<&Vec<Vec<u8>>> {
        let key_bytes = Self::value_to_key_bytes(literal);
        self.by_column.get(&(col_idx, key_bytes))
    }

    /// Convert a Value to the raw key bytes for index lookup.
    fn value_to_key_bytes(v: &Value) -> Vec<u8> {
        match v {
            Value::Int64(n) => n.to_le_bytes().to_vec(),
            Value::Float64(f) => f.to_le_bytes().to_vec(),
            Value::Text(s) => s.as_bytes().to_vec(),
            Value::Bool(b) => vec![if *b { 1 } else { 0 }],
            Value::Bytes(b) => b.clone(),
            Value::Null => vec![0xFF, 0xFF, 0xFF, 0xFF],
        }
    }

    /// Build a range index (BTreeMap) for a specific column from mirror data.
    fn build_range_index(mirror: &TableMirror, columns: &[ColumnInfo], col_idx: usize) -> BTreeMap<OrderableValue, Vec<Vec<u8>>> {
        let mut btree: BTreeMap<OrderableValue, Vec<Vec<u8>>> = BTreeMap::new();
        let col_type = columns[col_idx].type_tag;
        for (pk, raw) in mirror.iter() {
            let val = raw_field_decode(&raw, col_idx, col_type);
            btree.entry(OrderableValue(val)).or_default().push(pk);
        }
        btree
    }

    /// Build a sorted index (Vec sorted by column value) for a specific column.
    fn build_sorted_index(mirror: &TableMirror, columns: &[ColumnInfo], col_idx: usize) -> Vec<(OrderableValue, Vec<u8>)> {
        let col_type = columns[col_idx].type_tag;
        let mut entries: Vec<(OrderableValue, Vec<u8>)> = mirror
            .iter()
            .map(|(pk, raw)| {
                let val = raw_field_decode(&raw, col_idx, col_type);
                (OrderableValue(val), pk)
            })
            .collect();
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        entries
    }

    /// Get range query results for a column with a comparison operator.
    /// Returns PK bytes for matching rows, or None if range index not available.
    fn range_lookup(
        &self,
        col_idx: usize,
        op: &CmpOp,
        literal: &Value,
    ) -> Option<Vec<Vec<u8>>> {
        let btree = self.range_indexes.get(&col_idx)?;
        let key = OrderableValue(literal.clone());
        let pks: Vec<Vec<u8>> = match op {
            CmpOp::Gt => btree
                .range((std::ops::Bound::Excluded(key), std::ops::Bound::Unbounded))
                .flat_map(|(_, pks)| pks.iter().cloned())
                .collect(),
            CmpOp::Gte => btree
                .range((std::ops::Bound::Included(key), std::ops::Bound::Unbounded))
                .flat_map(|(_, pks)| pks.iter().cloned())
                .collect(),
            CmpOp::Lt => btree
                .range((std::ops::Bound::Unbounded, std::ops::Bound::Excluded(key)))
                .flat_map(|(_, pks)| pks.iter().cloned())
                .collect(),
            CmpOp::Lte => btree
                .range((std::ops::Bound::Unbounded, std::ops::Bound::Included(key)))
                .flat_map(|(_, pks)| pks.iter().cloned())
                .collect(),
            _ => return None,
        };
        Some(pks)
    }

    /// Get the top-N PKs by column value for ORDER BY + LIMIT.
    /// Returns None if sorted index not available for this column.
    fn sorted_lookup(
        &self,
        col_idx: usize,
        descending: bool,
        limit: usize,
    ) -> Option<Vec<Vec<u8>>> {
        let sorted = self.sorted_indexes.get(&col_idx)?;
        let pks: Vec<Vec<u8>> = if descending {
            sorted.iter().rev().take(limit).map(|(_, pk)| pk.clone()).collect()
        } else {
            sorted.iter().take(limit).map(|(_, pk)| pk.clone()).collect()
        };
        Some(pks)
    }
}

/// Global secondary index cache: (table_name, instance_id) → MirrorIndex.
/// Keyed by both table name and mirror instance ID to avoid cross-test pollution.
fn index_cache() -> &'static RwLock<HashMap<(String, u64), MirrorIndex>> {
    static CACHE: std::sync::OnceLock<RwLock<HashMap<(String, u64), MirrorIndex>>> =
        std::sync::OnceLock::new();
    CACHE.get_or_init(|| RwLock::new(HashMap::new()))
}

/// Get or rebuild the secondary index for a table.
fn get_or_build_index(
    table: &str,
    mirror: &TableMirror,
    columns: &[ColumnInfo],
) {
    let key = (table.to_string(), mirror.instance_id());
    // Fast check with read lock
    {
        let cache = index_cache().read();
        if let Some(idx) = cache.get(&key) {
            if idx.version == mirror.version() && idx.row_count == mirror.len() {
                return;
            }
        }
    }
    // Rebuild with write lock
    let idx = MirrorIndex::build(mirror, columns);
    let mut cache = index_cache().write();
    cache.insert(key, idx);
}

/// Try to use the secondary index for a single equality condition.
/// Returns the list of PK bytes if the index can serve this query.
fn index_lookup_eq(
    table: &str,
    mirror: &TableMirror,
    col_idx: usize,
    literal: &Value,
) -> Option<Vec<Vec<u8>>> {
    let key = (table.to_string(), mirror.instance_id());
    let cache = index_cache().read();
    let idx = cache.get(&key)?;
    idx.lookup(col_idx, literal).cloned()
}

/// Ensure the range index exists for a specific column, building it lazily if needed.
fn ensure_range_index(
    table: &str,
    mirror: &TableMirror,
    columns: &[ColumnInfo],
    col_idx: usize,
) {
    // First ensure the base SI index exists
    get_or_build_index(table, mirror, columns);
    let key = (table.to_string(), mirror.instance_id());
    // Check if range index already exists for this column
    {
        let cache = index_cache().read();
        if let Some(idx) = cache.get(&key) {
            if idx.range_indexes.contains_key(&col_idx) {
                return;
            }
        }
    }
    // Build range index under write lock
    let btree = MirrorIndex::build_range_index(mirror, columns, col_idx);
    let mut cache = index_cache().write();
    if let Some(idx) = cache.get_mut(&key) {
        idx.range_indexes.insert(col_idx, btree);
    }
}

/// Look up PKs matching a range condition via the range index.
fn index_lookup_range(
    table: &str,
    mirror: &TableMirror,
    col_idx: usize,
    op: &CmpOp,
    literal: &Value,
) -> Option<Vec<Vec<u8>>> {
    let key = (table.to_string(), mirror.instance_id());
    let cache = index_cache().read();
    let idx = cache.get(&key)?;
    idx.range_lookup(col_idx, op, literal)
}

/// Ensure the sorted index exists for a specific column, building it lazily if needed.
fn ensure_sorted_index(
    table: &str,
    mirror: &TableMirror,
    columns: &[ColumnInfo],
    col_idx: usize,
) {
    // First ensure the base SI index exists
    get_or_build_index(table, mirror, columns);
    let key = (table.to_string(), mirror.instance_id());
    {
        let cache = index_cache().read();
        if let Some(idx) = cache.get(&key) {
            if idx.sorted_indexes.contains_key(&col_idx) {
                return;
            }
        }
    }
    let sorted = MirrorIndex::build_sorted_index(mirror, columns, col_idx);
    let mut cache = index_cache().write();
    if let Some(idx) = cache.get_mut(&key) {
        idx.sorted_indexes.insert(col_idx, sorted);
    }
}

/// Look up top-N PKs by column value for ORDER BY + LIMIT.
fn index_lookup_sorted(
    table: &str,
    mirror: &TableMirror,
    col_idx: usize,
    descending: bool,
    limit: usize,
) -> Option<Vec<Vec<u8>>> {
    let key = (table.to_string(), mirror.instance_id());
    let cache = index_cache().read();
    let idx = cache.get(&key)?;
    idx.sorted_lookup(col_idx, descending, limit)
}

/// Result of a locally-executed query.
pub struct LocalResult {
    /// Column metadata for the result set.
    pub columns: Vec<ColumnInfo>,
    /// Decoded rows matching the query.
    pub rows: Vec<Row>,
}

// ── SQL parser (regex-free, simple string matching) ─────────────────────────

/// A parsed SELECT query.
#[derive(Debug, Clone)]
struct ParsedSelect {
    /// Column names to project ("*" means all).
    select_cols: Vec<SelectCol>,
    /// Primary table.
    from_table: TableRef,
    /// Optional JOIN.
    join: Option<JoinClause>,
    /// WHERE conditions (ANDed together).
    where_conds: Vec<Condition>,
    /// ORDER BY clause.
    order_by: Option<OrderBy>,
    /// LIMIT value.
    limit: Option<usize>,
}

#[derive(Debug, Clone)]
struct SelectCol {
    /// Optional table alias prefix (e.g. "p" in "p.name").
    table_alias: Option<String>,
    /// Column name, or "*" for all.
    name: String,
}

#[derive(Debug, Clone)]
struct TableRef {
    name: String,
    alias: Option<String>,
}

#[derive(Debug, Clone)]
struct JoinClause {
    table: TableRef,
    /// Left side of ON: (alias, column).
    left: (String, String),
    /// Right side of ON: (alias, column).
    right: (String, String),
}

#[derive(Debug, Clone)]
enum CmpOp {
    Eq,
    Gt,
    Lt,
    Gte,
    Lte,
    Ne,
    In,
}

#[derive(Debug, Clone)]
struct Condition {
    /// Optional table alias.
    table_alias: Option<String>,
    column: String,
    op: CmpOp,
    /// For IN, multiple values; otherwise single value.
    values: Vec<String>,
}

#[derive(Debug, Clone)]
struct OrderBy {
    table_alias: Option<String>,
    column: String,
    descending: bool,
}

/// Normalize SQL for parsing: collapse whitespace, trim.
fn normalize_sql(sql: &str) -> String {
    let mut result = String::with_capacity(sql.len());
    let mut last_was_space = false;
    for ch in sql.chars() {
        if ch.is_whitespace() {
            if !last_was_space && !result.is_empty() {
                result.push(' ');
                last_was_space = true;
            }
        } else {
            result.push(ch);
            last_was_space = false;
        }
    }
    result.trim().to_string()
}

/// Check if the SQL contains features we don't handle locally.
fn has_unsupported_features(upper: &str) -> bool {
    // GROUP BY, HAVING, aggregates, subqueries, UNION, window functions
    let blocklist = [
        "GROUP BY",
        "HAVING",
        "COUNT(",
        "SUM(",
        "AVG(",
        "MIN(",
        "MAX(",
        "UNION",
        "INTERSECT",
        "EXCEPT",
        "OVER(",
        "OVER (",
        "CASE ",
        "EXISTS(",
        "EXISTS (",
    ];
    for kw in &blocklist {
        if upper.contains(kw) {
            return true;
        }
    }
    // Subquery: SELECT inside parens (after the first SELECT)
    if let Some(rest) = upper.strip_prefix("SELECT") {
        if rest.contains("SELECT") {
            return true;
        }
    }
    false
}

/// Parse a table reference like "products p" or "products AS p" or just "products".
fn parse_table_ref(s: &str) -> TableRef {
    let s = s.trim();
    let parts: Vec<&str> = s.split_whitespace().collect();
    match parts.len() {
        1 => TableRef {
            name: parts[0].to_lowercase(),
            alias: None,
        },
        2 => {
            let name = parts[0].to_lowercase();
            let alias = parts[1].to_lowercase();
            // Skip "AS" keyword
            if alias == "as" {
                TableRef { name, alias: None }
            } else {
                TableRef {
                    name,
                    alias: Some(alias),
                }
            }
        }
        3 if parts[1].eq_ignore_ascii_case("AS") => TableRef {
            name: parts[0].to_lowercase(),
            alias: Some(parts[2].to_lowercase()),
        },
        _ => TableRef {
            name: s.to_lowercase(),
            alias: None,
        },
    }
}

/// Split a qualified column ref "alias.col" into (Some(alias), col) or (None, col).
fn split_qualified(s: &str) -> (Option<String>, String) {
    let s = s.trim();
    if let Some(dot_pos) = s.find('.') {
        let alias = s[..dot_pos].trim().to_lowercase();
        let col = s[dot_pos + 1..].trim().to_lowercase();
        (Some(alias), col)
    } else {
        (None, s.to_lowercase())
    }
}

/// Strip surrounding single quotes from a string literal.
fn strip_quotes(s: &str) -> String {
    let s = s.trim();
    if s.len() >= 2 && s.starts_with('\'') && s.ends_with('\'') {
        s[1..s.len() - 1].to_string()
    } else {
        s.to_string()
    }
}

/// Parse a WHERE clause into conditions. Only supports simple AND-chained conditions.
fn parse_where(where_str: &str) -> Option<Vec<Condition>> {
    let mut conditions = Vec::new();
    // Split on AND (case-insensitive)
    let upper = where_str.to_uppercase();
    let mut parts = Vec::new();
    let mut last = 0;
    // Find all " AND " boundaries
    let bytes = upper.as_bytes();
    let orig_bytes = where_str.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if i + 5 <= bytes.len()
            && &bytes[i..i + 5] == b" AND "
        {
            parts.push(std::str::from_utf8(&orig_bytes[last..i]).unwrap().trim());
            last = i + 5;
            i += 5;
        } else {
            i += 1;
        }
    }
    parts.push(std::str::from_utf8(&orig_bytes[last..]).unwrap().trim());

    for part in parts {
        if part.is_empty() {
            continue;
        }
        if let Some(cond) = parse_condition(part) {
            conditions.push(cond);
        } else {
            // Can't parse this condition -- bail to server
            return None;
        }
    }
    Some(conditions)
}

/// Parse a single condition like "col = 42" or "col IN (1, 2, 3)".
fn parse_condition(s: &str) -> Option<Condition> {
    let s = s.trim();
    let upper = s.to_uppercase();

    // Check for IN
    if let Some(in_pos) = upper.find(" IN ") {
        let col_part = s[..in_pos].trim();
        let vals_part = s[in_pos + 4..].trim();
        let (alias, col) = split_qualified(col_part);

        // Parse "(val1, val2, ...)"
        let inner = vals_part.trim_start_matches('(').trim_end_matches(')');
        let values: Vec<String> = inner
            .split(',')
            .map(|v| strip_quotes(v.trim()))
            .collect();

        return Some(Condition {
            table_alias: alias,
            column: col,
            op: CmpOp::In,
            values,
        });
    }

    // Try operators in order of length (>= and <= before > and <, != before =)
    let ops = [
        (">=", CmpOp::Gte),
        ("<=", CmpOp::Lte),
        ("!=", CmpOp::Ne),
        ("<>", CmpOp::Ne),
        (">", CmpOp::Gt),
        ("<", CmpOp::Lt),
        ("=", CmpOp::Eq),
    ];

    for (op_str, op) in &ops {
        if let Some(pos) = s.find(op_str) {
            // Make sure we don't match >= when looking for >
            // (already handled by checking longer ops first)
            let col_part = s[..pos].trim();
            let val_part = s[pos + op_str.len()..].trim();
            let (alias, col) = split_qualified(col_part);
            let val = strip_quotes(val_part);
            return Some(Condition {
                table_alias: alias,
                column: col,
                op: op.clone(),
                values: vec![val],
            });
        }
    }

    None
}


/// Find a keyword position in the upper-cased SQL, respecting word boundaries.
fn find_keyword(upper: &str, kw: &str) -> Option<usize> {
    let mut start = 0;
    loop {
        if let Some(pos) = upper[start..].find(kw) {
            let abs = start + pos;
            // Check word boundary before
            let before_ok = abs == 0 || upper.as_bytes()[abs - 1].is_ascii_whitespace();
            let after_pos = abs + kw.len();
            let after_ok =
                after_pos >= upper.len() || upper.as_bytes()[after_pos].is_ascii_whitespace();
            if before_ok && after_ok {
                return Some(abs);
            }
            start = abs + 1;
        } else {
            return None;
        }
    }
}

/// Parse a SELECT statement. Returns None if too complex.
fn parse_select(sql: &str) -> Option<ParsedSelect> {
    let normalized = normalize_sql(sql);
    let upper = normalized.to_uppercase();

    if !upper.starts_with("SELECT ") {
        return None;
    }

    if has_unsupported_features(&upper) {
        return None;
    }

    // Find FROM position
    let from_pos = find_keyword(&upper, "FROM")?;

    // Parse select columns
    let select_str = normalized[7..from_pos].trim();
    let select_cols = parse_select_cols(select_str);

    // Find extent of FROM clause (up to WHERE, ORDER BY, LIMIT, or JOIN)
    let after_from = &normalized[from_pos + 5..];
    let upper_after = &upper[from_pos + 5..];

    // Check for JOIN
    let join_pos = find_keyword(upper_after, "JOIN");
    let where_pos = find_keyword(upper_after, "WHERE");
    let order_pos = find_keyword(upper_after, "ORDER BY");
    let limit_pos = find_keyword(upper_after, "LIMIT");

    // Parse based on whether we have a JOIN
    let (from_table, join, rest_start) = if let Some(jp) = join_pos {
        // We have a JOIN
        let from_part = after_from[..jp].trim();
        let from_table = parse_table_ref(from_part);

        // Parse JOIN table and ON condition
        let after_join = &after_from[jp + 5..]; // skip "JOIN "
        let upper_join = &upper_after[jp + 5..];

        // Find ON
        let on_pos = find_keyword(upper_join, "ON")?;
        let join_table_str = after_join[..on_pos].trim();
        let join_table = parse_table_ref(join_table_str);

        let after_on = &after_join[on_pos + 3..]; // skip "ON "

        // Find extent of ON condition (up to WHERE, ORDER BY, LIMIT)
        let on_upper = after_on.to_uppercase();
        let on_end = [
            find_keyword(&on_upper, "WHERE"),
            find_keyword(&on_upper, "ORDER BY"),
            find_keyword(&on_upper, "LIMIT"),
        ]
        .iter()
        .filter_map(|x| *x)
        .min()
        .unwrap_or(after_on.len());

        let on_cond = after_on[..on_end].trim();
        // Parse ON condition: "left_col = right_col"
        let eq_pos = on_cond.find('=')?;
        let left_str = on_cond[..eq_pos].trim();
        let right_str = on_cond[eq_pos + 1..].trim();
        let left = split_qualified(left_str);
        let right = split_qualified(right_str);

        let join_clause = JoinClause {
            table: join_table,
            left: (left.0.unwrap_or_default(), left.1),
            right: (right.0.unwrap_or_default(), right.1),
        };

        // Compute the offset in after_from where the rest (WHERE/ORDER/LIMIT) starts
        let rest_offset = jp + 5 + on_pos + 3 + on_end;
        (from_table, Some(join_clause), rest_offset)
    } else {
        // No JOIN: parse simple FROM
        let end = [where_pos, order_pos, limit_pos]
            .iter()
            .filter_map(|x| *x)
            .min()
            .unwrap_or(after_from.len());
        let from_part = after_from[..end].trim();
        let from_table = parse_table_ref(from_part);
        (from_table, None, end)
    };

    let rest = &after_from[rest_start..];
    let rest_upper = rest.to_uppercase();

    // Parse WHERE
    let where_conds = if let Some(wp) = find_keyword(&rest_upper, "WHERE") {
        let after_where = &rest[wp + 6..]; // skip "WHERE "
        let aw_upper = after_where.to_uppercase();
        let where_end = [
            find_keyword(&aw_upper, "ORDER BY"),
            find_keyword(&aw_upper, "LIMIT"),
        ]
        .iter()
        .filter_map(|x| *x)
        .min()
        .unwrap_or(after_where.len());
        let where_str = after_where[..where_end].trim();
        parse_where(where_str)?
    } else {
        Vec::new()
    };

    // Parse ORDER BY
    let order_by = if let Some(op) = find_keyword(&rest_upper, "ORDER BY") {
        let after_order = &rest[op + 9..]; // skip "ORDER BY "
        let ao_upper = after_order.to_uppercase();
        let order_end = find_keyword(&ao_upper, "LIMIT").unwrap_or(after_order.len());
        let order_str = after_order[..order_end].trim();
        let parts: Vec<&str> = order_str.split_whitespace().collect();
        if parts.is_empty() {
            None
        } else {
            let (alias, col) = split_qualified(parts[0]);
            let desc = parts
                .get(1)
                .map(|s| s.eq_ignore_ascii_case("DESC"))
                .unwrap_or(false);
            Some(OrderBy {
                table_alias: alias,
                column: col,
                descending: desc,
            })
        }
    } else {
        None
    };

    // Parse LIMIT
    let limit = if let Some(lp) = find_keyword(&rest_upper, "LIMIT") {
        let after_limit = rest[lp + 6..].trim(); // skip "LIMIT "
        let num_str = after_limit.split_whitespace().next()?;
        num_str.parse::<usize>().ok()
    } else {
        None
    };

    // Check for 3+ table joins
    if join.is_some() {
        let remaining_upper = rest_upper.clone();
        if remaining_upper.contains("JOIN") {
            return None; // 3+ table join
        }
    }

    Some(ParsedSelect {
        select_cols,
        from_table,
        join,
        where_conds,
        order_by,
        limit,
    })
}

/// Try to parse a FIND statement (PyroSQL native syntax) and convert it to a ParsedSelect.
///
/// Patterns:
/// - `FIND table` → SELECT * FROM table
/// - `FIND table WHERE ...` → SELECT * FROM table WHERE ...
/// - `FIND table.col1, table.col2 WHERE ...` → SELECT col1, col2 FROM table WHERE ...
/// - `FIND TOP N table [WHERE ...] [SORT BY col [ASC|DESC]]` → SELECT * FROM table ... ORDER BY col LIMIT N
/// - `FIND table WITH table2 ON ... [WHERE ...]` → JOIN
fn parse_find(sql: &str) -> Option<ParsedSelect> {
    let normalized = normalize_sql(sql);
    let upper = normalized.to_uppercase();

    if !upper.starts_with("FIND ") {
        return None;
    }

    // Reject unsupported FIND features that should go to server
    let find_blocklist = [
        " COUNT", " SUM ", " AVERAGE ", " MIN ", " MAX ",
        "FIND UNIQUE ",
    ];
    for kw in &find_blocklist {
        if upper.contains(kw) {
            return None;
        }
    }
    // Also reject subqueries
    if has_unsupported_features(&upper) {
        return None;
    }

    let after_find = &normalized[5..]; // skip "FIND "
    let upper_after = &upper[5..];

    // Check for TOP N
    let (limit, after_top, upper_top) = if upper_after.starts_with("TOP ") {
        let rest = &after_find[4..]; // skip "TOP "
        let num_end = rest.find(' ').unwrap_or(rest.len());
        let num_str = &rest[..num_end];
        let n = num_str.parse::<usize>().ok()?;
        let remaining = if num_end < rest.len() {
            rest[num_end + 1..].to_string()
        } else {
            String::new()
        };
        let upper_remaining = remaining.to_uppercase();
        (Some(n), remaining, upper_remaining)
    } else {
        (None, after_find.to_string(), upper_after.to_string())
    };

    // Check for WITH (join)
    let with_pos = find_keyword(&upper_top, "WITH");
    // Check for MAYBE WITH (left join — not supported locally, delegate)
    if let Some(mw) = find_keyword(&upper_top, "MAYBE WITH") {
        let _ = mw;
        return None;
    }

    let where_pos = find_keyword(&upper_top, "WHERE");
    let sort_pos = find_keyword(&upper_top, "SORT BY");

    if let Some(wp) = with_pos {
        // JOIN syntax: FIND table WITH table2 ON cond [WHERE cond]
        return parse_find_join(&after_top, &upper_top, wp, limit);
    }

    // Determine if we have table.col syntax or just table name
    // The part before WHERE/SORT BY/end is the "target" portion
    let target_end = [where_pos, sort_pos]
        .iter()
        .filter_map(|x| *x)
        .min()
        .unwrap_or(after_top.len());
    let target = after_top[..target_end].trim();

    // Parse target: could be "table" or "table.col1, table.col2"
    let (table_name, select_cols) = if target.contains('.') {
        // Column references: table.col1, table.col2
        let parts: Vec<&str> = target.split(',').collect();
        let mut table = None;
        let mut cols = Vec::new();
        for part in &parts {
            let part = part.trim();
            if let Some(dot) = part.find('.') {
                let t = part[..dot].trim().to_lowercase();
                let c = part[dot + 1..].trim().to_lowercase();
                if table.is_none() {
                    table = Some(t.clone());
                }
                cols.push(SelectCol {
                    table_alias: Some(t),
                    name: c,
                });
            } else {
                // Plain column name — use table from first qualified ref
                cols.push(SelectCol {
                    table_alias: None,
                    name: part.to_lowercase(),
                });
            }
        }
        (table.unwrap_or_else(|| target.to_lowercase()), cols)
    } else if target.contains(',') {
        // Shouldn't happen without dots, but handle gracefully
        return None;
    } else {
        // Just a table name
        (target.to_lowercase(), vec![SelectCol {
            table_alias: None,
            name: "*".to_string(),
        }])
    };

    // Parse WHERE
    let where_conds = if let Some(wp) = where_pos {
        let after_where = &after_top[wp + 6..]; // skip "WHERE "
        let aw_upper = after_where.to_uppercase();
        let where_end = find_keyword(&aw_upper, "SORT BY").unwrap_or(after_where.len());
        let where_str = after_where[..where_end].trim();
        parse_where(where_str)?
    } else {
        Vec::new()
    };

    // Parse SORT BY
    let order_by = if let Some(sp) = sort_pos {
        let after_sort = &after_top[sp + 8..]; // skip "SORT BY "
        let parts: Vec<&str> = after_sort.trim().split_whitespace().collect();
        if parts.is_empty() {
            None
        } else {
            let (alias, col) = split_qualified(parts[0]);
            let desc = parts
                .get(1)
                .map(|s| s.eq_ignore_ascii_case("DESC"))
                .unwrap_or(false);
            Some(OrderBy {
                table_alias: alias,
                column: col,
                descending: desc,
            })
        }
    } else {
        None
    };

    Some(ParsedSelect {
        select_cols,
        from_table: TableRef {
            name: table_name,
            alias: None,
        },
        join: None,
        where_conds,
        order_by,
        limit,
    })
}

/// Parse FIND ... WITH ... ON ... (join syntax).
fn parse_find_join(
    after_top: &str,
    _upper_top: &str,
    with_pos: usize,
    limit: Option<usize>,
) -> Option<ParsedSelect> {
    // Left table is before WITH
    let left_part = after_top[..with_pos].trim();

    // Parse left side: could be "table" or "table.col1, table.col2"
    let (left_table_name, select_cols) = if left_part.contains('.') {
        let parts: Vec<&str> = left_part.split(',').collect();
        let mut table = None;
        let mut cols = Vec::new();
        for part in &parts {
            let part = part.trim();
            if let Some(dot) = part.find('.') {
                let t = part[..dot].trim().to_lowercase();
                let c = part[dot + 1..].trim().to_lowercase();
                if table.is_none() {
                    table = Some(t.clone());
                }
                cols.push(SelectCol {
                    table_alias: Some(t),
                    name: c,
                });
            }
        }
        (table?, cols)
    } else {
        (left_part.to_lowercase(), vec![SelectCol {
            table_alias: None,
            name: "*".to_string(),
        }])
    };

    // After WITH: "table2 ON cond [WHERE cond] [SORT BY col]"
    let after_with = &after_top[with_pos + 5..]; // skip "WITH "
    let aw_upper = after_with.to_uppercase();

    let on_pos = find_keyword(&aw_upper, "ON")?;
    let right_table_str = after_with[..on_pos].trim();
    let right_table = parse_table_ref(right_table_str);

    let after_on = &after_with[on_pos + 3..]; // skip "ON "
    let on_upper = after_on.to_uppercase();

    let on_end = [
        find_keyword(&on_upper, "WHERE"),
        find_keyword(&on_upper, "SORT BY"),
    ]
    .iter()
    .filter_map(|x| *x)
    .min()
    .unwrap_or(after_on.len());

    let on_cond = after_on[..on_end].trim();
    let eq_pos = on_cond.find('=')?;
    let left_on = on_cond[..eq_pos].trim();
    let right_on = on_cond[eq_pos + 1..].trim();
    let left_pair = split_qualified(left_on);
    let right_pair = split_qualified(right_on);

    let rest = &after_on[on_end..];
    let rest_upper = rest.to_uppercase();

    // WHERE
    let where_conds = if let Some(wp) = find_keyword(&rest_upper, "WHERE") {
        let after_where = &rest[wp + 6..];
        let aw2 = after_where.to_uppercase();
        let end = find_keyword(&aw2, "SORT BY").unwrap_or(after_where.len());
        parse_where(after_where[..end].trim())?
    } else {
        Vec::new()
    };

    // SORT BY
    let order_by = if let Some(sp) = find_keyword(&rest_upper, "SORT BY") {
        let after_sort = rest[sp + 8..].trim();
        let parts: Vec<&str> = after_sort.split_whitespace().collect();
        if parts.is_empty() {
            None
        } else {
            let (alias, col) = split_qualified(parts[0]);
            let desc = parts.get(1).map(|s| s.eq_ignore_ascii_case("DESC")).unwrap_or(false);
            Some(OrderBy { table_alias: alias, column: col, descending: desc })
        }
    } else {
        None
    };

    Some(ParsedSelect {
        select_cols,
        from_table: TableRef {
            name: left_table_name,
            alias: None,
        },
        join: Some(JoinClause {
            table: right_table,
            left: (left_pair.0.unwrap_or_default(), left_pair.1),
            right: (right_pair.0.unwrap_or_default(), right_pair.1),
        }),
        where_conds,
        order_by,
        limit,
    })
}

/// Parse the column list from SELECT.
fn parse_select_cols(s: &str) -> Vec<SelectCol> {
    if s.trim() == "*" {
        return vec![SelectCol {
            table_alias: None,
            name: "*".to_string(),
        }];
    }

    s.split(',')
        .map(|part| {
            let part = part.trim();
            let (alias, name) = split_qualified(part);
            SelectCol {
                table_alias: alias,
                name,
            }
        })
        .collect()
}

// ── Query execution ─────────────────────────────────────────────────────────

/// Execute a SELECT query against local mirrors.
///
/// Returns `Some(LocalResult)` if the query can be resolved locally.
/// Returns `None` if the query is too complex (delegate to server).
pub fn try_execute_local(
    sql: &str,
    mirrors: &DashMap<String, Arc<TableMirror>>,
    schemas: &DashMap<String, Vec<ColumnInfo>>,
) -> Option<LocalResult> {
    // Query pattern cache: avoid re-parsing identical SQL strings.
    let parsed = {
        // Fast path: read lock for cache hit
        {
            let cache = query_cache().read();
            if let Some(cached) = cache.get(sql) {
                match cached.clone() {
                    Some(p) => return if p.join.is_some() {
                        execute_join(&p, mirrors, schemas)
                    } else {
                        execute_simple(&p, mirrors, schemas)
                    },
                    None => return None,
                }
            }
        }
        // Slow path: parse and cache with write lock
        let result = parse_select(sql).or_else(|| parse_find(sql));
        {
            let mut cache = query_cache().write();
            if cache.len() < 4096 {
                cache.insert(sql.to_string(), result.clone());
            }
        }
        match result {
            Some(p) => p,
            None => return None,
        }
    };

    if parsed.join.is_some() {
        execute_join(&parsed, mirrors, schemas)
    } else {
        execute_simple(&parsed, mirrors, schemas)
    }
}

/// Resolve the column indices and pre-parsed literal values for all WHERE conditions.
/// Returns None if any condition references an unknown column.
struct ResolvedCondition {
    col_idx: usize,
    col_type: ColumnType,
    op: CmpOp,
    /// Pre-parsed literal values for comparison.
    literals: Vec<Value>,
}

fn resolve_conditions(
    conditions: &[Condition],
    columns: &[ColumnInfo],
    table_ref: &TableRef,
) -> Option<Vec<ResolvedCondition>> {
    let mut resolved = Vec::with_capacity(conditions.len());
    for cond in conditions {
        if let Some(ref alias) = cond.table_alias {
            let table_alias = table_ref.alias.as_deref().unwrap_or(&table_ref.name);
            if alias != table_alias {
                return None;
            }
        }
        let col_idx = find_column_idx(&cond.column, columns)?;
        let col_type = columns[col_idx].type_tag;
        let literals: Vec<Value> = cond
            .values
            .iter()
            .filter_map(|v| Value::parse_literal(v, col_type))
            .collect();
        if literals.len() != cond.values.len() {
            return None; // Couldn't parse all literals
        }
        resolved.push(ResolvedCondition {
            col_idx,
            col_type,
            op: cond.op.clone(),
            literals,
        });
    }
    Some(resolved)
}

/// Check if a raw row matches all resolved conditions using lazy byte-level comparison.
/// Only decodes individual fields when needed (for range comparisons).
fn matches_raw(raw: &[u8], conditions: &[ResolvedCondition]) -> bool {
    for cond in conditions {
        match cond.op {
            CmpOp::Eq => {
                if cond.literals.len() != 1 {
                    return false;
                }
                if !raw_field_eq(raw, cond.col_idx, &cond.literals[0]) {
                    return false;
                }
            }
            CmpOp::In => {
                let mut found = false;
                for lit in &cond.literals {
                    if raw_field_eq(raw, cond.col_idx, lit) {
                        found = true;
                        break;
                    }
                }
                if !found {
                    return false;
                }
            }
            // For range ops, decode just the single field
            ref op => {
                let val = raw_field_decode(raw, cond.col_idx, cond.col_type);
                if cond.literals.len() != 1 {
                    return false;
                }
                let lit = &cond.literals[0];
                let pass = match op {
                    CmpOp::Ne => &val != lit,
                    CmpOp::Gt => matches!(val.partial_cmp(lit), Some(std::cmp::Ordering::Greater)),
                    CmpOp::Lt => matches!(val.partial_cmp(lit), Some(std::cmp::Ordering::Less)),
                    CmpOp::Gte => matches!(
                        val.partial_cmp(lit),
                        Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
                    ),
                    CmpOp::Lte => matches!(
                        val.partial_cmp(lit),
                        Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
                    ),
                    _ => unreachable!(),
                };
                if !pass {
                    return false;
                }
            }
        }
    }
    true
}

/// Compute the column indices needed for projection pushdown.
/// Returns None if SELECT * (all columns needed).
fn compute_projection_indices(
    select_cols: &[SelectCol],
    columns: &[ColumnInfo],
    order_by: Option<&OrderBy>,
) -> Option<Vec<usize>> {
    if select_cols.len() == 1 && select_cols[0].name == "*" {
        return None; // All columns
    }
    let mut indices: Vec<usize> = Vec::new();
    for sc in select_cols {
        if let Some(idx) = find_column_idx(&sc.name, columns) {
            if !indices.contains(&idx) {
                indices.push(idx);
            }
        }
    }
    // If ORDER BY references a column not in SELECT, add it for sorting
    if let Some(ob) = order_by {
        if let Some(idx) = find_column_idx(&ob.column, columns) {
            if !indices.contains(&idx) {
                indices.push(idx);
            }
        }
    }
    Some(indices)
}

/// Execute a simple single-table SELECT with three optimizations:
///
/// 1. **Range index**: BTreeMap-based sorted index for range predicates (>, <, >=, <=).
///    Built lazily on first range query for a given column, then reused for subsequent queries.
///
/// 2. **Pre-sorted index for ORDER BY + LIMIT**: Vec of PKs sorted by column value.
///    Built lazily on first ORDER BY query, enabling O(LIMIT) retrieval instead of O(N log N) sort.
///    Falls back to partial sort (select_nth_unstable) when index is not yet available.
///
/// 3. **Projection pushdown**: When SELECT specifies a subset of columns, only decode those
///    columns from each raw row, skipping unneeded fields at the byte level.
fn execute_simple(
    parsed: &ParsedSelect,
    mirrors: &DashMap<String, Arc<TableMirror>>,
    schemas: &DashMap<String, Vec<ColumnInfo>>,
) -> Option<LocalResult> {
    let table = &parsed.from_table.name;
    let mirror = mirrors.get(table)?;
    let columns = schemas.get(table)?;
    let columns = columns.value().clone();

    // Pre-resolve conditions to avoid repeated string parsing per row.
    let resolved = resolve_conditions(&parsed.where_conds, &columns, &parsed.from_table);

    // ── Fast path: ORDER BY col [ASC|DESC] LIMIT N with no WHERE ─────────
    // Use pre-sorted index to return top-N directly without scanning/decoding all rows.
    if parsed.where_conds.is_empty() {
        if let Some(ref order) = parsed.order_by {
            if let Some(limit) = parsed.limit {
                if let Some(order_col_idx) = find_column_idx(&order.column, &columns) {
                    // Ensure sorted index exists (lazy build on first call)
                    ensure_sorted_index(table, &mirror, &columns, order_col_idx);
                    if let Some(pks) = index_lookup_sorted(table, &mirror, order_col_idx, order.descending, limit) {
                        // Fetch and decode only the top-N rows
                        let proj = compute_projection_indices(&parsed.select_cols, &columns, None);
                        let mut rows = Vec::with_capacity(pks.len());
                        for pk in &pks {
                            if let Some(row_ref) = mirror.get(pk) {
                                let row = match &proj {
                                    Some(indices) => Row::decode_projected(row_ref.value(), &columns, indices),
                                    None => Row::decode(row_ref.value(), &columns),
                                };
                                rows.push(row);
                            }
                        }
                        // Project columns (remap indices if needed)
                        let (result_columns, rows) = if proj.is_some() {
                            project_from_projected(&parsed.select_cols, &columns, rows, parsed.order_by.as_ref())
                        } else {
                            project_columns(&parsed.select_cols, &columns, rows, None, &parsed.from_table)
                        };
                        return Some(LocalResult { columns: result_columns, rows });
                    }
                }
            }
        }
    }

    // ── Main path: WHERE filtering with index acceleration ────────────────
    let mut rows: Vec<Row> = if let Some(ref resolved_conds) = resolved {
        if resolved_conds.is_empty() {
            // No WHERE, no ORDER BY+LIMIT fast path: decode all rows with projection
            let proj = compute_projection_indices(&parsed.select_cols, &columns, parsed.order_by.as_ref());
            let raw_rows: Vec<Row> = mirror
                .iter()
                .map(|(_pk, raw)| match &proj {
                    Some(indices) => Row::decode_projected(&raw, &columns, indices),
                    None => Row::decode(&raw, &columns),
                })
                .collect();
            // ORDER BY on projected rows
            if let Some(ref order) = parsed.order_by {
                return execute_with_order_and_project(raw_rows, order, parsed.limit, &parsed.select_cols, &columns, &proj);
            }
            // LIMIT without ORDER BY
            let mut result = raw_rows;
            if let Some(limit) = parsed.limit {
                result.truncate(limit);
            }
            let (result_columns, result) = if proj.is_some() {
                project_from_projected(&parsed.select_cols, &columns, result, parsed.order_by.as_ref())
            } else {
                project_columns(&parsed.select_cols, &columns, result, None, &parsed.from_table)
            };
            return Some(LocalResult { columns: result_columns, rows: result });
        } else {
            // Try secondary index: find the first Eq condition for index lookup
            let mut index_pks: Option<Vec<Vec<u8>>> = None;
            for cond in resolved_conds.iter() {
                if matches!(cond.op, CmpOp::Eq) && cond.literals.len() == 1 {
                    get_or_build_index(table, &mirror, &columns);
                    if let Some(pks) = index_lookup_eq(table, &mirror, cond.col_idx, &cond.literals[0]) {
                        index_pks = Some(pks);
                        break;
                    }
                }
            }

            // ── Range index acceleration ──────────────────────────────────
            // If no Eq index hit, try range index for the first range condition
            if index_pks.is_none() {
                for cond in resolved_conds.iter() {
                    match cond.op {
                        CmpOp::Gt | CmpOp::Gte | CmpOp::Lt | CmpOp::Lte => {
                            if cond.literals.len() == 1 {
                                ensure_range_index(table, &mirror, &columns, cond.col_idx);
                                if let Some(pks) = index_lookup_range(table, &mirror, cond.col_idx, &cond.op, &cond.literals[0]) {
                                    index_pks = Some(pks);
                                    break;
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }

            if let Some(pks) = index_pks {
                // Index-driven: fetch only matched PKs, then apply remaining conditions
                // Use projection pushdown for decode
                let proj = compute_projection_indices(&parsed.select_cols, &columns, parsed.order_by.as_ref());
                let mut result = Vec::with_capacity(pks.len());
                for pk in &pks {
                    if let Some(row_ref) = mirror.get(pk) {
                        let raw = row_ref.value();
                        if matches_raw(raw, resolved_conds) {
                            let row = match &proj {
                                Some(indices) => Row::decode_projected(raw, &columns, indices),
                                None => Row::decode(raw, &columns),
                            };
                            result.push(row);
                        }
                    }
                }
                // Handle ORDER BY + LIMIT on projected results
                if let Some(ref order) = parsed.order_by {
                    return execute_with_order_and_project(result, order, parsed.limit, &parsed.select_cols, &columns, &proj);
                }
                if let Some(limit) = parsed.limit {
                    result.truncate(limit);
                }
                let (result_columns, result) = if proj.is_some() {
                    project_from_projected(&parsed.select_cols, &columns, result, parsed.order_by.as_ref())
                } else {
                    project_columns(&parsed.select_cols, &columns, result, None, &parsed.from_table)
                };
                return Some(LocalResult { columns: result_columns, rows: result });
            } else {
                // Full scan with raw byte filtering + projection pushdown
                let proj = compute_projection_indices(&parsed.select_cols, &columns, parsed.order_by.as_ref());
                let result: Vec<Row> = mirror
                    .iter()
                    .filter(|(_pk, raw)| matches_raw(raw, resolved_conds))
                    .map(|(_pk, raw)| match &proj {
                        Some(indices) => Row::decode_projected(&raw, &columns, indices),
                        None => Row::decode(&raw, &columns),
                    })
                    .collect();
                if let Some(ref order) = parsed.order_by {
                    return execute_with_order_and_project(result, order, parsed.limit, &parsed.select_cols, &columns, &proj);
                }
                let mut result = result;
                if let Some(limit) = parsed.limit {
                    result.truncate(limit);
                }
                let (result_columns, result) = if proj.is_some() {
                    project_from_projected(&parsed.select_cols, &columns, result, parsed.order_by.as_ref())
                } else {
                    project_columns(&parsed.select_cols, &columns, result, None, &parsed.from_table)
                };
                return Some(LocalResult { columns: result_columns, rows: result });
            }
        }
    } else {
        // Fallback: decode all, filter with original method
        mirror
            .iter()
            .map(|(_pk, raw)| Row::decode(&raw, &columns))
            .filter(|row| matches_where(row, &parsed.where_conds, &columns, None, &parsed.from_table))
            .collect()
    };

    // ORDER BY (fallback path -- only reached from the else branch above)
    if let Some(ref order) = parsed.order_by {
        if let Some(col_idx) = find_column_idx(&order.column, &columns) {
            let desc = order.descending;
            // Use partial sort (select_nth_unstable) when LIMIT is present
            if let Some(limit) = parsed.limit {
                if limit < rows.len() {
                    rows.select_nth_unstable_by(limit, |a, b| {
                        let cmp = a.values[col_idx]
                            .partial_cmp(&b.values[col_idx])
                            .unwrap_or(std::cmp::Ordering::Equal);
                        if desc { cmp.reverse() } else { cmp }
                    });
                    rows.truncate(limit);
                }
                // Sort the final LIMIT rows
                rows.sort_by(|a, b| {
                    let cmp = a.values[col_idx]
                        .partial_cmp(&b.values[col_idx])
                        .unwrap_or(std::cmp::Ordering::Equal);
                    if desc { cmp.reverse() } else { cmp }
                });
            } else {
                rows.sort_by(|a, b| {
                    let cmp = a.values[col_idx]
                        .partial_cmp(&b.values[col_idx])
                        .unwrap_or(std::cmp::Ordering::Equal);
                    if desc { cmp.reverse() } else { cmp }
                });
            }
        }
    }

    // LIMIT (for non-ORDER BY paths)
    if parsed.order_by.is_none() {
        if let Some(limit) = parsed.limit {
            rows.truncate(limit);
        }
    }

    // Project columns
    let (result_columns, rows) = project_columns(&parsed.select_cols, &columns, rows, None, &parsed.from_table);

    Some(LocalResult {
        columns: result_columns,
        rows,
    })
}

/// Handle ORDER BY + optional LIMIT on already-projected rows, then final column projection.
fn execute_with_order_and_project(
    mut rows: Vec<Row>,
    order: &OrderBy,
    limit: Option<usize>,
    select_cols: &[SelectCol],
    columns: &[ColumnInfo],
    proj: &Option<Vec<usize>>,
) -> Option<LocalResult> {
    // Find the ORDER BY column index in the projected row
    let order_col_idx = if let Some(ref indices) = proj {
        // In projected rows, find which position the ORDER BY column maps to
        let original_idx = find_column_idx(&order.column, columns)?;
        indices.iter().position(|&i| i == original_idx)?
    } else {
        find_column_idx(&order.column, columns)?
    };

    let desc = order.descending;

    // Use partial sort when LIMIT is present and smaller than total rows
    if let Some(limit) = limit {
        if limit < rows.len() {
            rows.select_nth_unstable_by(limit, |a, b| {
                let cmp = a.values[order_col_idx]
                    .partial_cmp(&b.values[order_col_idx])
                    .unwrap_or(std::cmp::Ordering::Equal);
                if desc { cmp.reverse() } else { cmp }
            });
            rows.truncate(limit);
        }
    }
    // Sort the (potentially truncated) result
    rows.sort_by(|a, b| {
        let cmp = a.values[order_col_idx]
            .partial_cmp(&b.values[order_col_idx])
            .unwrap_or(std::cmp::Ordering::Equal);
        if desc { cmp.reverse() } else { cmp }
    });

    // Final projection: remove ORDER BY column if it was added only for sorting
    let (result_columns, rows) = if proj.is_some() {
        project_from_projected(select_cols, columns, rows, Some(order))
    } else {
        let table_ref = TableRef { name: String::new(), alias: None };
        project_columns(select_cols, columns, rows, None, &table_ref)
    };

    Some(LocalResult { columns: result_columns, rows })
}

/// Project columns from an already-projected row.
/// The input rows have values at positions matching `compute_projection_indices` output.
/// We need to map SELECT column names to their positions in the projected row.
fn project_from_projected(
    select_cols: &[SelectCol],
    columns: &[ColumnInfo],
    rows: Vec<Row>,
    order_by: Option<&OrderBy>,
) -> (Vec<ColumnInfo>, Vec<Row>) {
    if select_cols.len() == 1 && select_cols[0].name == "*" {
        return (columns.to_vec(), rows);
    }

    // Reconstruct the projection indices to know the mapping
    let proj_indices = compute_projection_indices(select_cols, columns, order_by)
        .unwrap_or_else(|| (0..columns.len()).collect());

    // For each SELECT column, find its position in the projected row
    let mut out_indices = Vec::new();
    let mut result_cols = Vec::new();

    for sc in select_cols {
        if let Some(orig_idx) = find_column_idx(&sc.name, columns) {
            if let Some(proj_pos) = proj_indices.iter().position(|&i| i == orig_idx) {
                out_indices.push(proj_pos);
                result_cols.push(columns[orig_idx].clone());
            }
        }
    }

    let projected_rows = rows
        .into_iter()
        .map(|row| {
            let values = out_indices.iter().map(|&i| {
                if i < row.values.len() {
                    row.values[i].clone()
                } else {
                    Value::Null
                }
            }).collect();
            Row { values }
        })
        .collect();

    (result_cols, projected_rows)
}


/// Execute a 2-table JOIN query.
fn execute_join(
    parsed: &ParsedSelect,
    mirrors: &DashMap<String, Arc<TableMirror>>,
    schemas: &DashMap<String, Vec<ColumnInfo>>,
) -> Option<LocalResult> {
    let join = parsed.join.as_ref()?;

    let left_table = &parsed.from_table;
    let right_table = &join.table;

    let left_mirror = mirrors.get(&left_table.name)?;
    let right_mirror = mirrors.get(&right_table.name)?;
    let left_cols = schemas.get(&left_table.name)?.value().clone();
    let right_cols = schemas.get(&right_table.name)?.value().clone();

    // Resolve which ON column belongs to which table
    let (left_join_col, right_join_col) = resolve_join_columns(
        &join.left,
        &join.right,
        left_table,
        right_table,
        &left_cols,
        &right_cols,
    )?;

    let left_join_idx = find_column_idx(&left_join_col, &left_cols)?;
    let right_join_idx = find_column_idx(&right_join_col, &right_cols)?;

    // Build combined column list
    let mut combined_cols: Vec<ColumnInfo> = Vec::new();
    let left_alias = left_table.alias.as_deref().unwrap_or(&left_table.name);
    let right_alias = right_table.alias.as_deref().unwrap_or(&right_table.name);

    for c in &left_cols {
        combined_cols.push(ColumnInfo {
            name: format!("{}.{}", left_alias, c.name),
            type_tag: c.type_tag,
        });
    }
    for c in &right_cols {
        combined_cols.push(ColumnInfo {
            name: format!("{}.{}", right_alias, c.name),
            type_tag: c.type_tag,
        });
    }

    // ── Optimization: Filter left rows before joining ──────────────────
    // If there are WHERE conditions that apply to the left table only,
    // filter the left rows first to reduce the join input.
    let left_resolved = {
        let left_alias = left_table.alias.as_deref().unwrap_or(&left_table.name);
        let left_only_conds: Vec<Condition> = parsed
            .where_conds
            .iter()
            .filter(|c| {
                // Conditions that apply to left table (or have no alias and match a left column)
                if let Some(ref a) = c.table_alias {
                    a == left_alias
                } else {
                    find_column_idx(&c.column, &left_cols).is_some()
                        && find_column_idx(&c.column, &right_cols).is_none()
                }
            })
            .cloned()
            .collect();
        resolve_conditions(&left_only_conds, &left_cols, left_table)
    };

    // Use secondary index for left table equality conditions, or scan with lazy decode
    let left_rows: Vec<Row> = if let Some(ref resolved) = left_resolved {
        if !resolved.is_empty() {
            // Try secondary index for an Eq condition on the left table
            let mut idx_pks: Option<Vec<Vec<u8>>> = None;
            for cond in resolved.iter() {
                if matches!(cond.op, CmpOp::Eq) && cond.literals.len() == 1 {
                    get_or_build_index(&left_table.name, &left_mirror, &left_cols);
                    if let Some(pks) = index_lookup_eq(&left_table.name, &left_mirror, cond.col_idx, &cond.literals[0]) {
                        idx_pks = Some(pks);
                        break;
                    }
                }
            }
            if let Some(pks) = idx_pks {
                let mut result = Vec::with_capacity(pks.len());
                for pk in &pks {
                    if let Some(row_ref) = left_mirror.get(pk) {
                        let raw = row_ref.value();
                        if matches_raw(raw, resolved) {
                            result.push(Row::decode(raw, &left_cols));
                        }
                    }
                }
                result
            } else {
                // Scan with lazy decode
                left_mirror
                    .iter()
                    .filter(|(_pk, raw)| matches_raw(raw, resolved))
                    .map(|(_pk, raw)| Row::decode(&raw, &left_cols))
                    .collect()
            }
        } else {
            left_mirror
                .iter()
                .map(|(_pk, raw)| Row::decode(&raw, &left_cols))
                .collect()
        }
    } else {
        left_mirror
            .iter()
            .map(|(_pk, raw)| Row::decode(&raw, &left_cols))
            .collect()
    };

    // ── Hash join: build hash map on right table's join column ──────────
    // Build secondary index on right table, then use it for lookup
    get_or_build_index(&right_table.name, &right_mirror, &right_cols);

    let mut joined_rows: Vec<Row> = Vec::new();
    for l_row in &left_rows {
        let l_val = &l_row.values[left_join_idx];

        // Use secondary index to find matching right rows by join column
        let matching_pks = index_lookup_eq(&right_table.name, &right_mirror, right_join_idx, l_val);
        if let Some(pks) = matching_pks {
            for pk in &pks {
                if let Some(row_ref) = right_mirror.get(pk) {
                    let r_row = Row::decode(row_ref.value(), &right_cols);
                    // Combine row values
                    let mut combined = l_row.values.clone();
                    combined.extend_from_slice(&r_row.values);
                    let combined_row = Row { values: combined };

                    // Apply WHERE filter (for conditions on right table or cross-table)
                    if matches_join_where(
                        &combined_row,
                        &parsed.where_conds,
                        left_table,
                        right_table,
                        &left_cols,
                        &right_cols,
                    ) {
                        joined_rows.push(combined_row);
                    }
                }
            }
        }
        // If no index pks found, the left value has no match -- skip (inner join)
    }

    // ORDER BY
    if let Some(ref order) = parsed.order_by {
        if let Some(col_idx) = find_combined_col_idx(
            &order.table_alias,
            &order.column,
            left_table,
            right_table,
            &left_cols,
            &right_cols,
        ) {
            let desc = order.descending;
            joined_rows.sort_by(|a, b| {
                let cmp = a.values[col_idx]
                    .partial_cmp(&b.values[col_idx])
                    .unwrap_or(std::cmp::Ordering::Equal);
                if desc {
                    cmp.reverse()
                } else {
                    cmp
                }
            });
        }
    }

    // LIMIT
    if let Some(limit) = parsed.limit {
        joined_rows.truncate(limit);
    }

    // Project columns
    let (result_columns, result_rows) = project_join_columns(
        &parsed.select_cols,
        left_table,
        right_table,
        &left_cols,
        &right_cols,
        joined_rows,
    );

    Some(LocalResult {
        columns: result_columns,
        rows: result_rows,
    })
}

/// Resolve which ON column pair belongs to left vs right table.
fn resolve_join_columns(
    left: &(String, String),
    right: &(String, String),
    left_table: &TableRef,
    right_table: &TableRef,
    left_cols: &[ColumnInfo],
    right_cols: &[ColumnInfo],
) -> Option<(String, String)> {
    let left_alias = left_table
        .alias
        .as_deref()
        .unwrap_or(&left_table.name);
    let right_alias = right_table
        .alias
        .as_deref()
        .unwrap_or(&right_table.name);

    // Try: left.0 matches left_table, right.0 matches right_table
    if (left.0 == left_alias || left.0.is_empty()) && (right.0 == right_alias || right.0.is_empty())
    {
        // Verify columns exist
        if find_column_idx(&left.1, left_cols).is_some()
            && find_column_idx(&right.1, right_cols).is_some()
        {
            return Some((left.1.clone(), right.1.clone()));
        }
    }

    // Try swapped: left.0 matches right_table, right.0 matches left_table
    if (left.0 == right_alias || left.0.is_empty())
        && (right.0 == left_alias || right.0.is_empty())
    {
        if find_column_idx(&left.1, right_cols).is_some()
            && find_column_idx(&right.1, left_cols).is_some()
        {
            return Some((right.1.clone(), left.1.clone()));
        }
    }

    // Fallback: try by column existence
    if find_column_idx(&left.1, left_cols).is_some()
        && find_column_idx(&right.1, right_cols).is_some()
    {
        return Some((left.1.clone(), right.1.clone()));
    }
    if find_column_idx(&right.1, left_cols).is_some()
        && find_column_idx(&left.1, right_cols).is_some()
    {
        return Some((right.1.clone(), left.1.clone()));
    }

    None
}

/// Find column index by name (case-insensitive).
fn find_column_idx(name: &str, columns: &[ColumnInfo]) -> Option<usize> {
    let lower = name.to_lowercase();
    columns.iter().position(|c| c.name.to_lowercase() == lower)
}

/// Check if a row matches all WHERE conditions (single-table query).
fn matches_where(
    row: &Row,
    conditions: &[Condition],
    columns: &[ColumnInfo],
    _join_cols: Option<&[ColumnInfo]>,
    table_ref: &TableRef,
) -> bool {
    for cond in conditions {
        // If condition has a table alias, check it matches
        if let Some(ref alias) = cond.table_alias {
            let table_alias = table_ref.alias.as_deref().unwrap_or(&table_ref.name);
            if alias != table_alias {
                return false; // Alias mismatch in single-table query
            }
        }

        let col_idx = match find_column_idx(&cond.column, columns) {
            Some(idx) => idx,
            None => return false,
        };

        let row_val = &row.values[col_idx];
        let col_type = columns[col_idx].type_tag;

        if !eval_condition(row_val, &cond.op, &cond.values, col_type) {
            return false;
        }
    }
    true
}

/// Check if a joined row matches all WHERE conditions.
fn matches_join_where(
    row: &Row,
    conditions: &[Condition],
    left_table: &TableRef,
    right_table: &TableRef,
    left_cols: &[ColumnInfo],
    right_cols: &[ColumnInfo],
) -> bool {
    for cond in conditions {
        let col_idx = match find_combined_col_idx(
            &cond.table_alias,
            &cond.column,
            left_table,
            right_table,
            left_cols,
            right_cols,
        ) {
            Some(idx) => idx,
            None => return false,
        };

        let col_type = if col_idx < left_cols.len() {
            left_cols[col_idx].type_tag
        } else {
            right_cols[col_idx - left_cols.len()].type_tag
        };

        let row_val = &row.values[col_idx];
        if !eval_condition(row_val, &cond.op, &cond.values, col_type) {
            return false;
        }
    }
    true
}

/// Find column index in a combined (left ++ right) column list.
fn find_combined_col_idx(
    alias: &Option<String>,
    col_name: &str,
    left_table: &TableRef,
    right_table: &TableRef,
    left_cols: &[ColumnInfo],
    right_cols: &[ColumnInfo],
) -> Option<usize> {
    let left_alias = left_table.alias.as_deref().unwrap_or(&left_table.name);
    let right_alias = right_table.alias.as_deref().unwrap_or(&right_table.name);

    if let Some(ref a) = alias {
        if a == left_alias {
            return find_column_idx(col_name, left_cols);
        } else if a == right_alias {
            return find_column_idx(col_name, right_cols).map(|i| i + left_cols.len());
        }
    }

    // No alias: try left first, then right
    if let Some(idx) = find_column_idx(col_name, left_cols) {
        return Some(idx);
    }
    find_column_idx(col_name, right_cols).map(|i| i + left_cols.len())
}

/// Evaluate a single condition against a value.
fn eval_condition(
    row_val: &Value,
    op: &CmpOp,
    literal_values: &[String],
    col_type: crate::protocol::ColumnType,
) -> bool {
    match op {
        CmpOp::In => {
            for lit in literal_values {
                if let Some(parsed) = Value::parse_literal(lit, col_type) {
                    if row_val == &parsed {
                        return true;
                    }
                }
            }
            false
        }
        _ => {
            if literal_values.is_empty() {
                return false;
            }
            let parsed = match Value::parse_literal(&literal_values[0], col_type) {
                Some(v) => v,
                None => return false,
            };
            match op {
                CmpOp::Eq => row_val == &parsed,
                CmpOp::Ne => row_val != &parsed,
                CmpOp::Gt => matches!(row_val.partial_cmp(&parsed), Some(std::cmp::Ordering::Greater)),
                CmpOp::Lt => matches!(row_val.partial_cmp(&parsed), Some(std::cmp::Ordering::Less)),
                CmpOp::Gte => matches!(
                    row_val.partial_cmp(&parsed),
                    Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
                ),
                CmpOp::Lte => matches!(
                    row_val.partial_cmp(&parsed),
                    Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
                ),
                CmpOp::In => unreachable!(),
            }
        }
    }
}

/// Project columns for a single-table SELECT.
fn project_columns(
    select_cols: &[SelectCol],
    columns: &[ColumnInfo],
    rows: Vec<Row>,
    _join_cols: Option<&[ColumnInfo]>,
    _table_ref: &TableRef,
) -> (Vec<ColumnInfo>, Vec<Row>) {
    if select_cols.len() == 1 && select_cols[0].name == "*" {
        return (columns.to_vec(), rows);
    }

    let mut indices = Vec::new();
    let mut result_cols = Vec::new();

    for sc in select_cols {
        if let Some(idx) = find_column_idx(&sc.name, columns) {
            indices.push(idx);
            result_cols.push(columns[idx].clone());
        }
    }

    let projected_rows = rows
        .into_iter()
        .map(|row| {
            let values = indices.iter().map(|&i| row.values[i].clone()).collect();
            Row { values }
        })
        .collect();

    (result_cols, projected_rows)
}

/// Project columns for a JOIN result.
fn project_join_columns(
    select_cols: &[SelectCol],
    left_table: &TableRef,
    right_table: &TableRef,
    left_cols: &[ColumnInfo],
    right_cols: &[ColumnInfo],
    rows: Vec<Row>,
) -> (Vec<ColumnInfo>, Vec<Row>) {
    if select_cols.len() == 1 && select_cols[0].name == "*" {
        let mut all_cols = left_cols.to_vec();
        all_cols.extend_from_slice(right_cols);
        return (all_cols, rows);
    }

    let mut indices = Vec::new();
    let mut result_cols = Vec::new();

    for sc in select_cols {
        if let Some(idx) = find_combined_col_idx(
            &sc.table_alias,
            &sc.name,
            left_table,
            right_table,
            left_cols,
            right_cols,
        ) {
            indices.push(idx);
            let col = if idx < left_cols.len() {
                left_cols[idx].clone()
            } else {
                right_cols[idx - left_cols.len()].clone()
            };
            result_cols.push(col);
        }
    }

    let projected_rows = rows
        .into_iter()
        .map(|row| {
            let values = indices.iter().map(|&i| row.values[i].clone()).collect();
            Row { values }
        })
        .collect();

    (result_cols, projected_rows)
}

// ── Helper: populate mirror for tests ───────────────────────────────────────

/// Create a mirror populated with encoded rows. Used by tests.
pub fn create_test_mirror(
    table: &str,
    columns: &[ColumnInfo],
    data: &[Vec<Value>],
    mirrors: &DashMap<String, Arc<TableMirror>>,
    schemas: &DashMap<String, Vec<ColumnInfo>>,
) {
    use crate::protocol::Snapshot;

    let mirror = Arc::new(TableMirror::new(1));
    let rows: Vec<(Vec<u8>, Vec<u8>)> = data
        .iter()
        .enumerate()
        .map(|(i, values)| {
            let pk = (i as u64).to_le_bytes().to_vec();
            let row = encode_row(values);
            (pk, row)
        })
        .collect();

    mirror.load_snapshot(Snapshot {
        sub_id: 1,
        version: 1,
        columns: columns.to_vec(),
        rows,
    });

    mirrors.insert(table.to_string(), mirror);
    schemas.insert(table.to_string(), columns.to_vec());
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::ColumnType;

    fn product_columns() -> Vec<ColumnInfo> {
        vec![
            ColumnInfo {
                name: "id".into(),
                type_tag: ColumnType::Int64,
            },
            ColumnInfo {
                name: "name".into(),
                type_tag: ColumnType::Text,
            },
            ColumnInfo {
                name: "price".into(),
                type_tag: ColumnType::Float64,
            },
            ColumnInfo {
                name: "category".into(),
                type_tag: ColumnType::Text,
            },
        ]
    }

    fn review_columns() -> Vec<ColumnInfo> {
        vec![
            ColumnInfo {
                name: "id".into(),
                type_tag: ColumnType::Int64,
            },
            ColumnInfo {
                name: "product_id".into(),
                type_tag: ColumnType::Int64,
            },
            ColumnInfo {
                name: "rating".into(),
                type_tag: ColumnType::Int64,
            },
        ]
    }

    fn make_products(n: usize) -> Vec<Vec<Value>> {
        (0..n)
            .map(|i| {
                vec![
                    Value::Int64(i as i64),
                    Value::Text(format!("Product_{}", i)),
                    Value::Float64(10.0 + (i as f64) * 5.0),
                    Value::Text(if i % 3 == 0 {
                        "electronics".to_string()
                    } else if i % 3 == 1 {
                        "clothing".to_string()
                    } else {
                        "food".to_string()
                    }),
                ]
            })
            .collect()
    }

    fn setup_products(n: usize) -> (DashMap<String, Arc<TableMirror>>, DashMap<String, Vec<ColumnInfo>>) {
        let mirrors = DashMap::new();
        let schemas = DashMap::new();
        create_test_mirror("products", &product_columns(), &make_products(n), &mirrors, &schemas);
        (mirrors, schemas)
    }

    #[test]
    fn local_select_all() {
        let (mirrors, schemas) = setup_products(100);
        let result =
            try_execute_local("SELECT * FROM products", &mirrors, &schemas).expect("should resolve locally");
        assert_eq!(result.rows.len(), 100);
        assert_eq!(result.columns.len(), 4);
    }

    #[test]
    fn local_select_where_eq() {
        let (mirrors, schemas) = setup_products(100);
        let result = try_execute_local(
            "SELECT * FROM products WHERE id = 42",
            &mirrors,
            &schemas,
        )
        .expect("should resolve locally");
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].values[0], Value::Int64(42));
        assert_eq!(result.rows[0].values[1], Value::Text("Product_42".into()));
    }

    #[test]
    fn local_select_where_eq_string() {
        let (mirrors, schemas) = setup_products(100);
        let result = try_execute_local(
            "SELECT * FROM products WHERE category = 'electronics'",
            &mirrors,
            &schemas,
        )
        .expect("should resolve locally");
        // IDs 0,3,6,9,...,99 are electronics (i%3==0), that's 34 rows
        assert_eq!(result.rows.len(), 34);
        for row in &result.rows {
            assert_eq!(row.values[3], Value::Text("electronics".into()));
        }
    }

    #[test]
    fn local_select_where_gt() {
        let (mirrors, schemas) = setup_products(100);
        let result = try_execute_local(
            "SELECT * FROM products WHERE price > 500",
            &mirrors,
            &schemas,
        )
        .expect("should resolve locally");
        // price = 10 + i*5, so price > 500 means i > 98, i.e. i=99 => price=505
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].values[0], Value::Int64(99));
    }

    #[test]
    fn local_select_where_gte() {
        let (mirrors, schemas) = setup_products(100);
        let result = try_execute_local(
            "SELECT * FROM products WHERE price >= 505",
            &mirrors,
            &schemas,
        )
        .expect("should resolve locally");
        assert_eq!(result.rows.len(), 1);
    }

    #[test]
    fn local_select_where_lt() {
        let (mirrors, schemas) = setup_products(100);
        let result = try_execute_local(
            "SELECT * FROM products WHERE price < 15",
            &mirrors,
            &schemas,
        )
        .expect("should resolve locally");
        // price=10 for i=0 only
        assert_eq!(result.rows.len(), 1);
    }

    #[test]
    fn local_select_where_in() {
        let (mirrors, schemas) = setup_products(100);
        let result = try_execute_local(
            "SELECT * FROM products WHERE id IN (1, 5, 10)",
            &mirrors,
            &schemas,
        )
        .expect("should resolve locally");
        assert_eq!(result.rows.len(), 3);
    }

    #[test]
    fn local_select_order_by_limit() {
        let (mirrors, schemas) = setup_products(100);
        let result = try_execute_local(
            "SELECT * FROM products ORDER BY price DESC LIMIT 5",
            &mirrors,
            &schemas,
        )
        .expect("should resolve locally");
        assert_eq!(result.rows.len(), 5);
        // First row should have highest price (i=99 -> price=505)
        assert_eq!(result.rows[0].values[0], Value::Int64(99));
        assert_eq!(result.rows[1].values[0], Value::Int64(98));
    }

    #[test]
    fn local_select_order_by_asc() {
        let (mirrors, schemas) = setup_products(10);
        let result = try_execute_local(
            "SELECT * FROM products ORDER BY price ASC LIMIT 3",
            &mirrors,
            &schemas,
        )
        .expect("should resolve locally");
        assert_eq!(result.rows.len(), 3);
        assert_eq!(result.rows[0].values[0], Value::Int64(0));
        assert_eq!(result.rows[1].values[0], Value::Int64(1));
        assert_eq!(result.rows[2].values[0], Value::Int64(2));
    }

    #[test]
    fn local_select_columns() {
        let (mirrors, schemas) = setup_products(100);
        let result = try_execute_local(
            "SELECT name, price FROM products WHERE id = 42",
            &mirrors,
            &schemas,
        )
        .expect("should resolve locally");
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.columns.len(), 2);
        assert_eq!(result.columns[0].name, "name");
        assert_eq!(result.columns[1].name, "price");
        assert_eq!(result.rows[0].values[0], Value::Text("Product_42".into()));
        assert_eq!(result.rows[0].values[1], Value::Float64(10.0 + 42.0 * 5.0));
    }

    #[test]
    fn local_select_where_and() {
        let (mirrors, schemas) = setup_products(100);
        let result = try_execute_local(
            "SELECT * FROM products WHERE category = 'electronics' AND price > 200",
            &mirrors,
            &schemas,
        )
        .expect("should resolve locally");
        for row in &result.rows {
            assert_eq!(row.values[3], Value::Text("electronics".into()));
            if let Value::Float64(p) = row.values[2] {
                assert!(p > 200.0);
            }
        }
        assert!(!result.rows.is_empty());
    }

    #[test]
    fn local_select_with_where_order_limit() {
        let (mirrors, schemas) = setup_products(100);
        let result = try_execute_local(
            "SELECT * FROM products WHERE category = 'electronics' ORDER BY price ASC LIMIT 3",
            &mirrors,
            &schemas,
        )
        .expect("should resolve locally");
        assert_eq!(result.rows.len(), 3);
        // Should be the 3 cheapest electronics (ids 0, 3, 6)
        assert_eq!(result.rows[0].values[0], Value::Int64(0));
        assert_eq!(result.rows[1].values[0], Value::Int64(3));
        assert_eq!(result.rows[2].values[0], Value::Int64(6));
    }

    #[test]
    fn local_join_two_mirrors() {
        let mirrors: DashMap<String, Arc<TableMirror>> = DashMap::new();
        let schemas: DashMap<String, Vec<ColumnInfo>> = DashMap::new();

        // Create products
        let prods = vec![
            vec![Value::Int64(1), Value::Text("Widget".into()), Value::Float64(19.99), Value::Text("electronics".into())],
            vec![Value::Int64(2), Value::Text("Gadget".into()), Value::Float64(29.99), Value::Text("electronics".into())],
        ];
        create_test_mirror("products", &product_columns(), &prods, &mirrors, &schemas);

        // Create reviews
        let revs = vec![
            vec![Value::Int64(1), Value::Int64(1), Value::Int64(5)],
            vec![Value::Int64(2), Value::Int64(1), Value::Int64(4)],
            vec![Value::Int64(3), Value::Int64(2), Value::Int64(3)],
        ];
        create_test_mirror("reviews", &review_columns(), &revs, &mirrors, &schemas);

        let result = try_execute_local(
            "SELECT p.name, r.rating FROM products p JOIN reviews r ON r.product_id = p.id WHERE p.id = 1",
            &mirrors,
            &schemas,
        )
        .expect("should resolve locally");

        assert_eq!(result.columns.len(), 2);
        assert_eq!(result.columns[0].name, "name");
        assert_eq!(result.columns[1].name, "rating");
        assert_eq!(result.rows.len(), 2); // product 1 has 2 reviews
        for row in &result.rows {
            assert_eq!(row.values[0], Value::Text("Widget".into()));
        }
    }

    #[test]
    fn local_join_all_rows() {
        let mirrors: DashMap<String, Arc<TableMirror>> = DashMap::new();
        let schemas: DashMap<String, Vec<ColumnInfo>> = DashMap::new();

        let prods = vec![
            vec![Value::Int64(1), Value::Text("A".into()), Value::Float64(10.0), Value::Text("cat".into())],
            vec![Value::Int64(2), Value::Text("B".into()), Value::Float64(20.0), Value::Text("cat".into())],
        ];
        create_test_mirror("products", &product_columns(), &prods, &mirrors, &schemas);

        let revs = vec![
            vec![Value::Int64(1), Value::Int64(1), Value::Int64(5)],
            vec![Value::Int64(2), Value::Int64(2), Value::Int64(3)],
        ];
        create_test_mirror("reviews", &review_columns(), &revs, &mirrors, &schemas);

        let result = try_execute_local(
            "SELECT p.name, r.rating FROM products p JOIN reviews r ON r.product_id = p.id",
            &mirrors,
            &schemas,
        )
        .expect("should resolve locally");

        assert_eq!(result.rows.len(), 2);
    }

    #[test]
    fn local_fallback_group_by() {
        let (mirrors, schemas) = setup_products(10);
        let result = try_execute_local(
            "SELECT category, COUNT(*) FROM products GROUP BY category",
            &mirrors,
            &schemas,
        );
        assert!(result.is_none(), "GROUP BY should delegate to server");
    }

    #[test]
    fn local_fallback_subquery() {
        let (mirrors, schemas) = setup_products(10);
        let result = try_execute_local(
            "SELECT * FROM products WHERE id IN (SELECT product_id FROM reviews)",
            &mirrors,
            &schemas,
        );
        assert!(result.is_none(), "Subquery should delegate to server");
    }

    #[test]
    fn local_fallback_aggregate_sum() {
        let (mirrors, schemas) = setup_products(10);
        let result = try_execute_local(
            "SELECT SUM(price) FROM products",
            &mirrors,
            &schemas,
        );
        assert!(result.is_none(), "SUM should delegate to server");
    }

    #[test]
    fn local_fallback_having() {
        let (mirrors, schemas) = setup_products(10);
        let result = try_execute_local(
            "SELECT category FROM products GROUP BY category HAVING COUNT(*) > 1",
            &mirrors,
            &schemas,
        );
        assert!(result.is_none(), "HAVING should delegate to server");
    }

    #[test]
    fn local_fallback_union() {
        let (mirrors, schemas) = setup_products(10);
        let result = try_execute_local(
            "SELECT * FROM products UNION SELECT * FROM products",
            &mirrors,
            &schemas,
        );
        assert!(result.is_none(), "UNION should delegate to server");
    }

    #[test]
    fn local_fallback_window_function() {
        let (mirrors, schemas) = setup_products(10);
        let result = try_execute_local(
            "SELECT name, ROW_NUMBER() OVER (ORDER BY price) FROM products",
            &mirrors,
            &schemas,
        );
        assert!(result.is_none(), "Window function should delegate to server");
    }

    #[test]
    fn local_select_nonexistent_table() {
        let (mirrors, schemas) = setup_products(10);
        let result = try_execute_local(
            "SELECT * FROM nonexistent WHERE id = 1",
            &mirrors,
            &schemas,
        );
        assert!(result.is_none(), "Nonexistent table should return None");
    }

    #[test]
    fn local_select_where_ne() {
        let (mirrors, schemas) = setup_products(10);
        let result = try_execute_local(
            "SELECT * FROM products WHERE id != 0",
            &mirrors,
            &schemas,
        )
        .expect("should resolve locally");
        assert_eq!(result.rows.len(), 9);
    }

    #[test]
    fn local_not_a_select() {
        let (mirrors, schemas) = setup_products(10);
        assert!(try_execute_local("INSERT INTO products VALUES (1)", &mirrors, &schemas).is_none());
        assert!(try_execute_local("UPDATE products SET price = 0", &mirrors, &schemas).is_none());
        assert!(try_execute_local("DELETE FROM products", &mirrors, &schemas).is_none());
    }

    #[test]
    fn local_case_insensitive_keywords() {
        let (mirrors, schemas) = setup_products(100);
        let result = try_execute_local(
            "select * from products where id = 42",
            &mirrors,
            &schemas,
        )
        .expect("should resolve locally");
        assert_eq!(result.rows.len(), 1);
    }

    #[test]
    fn local_select_limit_only() {
        let (mirrors, schemas) = setup_products(100);
        let result = try_execute_local(
            "SELECT * FROM products LIMIT 10",
            &mirrors,
            &schemas,
        )
        .expect("should resolve locally");
        assert_eq!(result.rows.len(), 10);
    }

    // ── FIND (PyroSQL native syntax) tests ──────────────────────────────

    #[test]
    fn find_all() {
        let (mirrors, schemas) = setup_products(50);
        let result =
            try_execute_local("FIND products", &mirrors, &schemas).expect("should resolve locally");
        assert_eq!(result.rows.len(), 50);
        assert_eq!(result.columns.len(), 4);
    }

    #[test]
    fn find_where_eq() {
        let (mirrors, schemas) = setup_products(100);
        let result = try_execute_local(
            "FIND products WHERE id = 7",
            &mirrors,
            &schemas,
        )
        .expect("should resolve locally");
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].values[0], Value::Int64(7));
    }

    #[test]
    fn find_where_string() {
        let (mirrors, schemas) = setup_products(100);
        let result = try_execute_local(
            "FIND products WHERE category = 'electronics'",
            &mirrors,
            &schemas,
        )
        .expect("should resolve locally");
        assert_eq!(result.rows.len(), 34);
    }

    #[test]
    fn find_columns() {
        let (mirrors, schemas) = setup_products(100);
        let result = try_execute_local(
            "FIND products.name, products.price WHERE id = 42",
            &mirrors,
            &schemas,
        )
        .expect("should resolve locally");
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.columns.len(), 2);
        assert_eq!(result.columns[0].name, "name");
        assert_eq!(result.columns[1].name, "price");
    }

    #[test]
    fn find_top_n_sort_by() {
        let (mirrors, schemas) = setup_products(100);
        let result = try_execute_local(
            "FIND TOP 5 products SORT BY price DESC",
            &mirrors,
            &schemas,
        )
        .expect("should resolve locally");
        assert_eq!(result.rows.len(), 5);
        // First should be highest price (id=99, price=505)
        assert_eq!(result.rows[0].values[0], Value::Int64(99));
    }

    #[test]
    fn find_top_n_with_where() {
        let (mirrors, schemas) = setup_products(100);
        let result = try_execute_local(
            "FIND TOP 3 products WHERE category = 'electronics' SORT BY price ASC",
            &mirrors,
            &schemas,
        )
        .expect("should resolve locally");
        assert_eq!(result.rows.len(), 3);
        // Cheapest electronics: ids 0, 3, 6
        assert_eq!(result.rows[0].values[0], Value::Int64(0));
        assert_eq!(result.rows[1].values[0], Value::Int64(3));
        assert_eq!(result.rows[2].values[0], Value::Int64(6));
    }

    #[test]
    fn find_with_join() {
        let mirrors: DashMap<String, Arc<TableMirror>> = DashMap::new();
        let schemas: DashMap<String, Vec<ColumnInfo>> = DashMap::new();

        let prods = vec![
            vec![Value::Int64(1), Value::Text("Widget".into()), Value::Float64(19.99), Value::Text("electronics".into())],
            vec![Value::Int64(2), Value::Text("Gadget".into()), Value::Float64(29.99), Value::Text("electronics".into())],
        ];
        create_test_mirror("products", &product_columns(), &prods, &mirrors, &schemas);

        let revs = vec![
            vec![Value::Int64(1), Value::Int64(1), Value::Int64(5)],
            vec![Value::Int64(2), Value::Int64(1), Value::Int64(4)],
            vec![Value::Int64(3), Value::Int64(2), Value::Int64(3)],
        ];
        create_test_mirror("reviews", &review_columns(), &revs, &mirrors, &schemas);

        let result = try_execute_local(
            "FIND products WITH reviews ON reviews.product_id = products.id WHERE products.id = 1",
            &mirrors,
            &schemas,
        )
        .expect("should resolve locally");

        assert_eq!(result.rows.len(), 2); // product 1 has 2 reviews
    }

    #[test]
    fn find_fallback_count() {
        let (mirrors, schemas) = setup_products(10);
        let result = try_execute_local("FIND products COUNT", &mirrors, &schemas);
        assert!(result.is_none(), "FIND COUNT should delegate to server");
    }

    #[test]
    fn find_fallback_unique() {
        let (mirrors, schemas) = setup_products(10);
        let result = try_execute_local("FIND UNIQUE products.category", &mirrors, &schemas);
        assert!(result.is_none(), "FIND UNIQUE should delegate to server");
    }

    #[test]
    fn find_fallback_sum() {
        let (mirrors, schemas) = setup_products(10);
        let result = try_execute_local("FIND products SUM price", &mirrors, &schemas);
        assert!(result.is_none(), "FIND SUM should delegate to server");
    }

    // ── Benchmark ────────────────────────────────────────────────────────

    #[test]
    fn bench_local_query_throughput() {
        let (mirrors, schemas) = setup_products(5000);

        // Warm up
        let _ = try_execute_local("SELECT * FROM products WHERE id = 42", &mirrors, &schemas);

        let start = std::time::Instant::now();
        let iterations = 100;
        for i in 0..iterations {
            let sql = format!("SELECT * FROM products WHERE id = {}", i % 5000);
            let result = try_execute_local(&sql, &mirrors, &schemas);
            assert!(result.is_some());
        }
        let elapsed = start.elapsed();
        let qps = iterations as f64 / elapsed.as_secs_f64();

        // In debug mode (unoptimized), threshold is lower. In release, expect 100K+.
        // Debug build in container: ~150 QPS (full table scan + decode per query).
        // We just verify it completes without errors and is non-trivial.
        assert!(
            qps > 10.0,
            "Local query throughput critically low: {:.0} QPS",
            qps,
        );

        eprintln!(
            "bench_local_query_throughput: {:.0} QPS ({} queries over 5K rows in {:.1}ms)",
            qps,
            iterations,
            elapsed.as_secs_f64() * 1000.0
        );
    }
}
