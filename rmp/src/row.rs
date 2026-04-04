//! Row decoder for local mirror data.
//!
//! Decodes raw bytes from [`TableMirror`] into typed [`Value`]s using
//! column metadata from [`ColumnInfo`].
//!
//! ## Wire format
//!
//! Each row is encoded as `[num_cols: u32 LE]` followed by that many fields.
//! Each field is either:
//! - `[0xFFFFFFFF]` for NULL
//! - `[len: u32 LE][data: len bytes]` for a value
//!
//! The data bytes are interpreted according to the column's [`ColumnType`]:
//! - `Int64`: 8 bytes, little-endian i64
//! - `Float64`: 8 bytes, little-endian f64
//! - `Text`: UTF-8 string
//! - `Bool`: 1 byte (0 = false, nonzero = true)
//! - `Bytes`: raw bytes

use crate::protocol::{ColumnInfo, ColumnType};

/// Sentinel for NULL fields in the wire format.
const NULL_SENTINEL: u32 = 0xFFFF_FFFF;

/// A typed value decoded from a mirror row.
#[derive(Debug, Clone)]
pub enum Value {
    /// SQL NULL.
    Null,
    /// 64-bit signed integer.
    Int64(i64),
    /// 64-bit floating point.
    Float64(f64),
    /// UTF-8 text.
    Text(String),
    /// Boolean.
    Bool(bool),
    /// Raw bytes / blob.
    Bytes(Vec<u8>),
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Null, Value::Null) => true,
            (Value::Int64(a), Value::Int64(b)) => a == b,
            (Value::Float64(a), Value::Float64(b)) => a.to_bits() == b.to_bits(),
            (Value::Text(a), Value::Text(b)) => a == b,
            (Value::Bool(a), Value::Bool(b)) => a == b,
            (Value::Bytes(a), Value::Bytes(b)) => a == b,
            _ => false,
        }
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (Value::Null, Value::Null) => Some(std::cmp::Ordering::Equal),
            (Value::Null, _) => Some(std::cmp::Ordering::Less),
            (_, Value::Null) => Some(std::cmp::Ordering::Greater),
            (Value::Int64(a), Value::Int64(b)) => a.partial_cmp(b),
            (Value::Float64(a), Value::Float64(b)) => a.partial_cmp(b),
            (Value::Text(a), Value::Text(b)) => a.partial_cmp(b),
            (Value::Bool(a), Value::Bool(b)) => a.partial_cmp(b),
            (Value::Bytes(a), Value::Bytes(b)) => a.partial_cmp(b),
            // Cross-type: Int64 vs Float64 compare numerically
            (Value::Int64(a), Value::Float64(b)) => (*a as f64).partial_cmp(b),
            (Value::Float64(a), Value::Int64(b)) => a.partial_cmp(&(*b as f64)),
            _ => None,
        }
    }
}

impl Value {
    /// Parse a string literal into a Value, using the target column type for guidance.
    pub fn parse_literal(s: &str, type_tag: ColumnType) -> Option<Self> {
        match type_tag {
            ColumnType::Int64 => s.parse::<i64>().ok().map(Value::Int64),
            ColumnType::Float64 => s.parse::<f64>().ok().map(Value::Float64),
            ColumnType::Text => Some(Value::Text(s.to_string())),
            ColumnType::Bool => match s.to_lowercase().as_str() {
                "true" | "1" => Some(Value::Bool(true)),
                "false" | "0" => Some(Value::Bool(false)),
                _ => None,
            },
            ColumnType::Bytes => Some(Value::Bytes(s.as_bytes().to_vec())),
        }
    }
}

/// A decoded row from a mirror.
#[derive(Debug, Clone)]
pub struct Row {
    /// Column values in schema order.
    pub values: Vec<Value>,
}

impl Row {
    /// Decode a row from raw bytes using column metadata.
    ///
    /// Wire format: `[num_cols: u32 LE]` then per field either
    /// `[0xFFFFFFFF]` for NULL or `[len: u32 LE][data: len bytes]`.
    pub fn decode(raw: &[u8], columns: &[ColumnInfo]) -> Self {
        // Read num_cols
        if raw.len() < 4 {
            return Row {
                values: vec![Value::Null; columns.len()],
            };
        }
        let num_cols = u32::from_le_bytes([raw[0], raw[1], raw[2], raw[3]]) as usize;
        let mut pos = 4;

        let mut values = Vec::with_capacity(num_cols.min(columns.len()));

        for i in 0..num_cols {
            if pos + 4 > raw.len() {
                // Truncated data: fill remaining with Null
                while values.len() < columns.len() {
                    values.push(Value::Null);
                }
                break;
            }

            let field_len =
                u32::from_le_bytes([raw[pos], raw[pos + 1], raw[pos + 2], raw[pos + 3]]);
            pos += 4;

            if field_len == NULL_SENTINEL {
                values.push(Value::Null);
                continue;
            }

            let len = field_len as usize;
            if pos + len > raw.len() {
                values.push(Value::Null);
                break;
            }

            let data = &raw[pos..pos + len];
            pos += len;

            // Decode based on column type (if we have metadata for this column)
            let val = if i < columns.len() {
                decode_field(data, columns[i].type_tag)
            } else {
                Value::Bytes(data.to_vec())
            };
            values.push(val);
        }

        // Pad with Null if fewer fields than columns
        while values.len() < columns.len() {
            values.push(Value::Null);
        }

        Row { values }
    }

    /// Get a value by column index.
    pub fn get(&self, idx: usize) -> &Value {
        &self.values[idx]
    }

    /// Get a value by column name.
    pub fn get_by_name<'a>(&'a self, name: &str, columns: &[ColumnInfo]) -> Option<&'a Value> {
        columns
            .iter()
            .position(|c| c.name == name)
            .map(|idx| &self.values[idx])
    }
}

/// Decode a single field's raw bytes into a typed Value.
fn decode_field(data: &[u8], type_tag: ColumnType) -> Value {
    match type_tag {
        ColumnType::Int64 => {
            if data.len() >= 8 {
                let v = i64::from_le_bytes([
                    data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
                ]);
                Value::Int64(v)
            } else {
                Value::Null
            }
        }
        ColumnType::Float64 => {
            if data.len() >= 8 {
                let v = f64::from_le_bytes([
                    data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
                ]);
                Value::Float64(v)
            } else {
                Value::Null
            }
        }
        ColumnType::Text => match std::str::from_utf8(data) {
            Ok(s) => Value::Text(s.to_string()),
            Err(_) => Value::Bytes(data.to_vec()),
        },
        ColumnType::Bool => {
            if data.is_empty() {
                Value::Null
            } else {
                Value::Bool(data[0] != 0)
            }
        }
        ColumnType::Bytes => Value::Bytes(data.to_vec()),
    }
}

/// Encode a row into the wire format used by mirrors.
///
/// This is the inverse of [`Row::decode`] and is used to create test data.
pub fn encode_row(values: &[Value]) -> Vec<u8> {
    let mut buf = Vec::new();
    // num_cols
    buf.extend_from_slice(&(values.len() as u32).to_le_bytes());

    for val in values {
        match val {
            Value::Null => {
                buf.extend_from_slice(&NULL_SENTINEL.to_le_bytes());
            }
            Value::Int64(v) => {
                let data = v.to_le_bytes();
                buf.extend_from_slice(&(data.len() as u32).to_le_bytes());
                buf.extend_from_slice(&data);
            }
            Value::Float64(v) => {
                let data = v.to_le_bytes();
                buf.extend_from_slice(&(data.len() as u32).to_le_bytes());
                buf.extend_from_slice(&data);
            }
            Value::Text(s) => {
                let data = s.as_bytes();
                buf.extend_from_slice(&(data.len() as u32).to_le_bytes());
                buf.extend_from_slice(data);
            }
            Value::Bool(b) => {
                buf.extend_from_slice(&1u32.to_le_bytes());
                buf.push(if *b { 1 } else { 0 });
            }
            Value::Bytes(data) => {
                buf.extend_from_slice(&(data.len() as u32).to_le_bytes());
                buf.extend_from_slice(data);
            }
        }
    }

    buf
}

#[cfg(test)]
mod tests {
    use super::*;

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
                name: "active".into(),
                type_tag: ColumnType::Bool,
            },
        ]
    }

    #[test]
    fn encode_decode_roundtrip() {
        let cols = product_columns();
        let values = vec![
            Value::Int64(42),
            Value::Text("Widget".into()),
            Value::Float64(19.99),
            Value::Bool(true),
        ];
        let encoded = encode_row(&values);
        let row = Row::decode(&encoded, &cols);
        assert_eq!(row.values.len(), 4);
        assert_eq!(row.values[0], Value::Int64(42));
        assert_eq!(row.values[1], Value::Text("Widget".into()));
        assert_eq!(row.values[2], Value::Float64(19.99));
        assert_eq!(row.values[3], Value::Bool(true));
    }

    #[test]
    fn decode_with_null() {
        let cols = product_columns();
        let values = vec![
            Value::Int64(1),
            Value::Null,
            Value::Float64(0.0),
            Value::Null,
        ];
        let encoded = encode_row(&values);
        let row = Row::decode(&encoded, &cols);
        assert_eq!(row.values[0], Value::Int64(1));
        assert_eq!(row.values[1], Value::Null);
        assert_eq!(row.values[3], Value::Null);
    }

    #[test]
    fn get_by_name() {
        let cols = product_columns();
        let values = vec![
            Value::Int64(7),
            Value::Text("Gadget".into()),
            Value::Float64(99.50),
            Value::Bool(false),
        ];
        let encoded = encode_row(&values);
        let row = Row::decode(&encoded, &cols);

        assert_eq!(row.get_by_name("name", &cols), Some(&Value::Text("Gadget".into())));
        assert_eq!(row.get_by_name("price", &cols), Some(&Value::Float64(99.50)));
        assert!(row.get_by_name("nonexistent", &cols).is_none());
    }

    #[test]
    fn get_by_index() {
        let cols = product_columns();
        let values = vec![
            Value::Int64(10),
            Value::Text("Item".into()),
            Value::Float64(5.0),
            Value::Bool(true),
        ];
        let encoded = encode_row(&values);
        let row = Row::decode(&encoded, &cols);
        assert_eq!(row.get(0), &Value::Int64(10));
        assert_eq!(row.get(3), &Value::Bool(true));
    }

    #[test]
    fn value_ordering() {
        assert!(Value::Int64(1) < Value::Int64(2));
        assert!(Value::Float64(1.0) < Value::Float64(2.0));
        assert!(Value::Text("a".into()) < Value::Text("b".into()));
        assert!(Value::Null < Value::Int64(0));
    }

    #[test]
    fn parse_literal_int() {
        assert_eq!(
            Value::parse_literal("42", ColumnType::Int64),
            Some(Value::Int64(42))
        );
        assert_eq!(
            Value::parse_literal("-5", ColumnType::Int64),
            Some(Value::Int64(-5))
        );
        assert!(Value::parse_literal("abc", ColumnType::Int64).is_none());
    }

    #[test]
    fn parse_literal_float() {
        assert_eq!(
            Value::parse_literal("3.14", ColumnType::Float64),
            Some(Value::Float64(3.14))
        );
    }

    #[test]
    fn parse_literal_bool() {
        assert_eq!(
            Value::parse_literal("true", ColumnType::Bool),
            Some(Value::Bool(true))
        );
        assert_eq!(
            Value::parse_literal("false", ColumnType::Bool),
            Some(Value::Bool(false))
        );
    }

    #[test]
    fn empty_raw_returns_nulls() {
        let cols = product_columns();
        let row = Row::decode(&[], &cols);
        assert_eq!(row.values.len(), 4);
        for v in &row.values {
            assert_eq!(v, &Value::Null);
        }
    }

    #[test]
    fn bytes_column_roundtrip() {
        let cols = vec![ColumnInfo {
            name: "data".into(),
            type_tag: ColumnType::Bytes,
        }];
        let values = vec![Value::Bytes(vec![0xDE, 0xAD, 0xBE, 0xEF])];
        let encoded = encode_row(&values);
        let row = Row::decode(&encoded, &cols);
        assert_eq!(row.values[0], Value::Bytes(vec![0xDE, 0xAD, 0xBE, 0xEF]));
    }
}
