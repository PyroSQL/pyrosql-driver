//! Row and column types for the PyroSQL sqlx driver.

use crate::pwire;
use crate::type_info::PyroSqlTypeInfo;
use crate::PyroSql;
use sqlx_core::column::Column;
use sqlx_core::database::Database;
use sqlx_core::row::Row;
use sqlx_core::value::ValueRef;
use std::sync::Arc;

/// A column in a PyroSQL result set.
#[derive(Debug, Clone)]
pub struct PyroSqlColumn {
    /// Zero-based ordinal position.
    pub ordinal: usize,
    /// Column name.
    pub name: String,
    /// Type information.
    pub type_info: PyroSqlTypeInfo,
}

impl Column for PyroSqlColumn {
    type Database = PyroSql;

    fn ordinal(&self) -> usize {
        self.ordinal
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn type_info(&self) -> &PyroSqlTypeInfo {
        &self.type_info
    }
}

/// A row from a PyroSQL result set.
#[derive(Debug, Clone)]
pub struct PyroSqlRow {
    /// Column metadata shared across all rows in a result set.
    pub columns: Arc<Vec<PyroSqlColumn>>,
    /// The values in this row.
    pub values: Vec<PyroSqlValueOwned>,
}

impl Row for PyroSqlRow {
    type Database = PyroSql;

    fn columns(&self) -> &[PyroSqlColumn] {
        &self.columns
    }

    fn try_get_raw<I>(&self, index: I) -> Result<PyroSqlValueRef<'_>, sqlx_core::error::Error>
    where
        I: sqlx_core::column::ColumnIndex<Self>,
    {
        let idx = index.index(self)?;
        if idx >= self.values.len() {
            return Err(sqlx_core::error::Error::ColumnIndexOutOfBounds {
                index: idx,
                len: self.values.len(),
            });
        }
        Ok(PyroSqlValueRef {
            inner: &self.values[idx].inner,
            type_info: &self.columns[idx].type_info,
        })
    }
}

impl sqlx_core::column::ColumnIndex<PyroSqlRow> for usize {
    fn index(&self, row: &PyroSqlRow) -> Result<usize, sqlx_core::error::Error> {
        if *self >= row.columns.len() {
            Err(sqlx_core::error::Error::ColumnIndexOutOfBounds {
                index: *self,
                len: row.columns.len(),
            })
        } else {
            Ok(*self)
        }
    }
}

impl sqlx_core::column::ColumnIndex<PyroSqlRow> for &str {
    fn index(&self, row: &PyroSqlRow) -> Result<usize, sqlx_core::error::Error> {
        row.columns
            .iter()
            .position(|c| c.name == *self)
            .ok_or_else(|| sqlx_core::error::Error::ColumnNotFound(self.to_string()))
    }
}

/// An owned value from a PyroSQL row.
#[derive(Debug, Clone)]
pub struct PyroSqlValueOwned {
    /// The inner PWire value.
    pub inner: pwire::Value,
}

/// A reference to a value in a PyroSQL row.
#[derive(Debug, Clone, Copy)]
pub struct PyroSqlValueRef<'r> {
    /// Reference to the inner value.
    pub inner: &'r pwire::Value,
    /// Type information for this value.
    pub type_info: &'r PyroSqlTypeInfo,
}

impl<'r> ValueRef<'r> for PyroSqlValueRef<'r> {
    type Database = PyroSql;

    fn to_owned(&self) -> PyroSqlValueOwned {
        PyroSqlValueOwned {
            inner: self.inner.clone(),
        }
    }

    fn type_info(&self) -> std::borrow::Cow<'_, PyroSqlTypeInfo> {
        std::borrow::Cow::Borrowed(self.type_info)
    }

    fn is_null(&self) -> bool {
        matches!(self.inner, pwire::Value::Null)
    }
}

impl sqlx_core::value::Value for PyroSqlValueOwned {
    type Database = PyroSql;

    fn as_ref(&self) -> PyroSqlValueRef<'_> {
        // Create a temporary type_info based on the value
        // In production this would come from the column metadata
        PyroSqlValueRef {
            inner: &self.inner,
            type_info: &PyroSqlTypeInfo::TEXT, // fallback; real type comes from column
        }
    }

    fn type_info(&self) -> std::borrow::Cow<'_, PyroSqlTypeInfo> {
        let ti = match &self.inner {
            pwire::Value::Null => PyroSqlTypeInfo::NULL,
            pwire::Value::I64(_) => PyroSqlTypeInfo::BIGINT,
            pwire::Value::F64(_) => PyroSqlTypeInfo::DOUBLE,
            pwire::Value::Text(_) => PyroSqlTypeInfo::TEXT,
            pwire::Value::Bool(_) => PyroSqlTypeInfo::BOOLEAN,
            pwire::Value::Bytes(_) => PyroSqlTypeInfo::BLOB,
        };
        std::borrow::Cow::Owned(ti)
    }

    fn is_null(&self) -> bool {
        matches!(self.inner, pwire::Value::Null)
    }
}

/// Helper methods for extracting typed values from a PyroSqlRow.
impl PyroSqlRow {
    /// Get a column value by ordinal as i64.
    pub fn get_i64(&self, index: usize) -> Option<i64> {
        match &self.values.get(index)?.inner {
            pwire::Value::I64(v) => Some(*v),
            pwire::Value::Text(s) => s.parse().ok(),
            _ => None,
        }
    }

    /// Get a column value by ordinal as f64.
    pub fn get_f64(&self, index: usize) -> Option<f64> {
        match &self.values.get(index)?.inner {
            pwire::Value::F64(v) => Some(*v),
            pwire::Value::I64(v) => Some(*v as f64),
            pwire::Value::Text(s) => s.parse().ok(),
            _ => None,
        }
    }

    /// Get a column value by ordinal as String.
    pub fn get_string(&self, index: usize) -> Option<String> {
        match &self.values.get(index)?.inner {
            pwire::Value::Text(s) => Some(s.clone()),
            pwire::Value::I64(v) => Some(v.to_string()),
            pwire::Value::F64(v) => Some(v.to_string()),
            pwire::Value::Bool(v) => Some(v.to_string()),
            pwire::Value::Null => None,
            pwire::Value::Bytes(b) => Some(String::from_utf8_lossy(b).into_owned()),
        }
    }

    /// Get a column value by ordinal as bool.
    pub fn get_bool(&self, index: usize) -> Option<bool> {
        match &self.values.get(index)?.inner {
            pwire::Value::Bool(v) => Some(*v),
            pwire::Value::I64(v) => Some(*v != 0),
            _ => None,
        }
    }

    /// Get a column value by ordinal as bytes.
    pub fn get_bytes(&self, index: usize) -> Option<&[u8]> {
        match &self.values.get(index)?.inner {
            pwire::Value::Bytes(v) => Some(v),
            _ => None,
        }
    }

    /// Check if a column value is null.
    pub fn is_null(&self, index: usize) -> bool {
        self.values
            .get(index)
            .map(|v| matches!(v.inner, pwire::Value::Null))
            .unwrap_or(true)
    }

    /// Get a column value by name as String.
    pub fn get_by_name(&self, name: &str) -> Option<String> {
        let idx = self.columns.iter().position(|c| c.name == name)?;
        self.get_string(idx)
    }
}
