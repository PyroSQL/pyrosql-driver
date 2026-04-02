//! Type information for the PyroSQL sqlx driver.

use crate::pwire;
use crate::PyroSql;
use sqlx_core::type_info::TypeInfo;
use std::fmt;

/// Type information for a PyroSQL column or value.
///
/// Maps between the PWire protocol type tags and sqlx's type system.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PyroSqlTypeInfo {
    /// The type name as used in SQL (e.g., "BIGINT", "TEXT").
    pub name: &'static str,
    /// The PWire type tag.
    pub type_tag: u8,
}

impl PyroSqlTypeInfo {
    /// NULL type.
    pub const NULL: Self = Self { name: "NULL", type_tag: pwire::TYPE_NULL };
    /// 64-bit integer type.
    pub const BIGINT: Self = Self { name: "BIGINT", type_tag: pwire::TYPE_I64 };
    /// 64-bit floating point type.
    pub const DOUBLE: Self = Self { name: "DOUBLE", type_tag: pwire::TYPE_F64 };
    /// Text/string type.
    pub const TEXT: Self = Self { name: "TEXT", type_tag: pwire::TYPE_TEXT };
    /// Boolean type.
    pub const BOOLEAN: Self = Self { name: "BOOLEAN", type_tag: pwire::TYPE_BOOL };
    /// Binary/blob type.
    pub const BLOB: Self = Self { name: "BLOB", type_tag: pwire::TYPE_BYTES };

    /// Create a type info from a PWire type tag.
    pub fn from_tag(tag: u8) -> Self {
        match tag {
            pwire::TYPE_NULL => Self::NULL,
            pwire::TYPE_I64 => Self::BIGINT,
            pwire::TYPE_F64 => Self::DOUBLE,
            pwire::TYPE_TEXT => Self::TEXT,
            pwire::TYPE_BOOL => Self::BOOLEAN,
            pwire::TYPE_BYTES => Self::BLOB,
            _ => Self { name: "UNKNOWN", type_tag: tag },
        }
    }

    /// Check if this type is numeric (integer or float).
    pub fn is_numeric(&self) -> bool {
        matches!(self.type_tag, pwire::TYPE_I64 | pwire::TYPE_F64)
    }

    /// Check if this type is textual.
    pub fn is_text(&self) -> bool {
        self.type_tag == pwire::TYPE_TEXT
    }

    /// Check if this type is binary.
    pub fn is_binary(&self) -> bool {
        self.type_tag == pwire::TYPE_BYTES
    }

    /// Get the compatible CLR/Rust type name for documentation purposes.
    pub fn rust_type_name(&self) -> &'static str {
        match self.type_tag {
            pwire::TYPE_NULL => "()",
            pwire::TYPE_I64 => "i64",
            pwire::TYPE_F64 => "f64",
            pwire::TYPE_TEXT => "String",
            pwire::TYPE_BOOL => "bool",
            pwire::TYPE_BYTES => "Vec<u8>",
            _ => "Unknown",
        }
    }
}

impl fmt::Display for PyroSqlTypeInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name)
    }
}

impl TypeInfo for PyroSqlTypeInfo {
    fn is_null(&self) -> bool {
        self.type_tag == pwire::TYPE_NULL
    }

    fn name(&self) -> &str {
        self.name
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_tag_i64() {
        let ti = PyroSqlTypeInfo::from_tag(pwire::TYPE_I64);
        assert_eq!(ti.name, "BIGINT");
        assert_eq!(ti.type_tag, 1);
        assert!(ti.is_numeric());
        assert!(!ti.is_text());
    }

    #[test]
    fn test_from_tag_f64() {
        let ti = PyroSqlTypeInfo::from_tag(pwire::TYPE_F64);
        assert_eq!(ti.name, "DOUBLE");
        assert_eq!(ti.type_tag, 2);
        assert!(ti.is_numeric());
    }

    #[test]
    fn test_from_tag_text() {
        let ti = PyroSqlTypeInfo::from_tag(pwire::TYPE_TEXT);
        assert_eq!(ti.name, "TEXT");
        assert!(ti.is_text());
        assert!(!ti.is_numeric());
    }

    #[test]
    fn test_from_tag_bool() {
        let ti = PyroSqlTypeInfo::from_tag(pwire::TYPE_BOOL);
        assert_eq!(ti.name, "BOOLEAN");
        assert!(!ti.is_numeric());
    }

    #[test]
    fn test_from_tag_bytes() {
        let ti = PyroSqlTypeInfo::from_tag(pwire::TYPE_BYTES);
        assert_eq!(ti.name, "BLOB");
        assert!(ti.is_binary());
    }

    #[test]
    fn test_from_tag_null() {
        let ti = PyroSqlTypeInfo::from_tag(pwire::TYPE_NULL);
        assert_eq!(ti.name, "NULL");
        assert!(ti.is_null());
    }

    #[test]
    fn test_from_tag_unknown() {
        let ti = PyroSqlTypeInfo::from_tag(99);
        assert_eq!(ti.name, "UNKNOWN");
        assert!(!ti.is_null());
        assert!(!ti.is_numeric());
    }

    #[test]
    fn test_display() {
        assert_eq!(format!("{}", PyroSqlTypeInfo::BIGINT), "BIGINT");
        assert_eq!(format!("{}", PyroSqlTypeInfo::TEXT), "TEXT");
    }

    #[test]
    fn test_equality() {
        assert_eq!(PyroSqlTypeInfo::BIGINT, PyroSqlTypeInfo::from_tag(1));
        assert_ne!(PyroSqlTypeInfo::BIGINT, PyroSqlTypeInfo::TEXT);
    }

    #[test]
    fn test_rust_type_name() {
        assert_eq!(PyroSqlTypeInfo::BIGINT.rust_type_name(), "i64");
        assert_eq!(PyroSqlTypeInfo::DOUBLE.rust_type_name(), "f64");
        assert_eq!(PyroSqlTypeInfo::TEXT.rust_type_name(), "String");
        assert_eq!(PyroSqlTypeInfo::BOOLEAN.rust_type_name(), "bool");
        assert_eq!(PyroSqlTypeInfo::BLOB.rust_type_name(), "Vec<u8>");
    }
}
