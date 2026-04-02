//! Type mapping between Diesel SQL types and the PyroSQL/PWire type system.

use crate::pwire;
use crate::{PyroSqlBackend, PyroSqlTypeMetadata, PyroSqlValue};
use diesel::deserialize::{self, FromSql};
use diesel::serialize::{self, IsNull, Output, ToSql};
use diesel::sql_types::*;
use std::io::Write;

// ── HasSqlType implementations ─────────────────────────────────────────────

impl HasSqlType<BigInt> for PyroSqlBackend {
    fn metadata(_: &mut ()) -> PyroSqlTypeMetadata {
        PyroSqlTypeMetadata { type_tag: pwire::TYPE_I64 }
    }
}

impl HasSqlType<Integer> for PyroSqlBackend {
    fn metadata(_: &mut ()) -> PyroSqlTypeMetadata {
        PyroSqlTypeMetadata { type_tag: pwire::TYPE_I64 }
    }
}

impl HasSqlType<SmallInt> for PyroSqlBackend {
    fn metadata(_: &mut ()) -> PyroSqlTypeMetadata {
        PyroSqlTypeMetadata { type_tag: pwire::TYPE_I64 }
    }
}

impl HasSqlType<Double> for PyroSqlBackend {
    fn metadata(_: &mut ()) -> PyroSqlTypeMetadata {
        PyroSqlTypeMetadata { type_tag: pwire::TYPE_F64 }
    }
}

impl HasSqlType<Float> for PyroSqlBackend {
    fn metadata(_: &mut ()) -> PyroSqlTypeMetadata {
        PyroSqlTypeMetadata { type_tag: pwire::TYPE_F64 }
    }
}

impl HasSqlType<Text> for PyroSqlBackend {
    fn metadata(_: &mut ()) -> PyroSqlTypeMetadata {
        PyroSqlTypeMetadata { type_tag: pwire::TYPE_TEXT }
    }
}

impl HasSqlType<Bool> for PyroSqlBackend {
    fn metadata(_: &mut ()) -> PyroSqlTypeMetadata {
        PyroSqlTypeMetadata { type_tag: pwire::TYPE_BOOL }
    }
}

impl HasSqlType<Binary> for PyroSqlBackend {
    fn metadata(_: &mut ()) -> PyroSqlTypeMetadata {
        PyroSqlTypeMetadata { type_tag: pwire::TYPE_BYTES }
    }
}

impl HasSqlType<Nullable<BigInt>> for PyroSqlBackend {
    fn metadata(lookup: &mut ()) -> PyroSqlTypeMetadata {
        <Self as HasSqlType<BigInt>>::metadata(lookup)
    }
}

impl HasSqlType<Nullable<Integer>> for PyroSqlBackend {
    fn metadata(lookup: &mut ()) -> PyroSqlTypeMetadata {
        <Self as HasSqlType<Integer>>::metadata(lookup)
    }
}

impl HasSqlType<Nullable<SmallInt>> for PyroSqlBackend {
    fn metadata(lookup: &mut ()) -> PyroSqlTypeMetadata {
        <Self as HasSqlType<SmallInt>>::metadata(lookup)
    }
}

impl HasSqlType<Nullable<Double>> for PyroSqlBackend {
    fn metadata(lookup: &mut ()) -> PyroSqlTypeMetadata {
        <Self as HasSqlType<Double>>::metadata(lookup)
    }
}

impl HasSqlType<Nullable<Float>> for PyroSqlBackend {
    fn metadata(lookup: &mut ()) -> PyroSqlTypeMetadata {
        <Self as HasSqlType<Float>>::metadata(lookup)
    }
}

impl HasSqlType<Nullable<Text>> for PyroSqlBackend {
    fn metadata(lookup: &mut ()) -> PyroSqlTypeMetadata {
        <Self as HasSqlType<Text>>::metadata(lookup)
    }
}

impl HasSqlType<Nullable<Bool>> for PyroSqlBackend {
    fn metadata(lookup: &mut ()) -> PyroSqlTypeMetadata {
        <Self as HasSqlType<Bool>>::metadata(lookup)
    }
}

impl HasSqlType<Nullable<Binary>> for PyroSqlBackend {
    fn metadata(lookup: &mut ()) -> PyroSqlTypeMetadata {
        <Self as HasSqlType<Binary>>::metadata(lookup)
    }
}

// ── ToSql implementations ──────────────────────────────────────────────────

impl ToSql<BigInt, PyroSqlBackend> for i64 {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, PyroSqlBackend>) -> serialize::Result {
        write!(out, "{}", self)?;
        Ok(IsNull::No)
    }
}

impl ToSql<Integer, PyroSqlBackend> for i32 {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, PyroSqlBackend>) -> serialize::Result {
        write!(out, "{}", self)?;
        Ok(IsNull::No)
    }
}

impl ToSql<SmallInt, PyroSqlBackend> for i16 {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, PyroSqlBackend>) -> serialize::Result {
        write!(out, "{}", self)?;
        Ok(IsNull::No)
    }
}

impl ToSql<Double, PyroSqlBackend> for f64 {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, PyroSqlBackend>) -> serialize::Result {
        write!(out, "{}", self)?;
        Ok(IsNull::No)
    }
}

impl ToSql<Float, PyroSqlBackend> for f32 {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, PyroSqlBackend>) -> serialize::Result {
        write!(out, "{}", self)?;
        Ok(IsNull::No)
    }
}

impl ToSql<Text, PyroSqlBackend> for str {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, PyroSqlBackend>) -> serialize::Result {
        write!(out, "{}", self)?;
        Ok(IsNull::No)
    }
}

impl ToSql<Text, PyroSqlBackend> for String {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, PyroSqlBackend>) -> serialize::Result {
        <str as ToSql<Text, PyroSqlBackend>>::to_sql(self.as_str(), out)
    }
}

impl ToSql<Bool, PyroSqlBackend> for bool {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, PyroSqlBackend>) -> serialize::Result {
        write!(out, "{}", if *self { "1" } else { "0" })?;
        Ok(IsNull::No)
    }
}

impl ToSql<Binary, PyroSqlBackend> for [u8] {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, PyroSqlBackend>) -> serialize::Result {
        out.write_all(self)?;
        Ok(IsNull::No)
    }
}

impl ToSql<Binary, PyroSqlBackend> for Vec<u8> {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, PyroSqlBackend>) -> serialize::Result {
        <[u8] as ToSql<Binary, PyroSqlBackend>>::to_sql(self.as_slice(), out)
    }
}

// ── FromSql implementations ────────────────────────────────────────────────

impl FromSql<BigInt, PyroSqlBackend> for i64 {
    fn from_sql(value: PyroSqlValue<'_>) -> deserialize::Result<Self> {
        match value.inner {
            Some(pwire::Value::I64(v)) => Ok(*v),
            Some(pwire::Value::Text(s)) => s.parse().map_err(|e| Box::new(e).into()),
            _ => Err("Expected i64 value".into()),
        }
    }
}

impl FromSql<Integer, PyroSqlBackend> for i32 {
    fn from_sql(value: PyroSqlValue<'_>) -> deserialize::Result<Self> {
        match value.inner {
            Some(pwire::Value::I64(v)) => Ok(*v as i32),
            Some(pwire::Value::Text(s)) => s.parse().map_err(|e| Box::new(e).into()),
            _ => Err("Expected i32 value".into()),
        }
    }
}

impl FromSql<SmallInt, PyroSqlBackend> for i16 {
    fn from_sql(value: PyroSqlValue<'_>) -> deserialize::Result<Self> {
        match value.inner {
            Some(pwire::Value::I64(v)) => Ok(*v as i16),
            Some(pwire::Value::Text(s)) => s.parse().map_err(|e| Box::new(e).into()),
            _ => Err("Expected i16 value".into()),
        }
    }
}

impl FromSql<Double, PyroSqlBackend> for f64 {
    fn from_sql(value: PyroSqlValue<'_>) -> deserialize::Result<Self> {
        match value.inner {
            Some(pwire::Value::F64(v)) => Ok(*v),
            Some(pwire::Value::I64(v)) => Ok(*v as f64),
            Some(pwire::Value::Text(s)) => s.parse().map_err(|e| Box::new(e).into()),
            _ => Err("Expected f64 value".into()),
        }
    }
}

impl FromSql<Float, PyroSqlBackend> for f32 {
    fn from_sql(value: PyroSqlValue<'_>) -> deserialize::Result<Self> {
        match value.inner {
            Some(pwire::Value::F64(v)) => Ok(*v as f32),
            Some(pwire::Value::I64(v)) => Ok(*v as f32),
            Some(pwire::Value::Text(s)) => s.parse().map_err(|e| Box::new(e).into()),
            _ => Err("Expected f32 value".into()),
        }
    }
}

impl FromSql<Text, PyroSqlBackend> for String {
    fn from_sql(value: PyroSqlValue<'_>) -> deserialize::Result<Self> {
        match value.inner {
            Some(pwire::Value::Text(s)) => Ok(s.clone()),
            Some(pwire::Value::I64(v)) => Ok(v.to_string()),
            Some(pwire::Value::F64(v)) => Ok(v.to_string()),
            Some(pwire::Value::Bool(v)) => Ok(v.to_string()),
            _ => Err("Expected text value".into()),
        }
    }
}

impl FromSql<Bool, PyroSqlBackend> for bool {
    fn from_sql(value: PyroSqlValue<'_>) -> deserialize::Result<Self> {
        match value.inner {
            Some(pwire::Value::Bool(v)) => Ok(*v),
            Some(pwire::Value::I64(v)) => Ok(*v != 0),
            Some(pwire::Value::Text(s)) => match s.as_str() {
                "1" | "true" | "t" | "TRUE" => Ok(true),
                "0" | "false" | "f" | "FALSE" => Ok(false),
                _ => Err(format!("Cannot parse '{}' as bool", s).into()),
            },
            _ => Err("Expected bool value".into()),
        }
    }
}

impl FromSql<Binary, PyroSqlBackend> for Vec<u8> {
    fn from_sql(value: PyroSqlValue<'_>) -> deserialize::Result<Self> {
        match value.inner {
            Some(pwire::Value::Bytes(v)) => Ok(v.clone()),
            Some(pwire::Value::Text(s)) => Ok(s.as_bytes().to_vec()),
            _ => Err("Expected binary value".into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_type_metadata_i64() {
        let meta = <PyroSqlBackend as HasSqlType<BigInt>>::metadata(&mut ());
        assert_eq!(meta.type_tag, pwire::TYPE_I64);
    }

    #[test]
    fn test_type_metadata_f64() {
        let meta = <PyroSqlBackend as HasSqlType<Double>>::metadata(&mut ());
        assert_eq!(meta.type_tag, pwire::TYPE_F64);
    }

    #[test]
    fn test_type_metadata_text() {
        let meta = <PyroSqlBackend as HasSqlType<Text>>::metadata(&mut ());
        assert_eq!(meta.type_tag, pwire::TYPE_TEXT);
    }

    #[test]
    fn test_type_metadata_bool() {
        let meta = <PyroSqlBackend as HasSqlType<Bool>>::metadata(&mut ());
        assert_eq!(meta.type_tag, pwire::TYPE_BOOL);
    }

    #[test]
    fn test_type_metadata_bytes() {
        let meta = <PyroSqlBackend as HasSqlType<Binary>>::metadata(&mut ());
        assert_eq!(meta.type_tag, pwire::TYPE_BYTES);
    }

    #[test]
    fn test_type_metadata_integer_maps_to_i64() {
        let meta = <PyroSqlBackend as HasSqlType<Integer>>::metadata(&mut ());
        assert_eq!(meta.type_tag, pwire::TYPE_I64);
    }

    #[test]
    fn test_type_metadata_nullable() {
        let meta = <PyroSqlBackend as HasSqlType<Nullable<BigInt>>>::metadata(&mut ());
        assert_eq!(meta.type_tag, pwire::TYPE_I64);
    }

    #[test]
    fn test_from_sql_i64() {
        let val = pwire::Value::I64(42);
        let pv = PyroSqlValue {
            inner: Some(&val),
            type_metadata: PyroSqlTypeMetadata { type_tag: pwire::TYPE_I64 },
        };
        let result: i64 = FromSql::<BigInt, PyroSqlBackend>::from_sql(pv).unwrap();
        assert_eq!(result, 42);
    }

    #[test]
    fn test_from_sql_f64() {
        let val = pwire::Value::F64(3.14);
        let pv = PyroSqlValue {
            inner: Some(&val),
            type_metadata: PyroSqlTypeMetadata { type_tag: pwire::TYPE_F64 },
        };
        let result: f64 = FromSql::<Double, PyroSqlBackend>::from_sql(pv).unwrap();
        assert!((result - 3.14).abs() < f64::EPSILON);
    }

    #[test]
    fn test_from_sql_text() {
        let val = pwire::Value::Text("hello".to_string());
        let pv = PyroSqlValue {
            inner: Some(&val),
            type_metadata: PyroSqlTypeMetadata { type_tag: pwire::TYPE_TEXT },
        };
        let result: String = FromSql::<Text, PyroSqlBackend>::from_sql(pv).unwrap();
        assert_eq!(result, "hello");
    }

    #[test]
    fn test_from_sql_bool_true() {
        let val = pwire::Value::Bool(true);
        let pv = PyroSqlValue {
            inner: Some(&val),
            type_metadata: PyroSqlTypeMetadata { type_tag: pwire::TYPE_BOOL },
        };
        let result: bool = FromSql::<Bool, PyroSqlBackend>::from_sql(pv).unwrap();
        assert!(result);
    }

    #[test]
    fn test_from_sql_bool_false() {
        let val = pwire::Value::Bool(false);
        let pv = PyroSqlValue {
            inner: Some(&val),
            type_metadata: PyroSqlTypeMetadata { type_tag: pwire::TYPE_BOOL },
        };
        let result: bool = FromSql::<Bool, PyroSqlBackend>::from_sql(pv).unwrap();
        assert!(!result);
    }

    #[test]
    fn test_from_sql_bytes() {
        let val = pwire::Value::Bytes(vec![1, 2, 3]);
        let pv = PyroSqlValue {
            inner: Some(&val),
            type_metadata: PyroSqlTypeMetadata { type_tag: pwire::TYPE_BYTES },
        };
        let result: Vec<u8> = FromSql::<Binary, PyroSqlBackend>::from_sql(pv).unwrap();
        assert_eq!(result, vec![1, 2, 3]);
    }

    #[test]
    fn test_from_sql_i32_from_i64() {
        let val = pwire::Value::I64(100);
        let pv = PyroSqlValue {
            inner: Some(&val),
            type_metadata: PyroSqlTypeMetadata { type_tag: pwire::TYPE_I64 },
        };
        let result: i32 = FromSql::<Integer, PyroSqlBackend>::from_sql(pv).unwrap();
        assert_eq!(result, 100);
    }
}
