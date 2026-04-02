//! Diesel backend for PyroSQL.
//!
//! This crate provides a [`PyroSqlBackend`] implementation for Diesel,
//! enabling PyroSQL as a first-class Diesel database target. It speaks
//! the PWire binary protocol natively over TCP.
//!
//! # Example
//!
//! ```no_run
//! use diesel::prelude::*;
//! use diesel_pyrosql::PyroSqlConnection;
//!
//! let mut conn = PyroSqlConnection::establish("pyrosql://admin:secret@localhost:12520/mydb")
//!     .expect("Failed to connect");
//! ```

pub mod connection;
pub mod query_builder;
pub mod types;

use diesel::backend::{Backend, DieselReserveSpecialization};
use diesel::sql_types::TypeMetadata;

/// The PyroSQL Diesel backend.
///
/// This type implements [`Backend`] to plug PyroSQL into Diesel's
/// query building, type system, and connection machinery.
#[derive(Debug, Copy, Clone, Hash, PartialEq, Eq)]
pub struct PyroSqlBackend;

/// Metadata for PyroSQL types, identified by a byte tag matching
/// the PWire protocol value type constants.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PyroSqlTypeMetadata {
    /// The PWire type tag.
    pub type_tag: u8,
}

/// PWire protocol constants.
pub mod pwire {
    // Client -> server message types
    pub const MSG_QUERY: u8 = 0x01;
    pub const MSG_PREPARE: u8 = 0x02;
    pub const MSG_EXECUTE: u8 = 0x03;
    pub const MSG_CLOSE: u8 = 0x04;
    pub const MSG_PING: u8 = 0x05;
    pub const MSG_AUTH: u8 = 0x06;
    pub const MSG_QUIT: u8 = 0xFF;

    // Server -> client response types
    pub const RESP_RESULT_SET: u8 = 0x01;
    pub const RESP_OK: u8 = 0x02;
    pub const RESP_ERROR: u8 = 0x03;
    pub const RESP_PONG: u8 = 0x04;
    pub const RESP_READY: u8 = 0x05;

    // Value type tags
    pub const TYPE_NULL: u8 = 0;
    pub const TYPE_I64: u8 = 1;
    pub const TYPE_F64: u8 = 2;
    pub const TYPE_TEXT: u8 = 3;
    pub const TYPE_BOOL: u8 = 4;
    pub const TYPE_BYTES: u8 = 5;

    pub const HEADER_SIZE: usize = 5;

    /// Build a PWire frame: 1-byte type + 4-byte LE length + payload.
    pub fn frame(msg_type: u8, payload: &[u8]) -> Vec<u8> {
        let mut buf = Vec::with_capacity(HEADER_SIZE + payload.len());
        buf.push(msg_type);
        buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        buf.extend_from_slice(payload);
        buf
    }

    /// Encode an AUTH message.
    pub fn encode_auth(user: &str, password: &str) -> Vec<u8> {
        let user_bytes = user.as_bytes();
        let pass_bytes = password.as_bytes();
        let mut payload = Vec::with_capacity(2 + user_bytes.len() + pass_bytes.len());
        payload.push(user_bytes.len() as u8);
        payload.extend_from_slice(user_bytes);
        payload.push(pass_bytes.len() as u8);
        payload.extend_from_slice(pass_bytes);
        frame(MSG_AUTH, &payload)
    }

    /// Encode a QUERY message.
    pub fn encode_query(sql: &str) -> Vec<u8> {
        frame(MSG_QUERY, sql.as_bytes())
    }

    /// Encode a PREPARE message.
    pub fn encode_prepare(sql: &str) -> Vec<u8> {
        frame(MSG_PREPARE, sql.as_bytes())
    }

    /// Encode an EXECUTE message.
    pub fn encode_execute(handle: u32, params: &[String]) -> Vec<u8> {
        let mut payload = Vec::new();
        payload.extend_from_slice(&handle.to_le_bytes());
        payload.extend_from_slice(&(params.len() as u16).to_le_bytes());
        for p in params {
            let bytes = p.as_bytes();
            payload.extend_from_slice(&(bytes.len() as u16).to_le_bytes());
            payload.extend_from_slice(bytes);
        }
        frame(MSG_EXECUTE, &payload)
    }

    /// Encode a CLOSE message.
    pub fn encode_close(handle: u32) -> Vec<u8> {
        frame(MSG_CLOSE, &handle.to_le_bytes())
    }

    /// Encode a PING message.
    pub fn encode_ping() -> Vec<u8> {
        frame(MSG_PING, &[])
    }

    /// Encode a QUIT message.
    pub fn encode_quit() -> Vec<u8> {
        frame(MSG_QUIT, &[])
    }

    /// Column metadata from a result set.
    #[derive(Debug, Clone)]
    pub struct ColumnInfo {
        pub name: String,
        pub type_tag: u8,
    }

    /// A decoded result set.
    #[derive(Debug, Clone)]
    pub struct ResultSet {
        pub columns: Vec<ColumnInfo>,
        pub rows: Vec<Vec<Option<Value>>>,
    }

    /// A typed value from the wire protocol.
    #[derive(Debug, Clone)]
    pub enum Value {
        I64(i64),
        F64(f64),
        Text(String),
        Bool(bool),
        Bytes(Vec<u8>),
    }

    /// A decoded OK response.
    #[derive(Debug, Clone)]
    pub struct OkResponse {
        pub rows_affected: i64,
        pub tag: String,
    }

    /// A decoded ERROR response.
    #[derive(Debug, Clone)]
    pub struct ErrorResponse {
        pub sql_state: String,
        pub message: String,
    }

    /// Read exactly `count` bytes from a reader.
    pub fn read_exact(reader: &mut dyn std::io::Read, count: usize) -> std::io::Result<Vec<u8>> {
        let mut buf = vec![0u8; count];
        reader.read_exact(&mut buf)?;
        Ok(buf)
    }

    /// Read a PWire frame from a reader. Returns (type, payload).
    pub fn read_frame(reader: &mut dyn std::io::Read) -> std::io::Result<(u8, Vec<u8>)> {
        let header = read_exact(reader, HEADER_SIZE)?;
        let msg_type = header[0];
        let length = u32::from_le_bytes([header[1], header[2], header[3], header[4]]) as usize;
        let payload = if length > 0 {
            read_exact(reader, length)?
        } else {
            Vec::new()
        };
        Ok((msg_type, payload))
    }

    /// Decode a RESULT_SET response payload.
    pub fn decode_result_set(payload: &[u8]) -> Result<ResultSet, String> {
        let mut pos = 0;
        if payload.len() < 2 {
            return Err("Malformed result set".into());
        }
        let col_count = u16::from_le_bytes([payload[pos], payload[pos + 1]]) as usize;
        pos += 2;

        let mut columns = Vec::with_capacity(col_count);
        for _ in 0..col_count {
            if pos >= payload.len() {
                return Err("Unexpected end in column definitions".into());
            }
            let name_len = payload[pos] as usize;
            pos += 1;
            if pos + name_len + 1 > payload.len() {
                return Err("Column name overflow".into());
            }
            let name = String::from_utf8_lossy(&payload[pos..pos + name_len]).into_owned();
            pos += name_len;
            let type_tag = payload[pos];
            pos += 1;
            columns.push(ColumnInfo { name, type_tag });
        }

        if pos + 4 > payload.len() {
            return Err("Missing row count".into());
        }
        let row_count = u32::from_le_bytes([
            payload[pos], payload[pos + 1], payload[pos + 2], payload[pos + 3],
        ]) as usize;
        pos += 4;

        let null_bitmap_len = (col_count + 7) / 8;
        let mut rows = Vec::with_capacity(row_count);

        for _ in 0..row_count {
            if pos + null_bitmap_len > payload.len() {
                return Err("Missing null bitmap".into());
            }
            let bitmap = &payload[pos..pos + null_bitmap_len];
            pos += null_bitmap_len;

            let mut row = Vec::with_capacity(col_count);
            for c in 0..col_count {
                let byte_idx = c / 8;
                let bit_idx = c % 8;
                let is_null = byte_idx < bitmap.len() && (bitmap[byte_idx] >> bit_idx) & 1 == 1;

                if is_null {
                    row.push(None);
                    continue;
                }

                let val = match columns[c].type_tag {
                    TYPE_I64 => {
                        if pos + 8 > payload.len() {
                            return Err("i64 overflow".into());
                        }
                        let v = i64::from_le_bytes([
                            payload[pos], payload[pos+1], payload[pos+2], payload[pos+3],
                            payload[pos+4], payload[pos+5], payload[pos+6], payload[pos+7],
                        ]);
                        pos += 8;
                        Value::I64(v)
                    }
                    TYPE_F64 => {
                        if pos + 8 > payload.len() {
                            return Err("f64 overflow".into());
                        }
                        let v = f64::from_le_bytes([
                            payload[pos], payload[pos+1], payload[pos+2], payload[pos+3],
                            payload[pos+4], payload[pos+5], payload[pos+6], payload[pos+7],
                        ]);
                        pos += 8;
                        Value::F64(v)
                    }
                    TYPE_BOOL => {
                        if pos >= payload.len() {
                            return Err("bool overflow".into());
                        }
                        let v = payload[pos] != 0;
                        pos += 1;
                        Value::Bool(v)
                    }
                    TYPE_TEXT => {
                        if pos + 2 > payload.len() {
                            return Err("text length overflow".into());
                        }
                        let len = u16::from_le_bytes([payload[pos], payload[pos + 1]]) as usize;
                        pos += 2;
                        if pos + len > payload.len() {
                            return Err("text data overflow".into());
                        }
                        let v = String::from_utf8_lossy(&payload[pos..pos + len]).into_owned();
                        pos += len;
                        Value::Text(v)
                    }
                    TYPE_BYTES => {
                        if pos + 2 > payload.len() {
                            return Err("bytes length overflow".into());
                        }
                        let len = u16::from_le_bytes([payload[pos], payload[pos + 1]]) as usize;
                        pos += 2;
                        if pos + len > payload.len() {
                            return Err("bytes data overflow".into());
                        }
                        let v = payload[pos..pos + len].to_vec();
                        pos += len;
                        Value::Bytes(v)
                    }
                    _ => {
                        if pos + 2 > payload.len() {
                            return Err("unknown type length overflow".into());
                        }
                        let len = u16::from_le_bytes([payload[pos], payload[pos + 1]]) as usize;
                        pos += 2;
                        if pos + len > payload.len() {
                            return Err("unknown type data overflow".into());
                        }
                        let v = String::from_utf8_lossy(&payload[pos..pos + len]).into_owned();
                        pos += len;
                        Value::Text(v)
                    }
                };
                row.push(Some(val));
            }
            rows.push(row);
        }

        Ok(ResultSet { columns, rows })
    }

    /// Decode an OK response payload.
    pub fn decode_ok(payload: &[u8]) -> Result<OkResponse, String> {
        if payload.len() < 9 {
            return Err("Malformed OK response".into());
        }
        let rows_affected = i64::from_le_bytes([
            payload[0], payload[1], payload[2], payload[3],
            payload[4], payload[5], payload[6], payload[7],
        ]);
        let tag_len = payload[8] as usize;
        if 9 + tag_len > payload.len() {
            return Err("OK tag overflow".into());
        }
        let tag = String::from_utf8_lossy(&payload[9..9 + tag_len]).into_owned();
        Ok(OkResponse { rows_affected, tag })
    }

    /// Decode an ERROR response payload.
    pub fn decode_error(payload: &[u8]) -> Result<ErrorResponse, String> {
        if payload.len() < 7 {
            return Err("Malformed ERROR response".into());
        }
        let sql_state = String::from_utf8_lossy(&payload[0..5]).into_owned();
        let msg_len = u16::from_le_bytes([payload[5], payload[6]]) as usize;
        if 7 + msg_len > payload.len() {
            return Err("Error message overflow".into());
        }
        let message = String::from_utf8_lossy(&payload[7..7 + msg_len]).into_owned();
        Ok(ErrorResponse { sql_state, message })
    }
}

impl Backend for PyroSqlBackend {
    type QueryBuilder = query_builder::PyroSqlQueryBuilder;
    type RawValue<'a> = PyroSqlValue<'a>;
    type BindCollector<'a> = PyroSqlBindCollector;
}

impl DieselReserveSpecialization for PyroSqlBackend {}

impl TypeMetadata for PyroSqlBackend {
    type TypeId = PyroSqlTypeMetadata;
    type MetadataLookup = ();
}

/// A reference to a raw value in a PyroSQL row.
#[derive(Debug)]
pub struct PyroSqlValue<'a> {
    /// The inner value reference.
    pub inner: Option<&'a pwire::Value>,
    /// The type metadata.
    pub type_metadata: PyroSqlTypeMetadata,
}

/// Bind parameter collector for PyroSQL.
#[derive(Debug, Default)]
pub struct PyroSqlBindCollector {
    /// The collected bind parameters as strings.
    pub binds: Vec<(PyroSqlTypeMetadata, Option<String>)>,
}

impl<'a> diesel::query_builder::BindCollector<'a, PyroSqlBackend> for PyroSqlBindCollector {
    type Buffer = String;

    fn push_bound_value<T, U>(
        &mut self,
        bind: &'a U,
        metadata_lookup: &mut (),
    ) -> diesel::QueryResult<()>
    where
        T: diesel::sql_types::HasSqlType<PyroSqlBackend>,
        U: diesel::serialize::ToSql<T, PyroSqlBackend> + ?Sized,
    {
        let metadata = <T as diesel::sql_types::HasSqlType<PyroSqlBackend>>::metadata(metadata_lookup);
        let mut buf = diesel::serialize::Output::test();
        let is_null = bind.to_sql(&mut buf).map_err(|e| diesel::result::Error::SerializationError(e))?;
        match is_null {
            diesel::serialize::IsNull::Yes => {
                self.binds.push((metadata, None));
            }
            diesel::serialize::IsNull::No => {
                self.binds.push((metadata, Some(String::from_utf8_lossy(buf.as_ref()).into_owned())));
            }
        }
        Ok(())
    }
}

pub use connection::PyroSqlConnection;
