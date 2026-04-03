//! RMP wire protocol types and binary codec.
//!
//! Wire format for all RMP messages:
//!
//! ```text
//! ┌────────┬──────────┬──────────────────────────┐
//! │ type 1B│ len 4B LE│ payload                  │
//! └────────┴──────────┴──────────────────────────┘
//! ```

use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::fmt;

// ── Wire type constants ──────────────────────────────────────────────────────

const MSG_SUBSCRIBE: u8 = 0x20;
const MSG_UNSUBSCRIBE: u8 = 0x21;
const MSG_SNAPSHOT: u8 = 0x22;
const MSG_DELTA: u8 = 0x23;
const MSG_MUTATE: u8 = 0x24;

// ── Core types ───────────────────────────────────────────────────────────────

/// A subscription request from client to server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Subscribe {
    /// Client-assigned subscription ID.
    pub sub_id: u64,
    /// Table name to subscribe to.
    pub table: String,
    /// Filter predicate for rows of interest.
    pub predicate: Predicate,
}

/// An unsubscribe request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unsubscribe {
    /// Subscription ID to cancel.
    pub sub_id: u64,
}

/// A full snapshot of subscribed data, sent once after subscription.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    /// Subscription ID this snapshot belongs to.
    pub sub_id: u64,
    /// Server version at snapshot time.
    pub version: u64,
    /// Column metadata.
    pub columns: Vec<ColumnInfo>,
    /// All rows: each entry is (pk_bytes, row_bytes).
    pub rows: Vec<(Vec<u8>, Vec<u8>)>,
}

/// An incremental delta pushed from server to client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Delta {
    /// Subscription ID this delta belongs to.
    pub sub_id: u64,
    /// Server version after this delta.
    pub version: u64,
    /// Individual row changes in this delta.
    pub changes: Vec<RowChange>,
}

/// A mutation request from client to server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mutate {
    /// Table to mutate.
    pub table: String,
    /// The operation to perform.
    pub op: DeltaOp,
    /// Primary key bytes of the target row.
    pub pk: Vec<u8>,
    /// Row data (required for Insert and Update, None for Delete).
    pub row: Option<Vec<u8>>,
}

/// A single row-level change within a delta.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowChange {
    /// The operation that occurred.
    pub op: DeltaOp,
    /// Primary key of the affected row.
    pub pk: Vec<u8>,
    /// New row data (present for Insert and Update).
    pub row: Option<Vec<u8>>,
}

/// The type of mutation in a delta or mutate request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeltaOp {
    /// A new row was inserted.
    Insert = 0,
    /// An existing row was updated.
    Update = 1,
    /// A row was deleted.
    Delete = 2,
}

/// A filter predicate for subscriptions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Predicate {
    /// Subscribe to all rows in the table.
    All,
    /// Subscribe to rows matching a specific column value.
    Eq {
        /// Column name to filter on.
        column: String,
        /// Value to match (encoded as bytes).
        value: Vec<u8>,
    },
    /// Subscribe to rows within a key range (inclusive).
    Range {
        /// Start of the range (inclusive).
        start: Vec<u8>,
        /// End of the range (inclusive).
        end: Vec<u8>,
    },
}

/// Column metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnInfo {
    /// Column name.
    pub name: String,
    /// Column type tag.
    pub type_tag: ColumnType,
}

/// Supported column types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnType {
    /// 64-bit signed integer.
    Int64 = 0,
    /// UTF-8 text.
    Text = 1,
    /// Raw bytes / blob.
    Bytes = 2,
    /// Boolean.
    Bool = 3,
    /// 64-bit floating point.
    Float64 = 4,
}

// ── Display ──────────────────────────────────────────────────────────────────

impl fmt::Display for DeltaOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DeltaOp::Insert => write!(f, "INSERT"),
            DeltaOp::Update => write!(f, "UPDATE"),
            DeltaOp::Delete => write!(f, "DELETE"),
        }
    }
}

impl fmt::Display for Predicate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Predicate::All => write!(f, "ALL"),
            Predicate::Eq { column, .. } => write!(f, "EQ({column})"),
            Predicate::Range { .. } => write!(f, "RANGE"),
        }
    }
}

// ── Binary codec ─────────────────────────────────────────────────────────────

/// Error returned by decode functions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodecError {
    /// Not enough bytes to decode the message.
    UnexpectedEof,
    /// Unknown message type byte.
    UnknownMessageType(u8),
    /// Unknown DeltaOp discriminant.
    UnknownDeltaOp(u8),
    /// Unknown Predicate discriminant.
    UnknownPredicate(u8),
    /// Unknown ColumnType discriminant.
    UnknownColumnType(u8),
    /// Invalid UTF-8 in a string field.
    InvalidUtf8,
}

impl fmt::Display for CodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CodecError::UnexpectedEof => write!(f, "unexpected end of input"),
            CodecError::UnknownMessageType(b) => write!(f, "unknown message type: {b:#04x}"),
            CodecError::UnknownDeltaOp(b) => write!(f, "unknown delta op: {b}"),
            CodecError::UnknownPredicate(b) => write!(f, "unknown predicate: {b}"),
            CodecError::UnknownColumnType(b) => write!(f, "unknown column type: {b}"),
            CodecError::InvalidUtf8 => write!(f, "invalid UTF-8 in string field"),
        }
    }
}

impl std::error::Error for CodecError {}

/// An RMP message that can be sent over the wire.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message {
    /// Client subscribes to a table.
    Subscribe(Subscribe),
    /// Client unsubscribes.
    Unsubscribe(Unsubscribe),
    /// Server sends initial snapshot.
    Snapshot(Snapshot),
    /// Server pushes incremental delta.
    Delta(Delta),
    /// Client sends a mutation.
    Mutate(Mutate),
}

// ── Encode helpers ───────────────────────────────────────────────────────────

fn encode_bytes(buf: &mut BytesMut, data: &[u8]) {
    buf.put_u32_le(data.len() as u32);
    buf.put_slice(data);
}

fn encode_string(buf: &mut BytesMut, s: &str) {
    encode_bytes(buf, s.as_bytes());
}

fn encode_predicate(buf: &mut BytesMut, pred: &Predicate) {
    match pred {
        Predicate::All => {
            buf.put_u8(0);
        }
        Predicate::Eq { column, value } => {
            buf.put_u8(1);
            encode_string(buf, column);
            encode_bytes(buf, value);
        }
        Predicate::Range { start, end } => {
            buf.put_u8(2);
            encode_bytes(buf, start);
            encode_bytes(buf, end);
        }
    }
}

fn encode_column_info(buf: &mut BytesMut, col: &ColumnInfo) {
    encode_string(buf, &col.name);
    buf.put_u8(col.type_tag as u8);
}

fn encode_row_change(buf: &mut BytesMut, change: &RowChange) {
    buf.put_u8(change.op as u8);
    encode_bytes(buf, &change.pk);
    match &change.row {
        Some(data) => {
            buf.put_u8(1);
            encode_bytes(buf, data);
        }
        None => {
            buf.put_u8(0);
        }
    }
}

// ── Decode helpers ───────────────────────────────────────────────────────────

fn decode_bytes(buf: &mut Bytes) -> Result<Vec<u8>, CodecError> {
    if buf.remaining() < 4 {
        return Err(CodecError::UnexpectedEof);
    }
    let len = buf.get_u32_le() as usize;
    if buf.remaining() < len {
        return Err(CodecError::UnexpectedEof);
    }
    let data = buf.slice(..len).to_vec();
    buf.advance(len);
    Ok(data)
}

fn decode_string(buf: &mut Bytes) -> Result<String, CodecError> {
    let data = decode_bytes(buf)?;
    String::from_utf8(data).map_err(|_| CodecError::InvalidUtf8)
}

fn decode_predicate(buf: &mut Bytes) -> Result<Predicate, CodecError> {
    if buf.remaining() < 1 {
        return Err(CodecError::UnexpectedEof);
    }
    let tag = buf.get_u8();
    match tag {
        0 => Ok(Predicate::All),
        1 => {
            let column = decode_string(buf)?;
            let value = decode_bytes(buf)?;
            Ok(Predicate::Eq { column, value })
        }
        2 => {
            let start = decode_bytes(buf)?;
            let end = decode_bytes(buf)?;
            Ok(Predicate::Range { start, end })
        }
        other => Err(CodecError::UnknownPredicate(other)),
    }
}

fn decode_column_info(buf: &mut Bytes) -> Result<ColumnInfo, CodecError> {
    let name = decode_string(buf)?;
    if buf.remaining() < 1 {
        return Err(CodecError::UnexpectedEof);
    }
    let type_byte = buf.get_u8();
    let type_tag = match type_byte {
        0 => ColumnType::Int64,
        1 => ColumnType::Text,
        2 => ColumnType::Bytes,
        3 => ColumnType::Bool,
        4 => ColumnType::Float64,
        other => return Err(CodecError::UnknownColumnType(other)),
    };
    Ok(ColumnInfo { name, type_tag })
}

fn decode_delta_op(b: u8) -> Result<DeltaOp, CodecError> {
    match b {
        0 => Ok(DeltaOp::Insert),
        1 => Ok(DeltaOp::Update),
        2 => Ok(DeltaOp::Delete),
        other => Err(CodecError::UnknownDeltaOp(other)),
    }
}

fn decode_row_change(buf: &mut Bytes) -> Result<RowChange, CodecError> {
    if buf.remaining() < 1 {
        return Err(CodecError::UnexpectedEof);
    }
    let op = decode_delta_op(buf.get_u8())?;
    let pk = decode_bytes(buf)?;
    if buf.remaining() < 1 {
        return Err(CodecError::UnexpectedEof);
    }
    let has_row = buf.get_u8();
    let row = if has_row == 1 {
        Some(decode_bytes(buf)?)
    } else {
        None
    };
    Ok(RowChange { op, pk, row })
}

// ── Public encode/decode ─────────────────────────────────────────────────────

/// Encode a message into a framed wire buffer: [type 1B | len 4B LE | payload].
pub fn encode_message(msg: &Message) -> Bytes {
    let mut payload = BytesMut::with_capacity(256);

    let type_byte = match msg {
        Message::Subscribe(sub) => {
            payload.put_u64_le(sub.sub_id);
            encode_string(&mut payload, &sub.table);
            encode_predicate(&mut payload, &sub.predicate);
            MSG_SUBSCRIBE
        }
        Message::Unsubscribe(unsub) => {
            payload.put_u64_le(unsub.sub_id);
            MSG_UNSUBSCRIBE
        }
        Message::Snapshot(snap) => {
            payload.put_u64_le(snap.sub_id);
            payload.put_u64_le(snap.version);
            // columns
            payload.put_u32_le(snap.columns.len() as u32);
            for col in &snap.columns {
                encode_column_info(&mut payload, col);
            }
            // rows
            payload.put_u32_le(snap.rows.len() as u32);
            for (pk, row) in &snap.rows {
                encode_bytes(&mut payload, pk);
                encode_bytes(&mut payload, row);
            }
            MSG_SNAPSHOT
        }
        Message::Delta(delta) => {
            payload.put_u64_le(delta.sub_id);
            payload.put_u64_le(delta.version);
            payload.put_u32_le(delta.changes.len() as u32);
            for change in &delta.changes {
                encode_row_change(&mut payload, change);
            }
            MSG_DELTA
        }
        Message::Mutate(mutate) => {
            encode_string(&mut payload, &mutate.table);
            payload.put_u8(mutate.op as u8);
            encode_bytes(&mut payload, &mutate.pk);
            match &mutate.row {
                Some(data) => {
                    payload.put_u8(1);
                    encode_bytes(&mut payload, data);
                }
                None => {
                    payload.put_u8(0);
                }
            }
            MSG_MUTATE
        }
    };

    // Build framed message: type + len + payload
    let payload_bytes = payload.freeze();
    let mut frame = BytesMut::with_capacity(5 + payload_bytes.len());
    frame.put_u8(type_byte);
    frame.put_u32_le(payload_bytes.len() as u32);
    frame.put_slice(&payload_bytes);
    frame.freeze()
}

/// Decode a framed message from wire bytes.
///
/// Expects: [type 1B | len 4B LE | payload].
pub fn decode_message(data: &[u8]) -> Result<Message, CodecError> {
    if data.len() < 5 {
        return Err(CodecError::UnexpectedEof);
    }
    let type_byte = data[0];
    let len = u32::from_le_bytes([data[1], data[2], data[3], data[4]]) as usize;
    if data.len() < 5 + len {
        return Err(CodecError::UnexpectedEof);
    }
    let mut payload = Bytes::copy_from_slice(&data[5..5 + len]);

    match type_byte {
        MSG_SUBSCRIBE => {
            if payload.remaining() < 8 {
                return Err(CodecError::UnexpectedEof);
            }
            let sub_id = payload.get_u64_le();
            let table = decode_string(&mut payload)?;
            let predicate = decode_predicate(&mut payload)?;
            Ok(Message::Subscribe(Subscribe {
                sub_id,
                table,
                predicate,
            }))
        }
        MSG_UNSUBSCRIBE => {
            if payload.remaining() < 8 {
                return Err(CodecError::UnexpectedEof);
            }
            let sub_id = payload.get_u64_le();
            Ok(Message::Unsubscribe(Unsubscribe { sub_id }))
        }
        MSG_SNAPSHOT => {
            if payload.remaining() < 16 {
                return Err(CodecError::UnexpectedEof);
            }
            let sub_id = payload.get_u64_le();
            let version = payload.get_u64_le();
            // columns
            if payload.remaining() < 4 {
                return Err(CodecError::UnexpectedEof);
            }
            let num_cols = payload.get_u32_le() as usize;
            let mut columns = Vec::with_capacity(num_cols);
            for _ in 0..num_cols {
                columns.push(decode_column_info(&mut payload)?);
            }
            // rows
            if payload.remaining() < 4 {
                return Err(CodecError::UnexpectedEof);
            }
            let num_rows = payload.get_u32_le() as usize;
            let mut rows = Vec::with_capacity(num_rows);
            for _ in 0..num_rows {
                let pk = decode_bytes(&mut payload)?;
                let row = decode_bytes(&mut payload)?;
                rows.push((pk, row));
            }
            Ok(Message::Snapshot(Snapshot {
                sub_id,
                version,
                columns,
                rows,
            }))
        }
        MSG_DELTA => {
            if payload.remaining() < 16 {
                return Err(CodecError::UnexpectedEof);
            }
            let sub_id = payload.get_u64_le();
            let version = payload.get_u64_le();
            if payload.remaining() < 4 {
                return Err(CodecError::UnexpectedEof);
            }
            let num_changes = payload.get_u32_le() as usize;
            let mut changes = Vec::with_capacity(num_changes);
            for _ in 0..num_changes {
                changes.push(decode_row_change(&mut payload)?);
            }
            Ok(Message::Delta(Delta {
                sub_id,
                version,
                changes,
            }))
        }
        MSG_MUTATE => {
            let table = decode_string(&mut payload)?;
            if payload.remaining() < 1 {
                return Err(CodecError::UnexpectedEof);
            }
            let op = decode_delta_op(payload.get_u8())?;
            let pk = decode_bytes(&mut payload)?;
            if payload.remaining() < 1 {
                return Err(CodecError::UnexpectedEof);
            }
            let has_row = payload.get_u8();
            let row = if has_row == 1 {
                Some(decode_bytes(&mut payload)?)
            } else {
                None
            };
            Ok(Message::Mutate(Mutate {
                table,
                op,
                pk,
                row,
            }))
        }
        other => Err(CodecError::UnknownMessageType(other)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subscribe_roundtrip_all() {
        let msg = Message::Subscribe(Subscribe {
            sub_id: 42,
            table: "users".into(),
            predicate: Predicate::All,
        });
        let encoded = encode_message(&msg);
        let decoded = decode_message(&encoded).unwrap();
        assert_eq!(msg, decoded);
    }

    #[test]
    fn subscribe_roundtrip_eq() {
        let msg = Message::Subscribe(Subscribe {
            sub_id: 7,
            table: "orders".into(),
            predicate: Predicate::Eq {
                column: "status".into(),
                value: b"active".to_vec(),
            },
        });
        let encoded = encode_message(&msg);
        let decoded = decode_message(&encoded).unwrap();
        assert_eq!(msg, decoded);
    }

    #[test]
    fn subscribe_roundtrip_range() {
        let msg = Message::Subscribe(Subscribe {
            sub_id: 99,
            table: "events".into(),
            predicate: Predicate::Range {
                start: vec![0, 0, 0, 1],
                end: vec![0, 0, 3, 232],
            },
        });
        let encoded = encode_message(&msg);
        let decoded = decode_message(&encoded).unwrap();
        assert_eq!(msg, decoded);
    }

    #[test]
    fn unsubscribe_roundtrip() {
        let msg = Message::Unsubscribe(Unsubscribe { sub_id: 123 });
        let encoded = encode_message(&msg);
        let decoded = decode_message(&encoded).unwrap();
        assert_eq!(msg, decoded);
    }

    #[test]
    fn snapshot_roundtrip() {
        let msg = Message::Snapshot(Snapshot {
            sub_id: 1,
            version: 100,
            columns: vec![
                ColumnInfo {
                    name: "id".into(),
                    type_tag: ColumnType::Int64,
                },
                ColumnInfo {
                    name: "name".into(),
                    type_tag: ColumnType::Text,
                },
            ],
            rows: vec![
                (vec![0, 0, 0, 1], b"Alice".to_vec()),
                (vec![0, 0, 0, 2], b"Bob".to_vec()),
            ],
        });
        let encoded = encode_message(&msg);
        let decoded = decode_message(&encoded).unwrap();
        assert_eq!(msg, decoded);
    }

    #[test]
    fn delta_roundtrip() {
        let msg = Message::Delta(Delta {
            sub_id: 1,
            version: 101,
            changes: vec![
                RowChange {
                    op: DeltaOp::Insert,
                    pk: vec![0, 0, 0, 3],
                    row: Some(b"Charlie".to_vec()),
                },
                RowChange {
                    op: DeltaOp::Update,
                    pk: vec![0, 0, 0, 1],
                    row: Some(b"Alice Updated".to_vec()),
                },
                RowChange {
                    op: DeltaOp::Delete,
                    pk: vec![0, 0, 0, 2],
                    row: None,
                },
            ],
        });
        let encoded = encode_message(&msg);
        let decoded = decode_message(&encoded).unwrap();
        assert_eq!(msg, decoded);
    }

    #[test]
    fn mutate_insert_roundtrip() {
        let msg = Message::Mutate(Mutate {
            table: "users".into(),
            op: DeltaOp::Insert,
            pk: vec![0, 0, 0, 4],
            row: Some(b"Dave".to_vec()),
        });
        let encoded = encode_message(&msg);
        let decoded = decode_message(&encoded).unwrap();
        assert_eq!(msg, decoded);
    }

    #[test]
    fn mutate_delete_roundtrip() {
        let msg = Message::Mutate(Mutate {
            table: "users".into(),
            op: DeltaOp::Delete,
            pk: vec![0, 0, 0, 1],
            row: None,
        });
        let encoded = encode_message(&msg);
        let decoded = decode_message(&encoded).unwrap();
        assert_eq!(msg, decoded);
    }

    #[test]
    fn decode_truncated_returns_error() {
        let msg = Message::Subscribe(Subscribe {
            sub_id: 1,
            table: "t".into(),
            predicate: Predicate::All,
        });
        let encoded = encode_message(&msg);
        // Truncate payload
        let result = decode_message(&encoded[..5]);
        assert!(result.is_err());
    }

    #[test]
    fn decode_unknown_type_returns_error() {
        let data = [0xFE, 0x00, 0x00, 0x00, 0x00]; // unknown type 0xFE, 0-length payload
        let result = decode_message(&data);
        assert_eq!(result, Err(CodecError::UnknownMessageType(0xFE)));
    }

    #[test]
    fn snapshot_empty_rows_roundtrip() {
        let msg = Message::Snapshot(Snapshot {
            sub_id: 5,
            version: 0,
            columns: vec![],
            rows: vec![],
        });
        let encoded = encode_message(&msg);
        let decoded = decode_message(&encoded).unwrap();
        assert_eq!(msg, decoded);
    }

    #[test]
    fn delta_empty_changes_roundtrip() {
        let msg = Message::Delta(Delta {
            sub_id: 5,
            version: 50,
            changes: vec![],
        });
        let encoded = encode_message(&msg);
        let decoded = decode_message(&encoded).unwrap();
        assert_eq!(msg, decoded);
    }

    #[test]
    fn wire_format_header_structure() {
        let msg = Message::Unsubscribe(Unsubscribe { sub_id: 1 });
        let encoded = encode_message(&msg);
        // Type byte
        assert_eq!(encoded[0], MSG_UNSUBSCRIBE);
        // Length is LE u32 = 8 (one u64)
        let len = u32::from_le_bytes([encoded[1], encoded[2], encoded[3], encoded[4]]);
        assert_eq!(len, 8);
        // Total frame = 5 header + 8 payload
        assert_eq!(encoded.len(), 13);
    }

    #[test]
    fn all_column_types_roundtrip() {
        let columns = vec![
            ColumnInfo { name: "a".into(), type_tag: ColumnType::Int64 },
            ColumnInfo { name: "b".into(), type_tag: ColumnType::Text },
            ColumnInfo { name: "c".into(), type_tag: ColumnType::Bytes },
            ColumnInfo { name: "d".into(), type_tag: ColumnType::Bool },
            ColumnInfo { name: "e".into(), type_tag: ColumnType::Float64 },
        ];
        let msg = Message::Snapshot(Snapshot {
            sub_id: 1,
            version: 1,
            columns,
            rows: vec![],
        });
        let encoded = encode_message(&msg);
        let decoded = decode_message(&encoded).unwrap();
        assert_eq!(msg, decoded);
    }
}
