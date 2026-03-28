//! Arrow IPC framing for PyroLink QUIC streams.
//!
//! Each message on a bidirectional QUIC stream is encoded as:
//!
//! ```text
//! ┌────────┬──────────┬──────────────────────────────────────┐
//! │ type 1B│ len 4B LE│ Arrow IPC message (Flight payload)   │
//! └────────┴──────────┴──────────────────────────────────────┘
//! ```
//!
//! For streaming responses (e.g. DoGet), the server sends multiple framed
//! messages followed by a single [`MsgType::Eos`] byte with no length field.
//!
//! All multi-byte integers are **little-endian**.

use crate::error::PyroLinkError;
use bytes::Bytes;
use quinn::{RecvStream, SendStream};

// ── Message type constants ────────────────────────────────────────────────────

/// Wire type byte for an Arrow Schema message.
pub const MSG_SCHEMA: u8 = 0x01;
/// Wire type byte for an Arrow RecordBatch message.
pub const MSG_RECORD_BATCH: u8 = 0x02;
/// Wire type byte for a GetFlightInfo request.
pub const MSG_GET_FLIGHT_INFO: u8 = 0x03;
/// Wire type byte for a DoGet request.
pub const MSG_DO_GET: u8 = 0x04;
/// Wire type byte for a DoAction request.
pub const MSG_DO_ACTION: u8 = 0x05;
/// Wire type byte for a DoPut request.
pub const MSG_DO_PUT: u8 = 0x06;
/// Wire type byte for a ListActions request.
pub const MSG_LIST_ACTIONS: u8 = 0x07;
/// Wire type byte for a PrepareStatement request.
pub const MSG_PREPARE_STATEMENT: u8 = 0x08;
/// Wire type byte for a direct SQL query (single-roundtrip: SQL in, results out).
pub const MSG_QUERY: u8 = 0x09;
/// Wire type byte for a topology request (adaptive transport negotiation).
pub const MSG_TOPOLOGY: u8 = 0x0A;
/// Wire type byte for a server-pushed notification (LISTEN/NOTIFY, WATCH).
///
/// Sent on server-initiated unidirectional streams.  Payload is a JSON object:
/// `{"channel": "<name>", "payload": "<text>"}`.
pub const MSG_NOTIFICATION: u8 = 0x0F;
/// Wire type byte marking end-of-stream (no length or payload follows).
pub const MSG_EOS: u8 = 0xFF;

// ── RPC type enum ─────────────────────────────────────────────────────────────

/// The RPC operation encoded in the first byte of a new QUIC stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RpcType {
    /// `GetFlightInfo` — plan a SQL query, return a ticket.
    GetFlightInfo,
    /// `DoGet` — stream query results for a previously planned ticket.
    DoGet,
    /// `DoPut` — bulk-insert a RecordBatch stream into a table.
    DoPut,
    /// `DoAction` — execute a named action (transactions, DDL, etc.).
    DoAction,
    /// `ListActions` — enumerate supported server actions.
    ListActions,
    /// `PrepareStatement` — create a prepared statement handle.
    PrepareStatement,
    /// `Query` — direct SQL execution in a single roundtrip (SQL in, results out).
    Query,
    /// `Topology` — request server topology hints for adaptive transport negotiation.
    Topology,
}

impl TryFrom<u8> for RpcType {
    type Error = PyroLinkError;

    fn try_from(byte: u8) -> Result<Self, Self::Error> {
        match byte {
            MSG_GET_FLIGHT_INFO => Ok(Self::GetFlightInfo),
            MSG_DO_GET => Ok(Self::DoGet),
            MSG_DO_PUT => Ok(Self::DoPut),
            MSG_DO_ACTION => Ok(Self::DoAction),
            MSG_LIST_ACTIONS => Ok(Self::ListActions),
            MSG_PREPARE_STATEMENT => Ok(Self::PrepareStatement),
            MSG_QUERY => Ok(Self::Query),
            MSG_TOPOLOGY => Ok(Self::Topology),
            other => Err(PyroLinkError::UnknownRpcType(other)),
        }
    }
}

// ── Low-level read helpers ────────────────────────────────────────────────────

/// Read the RPC type byte from the start of a new QUIC stream.
///
/// This is the first byte sent by the client on every new bidirectional stream.
/// It tells the server which Arrow Flight RPC this stream carries.
///
/// # Errors
///
/// Returns [`PyroLinkError`] if the stream ends before a byte is available
/// or if the byte value is not a known RPC type.
pub async fn read_rpc_type(recv: &mut RecvStream) -> Result<RpcType, PyroLinkError> {
    let mut buf = [0u8; 1];
    recv.read_exact(&mut buf).await?;
    RpcType::try_from(buf[0])
}

/// Read one framed message from the stream.
///
/// Reads the 1-byte type tag and 4-byte LE length, then reads exactly that many
/// bytes of payload.  Returns `None` if the stream signals EOS (`0xFF` type byte).
///
/// # Errors
///
/// Returns [`PyroLinkError::Framing`] if the length field overflows or the
/// stream ends prematurely.
pub async fn read_message(
    recv: &mut RecvStream,
) -> Result<Option<(u8, Bytes)>, PyroLinkError> {
    // Read type byte first (EOS has no length field).
    let mut header = [0u8; 5];
    recv.read_exact(&mut header[..1]).await?;

    if header[0] == MSG_EOS {
        return Ok(None);
    }

    // Read 4-byte LE length.
    recv.read_exact(&mut header[1..5]).await?;
    let len = u32::from_le_bytes([header[1], header[2], header[3], header[4]]) as usize;

    // Safety limit: 256 MiB per message
    if len > 256 * 1024 * 1024 {
        return Err(PyroLinkError::Framing(format!(
            "message length {len} exceeds the 256 MiB limit"
        )));
    }

    // Read payload
    let mut payload = vec![0u8; len];
    recv.read_exact(&mut payload).await?;

    Ok(Some((header[0], Bytes::from(payload))))
}

/// Write one framed message to the stream.
///
/// Encodes `type_byte + len(4B LE) + payload` and writes it to the stream.
///
/// # Errors
///
/// Returns [`PyroLinkError::Stream`] if the underlying write fails.
pub async fn write_message(
    send: &mut SendStream,
    type_byte: u8,
    payload: &[u8],
) -> Result<(), PyroLinkError> {
    // Stack-allocated 5-byte header avoids a Vec allocation per message.
    let len = (payload.len() as u32).to_le_bytes();
    let header = [type_byte, len[0], len[1], len[2], len[3]];
    send.write_all(&header)
        .await
        .map_err(|e| PyroLinkError::Stream(e.to_string()))?;
    if !payload.is_empty() {
        send.write_all(payload)
            .await
            .map_err(|e| PyroLinkError::Stream(e.to_string()))?;
    }
    Ok(())
}

/// Write the EOS marker (`0xFF`) to the stream and finish it.
///
/// No length or payload follows the EOS byte.
///
/// # Errors
///
/// Returns [`PyroLinkError::Stream`] if the write or finish call fails.
pub async fn write_eos(send: &mut SendStream) -> Result<(), PyroLinkError> {
    send.write_all(&[MSG_EOS])
        .await
        .map_err(|e| PyroLinkError::Stream(e.to_string()))?;
    send.finish()
        .map_err(|e| PyroLinkError::Stream(e.to_string()))
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rpc_type_round_trips() {
        let cases = [
            (MSG_GET_FLIGHT_INFO, RpcType::GetFlightInfo),
            (MSG_DO_GET, RpcType::DoGet),
            (MSG_DO_PUT, RpcType::DoPut),
            (MSG_DO_ACTION, RpcType::DoAction),
            (MSG_LIST_ACTIONS, RpcType::ListActions),
            (MSG_PREPARE_STATEMENT, RpcType::PrepareStatement),
            (MSG_QUERY, RpcType::Query),
            (MSG_TOPOLOGY, RpcType::Topology),
        ];
        for (byte, expected) in cases {
            let got = RpcType::try_from(byte).expect("known byte must convert");
            assert_eq!(got, expected);
        }
    }

    #[test]
    fn unknown_rpc_type_byte_returns_error() {
        let result = RpcType::try_from(0x42u8);
        assert!(matches!(result, Err(PyroLinkError::UnknownRpcType(0x42))));
    }

    #[test]
    fn eos_byte_is_distinct_from_all_rpc_types() {
        // MSG_EOS must not collide with any recognised RPC type byte.
        assert!(RpcType::try_from(MSG_EOS).is_err());
    }

    // ── Wire-frame encoding helpers ───────────────────────────────────────────
    //
    // `write_message` and `read_message` operate on live QUIC streams and
    // cannot be called in unit tests without a full QUIC endpoint.  Instead
    // we verify the *frame layout* produced by the same byte-packing logic
    // that `write_message` uses: [type_byte | len_le_u32 | payload].

    /// Build a PyroLink frame the same way `write_message` does.
    fn build_frame(type_byte: u8, payload: &[u8]) -> Vec<u8> {
        let len = payload.len() as u32;
        let mut frame = Vec::with_capacity(5 + payload.len());
        frame.push(type_byte);
        frame.extend_from_slice(&len.to_le_bytes());
        frame.extend_from_slice(payload);
        frame
    }

    /// Decode a frame built by `build_frame`, returning `(type_byte, payload)`.
    /// Returns `None` if the first byte is `MSG_EOS`.
    fn decode_frame(frame: &[u8]) -> Option<(u8, &[u8])> {
        assert!(frame.len() >= 1, "frame too short");
        let type_byte = frame[0];
        if type_byte == MSG_EOS {
            return None;
        }
        assert!(frame.len() >= 5, "frame missing length field");
        let len = u32::from_le_bytes([frame[1], frame[2], frame[3], frame[4]]) as usize;
        assert_eq!(frame.len(), 5 + len, "frame payload length mismatch");
        Some((type_byte, &frame[5..]))
    }

    #[test]
    fn frame_wire_format_type_and_length_prefix() {
        // A MSG_SCHEMA frame with a 3-byte payload must have:
        //   byte 0 = 0x01 (MSG_SCHEMA)
        //   bytes 1-4 = 3 as little-endian u32
        //   bytes 5-7 = payload
        let payload = b"abc";
        let frame = build_frame(MSG_SCHEMA, payload);
        assert_eq!(frame.len(), 8);
        assert_eq!(frame[0], MSG_SCHEMA);
        assert_eq!(&frame[1..5], &3u32.to_le_bytes());
        assert_eq!(&frame[5..], payload);
    }

    #[test]
    fn frame_roundtrip_record_batch() {
        let payload: Vec<u8> = (0u8..=15u8).collect();
        let frame = build_frame(MSG_RECORD_BATCH, &payload);
        let (got_type, got_payload) = decode_frame(&frame).expect("not EOS");
        assert_eq!(got_type, MSG_RECORD_BATCH);
        assert_eq!(got_payload, payload.as_slice());
    }

    #[test]
    fn frame_roundtrip_empty_payload() {
        let frame = build_frame(MSG_DO_ACTION, &[]);
        assert_eq!(frame.len(), 5, "header only for empty payload");
        let (got_type, got_payload) = decode_frame(&frame).expect("not EOS");
        assert_eq!(got_type, MSG_DO_ACTION);
        assert!(got_payload.is_empty());
    }

    #[test]
    fn eos_frame_is_single_byte() {
        // write_eos writes exactly one byte: MSG_EOS (0xFF), then finishes.
        let eos_frame = vec![MSG_EOS];
        let result = decode_frame(&eos_frame);
        assert!(result.is_none(), "EOS frame should return None");
    }

    #[test]
    fn frame_length_is_little_endian() {
        // 0x100 = 256 bytes.  In LE: [0x00, 0x01, 0x00, 0x00].
        let payload = vec![0u8; 256];
        let frame = build_frame(MSG_DO_GET, &payload);
        // Check LE byte order explicitly.
        assert_eq!(frame[1], 0x00, "LSB of 256");
        assert_eq!(frame[2], 0x01, "next byte of 256");
        assert_eq!(frame[3], 0x00);
        assert_eq!(frame[4], 0x00);
    }

    #[test]
    fn frame_large_payload_65536_bytes() {
        // Verify a >64KB payload encodes and decodes correctly.
        let payload: Vec<u8> = (0u8..=255u8).cycle().take(65_536).collect();
        let frame = build_frame(MSG_RECORD_BATCH, &payload);
        let (got_type, got_payload) = decode_frame(&frame).expect("not EOS");
        assert_eq!(got_type, MSG_RECORD_BATCH);
        assert_eq!(got_payload.len(), 65_536);
        assert_eq!(got_payload, payload.as_slice());
    }

    #[test]
    fn all_rpc_type_bytes_covered_by_try_from() {
        // Exhaustive check: only the seven known RPC bytes parse successfully.
        let known = [
            MSG_GET_FLIGHT_INFO,
            MSG_DO_GET,
            MSG_DO_PUT,
            MSG_DO_ACTION,
            MSG_LIST_ACTIONS,
            MSG_PREPARE_STATEMENT,
            MSG_QUERY,
            MSG_TOPOLOGY,
        ];
        for b in 0u8..=254u8 {
            let result = RpcType::try_from(b);
            if known.contains(&b) {
                assert!(result.is_ok(), "byte {b:#04x} should map to a known RpcType");
            } else {
                assert!(result.is_err(), "byte {b:#04x} should be unknown");
            }
        }
    }
}
