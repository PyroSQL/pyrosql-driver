use anyhow::{bail, Context, Result};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

// Message types (client -> server)
const MSG_QUERY: u8 = 0x01;
const MSG_AUTH: u8 = 0x06;
const MSG_QUIT: u8 = 0xFF;

// Response types (server -> client)
const RESP_RESULT_SET: u8 = 0x01;
const RESP_OK: u8 = 0x02;
const RESP_ERROR: u8 = 0x03;

// Value type tags
pub const TYPE_NULL: u8 = 0;
pub const TYPE_I64: u8 = 1;
pub const TYPE_F64: u8 = 2;
pub const TYPE_TEXT: u8 = 3;
pub const TYPE_BOOL: u8 = 4;
pub const TYPE_BYTES: u8 = 5;

const HEADER_SIZE: usize = 5;

// ---- Little-endian read helpers (from byte slices) ----

fn read_u16_le(data: &[u8]) -> u16 {
    u16::from_le_bytes([data[0], data[1]])
}

fn read_u32_le(data: &[u8]) -> u32 {
    u32::from_le_bytes([data[0], data[1], data[2], data[3]])
}

fn read_u64_le(data: &[u8]) -> u64 {
    u64::from_le_bytes([
        data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
    ])
}

fn read_i64_le(data: &[u8]) -> i64 {
    i64::from_le_bytes([
        data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
    ])
}

fn read_f64_le(data: &[u8]) -> f64 {
    f64::from_le_bytes([
        data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
    ])
}

// ---- Little-endian write helpers (append to Vec) ----

fn put_u16_le(buf: &mut Vec<u8>, v: u16) {
    buf.extend_from_slice(&v.to_le_bytes());
}

fn put_u32_le(buf: &mut Vec<u8>, v: u32) {
    buf.extend_from_slice(&v.to_le_bytes());
}

fn put_u64_le(buf: &mut Vec<u8>, v: u64) {
    buf.extend_from_slice(&v.to_le_bytes());
}

fn put_i64_le(buf: &mut Vec<u8>, v: i64) {
    buf.extend_from_slice(&v.to_le_bytes());
}

fn put_f64_le(buf: &mut Vec<u8>, v: f64) {
    buf.extend_from_slice(&v.to_le_bytes());
}

/// A column definition from a result set.
#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub type_tag: u8,
}

/// A single value in a result row.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    I64(i64),
    F64(f64),
    Text(String),
    Bool(bool),
    Bytes(Vec<u8>),
}

/// A full result set with columns and rows.
#[derive(Debug, Clone)]
pub struct ResultSet {
    pub columns: Vec<Column>,
    pub rows: Vec<Vec<Value>>,
}

/// An OK response.
#[derive(Debug, Clone)]
pub struct OkResponse {
    pub rows_affected: u64,
    pub tag: String,
}

/// Response from the server.
#[derive(Debug)]
pub enum Response {
    ResultSet(ResultSet),
    Ok(OkResponse),
}

/// PWire protocol client.
pub struct PwireClient {
    stream: TcpStream,
}

impl PwireClient {
    /// Connect to a PyroSQL server and authenticate.
    pub async fn connect(host: &str, port: u16, user: &str, password: &str) -> Result<Self> {
        let addr = format!("{}:{}", host, port);
        let stream = TcpStream::connect(&addr)
            .await
            .with_context(|| format!("Failed to connect to {}", addr))?;

        let mut client = PwireClient { stream };
        client.authenticate(user, password).await?;
        Ok(client)
    }

    /// Send raw frame: 1 byte type + 4 bytes LE length + payload.
    async fn send_frame(&mut self, msg_type: u8, payload: &[u8]) -> Result<()> {
        let mut header = Vec::with_capacity(HEADER_SIZE);
        header.push(msg_type);
        put_u32_le(&mut header, payload.len() as u32);
        self.stream.write_all(&header).await?;
        self.stream.write_all(payload).await?;
        self.stream.flush().await?;
        Ok(())
    }

    /// Read a full frame from the server.
    async fn recv_frame(&mut self) -> Result<(u8, Vec<u8>)> {
        let mut header = [0u8; HEADER_SIZE];
        self.stream
            .read_exact(&mut header)
            .await
            .context("Failed to read frame header")?;

        let resp_type = header[0];
        let length = read_u32_le(&header[1..5]) as usize;

        let mut payload = vec![0u8; length];
        if length > 0 {
            self.stream
                .read_exact(&mut payload)
                .await
                .context("Failed to read frame payload")?;
        }

        Ok((resp_type, payload))
    }

    /// Authenticate with the server.
    async fn authenticate(&mut self, user: &str, password: &str) -> Result<()> {
        let mut payload = Vec::new();
        payload.push(user.len() as u8);
        payload.extend_from_slice(user.as_bytes());
        payload.push(password.len() as u8);
        payload.extend_from_slice(password.as_bytes());

        self.send_frame(MSG_AUTH, &payload).await?;

        let (resp_type, resp_payload) = self.recv_frame().await?;
        match resp_type {
            RESP_OK => Ok(()),
            RESP_ERROR => {
                let err = decode_error(&resp_payload)?;
                bail!("Authentication failed: [{}] {}", err.0, err.1);
            }
            _ => bail!("Unexpected response type during auth: 0x{:02x}", resp_type),
        }
    }

    /// Execute a SQL query and return the response.
    pub async fn query(&mut self, sql: &str) -> Result<Response> {
        self.send_frame(MSG_QUERY, sql.as_bytes()).await?;

        let (resp_type, payload) = self.recv_frame().await?;
        match resp_type {
            RESP_RESULT_SET => {
                let rs = decode_result_set(&payload)?;
                Ok(Response::ResultSet(rs))
            }
            RESP_OK => {
                let ok = decode_ok(&payload)?;
                Ok(Response::Ok(ok))
            }
            RESP_ERROR => {
                let (sqlstate, message) = decode_error(&payload)?;
                bail!("Query error [{}]: {} -- SQL: {}", sqlstate, message, sql);
            }
            _ => bail!("Unexpected response type: 0x{:02x}", resp_type),
        }
    }

    /// Execute a query that returns a result set, or error.
    pub async fn query_resultset(&mut self, sql: &str) -> Result<ResultSet> {
        match self.query(sql).await? {
            Response::ResultSet(rs) => Ok(rs),
            Response::Ok(_) => bail!("Expected result set but got OK for: {}", sql),
        }
    }

    /// Execute a query that returns OK, or error.
    pub async fn execute(&mut self, sql: &str) -> Result<OkResponse> {
        match self.query(sql).await? {
            Response::Ok(ok) => Ok(ok),
            Response::ResultSet(_) => bail!("Expected OK but got result set for: {}", sql),
        }
    }

    /// Send QUIT and close.
    pub async fn close(mut self) -> Result<()> {
        self.send_frame(MSG_QUIT, &[]).await?;
        Ok(())
    }
}

/// Decode an ERROR payload: 5 bytes sqlstate + 2 bytes msg_len + msg.
fn decode_error(payload: &[u8]) -> Result<(String, String)> {
    if payload.len() < 7 {
        bail!("Malformed ERROR response: too short");
    }
    let sqlstate = String::from_utf8_lossy(&payload[0..5]).to_string();
    let msg_len = read_u16_le(&payload[5..7]) as usize;
    if 7 + msg_len > payload.len() {
        bail!("Malformed ERROR response: message overflow");
    }
    let message = String::from_utf8_lossy(&payload[7..7 + msg_len]).to_string();
    Ok((sqlstate, message))
}

/// Decode an OK payload: 8 bytes rows_affected (u64 LE) + 1 byte tag_len + tag.
fn decode_ok(payload: &[u8]) -> Result<OkResponse> {
    if payload.len() < 9 {
        bail!("Malformed OK response: too short");
    }
    let rows_affected = read_u64_le(&payload[0..8]);
    let tag_len = payload[8] as usize;
    if 9 + tag_len > payload.len() {
        bail!("Malformed OK response: tag overflow");
    }
    let tag = String::from_utf8_lossy(&payload[9..9 + tag_len]).to_string();
    Ok(OkResponse { rows_affected, tag })
}

/// Decode a RESULT_SET payload.
pub fn decode_result_set(payload: &[u8]) -> Result<ResultSet> {
    if payload.len() < 2 {
        bail!("Malformed result set: too short");
    }

    let col_count = read_u16_le(&payload[0..2]) as usize;
    let mut pos = 2;

    // Decode column definitions
    let mut columns = Vec::with_capacity(col_count);
    for _ in 0..col_count {
        if pos >= payload.len() {
            bail!("Malformed result set: unexpected end in column definitions");
        }
        let name_len = payload[pos] as usize;
        pos += 1;
        if pos + name_len + 1 > payload.len() {
            bail!("Malformed result set: column name overflow");
        }
        let name = String::from_utf8_lossy(&payload[pos..pos + name_len]).to_string();
        pos += name_len;
        let type_tag = payload[pos];
        pos += 1;
        columns.push(Column { name, type_tag });
    }

    // Row count
    if pos + 4 > payload.len() {
        bail!("Malformed result set: missing row count");
    }
    let row_count = read_u32_le(&payload[pos..pos + 4]) as usize;
    pos += 4;

    let null_bitmap_len = (col_count + 7) / 8;

    // Decode rows
    let mut rows = Vec::with_capacity(row_count);
    for _ in 0..row_count {
        if pos + null_bitmap_len > payload.len() {
            bail!("Malformed result set: missing null bitmap");
        }
        let bitmap = &payload[pos..pos + null_bitmap_len];
        pos += null_bitmap_len;

        let mut row = Vec::with_capacity(col_count);
        for (c, col) in columns.iter().enumerate() {
            let byte_idx = c / 8;
            let bit_idx = c % 8;
            let is_null = byte_idx < bitmap.len() && (bitmap[byte_idx] >> bit_idx & 1) == 1;

            if is_null {
                row.push(Value::Null);
                continue;
            }

            match col.type_tag {
                TYPE_I64 => {
                    if pos + 8 > payload.len() {
                        bail!("Malformed result set: i64 overflow");
                    }
                    let val = read_i64_le(&payload[pos..pos + 8]);
                    pos += 8;
                    row.push(Value::I64(val));
                }
                TYPE_F64 => {
                    if pos + 8 > payload.len() {
                        bail!("Malformed result set: f64 overflow");
                    }
                    let val = read_f64_le(&payload[pos..pos + 8]);
                    pos += 8;
                    row.push(Value::F64(val));
                }
                TYPE_BOOL => {
                    if pos >= payload.len() {
                        bail!("Malformed result set: bool overflow");
                    }
                    let val = payload[pos] != 0;
                    pos += 1;
                    row.push(Value::Bool(val));
                }
                TYPE_TEXT => {
                    if pos + 2 > payload.len() {
                        bail!("Malformed result set: text length overflow");
                    }
                    let len = read_u16_le(&payload[pos..pos + 2]) as usize;
                    pos += 2;
                    if pos + len > payload.len() {
                        bail!("Malformed result set: text data overflow");
                    }
                    let val = String::from_utf8_lossy(&payload[pos..pos + len]).to_string();
                    pos += len;
                    row.push(Value::Text(val));
                }
                TYPE_BYTES => {
                    if pos + 2 > payload.len() {
                        bail!("Malformed result set: bytes length overflow");
                    }
                    let len = read_u16_le(&payload[pos..pos + 2]) as usize;
                    pos += 2;
                    if pos + len > payload.len() {
                        bail!("Malformed result set: bytes data overflow");
                    }
                    let val = payload[pos..pos + len].to_vec();
                    pos += len;
                    row.push(Value::Bytes(val));
                }
                _ => {
                    // Unknown types: treat as bytes (same length-prefixed encoding)
                    if pos + 2 > payload.len() {
                        bail!("Malformed result set: unknown type length overflow");
                    }
                    let len = read_u16_le(&payload[pos..pos + 2]) as usize;
                    pos += 2;
                    if pos + len > payload.len() {
                        bail!("Malformed result set: unknown type data overflow");
                    }
                    let val = payload[pos..pos + len].to_vec();
                    pos += len;
                    row.push(Value::Bytes(val));
                }
            }
        }
        rows.push(row);
    }

    Ok(ResultSet { columns, rows })
}

/// Encode a QUERY frame (for testing).
pub fn encode_query_frame(sql: &str) -> Vec<u8> {
    let payload = sql.as_bytes();
    let mut buf = Vec::with_capacity(HEADER_SIZE + payload.len());
    buf.push(MSG_QUERY);
    put_u32_le(&mut buf, payload.len() as u32);
    buf.extend_from_slice(payload);
    buf
}

/// Encode an AUTH frame (for testing).
pub fn encode_auth_frame(user: &str, password: &str) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.push(user.len() as u8);
    payload.extend_from_slice(user.as_bytes());
    payload.push(password.len() as u8);
    payload.extend_from_slice(password.as_bytes());

    let mut buf = Vec::with_capacity(HEADER_SIZE + payload.len());
    buf.push(MSG_AUTH);
    put_u32_le(&mut buf, payload.len() as u32);
    buf.extend_from_slice(&payload);
    buf
}

/// Build a RESULT_SET payload from columns and rows (for testing).
pub fn encode_result_set_payload(columns: &[Column], rows: &[Vec<Value>]) -> Vec<u8> {
    let mut buf = Vec::new();

    // Column count (u16 LE)
    put_u16_le(&mut buf, columns.len() as u16);

    // Column definitions
    for col in columns {
        let name_bytes = col.name.as_bytes();
        buf.push(name_bytes.len() as u8);
        buf.extend_from_slice(name_bytes);
        buf.push(col.type_tag);
    }

    // Row count (u32 LE)
    put_u32_le(&mut buf, rows.len() as u32);

    let col_count = columns.len();
    let null_bitmap_len = (col_count + 7) / 8;

    for row in rows {
        // Build null bitmap
        let mut bitmap = vec![0u8; null_bitmap_len];
        for (c, val) in row.iter().enumerate() {
            if matches!(val, Value::Null) {
                let byte_idx = c / 8;
                let bit_idx = c % 8;
                bitmap[byte_idx] |= 1 << bit_idx;
            }
        }
        buf.extend_from_slice(&bitmap);

        // Encode values
        for val in row.iter() {
            match val {
                Value::Null => { /* already in bitmap */ }
                Value::I64(v) => {
                    put_i64_le(&mut buf, *v);
                }
                Value::F64(v) => {
                    put_f64_le(&mut buf, *v);
                }
                Value::Bool(v) => {
                    buf.push(if *v { 1 } else { 0 });
                }
                Value::Text(v) => {
                    let bytes = v.as_bytes();
                    put_u16_le(&mut buf, bytes.len() as u16);
                    buf.extend_from_slice(bytes);
                }
                Value::Bytes(v) => {
                    put_u16_le(&mut buf, v.len() as u16);
                    buf.extend_from_slice(v);
                }
            }
        }
    }

    buf
}

/// Build a full RESULT_SET frame (type + length + payload) for testing.
pub fn encode_result_set_frame(columns: &[Column], rows: &[Vec<Value>]) -> Vec<u8> {
    let payload = encode_result_set_payload(columns, rows);
    let mut buf = Vec::with_capacity(HEADER_SIZE + payload.len());
    buf.push(RESP_RESULT_SET);
    put_u32_le(&mut buf, payload.len() as u32);
    buf.extend_from_slice(&payload);
    buf
}

/// Build an OK response frame for testing.
pub fn encode_ok_frame(rows_affected: u64, tag: &str) -> Vec<u8> {
    let tag_bytes = tag.as_bytes();
    let payload_len = 8 + 1 + tag_bytes.len();
    let mut buf = Vec::with_capacity(HEADER_SIZE + payload_len);
    buf.push(RESP_OK);
    put_u32_le(&mut buf, payload_len as u32);
    put_u64_le(&mut buf, rows_affected);
    buf.push(tag_bytes.len() as u8);
    buf.extend_from_slice(tag_bytes);
    buf
}

/// Build an ERROR response frame for testing.
pub fn encode_error_frame(sqlstate: &str, message: &str) -> Vec<u8> {
    let msg_bytes = message.as_bytes();
    let payload_len = 5 + 2 + msg_bytes.len();
    let mut buf = Vec::with_capacity(HEADER_SIZE + payload_len);
    buf.push(RESP_ERROR);
    put_u32_le(&mut buf, payload_len as u32);
    let sqlstate_bytes = sqlstate.as_bytes();
    let copy_len = 5.min(sqlstate_bytes.len());
    buf.extend_from_slice(&sqlstate_bytes[..copy_len]);
    for _ in copy_len..5 {
        buf.push(b'0');
    }
    put_u16_le(&mut buf, msg_bytes.len() as u16);
    buf.extend_from_slice(msg_bytes);
    buf
}
