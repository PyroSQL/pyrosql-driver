//! C-ABI shared library for PWire (PyroSQL raw TCP client).
//!
//! This crate exposes a blocking TCP client that speaks the PWire binary
//! protocol as a C-compatible shared library (`libpyrosql_ffi_pwire.so` /
//! `.dylib` / `.dll`) loadable via PHP FFI, Ruby FFI, Python ctypes, etc.
//!
//! # PWire protocol
//!
//! ```text
//! Frame: [type: u8][length: u32 LE][payload: bytes]
//!
//! Request types:
//!   MSG_QUERY (0x01) — send SQL text as payload
//!
//! Response types:
//!   RESULT_SET (0x01) — [col_count: u16 LE][col_defs][row_count: u32 LE][rows]
//!   OK         (0x02) — [rows_affected: u64 LE]
//!   ERROR      (0x03) — error message bytes
//! ```
//!
//! # Lifecycle
//!
//! ```text
//! pyro_pwire_init()                 — no-op (no async runtime needed)
//! pyro_pwire_connect(host, port)    — open a TCP connection, get opaque handle
//! pyro_pwire_query(h, sql)          — run SELECT, returns JSON (caller frees)
//! pyro_pwire_execute(h, sql)        — run INSERT/UPDATE/DELETE, returns rows affected
//! pyro_pwire_free_string(ptr)       — free a string returned by pyro_pwire_query
//! pyro_pwire_close(h)               — close connection + free handle
//! ```

#![allow(unsafe_code)]

use std::ffi::{CStr, CString};
use std::io::{Read, Write};
use std::os::raw::c_char;
use std::ptr;

// ── PWire protocol constants ────────────────────────────────────────────────

const MSG_QUERY: u8 = 0x01;

const RESP_RESULT_SET: u8 = 0x01;
const RESP_OK: u8 = 0x02;
const RESP_ERROR: u8 = 0x03;

// ── PWire type tags (in column definitions) ─────────────────────────────────

const TYPE_I64: u8 = 1;
const TYPE_F64: u8 = 2;
const TYPE_TEXT: u8 = 3;
const TYPE_BOOL: u8 = 4;
// 5 = BYTES, etc. — treated as text/blob in JSON output

// ── Connection handle ───────────────────────────────────────────────────────

/// A blocking TCP connection that speaks the PWire binary protocol.
///
/// All buffers are pre-allocated and reused across queries to minimize
/// allocations on the hot path.
struct PwireConnection {
    stream: std::net::TcpStream,
    send_buf: Vec<u8>,
    recv_buf: Vec<u8>,
}

impl PwireConnection {
    fn new(stream: std::net::TcpStream) -> Self {
        Self {
            stream,
            send_buf: Vec::with_capacity(4096),
            recv_buf: Vec::with_capacity(65536),
        }
    }

    /// Send a MSG_QUERY frame and read the full response into `recv_buf`.
    ///
    /// After this call, `recv_buf[0]` is the response type byte and
    /// `recv_buf[1..]` is the payload.
    fn query_raw(&mut self, sql: &[u8]) -> Result<(), String> {
        // Build request frame: [type: u8][length: u32 LE][payload]
        self.send_buf.clear();
        self.send_buf.push(MSG_QUERY);
        self.send_buf
            .extend_from_slice(&(sql.len() as u32).to_le_bytes());
        self.send_buf.extend_from_slice(sql);
        self.stream
            .write_all(&self.send_buf)
            .map_err(|e| format!("send: {e}"))?;

        // Read response header: [type: u8][length: u32 LE]
        let mut header = [0u8; 5];
        self.stream
            .read_exact(&mut header)
            .map_err(|e| format!("recv header: {e}"))?;

        let resp_type = header[0];
        let resp_len =
            u32::from_le_bytes([header[1], header[2], header[3], header[4]]) as usize;

        // Read payload into recv_buf (prepend type byte at index 0)
        self.recv_buf.clear();
        self.recv_buf.push(resp_type);
        if resp_len > 0 {
            let prev = self.recv_buf.len();
            self.recv_buf.resize(prev + resp_len, 0);
            self.stream
                .read_exact(&mut self.recv_buf[prev..prev + resp_len])
                .map_err(|e| format!("recv payload: {e}"))?;
        }

        Ok(())
    }

    /// Parse `recv_buf` and build a JSON string from the response.
    fn build_json(&self) -> String {
        let p = &self.recv_buf;
        if p.is_empty() {
            return r#"{"error":"empty response"}"#.to_string();
        }

        let resp_type = p[0];
        let payload = &p[1..];

        match resp_type {
            RESP_RESULT_SET => self.build_json_result_set(payload),
            RESP_OK => {
                let rows_affected = if payload.len() >= 8 {
                    u64::from_le_bytes([
                        payload[0], payload[1], payload[2], payload[3],
                        payload[4], payload[5], payload[6], payload[7],
                    ])
                } else {
                    0
                };
                format!(
                    r#"{{"columns":[],"rows":[],"rows_affected":{}}}"#,
                    rows_affected
                )
            }
            RESP_ERROR => {
                let msg = std::str::from_utf8(payload).unwrap_or("unknown error");
                let mut buf = Vec::with_capacity(32 + msg.len());
                buf.extend_from_slice(b"{\"error\":\"");
                write_json_escaped(&mut buf, msg.as_bytes());
                buf.extend_from_slice(b"\"}");
                // SAFETY: we only wrote valid UTF-8 + ASCII JSON tokens
                unsafe { String::from_utf8_unchecked(buf) }
            }
            _ => r#"{"error":"unexpected response type"}"#.to_string(),
        }
    }

    /// Parse a RESULT_SET payload into a JSON string.
    ///
    /// Format:
    /// ```text
    /// [col_count: u16 LE]
    /// for each column: [name_len: u8][name: bytes][type_tag: u8]
    /// [row_count: u32 LE]
    /// for each row: [null_bitmap][typed values...]
    /// ```
    fn build_json_result_set(&self, p: &[u8]) -> String {
        let mut buf = Vec::with_capacity(1024);
        let mut pos = 0usize;

        if p.len() < 2 {
            return r#"{"error":"result too short"}"#.to_string();
        }

        // Column count
        let col_count = u16::from_le_bytes([p[pos], p[pos + 1]]) as usize;
        pos += 2;

        // Parse column metadata
        buf.extend_from_slice(b"{\"columns\":[");

        // Store type tags for row parsing
        let mut type_tags = Vec::with_capacity(col_count);

        for i in 0..col_count {
            if pos >= p.len() {
                break;
            }
            let name_len = p[pos] as usize;
            pos += 1;

            if i > 0 {
                buf.push(b',');
            }
            buf.push(b'"');
            if pos + name_len <= p.len() {
                write_json_escaped(&mut buf, &p[pos..pos + name_len]);
            }
            buf.push(b'"');
            pos += name_len;

            let tt = if pos < p.len() { p[pos] } else { TYPE_TEXT };
            type_tags.push(tt);
            pos += 1;
        }

        buf.extend_from_slice(b"],\"rows\":[");

        // Row count
        if pos + 4 > p.len() {
            buf.extend_from_slice(b"],\"rows_affected\":0}");
            return unsafe { String::from_utf8_unchecked(buf) };
        }
        let row_count =
            u32::from_le_bytes([p[pos], p[pos + 1], p[pos + 2], p[pos + 3]]) as usize;
        pos += 4;

        let null_bitmap_len = (col_count + 7) / 8;

        for row_idx in 0..row_count {
            if pos + null_bitmap_len > p.len() {
                break;
            }
            let bitmap_start = pos;
            pos += null_bitmap_len;

            if row_idx > 0 {
                buf.push(b',');
            }
            buf.push(b'[');

            for col_idx in 0..col_count {
                if col_idx > 0 {
                    buf.push(b',');
                }

                // Check null bitmap
                let is_null = null_bitmap_len > 0
                    && (p[bitmap_start + col_idx / 8] >> (col_idx % 8)) & 1 == 1;

                if is_null {
                    buf.extend_from_slice(b"null");
                    continue;
                }

                let tt = if col_idx < type_tags.len() {
                    type_tags[col_idx]
                } else {
                    TYPE_TEXT
                };

                match tt {
                    TYPE_I64 => {
                        if pos + 8 > p.len() {
                            buf.extend_from_slice(b"null");
                            continue;
                        }
                        let v = i64::from_le_bytes([
                            p[pos],
                            p[pos + 1],
                            p[pos + 2],
                            p[pos + 3],
                            p[pos + 4],
                            p[pos + 5],
                            p[pos + 6],
                            p[pos + 7],
                        ]);
                        pos += 8;
                        // Format i64 without pulling in itoa
                        let s = v.to_string();
                        buf.extend_from_slice(s.as_bytes());
                    }
                    TYPE_F64 => {
                        if pos + 8 > p.len() {
                            buf.extend_from_slice(b"null");
                            continue;
                        }
                        let v = f64::from_le_bytes([
                            p[pos],
                            p[pos + 1],
                            p[pos + 2],
                            p[pos + 3],
                            p[pos + 4],
                            p[pos + 5],
                            p[pos + 6],
                            p[pos + 7],
                        ]);
                        pos += 8;
                        // serde_json handles special floats correctly
                        let s = serde_json::to_string(&v).unwrap_or_else(|_| "null".to_string());
                        buf.extend_from_slice(s.as_bytes());
                    }
                    TYPE_BOOL => {
                        if pos >= p.len() {
                            buf.extend_from_slice(b"null");
                            continue;
                        }
                        if p[pos] != 0 {
                            buf.extend_from_slice(b"true");
                        } else {
                            buf.extend_from_slice(b"false");
                        }
                        pos += 1;
                    }
                    _ => {
                        // TEXT (3), BYTES (5), etc. — [len: u16 LE][data]
                        if pos + 2 > p.len() {
                            buf.extend_from_slice(b"null");
                            continue;
                        }
                        let text_len =
                            u16::from_le_bytes([p[pos], p[pos + 1]]) as usize;
                        pos += 2;
                        if pos + text_len > p.len() {
                            buf.extend_from_slice(b"null");
                            continue;
                        }
                        buf.push(b'"');
                        write_json_escaped(&mut buf, &p[pos..pos + text_len]);
                        buf.push(b'"');
                        pos += text_len;
                    }
                }
            }
            buf.push(b']');
        }

        buf.extend_from_slice(b"],\"rows_affected\":0}");
        // SAFETY: we only wrote valid UTF-8 + ASCII JSON tokens
        unsafe { String::from_utf8_unchecked(buf) }
    }

    /// Parse rows_affected from an OK response in `recv_buf`.
    fn parse_rows_affected(&self) -> i64 {
        let p = &self.recv_buf;
        if p.is_empty() {
            return 0;
        }
        match p[0] {
            RESP_OK => {
                if p.len() >= 9 {
                    u64::from_le_bytes([
                        p[1], p[2], p[3], p[4], p[5], p[6], p[7], p[8],
                    ]) as i64
                } else {
                    0
                }
            }
            RESP_ERROR => -1,
            _ => 0,
        }
    }
}

// ── JSON helper ─────────────────────────────────────────────────────────────

/// Escape a byte slice for a JSON string value and append to `buf`.
/// Handles: `\`, `"`, and control characters < 0x20.
#[inline]
fn write_json_escaped(buf: &mut Vec<u8>, bytes: &[u8]) {
    static HEX: &[u8; 16] = b"0123456789abcdef";
    for &b in bytes {
        match b {
            b'\\' => buf.extend_from_slice(b"\\\\"),
            b'"' => buf.extend_from_slice(b"\\\""),
            b'\n' => buf.extend_from_slice(b"\\n"),
            b'\r' => buf.extend_from_slice(b"\\r"),
            b'\t' => buf.extend_from_slice(b"\\t"),
            b if b < 0x20 => {
                buf.extend_from_slice(b"\\u00");
                buf.push(HEX[(b >> 4) as usize]);
                buf.push(HEX[(b & 0xf) as usize]);
            }
            _ => buf.push(b),
        }
    }
}

/// Return a JSON error string as a C string, or NULL if allocation fails.
fn error_json(msg: &str) -> *mut c_char {
    let mut buf = Vec::with_capacity(32 + msg.len());
    buf.extend_from_slice(b"{\"error\":\"");
    write_json_escaped(&mut buf, msg.as_bytes());
    buf.extend_from_slice(b"\"}");
    let s = unsafe { String::from_utf8_unchecked(buf) };
    CString::new(s)
        .map(|cs| cs.into_raw())
        .unwrap_or(ptr::null_mut())
}

// ── C-ABI exports ───────────────────────────────────────────────────────────

/// Initialize the PWire FFI layer.
///
/// This is a no-op for the blocking TCP implementation (no async runtime
/// needed), but is provided for API symmetry with the QUIC FFI.
/// Safe to call multiple times.
#[no_mangle]
pub extern "C" fn pyro_pwire_init() {
    // No-op: blocking TCP needs no runtime initialization.
}

/// Open a TCP connection to a PyroSQL server.
///
/// Returns an opaque handle on success, or `NULL` on failure.
/// The caller must eventually pass the handle to `pyro_pwire_close`.
///
/// # Safety
///
/// `host` must be a valid null-terminated C string.
#[no_mangle]
pub extern "C" fn pyro_pwire_connect(
    host: *const c_char,
    port: u16,
) -> *mut std::ffi::c_void {
    if host.is_null() {
        return ptr::null_mut();
    }

    let host_str = match unsafe { CStr::from_ptr(host) }.to_str() {
        Ok(s) => s,
        Err(_) => return ptr::null_mut(),
    };

    let addr = format!("{host_str}:{port}");
    let sock_addr = match addr.parse::<std::net::SocketAddr>() {
        Ok(a) => a,
        Err(_) => {
            // Try DNS resolution for hostnames
            use std::net::ToSocketAddrs;
            match addr.to_socket_addrs() {
                Ok(mut addrs) => match addrs.next() {
                    Some(a) => a,
                    None => return ptr::null_mut(),
                },
                Err(_) => return ptr::null_mut(),
            }
        }
    };

    let stream = match std::net::TcpStream::connect_timeout(
        &sock_addr,
        std::time::Duration::from_secs(5),
    ) {
        Ok(s) => s,
        Err(_) => return ptr::null_mut(),
    };

    // Disable Nagle's algorithm for lower latency
    let _ = stream.set_nodelay(true);
    // Set read/write timeouts to avoid hanging indefinitely
    let _ = stream.set_read_timeout(Some(std::time::Duration::from_secs(30)));
    let _ = stream.set_write_timeout(Some(std::time::Duration::from_secs(10)));

    let conn = Box::new(PwireConnection::new(stream));
    Box::into_raw(conn) as *mut std::ffi::c_void
}

/// Execute a query (typically SELECT) and return the result as a JSON string.
///
/// The JSON format on success:
/// ```json
/// {"columns":["id","name"],"rows":[[1,"Alice"],[2,"Bob"]],"rows_affected":0}
/// ```
///
/// On error:
/// ```json
/// {"error":"description"}
/// ```
///
/// The caller **must** free the returned pointer with `pyro_pwire_free_string`.
/// Returns `NULL` only if the handle is null.
///
/// # Safety
///
/// - `conn` must be a valid handle from `pyro_pwire_connect` (or null).
/// - `sql` must be a valid null-terminated C string.
#[no_mangle]
pub extern "C" fn pyro_pwire_query(
    conn: *mut std::ffi::c_void,
    sql: *const c_char,
) -> *mut c_char {
    if conn.is_null() {
        return ptr::null_mut();
    }
    if sql.is_null() {
        return error_json("sql is null");
    }

    let handle = unsafe { &mut *(conn as *mut PwireConnection) };
    let sql_bytes = unsafe { CStr::from_ptr(sql) }.to_bytes();

    match handle.query_raw(sql_bytes) {
        Ok(()) => {
            let json = handle.build_json();
            CString::new(json)
                .map(|cs| cs.into_raw())
                .unwrap_or_else(|_| error_json("json contains null byte"))
        }
        Err(e) => error_json(&e),
    }
}

/// Execute a statement (INSERT/UPDATE/DELETE) and return the number of rows
/// affected.
///
/// Returns `-1` on error.
///
/// # Safety
///
/// - `conn` must be a valid handle from `pyro_pwire_connect` (or null).
/// - `sql` must be a valid null-terminated C string.
#[no_mangle]
pub extern "C" fn pyro_pwire_execute(
    conn: *mut std::ffi::c_void,
    sql: *const c_char,
) -> i64 {
    if conn.is_null() || sql.is_null() {
        return -1;
    }

    let handle = unsafe { &mut *(conn as *mut PwireConnection) };
    let sql_bytes = unsafe { CStr::from_ptr(sql) }.to_bytes();

    match handle.query_raw(sql_bytes) {
        Ok(()) => handle.parse_rows_affected(),
        Err(_) => -1,
    }
}

/// Free a JSON string returned by `pyro_pwire_query`.
///
/// Safe to call with `NULL`.
///
/// # Safety
///
/// `ptr` must be a pointer previously returned by `pyro_pwire_query`, or null.
/// Must not be called more than once for the same pointer.
#[no_mangle]
pub extern "C" fn pyro_pwire_free_string(ptr: *mut c_char) {
    if !ptr.is_null() {
        unsafe {
            drop(CString::from_raw(ptr));
        }
    }
}

/// Close a PWire connection and free all associated resources.
///
/// Safe to call with `NULL`.
///
/// # Safety
///
/// `conn` must be a pointer previously returned by `pyro_pwire_connect`, or null.
/// Must not be called more than once for the same pointer.
#[no_mangle]
pub extern "C" fn pyro_pwire_close(conn: *mut std::ffi::c_void) {
    if !conn.is_null() {
        unsafe {
            drop(Box::from_raw(conn as *mut PwireConnection));
        }
    }
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: build a fake RESULT_SET payload for testing JSON generation.
    fn make_result_set(cols: &[(&str, u8)], rows: &[Vec<Vec<u8>>]) -> Vec<u8> {
        let mut p = Vec::new();

        // Response type
        p.push(RESP_RESULT_SET);

        // col_count
        p.extend_from_slice(&(cols.len() as u16).to_le_bytes());

        // Column definitions
        for (name, type_tag) in cols {
            p.push(name.len() as u8);
            p.extend_from_slice(name.as_bytes());
            p.push(*type_tag);
        }

        // row_count
        p.extend_from_slice(&(rows.len() as u32).to_le_bytes());

        let null_bitmap_len = (cols.len() + 7) / 8;

        for row in rows {
            // null bitmap (all non-null for simplicity)
            for _ in 0..null_bitmap_len {
                p.push(0x00);
            }
            for cell in row {
                p.extend_from_slice(cell);
            }
        }

        p
    }

    #[test]
    fn test_build_json_ok_response() {
        let conn = PwireConnection {
            stream: unsafe { std::mem::zeroed() }, // not used in this test
            send_buf: Vec::new(),
            recv_buf: vec![
                RESP_OK,
                5, 0, 0, 0, 0, 0, 0, 0, // rows_affected = 5
            ],
        };
        let json = conn.build_json();
        assert_eq!(json, r#"{"columns":[],"rows":[],"rows_affected":5}"#);
    }

    #[test]
    fn test_build_json_error_response() {
        let mut recv_buf = vec![RESP_ERROR];
        recv_buf.extend_from_slice(b"table not found");
        let conn = PwireConnection {
            stream: unsafe { std::mem::zeroed() },
            send_buf: Vec::new(),
            recv_buf,
        };
        let json = conn.build_json();
        assert_eq!(json, r#"{"error":"table not found"}"#);
    }

    #[test]
    fn test_build_json_result_set() {
        let recv_buf = make_result_set(
            &[("id", TYPE_I64), ("name", TYPE_TEXT)],
            &[
                vec![
                    1i64.to_le_bytes().to_vec(),
                    {
                        let mut v = Vec::new();
                        let name = b"Alice";
                        v.extend_from_slice(&(name.len() as u16).to_le_bytes());
                        v.extend_from_slice(name);
                        v
                    },
                ],
            ],
        );

        let conn = PwireConnection {
            stream: unsafe { std::mem::zeroed() },
            send_buf: Vec::new(),
            recv_buf,
        };
        let json = conn.build_json();
        assert_eq!(
            json,
            r#"{"columns":["id","name"],"rows":[[1,"Alice"]],"rows_affected":0}"#
        );
    }

    #[test]
    fn test_parse_rows_affected() {
        let conn = PwireConnection {
            stream: unsafe { std::mem::zeroed() },
            send_buf: Vec::new(),
            recv_buf: vec![RESP_OK, 42, 0, 0, 0, 0, 0, 0, 0],
        };
        assert_eq!(conn.parse_rows_affected(), 42);
    }

    #[test]
    fn test_parse_rows_affected_error() {
        let conn = PwireConnection {
            stream: unsafe { std::mem::zeroed() },
            send_buf: Vec::new(),
            recv_buf: vec![RESP_ERROR, b'e'],
        };
        assert_eq!(conn.parse_rows_affected(), -1);
    }

    #[test]
    fn test_write_json_escaped() {
        let mut buf = Vec::new();
        write_json_escaped(&mut buf, b"hello \"world\"\nnewline\\backslash");
        assert_eq!(
            std::str::from_utf8(&buf).unwrap(),
            r#"hello \"world\"\nnewline\\backslash"#
        );
    }

    #[test]
    fn test_init_is_noop() {
        // Should not panic
        pyro_pwire_init();
        pyro_pwire_init();
    }

    #[test]
    fn test_null_safety() {
        // All functions should handle NULL gracefully
        assert!(pyro_pwire_connect(ptr::null(), 5432).is_null());
        assert!(pyro_pwire_query(ptr::null_mut(), ptr::null()).is_null());
        assert_eq!(pyro_pwire_execute(ptr::null_mut(), ptr::null()), -1);
        pyro_pwire_free_string(ptr::null_mut()); // should not panic
        pyro_pwire_close(ptr::null_mut()); // should not panic
    }
}
