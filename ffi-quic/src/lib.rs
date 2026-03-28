//! C-ABI shared library for PyroLink (PyroSQL QUIC client).
//!
//! This crate exposes the PyroLink Rust client as a C-compatible shared
//! library (`libpyrosql_ffi.so` / `.dylib` / `.dll`) that can be
//! loaded via PHP 7.4+ FFI, Ruby FFI, or any language with C FFI support.
//!
//! # Lifecycle
//!
//! ```text
//! vsql_init()            — create the async runtime (call once)
//! vsql_connect(url)      — open a QUIC connection, get an opaque handle
//! vsql_query(h, sql)     — run a SELECT, returns JSON (caller frees for non-Wire; borrowed for Wire)
//! vsql_execute(h, sql)   — run INSERT/UPDATE/DELETE, returns rows affected
//! vsql_begin(h)          — begin a transaction, returns tx_id (caller frees)
//! vsql_commit(h, tx_id)  — commit a transaction
//! vsql_rollback(h, tx_id)— rollback a transaction
//! vsql_bulk_insert(h, table, json_rows) — bulk insert rows
//! vsql_free_string(ptr)  — free a string returned by vsql_query/vsql_begin
//! vsql_close(h)          — close connection + free handle
//! vsql_shutdown()        — tear down the runtime
//! ```

#![allow(unsafe_code)]

use std::ffi::{CStr, CString};
use std::io::{Read, Write};
use std::os::raw::c_char;
use std::ptr;
use std::sync::OnceLock;

use pyrosql::{ConnectConfig, Pool, QueryResult, Value};

// ── Zero-alloc Wire fast path ────────────────────────────────────────────────

const VW_REQ_QUERY: u8 = 0x01;
const VW_RESP_RESULT_SET: u8 = 0x01;
const VW_RESP_OK: u8 = 0x02;
const VW_RESP_ERROR: u8 = 0x03;

/// Zero-allocation Wire connection for the FFI hot path.
///
/// All buffers are pre-allocated and reused across queries.
/// The JSON output buffer is returned directly as a `*const c_char`
/// pointer that remains valid until the next query call.
struct WireConnection {
    stream: std::net::TcpStream,
    send_buf: Vec<u8>,
    recv_buf: Vec<u8>,
    json_buf: Vec<u8>,
}

/// Tagged handle: either a zero-alloc Wire connection or a full Client.
enum FfiHandle {
    Wire(WireConnection),
    Client(pyrosql::Client),
}

impl WireConnection {
    fn new(stream: std::net::TcpStream) -> Self {
        // Enable TCP_QUICKACK on Linux to disable delayed ACKs
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::io::AsRawFd;
            let fd = stream.as_raw_fd();
            unsafe {
                let val: libc::c_int = 1;
                libc::setsockopt(
                    fd,
                    libc::IPPROTO_TCP,
                    libc::TCP_QUICKACK,
                    &val as *const _ as *const libc::c_void,
                    std::mem::size_of::<libc::c_int>() as libc::socklen_t,
                );
            }
        }
        Self {
            stream,
            send_buf: Vec::with_capacity(4096),
            recv_buf: Vec::with_capacity(65536),
            json_buf: Vec::with_capacity(65536),
        }
    }

    /// Re-enable TCP_QUICKACK (Linux resets it after each recv).
    #[cfg(target_os = "linux")]
    #[inline(always)]
    fn quickack(&self) {
        use std::os::unix::io::AsRawFd;
        unsafe {
            let val: libc::c_int = 1;
            libc::setsockopt(
                self.stream.as_raw_fd(),
                libc::IPPROTO_TCP,
                libc::TCP_QUICKACK,
                &val as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
        }
    }

    #[cfg(not(target_os = "linux"))]
    #[inline(always)]
    fn quickack(&self) {}

    /// Send query and receive raw response. Zero heap allocations on the
    /// steady-state path (buffers reuse their capacity).
    #[inline]
    fn query_raw(&mut self, sql: &[u8]) -> Result<(), String> {
        // Build frame into reused send_buf
        self.send_buf.clear();
        self.send_buf.push(VW_REQ_QUERY);
        self.send_buf.extend_from_slice(&(sql.len() as u32).to_le_bytes());
        self.send_buf.extend_from_slice(sql);
        self.stream.write_all(&self.send_buf).map_err(|e| e.to_string())?;

        // Read response header (5 bytes: type + u32 LE length)
        let mut header = [0u8; 5];
        self.stream.read_exact(&mut header).map_err(|e| e.to_string())?;
        self.quickack();
        let resp_len = u32::from_le_bytes([header[1], header[2], header[3], header[4]]) as usize;

        // Read payload into reused recv_buf — we reserve +1 for the prepended type byte
        self.recv_buf.clear();
        self.recv_buf.push(header[0]); // response type at index 0
        if resp_len > 0 {
            let prev_len = self.recv_buf.len();
            self.recv_buf.resize(prev_len + resp_len, 0);
            self.stream.read_exact(&mut self.recv_buf[prev_len..prev_len + resp_len]).map_err(|e| e.to_string())?;
            self.quickack();
        }

        Ok(())
    }

    /// Build JSON directly from binary response into `json_buf`.
    /// Returns a null-terminated pointer into `json_buf`.
    ///
    /// This is the core zero-alloc serialization: we iterate the binary
    /// payload and write JSON tokens directly, with no intermediate
    /// Value/Row/String allocations.
    #[inline]
    fn build_json(&mut self) -> *const c_char {
        let buf = &mut self.json_buf;
        buf.clear();

        let p = &self.recv_buf;
        if p.is_empty() {
            buf.extend_from_slice(b"{\"error\":\"empty response\"}\0");
            return buf.as_ptr() as *const c_char;
        }

        let resp_type = p[0];
        let payload = &p[1..];

        match resp_type {
            VW_RESP_RESULT_SET => {
                self.build_json_result_set(payload);
            }
            VW_RESP_OK => {
                let rows_affected = if payload.len() >= 8 {
                    u64::from_le_bytes([
                        payload[0], payload[1], payload[2], payload[3],
                        payload[4], payload[5], payload[6], payload[7],
                    ])
                } else { 0 };
                let buf = &mut self.json_buf;
                buf.extend_from_slice(b"{\"columns\":[],\"rows\":[],\"rows_affected\":");
                write_u64(buf, rows_affected);
                buf.push(b'}');
            }
            VW_RESP_ERROR => {
                let msg = if payload.len() > 6 {
                    let msg_len = payload[5] as usize;
                    std::str::from_utf8(&payload[6..6 + msg_len.min(payload.len() - 6)])
                        .unwrap_or("unknown error")
                } else {
                    std::str::from_utf8(payload).unwrap_or("unknown error")
                };
                let buf = &mut self.json_buf;
                buf.extend_from_slice(b"{\"error\":\"");
                write_json_escaped(buf, msg.as_bytes());
                buf.extend_from_slice(b"\"}");
            }
            _ => {
                self.json_buf.extend_from_slice(b"{\"error\":\"unexpected response type\"}");
            }
        }

        self.json_buf.push(0); // null terminator
        self.json_buf.as_ptr() as *const c_char
    }

    /// Parse binary RESULT_SET and write JSON directly into `json_buf`.
    #[inline]
    fn build_json_result_set(&mut self, payload: &[u8]) {
        // We need to borrow payload (which comes from recv_buf) and write to json_buf.
        // This is safe because they are separate fields but we need to work around
        // the borrow checker by using a raw pointer approach.
        let buf = &mut self.json_buf;
        let p = payload;
        let mut pos = 0usize;

        if p.len() < 2 {
            buf.extend_from_slice(b"{\"error\":\"result too short\"}");
            return;
        }

        let col_count = u16::from_le_bytes([p[pos], p[pos + 1]]) as usize;
        pos += 2;

        // Parse column metadata and write column names
        buf.extend_from_slice(b"{\"columns\":[");

        // We need type_tags for row parsing. Use a small stack buffer for
        // common cases (up to 32 columns), heap-alloc only for wide tables.
        let mut type_tags_stack = [0u8; 32];
        let mut type_tags_heap: Vec<u8>;
        let type_tags: &mut [u8];
        if col_count <= 32 {
            type_tags = &mut type_tags_stack[..col_count];
        } else {
            type_tags_heap = vec![0u8; col_count];
            type_tags = &mut type_tags_heap;
        }

        for i in 0..col_count {
            if pos >= p.len() { break; }
            let name_len = p[pos] as usize;
            pos += 1;
            if i > 0 { buf.push(b','); }
            buf.push(b'"');
            if pos + name_len <= p.len() {
                write_json_escaped(buf, &p[pos..pos + name_len]);
            }
            buf.push(b'"');
            pos += name_len;
            type_tags[i] = if pos < p.len() { p[pos] } else { 3 };
            pos += 1;
        }

        buf.extend_from_slice(b"],\"rows\":[");

        // Row count
        if pos + 4 > p.len() {
            buf.extend_from_slice(b"],\"rows_affected\":0}");
            return;
        }
        let row_count = u32::from_le_bytes([p[pos], p[pos + 1], p[pos + 2], p[pos + 3]]) as usize;
        pos += 4;

        let null_bitmap_len = (col_count + 7) / 8;

        // Scratch buffer for itoa conversions (max i64 = 20 digits + sign)
        let mut itoa_buf = itoa::Buffer::new();

        for row_idx in 0..row_count {
            if pos + null_bitmap_len > p.len() { break; }
            let bitmap_start = pos;
            pos += null_bitmap_len;

            if row_idx > 0 { buf.push(b','); }
            buf.push(b'[');

            for col_idx in 0..col_count {
                if col_idx > 0 { buf.push(b','); }

                let is_null = null_bitmap_len > 0
                    && (p[bitmap_start + col_idx / 8] >> (col_idx % 8)) & 1 == 1;

                if is_null {
                    buf.extend_from_slice(b"null");
                    continue;
                }

                let tt = type_tags[col_idx];
                match tt {
                    1 => { // I64
                        if pos + 8 > p.len() { buf.extend_from_slice(b"null"); continue; }
                        let v = i64::from_le_bytes([
                            p[pos], p[pos+1], p[pos+2], p[pos+3],
                            p[pos+4], p[pos+5], p[pos+6], p[pos+7],
                        ]);
                        pos += 8;
                        buf.extend_from_slice(itoa_buf.format(v).as_bytes());
                    }
                    2 => { // F64
                        if pos + 8 > p.len() { buf.extend_from_slice(b"null"); continue; }
                        let v = f64::from_le_bytes([
                            p[pos], p[pos+1], p[pos+2], p[pos+3],
                            p[pos+4], p[pos+5], p[pos+6], p[pos+7],
                        ]);
                        pos += 8;
                        let mut dtoa_buf = ryu::Buffer::new();
                        buf.extend_from_slice(dtoa_buf.format(v).as_bytes());
                    }
                    4 => { // BOOL
                        if pos >= p.len() { buf.extend_from_slice(b"null"); continue; }
                        if p[pos] != 0 {
                            buf.extend_from_slice(b"true");
                        } else {
                            buf.extend_from_slice(b"false");
                        }
                        pos += 1;
                    }
                    _ => { // TEXT (3), BYTES (5), etc.
                        if pos + 2 > p.len() { buf.extend_from_slice(b"null"); continue; }
                        let text_len = u16::from_le_bytes([p[pos], p[pos + 1]]) as usize;
                        pos += 2;
                        if pos + text_len > p.len() { buf.extend_from_slice(b"null"); continue; }
                        buf.push(b'"');
                        write_json_escaped(buf, &p[pos..pos + text_len]);
                        buf.push(b'"');
                        pos += text_len;
                    }
                }
            }
            buf.push(b']');
        }

        buf.extend_from_slice(b"],\"rows_affected\":0}");
    }
}

/// Write a u64 as decimal digits into a byte buffer.
#[inline]
fn write_u64(buf: &mut Vec<u8>, v: u64) {
    let mut itoa_buf = itoa::Buffer::new();
    buf.extend_from_slice(itoa_buf.format(v).as_bytes());
}

/// Escape a byte slice for JSON string value and append to buf.
/// Handles: `\`, `"`, and control chars < 0x20.
#[inline]
fn write_json_escaped(buf: &mut Vec<u8>, bytes: &[u8]) {
    for &b in bytes {
        match b {
            b'\\' => buf.extend_from_slice(b"\\\\"),
            b'"' => buf.extend_from_slice(b"\\\""),
            b'\n' => buf.extend_from_slice(b"\\n"),
            b'\r' => buf.extend_from_slice(b"\\r"),
            b'\t' => buf.extend_from_slice(b"\\t"),
            b if b < 0x20 => {
                // \u00XX
                buf.extend_from_slice(b"\\u00");
                buf.push(HEX_DIGITS[(b >> 4) as usize]);
                buf.push(HEX_DIGITS[(b & 0xf) as usize]);
            }
            _ => buf.push(b),
        }
    }
}

static HEX_DIGITS: &[u8; 16] = b"0123456789abcdef";

/// Extract a `&pyrosql::Client` from an FfiHandle. Returns `None` for Wire handles.
#[inline]
fn as_client(handle: *mut std::ffi::c_void) -> Option<&'static pyrosql::Client> {
    let ffi = unsafe { &*(handle as *const FfiHandle) };
    match ffi {
        FfiHandle::Client(c) => Some(c),
        FfiHandle::Wire(_) => None,
    }
}

/// Extract a `&mut pyrosql::Client` from an FfiHandle. Returns `None` for Wire handles.
#[inline]
fn as_client_mut(handle: *mut std::ffi::c_void) -> Option<&'static mut pyrosql::Client> {
    let ffi = unsafe { &mut *(handle as *mut FfiHandle) };
    match ffi {
        FfiHandle::Client(c) => Some(c),
        FfiHandle::Wire(_) => None,
    }
}

/// Global Tokio runtime, initialized once by `vsql_init`.
static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();

/// Initialize the async runtime. Call once at startup.
///
/// Subsequent calls are harmless no-ops.
#[no_mangle]
pub extern "C" fn vsql_init() {
    RUNTIME.get_or_init(|| {
        tokio::runtime::Runtime::new().expect("failed to create tokio runtime")
    });
}

/// Run an async future without deadlocking from FFI callers.
/// Creates a fresh single-threaded tokio runtime on a dedicated OS thread.
/// This avoids all interaction with the global runtime and Python's GIL.
fn run_async<F, T>(fut: F) -> Result<T, String>
where
    F: std::future::Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| format!("runtime: {e}"))?;
        Ok(rt.block_on(fut))
    })
    .join()
    .map_err(|_| "thread panicked".to_string())?
}

/// Connect to PyroSQL. Returns an opaque handle (pointer).
///
/// Returns `NULL` on failure.  The caller must eventually pass the handle to
/// `vsql_close` to free resources.
///
/// For Wire connections (`vsql://` / `vsqlw://`), uses the zero-allocation
/// fast path with direct TCP I/O and buffer reuse.
#[no_mangle]
pub extern "C" fn vsql_connect(url: *const c_char) -> *mut std::ffi::c_void {
    let url = match unsafe { CStr::from_ptr(url) }.to_str() {
        Ok(s) => s,
        Err(_) => return ptr::null_mut(),
    };
    let is_wire = url.starts_with("vsql://") || url.starts_with("vsqlw://");

    if is_wire {
        // Zero-alloc Wire fast path: direct std::net, no tokio, no Client overhead.
        match pyrosql::ConnectConfig::from_url(url) {
            Ok(cfg) => {
                let addr = format!("{}:{}", cfg.host, cfg.port);
                match std::net::TcpStream::connect_timeout(
                    &addr.parse().unwrap_or_else(|_| {
                        use std::net::ToSocketAddrs;
                        addr.to_socket_addrs().unwrap().next().unwrap()
                    }),
                    std::time::Duration::from_secs(5),
                ) {
                    Ok(stream) => {
                        let _ = stream.set_nodelay(true);
                        let handle = FfiHandle::Wire(WireConnection::new(stream));
                        Box::into_raw(Box::new(handle)) as *mut _
                    }
                    Err(_) => ptr::null_mut(),
                }
            }
            Err(_) => ptr::null_mut(),
        }
    } else {
        // PG/MySQL/QUIC: need async runtime — run on a dedicated thread
        // to avoid block_on deadlocks from FFI callers.
        let url = url.to_owned();
        match run_async(async move {
            pyrosql::Client::connect_url(&url).await
        }) {
            Ok(Ok(client)) => {
                let handle = FfiHandle::Client(client);
                Box::into_raw(Box::new(handle)) as *mut _
            }
            _ => ptr::null_mut(),
        }
    }
}

/// Execute a query. Returns a JSON string.
///
/// The JSON format is:
/// ```json
/// {"columns":["id","name"],"rows":[[1,"Alice"],[2,"Bob"]],"rows_affected":0}
/// ```
///
/// On error, returns:
/// ```json
/// {"error":"description"}
/// ```
///
/// **Wire connections:** The returned pointer is borrowed from the connection's
/// internal buffer and remains valid until the next `vsql_query` call on the
/// same handle. Do NOT call `vsql_free_string` on it.
///
/// **Non-Wire connections:** The returned pointer must be freed with
/// `vsql_free_string`.
///
/// Returns `NULL` if the handle is null or the runtime is not initialized.
#[no_mangle]
pub extern "C" fn vsql_query(handle: *mut std::ffi::c_void, sql: *const c_char) -> *mut c_char {
    if handle.is_null() {
        return ptr::null_mut();
    }
    let ffi = unsafe { &mut *(handle as *mut FfiHandle) };

    match ffi {
        FfiHandle::Wire(conn) => {
            // ZERO-ALLOC HOT PATH
            let sql_bytes = unsafe { CStr::from_ptr(sql) }.to_bytes();
            match conn.query_raw(sql_bytes) {
                Ok(()) => conn.build_json() as *mut c_char,
                Err(e) => {
                    conn.json_buf.clear();
                    conn.json_buf.extend_from_slice(b"{\"error\":\"");
                    write_json_escaped(&mut conn.json_buf, e.as_bytes());
                    conn.json_buf.extend_from_slice(b"\"}\0");
                    conn.json_buf.as_ptr() as *mut c_char
                }
            }
        }
        FfiHandle::Client(_) => {
            // Legacy path for PG/MySQL/QUIC — uses run_async + JSON serialization
            let sql = match unsafe { CStr::from_ptr(sql) }.to_str() {
                Ok(s) => s.to_owned(),
                Err(_) => return ptr::null_mut(),
            };
            let client_ptr = match ffi {
                FfiHandle::Client(c) => c as *const pyrosql::Client as usize,
                _ => unreachable!(),
            };
            match run_async(async move {
                let client = unsafe { &*(client_ptr as *const pyrosql::Client) };
                client.query(&sql, &[]).await
            }) {
                Ok(Ok(result)) => {
                    let json = serialize_result(&result);
                    CString::new(json)
                        .map(|s| s.into_raw())
                        .unwrap_or(ptr::null_mut())
                }
                Ok(Err(e)) => {
                    let err = format!("{{\"error\":\"{}\"}}", e.to_string().replace('"', "\\\""));
                    CString::new(err)
                        .map(|s| s.into_raw())
                        .unwrap_or(ptr::null_mut())
                }
                Err(_) => ptr::null_mut(),
            }
        }
    }
}

/// Execute a statement (INSERT/UPDATE/DELETE). Returns rows affected, or -1 on error.
#[no_mangle]
pub extern "C" fn vsql_execute(handle: *mut std::ffi::c_void, sql: *const c_char) -> i64 {
    if handle.is_null() {
        return -1;
    }
    let ffi = unsafe { &mut *(handle as *mut FfiHandle) };

    match ffi {
        FfiHandle::Wire(conn) => {
            let sql_bytes = unsafe { CStr::from_ptr(sql) }.to_bytes();
            match conn.query_raw(sql_bytes) {
                Ok(()) => {
                    // Parse rows_affected from OK response
                    let p = &conn.recv_buf;
                    if !p.is_empty() && p[0] == VW_RESP_OK && p.len() >= 9 {
                        u64::from_le_bytes([
                            p[1], p[2], p[3], p[4], p[5], p[6], p[7], p[8],
                        ]) as i64
                    } else if !p.is_empty() && p[0] == VW_RESP_ERROR {
                        -1
                    } else {
                        0
                    }
                }
                Err(_) => -1,
            }
        }
        FfiHandle::Client(_) => {
            let sql = match unsafe { CStr::from_ptr(sql) }.to_str() {
                Ok(s) => s.to_owned(),
                Err(_) => return -1,
            };
            let client_ptr = match ffi {
                FfiHandle::Client(c) => c as *const pyrosql::Client as usize,
                _ => unreachable!(),
            };
            match run_async(async move {
                let client = unsafe { &*(client_ptr as *const pyrosql::Client) };
                client.execute(&sql, &[]).await
            }) {
                Ok(Ok(n)) => n as i64,
                _ => -1,
            }
        }
    }
}

/// Begin a transaction. Returns a JSON string with the transaction ID.
///
/// Returns a JSON string: `{"transaction_id":"..."}` on success,
/// `{"error":"..."}` on failure. Caller must free with `vsql_free_string`.
///
/// Returns `NULL` if the handle is null or the runtime is not initialized.
#[no_mangle]
pub extern "C" fn vsql_begin(handle: *mut std::ffi::c_void) -> *mut c_char {
    if handle.is_null() {
        return ptr::null_mut();
    }
    let client = match as_client(handle) { Some(c) => c, None => return ptr::null_mut() };
    let rt = match RUNTIME.get() {
        Some(rt) => rt,
        None => return ptr::null_mut(),
    };
    match rt.block_on(client.begin()) {
        Ok(tx) => {
            let tx_id = tx.id().to_string();
            // Forget the Transaction to avoid the drop warning — lifecycle managed by C caller
            std::mem::forget(tx);
            let json = format!("{{\"transaction_id\":\"{}\"}}", tx_id.replace('"', "\\\""));
            CString::new(json)
                .map(|s| s.into_raw())
                .unwrap_or(ptr::null_mut())
        }
        Err(e) => {
            let err = format!("{{\"error\":\"{}\"}}", e.to_string().replace('"', "\\\""));
            CString::new(err)
                .map(|s| s.into_raw())
                .unwrap_or(ptr::null_mut())
        }
    }
}

/// Commit a transaction. Returns 0 on success, -1 on error.
#[no_mangle]
pub extern "C" fn vsql_commit(handle: *mut std::ffi::c_void, tx_id: *const c_char) -> i32 {
    if handle.is_null() || tx_id.is_null() {
        return -1;
    }
    let client = match as_client(handle) { Some(c) => c, None => return -1 };
    let tx_id = match unsafe { CStr::from_ptr(tx_id) }.to_str() {
        Ok(s) => s,
        Err(_) => return -1,
    };
    let rt = match RUNTIME.get() {
        Some(rt) => rt,
        None => return -1,
    };
    let action = serde_json::json!({"type": "Commit", "transaction_id": tx_id});
    match rt.block_on(client.send_action(&action)) {
        Ok(_) => 0,
        Err(_) => -1,
    }
}

/// Rollback a transaction. Returns 0 on success, -1 on error.
#[no_mangle]
pub extern "C" fn vsql_rollback(handle: *mut std::ffi::c_void, tx_id: *const c_char) -> i32 {
    if handle.is_null() || tx_id.is_null() {
        return -1;
    }
    let client = match as_client(handle) { Some(c) => c, None => return -1 };
    let tx_id = match unsafe { CStr::from_ptr(tx_id) }.to_str() {
        Ok(s) => s,
        Err(_) => return -1,
    };
    let rt = match RUNTIME.get() {
        Some(rt) => rt,
        None => return -1,
    };
    let action = serde_json::json!({"type": "Rollback", "transaction_id": tx_id});
    match rt.block_on(client.send_action(&action)) {
        Ok(_) => 0,
        Err(_) => -1,
    }
}

/// Bulk insert rows into a table. Returns rows inserted, or -1 on error.
///
/// `json_rows` must be a JSON string with format:
/// ```json
/// {"columns":["col1","col2"],"rows":[[val1,val2],[val3,val4]]}
/// ```
#[no_mangle]
pub extern "C" fn vsql_bulk_insert(
    handle: *mut std::ffi::c_void,
    table: *const c_char,
    json_rows: *const c_char,
) -> i64 {
    if handle.is_null() || table.is_null() || json_rows.is_null() {
        return -1;
    }
    let client = match as_client(handle) { Some(c) => c, None => return -1 };
    let table = match unsafe { CStr::from_ptr(table) }.to_str() {
        Ok(s) => s,
        Err(_) => return -1,
    };
    let json_str = match unsafe { CStr::from_ptr(json_rows) }.to_str() {
        Ok(s) => s,
        Err(_) => return -1,
    };
    let rt = match RUNTIME.get() {
        Some(rt) => rt,
        None => return -1,
    };

    // Parse the JSON to extract columns and rows
    let parsed: serde_json::Value = match serde_json::from_str(json_str) {
        Ok(v) => v,
        Err(_) => return -1,
    };

    let columns: Vec<String> = match parsed["columns"].as_array() {
        Some(arr) => arr
            .iter()
            .filter_map(|v| v.as_str().map(|s| s.to_string()))
            .collect(),
        None => return -1,
    };

    let rows: Vec<Vec<Value>> = match parsed["rows"].as_array() {
        Some(arr) => arr
            .iter()
            .map(|row| {
                row.as_array()
                    .unwrap_or(&vec![])
                    .iter()
                    .map(json_to_value)
                    .collect()
            })
            .collect(),
        None => return -1,
    };

    let col_refs: Vec<&str> = columns.iter().map(|s| s.as_str()).collect();
    match rt.block_on(client.bulk_insert(table, &col_refs, &rows)) {
        Ok(n) => n as i64,
        Err(_) => -1,
    }
}

/// Free a string returned by `vsql_query`.
///
/// Safe to call with `NULL`.
#[no_mangle]
pub extern "C" fn vsql_free_string(s: *mut c_char) {
    if !s.is_null() {
        unsafe {
            drop(CString::from_raw(s));
        }
    }
}

/// Close connection and free handle.
///
/// Safe to call with `NULL`.
#[no_mangle]
pub extern "C" fn vsql_close(handle: *mut std::ffi::c_void) {
    if !handle.is_null() {
        unsafe {
            drop(Box::from_raw(handle as *mut FfiHandle));
        }
    }
}

/// Shutdown the runtime.
///
/// After calling this, all subsequent `vsql_connect` / `vsql_query` /
/// `vsql_execute` calls will fail gracefully (return `NULL` or `-1`).
///
/// Note: because `OnceLock` cannot be reset, this is effectively a no-op
/// for the runtime lifetime.  The runtime will be dropped when the process
/// exits.  This function exists for API symmetry and future extensibility.
#[no_mangle]
pub extern "C" fn vsql_shutdown() {
    // OnceLock does not support take(), so we rely on process exit for cleanup.
    // This is intentional — the runtime must outlive all handles.
}

// ── Prepared Statements ─────────────────────────────────────────────────────

/// Prepare a SQL statement. Returns a JSON string (caller must free with `vsql_free_string`).
///
/// The JSON format on success:
/// ```json
/// {"handle":"...","sql":"..."}
/// ```
///
/// On error:
/// ```json
/// {"error":"description"}
/// ```
///
/// Returns `NULL` if the handle is null or the runtime is not initialized.
#[no_mangle]
pub extern "C" fn vsql_prepare(handle: *mut std::ffi::c_void, sql: *const c_char) -> *mut c_char {
    if handle.is_null() {
        return ptr::null_mut();
    }
    let client = match as_client(handle) { Some(c) => c, None => return ptr::null_mut() };
    let sql = match unsafe { CStr::from_ptr(sql) }.to_str() {
        Ok(s) => s,
        Err(_) => return ptr::null_mut(),
    };
    let rt = match RUNTIME.get() {
        Some(rt) => rt,
        None => return ptr::null_mut(),
    };
    match rt.block_on(client.prepare(sql)) {
        Ok(stmt) => {
            let json = format!(
                "{{\"handle\":\"{}\",\"sql\":\"{}\"}}",
                stmt.handle().replace('"', "\\\""),
                stmt.sql().replace('"', "\\\"")
            );
            CString::new(json)
                .map(|s| s.into_raw())
                .unwrap_or(ptr::null_mut())
        }
        Err(e) => {
            let err = format!("{{\"error\":\"{}\"}}", e.to_string().replace('"', "\\\""));
            CString::new(err)
                .map(|s| s.into_raw())
                .unwrap_or(ptr::null_mut())
        }
    }
}

/// Execute a prepared statement. Returns a JSON result string (caller must free with `vsql_free_string`).
///
/// `prepared_json` is the JSON returned by `vsql_prepare`.
/// `params_json` is a JSON array of parameter values, e.g. `[1, "hello", null]`.
///
/// Returns the same JSON format as `vsql_query` for queries, or
/// `{"rows_affected": N}` for DML statements.
///
/// Returns `NULL` if any pointer is null or the runtime is not initialized.
#[no_mangle]
pub extern "C" fn vsql_execute_prepared(
    handle: *mut std::ffi::c_void,
    prepared_json: *const c_char,
    params_json: *const c_char,
) -> *mut c_char {
    if handle.is_null() || prepared_json.is_null() || params_json.is_null() {
        return ptr::null_mut();
    }
    let client = match as_client(handle) { Some(c) => c, None => return ptr::null_mut() };
    let prep_str = match unsafe { CStr::from_ptr(prepared_json) }.to_str() {
        Ok(s) => s,
        Err(_) => return ptr::null_mut(),
    };
    let params_str = match unsafe { CStr::from_ptr(params_json) }.to_str() {
        Ok(s) => s,
        Err(_) => return ptr::null_mut(),
    };
    let rt = match RUNTIME.get() {
        Some(rt) => rt,
        None => return ptr::null_mut(),
    };

    // Parse the prepared statement JSON to get the SQL
    let prep: serde_json::Value = match serde_json::from_str(prep_str) {
        Ok(v) => v,
        Err(_) => return ptr::null_mut(),
    };
    let sql = match prep["sql"].as_str() {
        Some(s) => s,
        None => return ptr::null_mut(),
    };

    // Parse parameters
    let params_arr: Vec<serde_json::Value> = match serde_json::from_str(params_str) {
        Ok(v) => v,
        Err(_) => return ptr::null_mut(),
    };
    let values: Vec<Value> = params_arr.iter().map(json_to_value).collect();

    // Execute as a query (returns result set)
    match rt.block_on(client.query(sql, &values)) {
        Ok(result) => {
            let json = serialize_result(&result);
            CString::new(json)
                .map(|s| s.into_raw())
                .unwrap_or(ptr::null_mut())
        }
        Err(e) => {
            let err = format!("{{\"error\":\"{}\"}}", e.to_string().replace('"', "\\\""));
            CString::new(err)
                .map(|s| s.into_raw())
                .unwrap_or(ptr::null_mut())
        }
    }
}

// ── Connection Pool ─────────────────────────────────────────────────────────

/// Create a connection pool. Returns an opaque handle (pointer).
///
/// `url` is a connection URL (e.g. `vsql://localhost:12520/mydb`).
/// `max_size` is the maximum number of connections in the pool.
///
/// Returns `NULL` on failure.  The caller must eventually pass the handle to
/// `vsql_pool_destroy` to free resources.
#[no_mangle]
pub extern "C" fn vsql_pool_create(url: *const c_char, max_size: u32) -> *mut std::ffi::c_void {
    let url = match unsafe { CStr::from_ptr(url) }.to_str() {
        Ok(s) => s,
        Err(_) => return ptr::null_mut(),
    };
    let config = match ConnectConfig::from_url(url) {
        Ok(c) => c,
        Err(_) => return ptr::null_mut(),
    };
    let pool = Pool::new(config, max_size as usize);
    Box::into_raw(Box::new(pool)) as *mut _
}

/// Get a client connection from the pool. Returns an opaque client handle.
///
/// The caller must return the handle via `vsql_pool_return` when done,
/// or close it with `vsql_close` (which will NOT return it to the pool).
///
/// Returns `NULL` if the pool is null, the runtime is not initialized, or
/// acquiring a connection fails.
#[no_mangle]
pub extern "C" fn vsql_pool_get(pool: *mut std::ffi::c_void) -> *mut std::ffi::c_void {
    if pool.is_null() {
        return ptr::null_mut();
    }
    let pool = unsafe { &*(pool as *const Pool) };
    let rt = match RUNTIME.get() {
        Some(rt) => rt,
        None => return ptr::null_mut(),
    };
    match rt.block_on(pool.get()) {
        Ok(pooled) => {
            // Box the PooledClient so we can hand it out as an opaque pointer.
            // The caller uses normal vsql_query/vsql_execute on the inner client.
            // We extract the inner Client to keep the FFI interface uniform.
            // Note: we leak the PooledClient — caller must return via vsql_pool_return.
            Box::into_raw(Box::new(pooled)) as *mut _
        }
        Err(_) => ptr::null_mut(),
    }
}

/// Return a client connection to the pool.
///
/// After this call the client handle is invalid and must not be used.
#[no_mangle]
pub extern "C" fn vsql_pool_return(pool: *mut std::ffi::c_void, client: *mut std::ffi::c_void) {
    if pool.is_null() || client.is_null() {
        return;
    }
    // Drop the PooledClient — its Drop impl returns the connection to the pool.
    unsafe {
        drop(Box::from_raw(
            client as *mut pyrosql::PooledClient,
        ));
    }
    // pool handle is not consumed — we just needed it for API clarity.
    let _ = pool;
}

/// Destroy the pool and free all resources.
///
/// All connections in the pool are closed. Outstanding pooled clients become
/// invalid.
///
/// Safe to call with `NULL`.
#[no_mangle]
pub extern "C" fn vsql_pool_destroy(pool: *mut std::ffi::c_void) {
    if !pool.is_null() {
        unsafe {
            drop(Box::from_raw(pool as *mut Pool));
        }
    }
}

// ── Auto-reconnect ──────────────────────────────────────────────────────────

/// Execute a query with auto-reconnect on connection failure.
///
/// Same as `vsql_query`, but if the query fails with a connection error,
/// the client will attempt to reconnect and retry once.
///
/// Returns a JSON string (caller must free with `vsql_free_string`).
/// Returns `NULL` if the handle is null or the runtime is not initialized.
#[no_mangle]
pub extern "C" fn vsql_query_retry(
    handle: *mut std::ffi::c_void,
    sql: *const c_char,
) -> *mut c_char {
    if handle.is_null() {
        return ptr::null_mut();
    }
    let client = match as_client_mut(handle) { Some(c) => c, None => return ptr::null_mut() };
    let sql = match unsafe { CStr::from_ptr(sql) }.to_str() {
        Ok(s) => s,
        Err(_) => return ptr::null_mut(),
    };
    let rt = match RUNTIME.get() {
        Some(rt) => rt,
        None => return ptr::null_mut(),
    };
    match rt.block_on(client.query_with_retry(sql, &[])) {
        Ok(result) => {
            let json = serialize_result(&result);
            CString::new(json)
                .map(|s| s.into_raw())
                .unwrap_or(ptr::null_mut())
        }
        Err(e) => {
            let err = format!("{{\"error\":\"{}\"}}", e.to_string().replace('"', "\\\""));
            CString::new(err)
                .map(|s| s.into_raw())
                .unwrap_or(ptr::null_mut())
        }
    }
}

/// Execute a DML statement with auto-reconnect on connection failure.
///
/// Same as `vsql_execute`, but retries once on connection error.
/// Returns rows affected, or -1 on error.
#[no_mangle]
pub extern "C" fn vsql_execute_retry(handle: *mut std::ffi::c_void, sql: *const c_char) -> i64 {
    if handle.is_null() {
        return -1;
    }
    let client = match as_client_mut(handle) { Some(c) => c, None => return -1 };
    let sql = match unsafe { CStr::from_ptr(sql) }.to_str() {
        Ok(s) => s,
        Err(_) => return -1,
    };
    let rt = match RUNTIME.get() {
        Some(rt) => rt,
        None => return -1,
    };
    match rt.block_on(client.execute_with_retry(sql, &[])) {
        Ok(n) => n as i64,
        Err(_) => -1,
    }
}

// ── WATCH / LISTEN / NOTIFY ──────────────────────────────────────────────

/// Subscribe to a reactive query via WATCH. Returns a JSON string with the channel name.
///
/// Returns `{"channel":"..."}` on success, `{"error":"..."}` on failure.
/// Caller must free with `vsql_free_string`.
#[no_mangle]
pub extern "C" fn vsql_watch(handle: *mut std::ffi::c_void, sql: *const c_char) -> *mut c_char {
    if handle.is_null() {
        return ptr::null_mut();
    }
    let client = match as_client(handle) { Some(c) => c, None => return ptr::null_mut() };
    let sql = match unsafe { CStr::from_ptr(sql) }.to_str() {
        Ok(s) => s,
        Err(_) => return ptr::null_mut(),
    };
    let rt = match RUNTIME.get() {
        Some(rt) => rt,
        None => return ptr::null_mut(),
    };
    match rt.block_on(client.watch(sql)) {
        Ok(channel) => {
            let json = format!("{{\"channel\":\"{}\"}}", channel.replace('"', "\\\""));
            CString::new(json)
                .map(|s| s.into_raw())
                .unwrap_or(ptr::null_mut())
        }
        Err(e) => {
            let err = format!("{{\"error\":\"{}\"}}", e.to_string().replace('"', "\\\""));
            CString::new(err)
                .map(|s| s.into_raw())
                .unwrap_or(ptr::null_mut())
        }
    }
}

/// Unsubscribe from a WATCH channel. Returns 0 on success, -1 on error.
#[no_mangle]
pub extern "C" fn vsql_unwatch(handle: *mut std::ffi::c_void, channel: *const c_char) -> i32 {
    if handle.is_null() || channel.is_null() {
        return -1;
    }
    let client = match as_client(handle) { Some(c) => c, None => return -1 };
    let channel = match unsafe { CStr::from_ptr(channel) }.to_str() {
        Ok(s) => s,
        Err(_) => return -1,
    };
    let rt = match RUNTIME.get() {
        Some(rt) => rt,
        None => return -1,
    };
    match rt.block_on(client.unwatch(channel)) {
        Ok(()) => 0,
        Err(_) => -1,
    }
}

/// Subscribe to a PubSub channel via LISTEN. Returns 0 on success, -1 on error.
#[no_mangle]
pub extern "C" fn vsql_listen(handle: *mut std::ffi::c_void, channel: *const c_char) -> i32 {
    if handle.is_null() || channel.is_null() {
        return -1;
    }
    let client = match as_client(handle) { Some(c) => c, None => return -1 };
    let channel = match unsafe { CStr::from_ptr(channel) }.to_str() {
        Ok(s) => s,
        Err(_) => return -1,
    };
    let rt = match RUNTIME.get() {
        Some(rt) => rt,
        None => return -1,
    };
    match rt.block_on(client.listen(channel)) {
        Ok(()) => 0,
        Err(_) => -1,
    }
}

/// Unsubscribe from a PubSub channel via UNLISTEN. Returns 0 on success, -1 on error.
#[no_mangle]
pub extern "C" fn vsql_unlisten(handle: *mut std::ffi::c_void, channel: *const c_char) -> i32 {
    if handle.is_null() || channel.is_null() {
        return -1;
    }
    let client = match as_client(handle) { Some(c) => c, None => return -1 };
    let channel = match unsafe { CStr::from_ptr(channel) }.to_str() {
        Ok(s) => s,
        Err(_) => return -1,
    };
    let rt = match RUNTIME.get() {
        Some(rt) => rt,
        None => return -1,
    };
    match rt.block_on(client.unlisten(channel)) {
        Ok(()) => 0,
        Err(_) => -1,
    }
}

/// Send a notification to a PubSub channel via NOTIFY. Returns 0 on success, -1 on error.
#[no_mangle]
pub extern "C" fn vsql_notify(
    handle: *mut std::ffi::c_void,
    channel: *const c_char,
    payload: *const c_char,
) -> i32 {
    if handle.is_null() || channel.is_null() || payload.is_null() {
        return -1;
    }
    let client = match as_client(handle) { Some(c) => c, None => return -1 };
    let channel = match unsafe { CStr::from_ptr(channel) }.to_str() {
        Ok(s) => s,
        Err(_) => return -1,
    };
    let payload = match unsafe { CStr::from_ptr(payload) }.to_str() {
        Ok(s) => s,
        Err(_) => return -1,
    };
    let rt = match RUNTIME.get() {
        Some(rt) => rt,
        None => return -1,
    };
    match rt.block_on(client.notify(channel, payload)) {
        Ok(()) => 0,
        Err(_) => -1,
    }
}

/// Register a callback for server-pushed notifications.
///
/// The callback function receives a JSON string: `{"channel":"...","payload":"..."}`.
/// The caller must free the JSON string with `vsql_free_string`.
///
/// Note: the callback is invoked from a background thread; the caller must
/// ensure thread safety.
#[no_mangle]
pub extern "C" fn vsql_on_notification(
    handle: *mut std::ffi::c_void,
    callback: extern "C" fn(*mut c_char),
) {
    if handle.is_null() {
        return;
    }
    let client = match as_client(handle) { Some(c) => c, None => return };
    client.on_notification(Box::new(move |notif| {
        let json = format!(
            "{{\"channel\":\"{}\",\"payload\":\"{}\"}}",
            notif.channel.replace('"', "\\\""),
            notif.payload.replace('"', "\\\"")
        );
        if let Ok(cstr) = CString::new(json) {
            callback(cstr.into_raw());
        }
    }));
}

// ── COPY ─────────────────────────────────────────────────────────────────

/// COPY OUT: execute a query and return CSV data as a string.
///
/// Caller must free with `vsql_free_string`.
/// Returns `NULL` on failure.
#[no_mangle]
pub extern "C" fn vsql_copy_out(handle: *mut std::ffi::c_void, sql: *const c_char) -> *mut c_char {
    if handle.is_null() {
        return ptr::null_mut();
    }
    let client = match as_client(handle) { Some(c) => c, None => return ptr::null_mut() };
    let sql = match unsafe { CStr::from_ptr(sql) }.to_str() {
        Ok(s) => s,
        Err(_) => return ptr::null_mut(),
    };
    let rt = match RUNTIME.get() {
        Some(rt) => rt,
        None => return ptr::null_mut(),
    };
    match rt.block_on(client.copy_out(sql)) {
        Ok(csv) => CString::new(csv)
            .map(|s| s.into_raw())
            .unwrap_or(ptr::null_mut()),
        Err(e) => {
            let err = format!("{{\"error\":\"{}\"}}", e.to_string().replace('"', "\\\""));
            CString::new(err)
                .map(|s| s.into_raw())
                .unwrap_or(ptr::null_mut())
        }
    }
}

/// COPY IN: send CSV data to a table. Returns rows inserted, or -1 on error.
///
/// `columns_json` is a JSON array of column names, e.g. `["id","name"]`.
/// `csv_data` is raw CSV lines (no header).
#[no_mangle]
pub extern "C" fn vsql_copy_in(
    handle: *mut std::ffi::c_void,
    table: *const c_char,
    columns_json: *const c_char,
    csv_data: *const c_char,
) -> i64 {
    if handle.is_null() || table.is_null() || columns_json.is_null() || csv_data.is_null() {
        return -1;
    }
    let client = match as_client(handle) { Some(c) => c, None => return -1 };
    let table = match unsafe { CStr::from_ptr(table) }.to_str() {
        Ok(s) => s,
        Err(_) => return -1,
    };
    let columns_str = match unsafe { CStr::from_ptr(columns_json) }.to_str() {
        Ok(s) => s,
        Err(_) => return -1,
    };
    let csv = match unsafe { CStr::from_ptr(csv_data) }.to_str() {
        Ok(s) => s,
        Err(_) => return -1,
    };
    let rt = match RUNTIME.get() {
        Some(rt) => rt,
        None => return -1,
    };

    let columns: Vec<String> = match serde_json::from_str(columns_str) {
        Ok(v) => v,
        Err(_) => return -1,
    };
    let col_refs: Vec<&str> = columns.iter().map(|s| s.as_str()).collect();

    match rt.block_on(client.copy_in(table, &col_refs, csv)) {
        Ok(n) => n as i64,
        Err(_) => -1,
    }
}

/// Subscribe to CDC events on a table. Returns a JSON string with the subscription ID.
///
/// Returns `{"subscription_id":"...","table":"..."}` on success,
/// `{"error":"..."}` on failure. Caller must free with `vsql_free_string`.
#[no_mangle]
pub extern "C" fn vsql_subscribe_cdc(
    handle: *mut std::ffi::c_void,
    table: *const c_char,
) -> *mut c_char {
    if handle.is_null() || table.is_null() {
        return ptr::null_mut();
    }
    let client = match as_client(handle) { Some(c) => c, None => return ptr::null_mut() };
    let table = match unsafe { CStr::from_ptr(table) }.to_str() {
        Ok(s) => s,
        Err(_) => return ptr::null_mut(),
    };
    let rt = match RUNTIME.get() {
        Some(rt) => rt,
        None => return ptr::null_mut(),
    };
    match rt.block_on(client.subscribe_cdc(table)) {
        Ok(stream) => {
            let json = format!(
                "{{\"subscription_id\":\"{}\",\"table\":\"{}\"}}",
                stream.subscription_id.replace('"', "\\\""),
                stream.table.replace('"', "\\\"")
            );
            CString::new(json)
                .map(|s| s.into_raw())
                .unwrap_or(ptr::null_mut())
        }
        Err(e) => {
            let err = format!("{{\"error\":\"{}\"}}", e.to_string().replace('"', "\\\""));
            CString::new(err)
                .map(|s| s.into_raw())
                .unwrap_or(ptr::null_mut())
        }
    }
}

// ── Query Cursor ─────────────────────────────────────────────────────────

/// Execute a query and return all rows as a JSON string (cursor-compatible).
///
/// This is an alias of [`vsql_query`] for v1.  The caller iterates rows
/// locally.  True server-side streaming cursors are planned for v2.
///
/// Returns a JSON string (caller must free with `vsql_free_string`).
/// Returns `NULL` if the handle is null or the runtime is not initialized.
#[no_mangle]
pub extern "C" fn vsql_query_cursor(
    handle: *mut std::ffi::c_void,
    sql: *const c_char,
) -> *mut c_char {
    // v1: identical to vsql_query — client paginates locally.
    vsql_query(handle, sql)
}

/// Fetch the next row from a cursor result set.
///
/// `cursor` is the JSON string returned by [`vsql_query_cursor`].
/// `index` is the 0-based row offset to return.
///
/// Returns a JSON array representing a single row, e.g. `[1,"Alice"]`.
/// Returns `NULL` when the index is out of range (cursor exhausted) or on
/// parse failure.
///
/// The caller must free the returned string with `vsql_free_string`.
/// The original `cursor` remains valid (not freed by this call).
#[no_mangle]
pub extern "C" fn vsql_cursor_next(
    cursor: *const c_char,
    index: u64,
) -> *mut c_char {
    if cursor.is_null() {
        return ptr::null_mut();
    }
    let json_str = match unsafe { CStr::from_ptr(cursor) }.to_str() {
        Ok(s) => s,
        Err(_) => return ptr::null_mut(),
    };
    let parsed: serde_json::Value = match serde_json::from_str(json_str) {
        Ok(v) => v,
        Err(_) => return ptr::null_mut(),
    };
    let rows = match parsed["rows"].as_array() {
        Some(r) => r,
        None => return ptr::null_mut(),
    };
    match rows.get(index as usize) {
        Some(row) => CString::new(row.to_string())
            .map(|s| s.into_raw())
            .unwrap_or(ptr::null_mut()),
        None => ptr::null_mut(),
    }
}

// ── JSON helpers ────────────────────────────────────────────────────────────

/// Convert a serde_json::Value to a PyroSQL Value.
fn json_to_value(v: &serde_json::Value) -> Value {
    match v {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Int(i)
            } else if let Some(f) = n.as_f64() {
                Value::Float(f)
            } else {
                Value::Null
            }
        }
        serde_json::Value::String(s) => Value::Text(s.clone()),
        _ => Value::Text(v.to_string()),
    }
}

// ── JSON serialization ──────────────────────────────────────────────────────

/// Serialize a [`QueryResult`] to a simple JSON string.
fn serialize_result(result: &QueryResult) -> String {
    let cols: Vec<String> = result
        .columns
        .iter()
        .map(|c| format!("\"{}\"", c.replace('"', "\\\"")))
        .collect();

    let rows: Vec<String> = result
        .rows
        .iter()
        .map(|row| {
            let vals: Vec<String> = row
                .values()
                .iter()
                .map(|v| match v {
                    Value::Null => "null".to_string(),
                    Value::Bool(b) => b.to_string(),
                    Value::Int(n) => n.to_string(),
                    Value::Float(f) => f.to_string(),
                    Value::Text(s) => {
                        format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\""))
                    }
                })
                .collect();
            format!("[{}]", vals.join(","))
        })
        .collect();

    format!(
        "{{\"columns\":[{}],\"rows\":[{}],\"rows_affected\":{}}}",
        cols.join(","),
        rows.join(","),
        result.rows_affected
    )
}
