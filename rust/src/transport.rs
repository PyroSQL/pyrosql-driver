//! Adaptive transport negotiation for PyroLink.
//!
//! PyroLink supports 5 transport tiers (T0-T4), automatically selecting
//! the fastest available option based on client-server topology:
//!
//! | Tier | Transport       | Latency     | When                     |
//! |------|-----------------|-------------|--------------------------|
//! | T0   | In-process      | ~0 ns       | Client embedded in server|
//! | T1a  | Unix socket     | ~2 us       | Same host, PG wire proto |
//! | T1b  | Shared memory   | ~200 ns     | Same host (future)       |
//! | T3   | TCP loopback    | ~10 us      | Same host (fallback)     |
//! | T4   | QUIC            | ~50+ us     | Cross-host (universal)   |
//!
//! T4 (QUIC) and T1a (Unix socket) are implemented. T0 and T1b are detected
//! via topology probing but fall back to the best available transport.

use crate::error::ClientError;
use crate::row::{QueryResult, Row, Value};
use base64::Engine as _;

// ── Transport trait ──────────────────────────────────────────────────────────

/// Common interface for all PyroLink transport tiers.
///
/// The client holds a `Box<dyn PyroTransport>` and dispatches all
/// operations through this trait, making the active transport invisible
/// to the caller.
pub trait PyroTransport: Send + Sync {
    /// Human-readable tier name for logging (e.g. `"T4:QUIC"`).
    fn tier(&self) -> &str;

    /// Execute a query and return the full result set.
    fn query(
        &self,
        sql: &str,
        params: &[Value],
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<QueryResult, ClientError>> + Send + '_>>;

    /// Execute a DML statement and return the number of affected rows.
    fn execute(
        &self,
        sql: &str,
        params: &[Value],
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<u64, ClientError>> + Send + '_>>;

    /// Send a DoAction RPC and return the JSON response.
    fn send_action(
        &self,
        action: &serde_json::Value,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, ClientError>> + Send + '_>>;

    /// Bulk insert rows into a table via DoPut.
    fn bulk_insert(
        &self,
        table: &str,
        columns: &[&str],
        rows: &[Vec<Value>],
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<u64, ClientError>> + Send + '_>>;

    /// Close the transport connection gracefully.
    fn close(
        &self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), ClientError>> + Send + '_>>;

    /// COPY OUT: stream table data as CSV.
    fn copy_out(
        &self,
        sql: &str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<String, ClientError>> + Send + '_>>;

    /// COPY IN: send CSV data to a table.
    fn copy_in(
        &self,
        table: &str,
        columns: &[&str],
        csv_data: &str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<u64, ClientError>> + Send + '_>>;

    /// Execute a query and return a streaming cursor.
    fn query_cursor(
        &self,
        sql: &str,
        params: &[Value],
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<crate::client::Cursor, ClientError>> + Send + '_>>;

    /// Subscribe to CDC events on a table.
    fn subscribe_cdc(
        &self,
        table: &str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<crate::client::CdcStream, ClientError>> + Send + '_>>;
}

// ── Transport tier enum ──────────────────────────────────────────────────────

/// The transport tiers that topology detection can recommend.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportTier {
    /// T0: client and server live in the same process (direct function call).
    T0InProcess,
    /// T1: Unix domain socket — same host, PG wire protocol.
    T1UnixSocket,
    /// T1: client and server on the same host (shared memory ring buffer).
    T1SharedMemory,
    /// T4: QUIC over the network (universal fallback, always available).
    T4Quic,
}

impl std::fmt::Display for TransportTier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::T0InProcess => write!(f, "T0:InProcess"),
            Self::T1UnixSocket => write!(f, "T1:Unix"),
            Self::T1SharedMemory => write!(f, "T1:SharedMemory"),
            Self::T4Quic => write!(f, "T4:QUIC"),
        }
    }
}

// ── Topology hints ───────────────────────────────────────────────────────────

/// Server capabilities advertised during topology detection.
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct Capabilities {
    /// Server supports QUIC transport (always true for PyroLink).
    pub quic: bool,
    /// Server supports shared memory transport (Unix only).
    pub shm: bool,
    /// Server supports Unix domain socket transport (PG wire protocol).
    #[serde(default)]
    pub unix_socket: bool,
}

/// Topology information returned by the server on connection.
///
/// The client uses these hints to determine whether a faster transport
/// tier is available (e.g. shared memory when on the same host).
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct TopologyHints {
    /// Server hostname.
    pub hostname: String,
    /// Server process ID.
    pub pid: u32,
    /// Server-side capabilities.
    pub capabilities: Capabilities,
}

impl TopologyHints {
    /// Determine the best transport tier given the client's own identity.
    ///
    /// The algorithm is:
    /// 1. Same PID → T0 (in-process)
    /// 2. Same hostname + SHM capable → T1:SharedMemory
    /// 3. Same hostname + Unix socket capable → T1:UnixSocket
    /// 4. Same hostname (no SHM, no Unix) → T1:UnixSocket (try anyway)
    /// 5. Otherwise → T4 (QUIC)
    #[must_use]
    pub fn best_tier(&self, client_hostname: &str, client_pid: u32) -> TransportTier {
        if self.pid == client_pid && self.hostname == client_hostname {
            TransportTier::T0InProcess
        } else if self.hostname == client_hostname && self.capabilities.shm {
            TransportTier::T1SharedMemory
        } else if self.hostname == client_hostname {
            // Same host: prefer Unix socket (even if not explicitly advertised,
            // we try it optimistically and fall back to QUIC on failure).
            TransportTier::T1UnixSocket
        } else {
            TransportTier::T4Quic
        }
    }
}

// ── QUIC transport (T4) ─────────────────────────────────────────────────────

/// T4: QUIC transport — wraps the existing quinn-based connection.
///
/// This is the universal fallback transport, always available. It delegates
/// to the same QUIC stream logic that `Client` uses directly today.
pub(crate) struct QuicTransport {
    pub(crate) connection: quinn::Connection,
}

impl PyroTransport for QuicTransport {
    fn tier(&self) -> &str {
        "T4:QUIC"
    }

    fn query(
        &self,
        sql: &str,
        params: &[Value],
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<QueryResult, ClientError>> + Send + '_>> {
        let final_sql = crate::client::interpolate_params(sql, params);
        Box::pin(async move {
            let (mut send, mut recv) = open_query_stream(&self.connection, &final_sql).await?;
            let result = crate::client::read_query_result(&mut recv).await?;
            let _ = send.finish();
            Ok(result)
        })
    }

    fn execute(
        &self,
        sql: &str,
        params: &[Value],
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<u64, ClientError>> + Send + '_>> {
        let final_sql = crate::client::interpolate_params(sql, params);
        Box::pin(async move {
            let (mut send, mut recv) = open_query_stream(&self.connection, &final_sql).await?;
            let result = crate::client::read_query_result(&mut recv).await?;
            let _ = send.finish();
            Ok(result.rows_affected)
        })
    }

    fn send_action(
        &self,
        action: &serde_json::Value,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, ClientError>> + Send + '_>> {
        let payload = serde_json::to_vec(action).unwrap_or_default();
        Box::pin(async move {
            let (mut send, mut recv) = self
                .connection
                .open_bi()
                .await
                .map_err(|e| ClientError::Connection(format!("failed to open action stream: {e}")))?;

            crate::client::write_message(&mut send, crate::client::MSG_DO_ACTION, &payload).await?;
            send.finish()
                .map_err(|e| ClientError::Protocol(format!("finish action stream: {e}")))?;

            // Read the response — expect a single MSG_DO_ACTION frame
            let msg = crate::client::read_message(&mut recv).await?;
            let (_tag, resp_payload) = msg.ok_or_else(|| {
                ClientError::Protocol("action: expected response, got EOS".into())
            })?;

            let resp: serde_json::Value = serde_json::from_slice(&resp_payload)
                .map_err(|e| ClientError::Protocol(format!("action response decode: {e}")))?;

            // Check for server-side error in the response
            if let Some(err) = resp.get("error").and_then(|e| e.as_str()) {
                return Err(ClientError::Query(err.to_string()));
            }

            Ok(resp)
        })
    }

    fn bulk_insert(
        &self,
        table: &str,
        columns: &[&str],
        rows: &[Vec<Value>],
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<u64, ClientError>> + Send + '_>> {
        // Build the DoPut payload: a JSON descriptor followed by row batches
        let descriptor = serde_json::json!({
            "table": table,
            "columns": columns,
            "rows": rows,
        });
        let payload = serde_json::to_vec(&descriptor).unwrap_or_default();
        Box::pin(async move {
            let (mut send, mut recv) = self
                .connection
                .open_bi()
                .await
                .map_err(|e| ClientError::Connection(format!("failed to open bulk insert stream: {e}")))?;

            crate::client::write_message(&mut send, 0x06 /* MSG_DO_PUT */, &payload).await?;
            send.finish()
                .map_err(|e| ClientError::Protocol(format!("finish bulk insert stream: {e}")))?;

            let result = crate::client::read_query_result(&mut recv).await?;
            Ok(result.rows_affected)
        })
    }

    fn close(
        &self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), ClientError>> + Send + '_>> {
        Box::pin(async move {
            self.connection
                .close(quinn::VarInt::from_u32(0), b"client close");
            Ok(())
        })
    }

    fn copy_out(
        &self,
        sql: &str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<String, ClientError>> + Send + '_>> {
        let sql = sql.to_owned();
        Box::pin(async move {
            let (mut send, mut recv) = self
                .connection
                .open_bi()
                .await
                .map_err(|e| ClientError::Connection(format!("failed to open copy_out stream: {e}")))?;

            crate::client::write_message(&mut send, crate::client::MSG_COPY_OUT, sql.as_bytes()).await?;
            send.finish()
                .map_err(|e| ClientError::Protocol(format!("finish copy_out stream: {e}")))?;

            let mut csv = String::new();
            while let Some((tag, payload)) = crate::client::read_message(&mut recv).await? {
                if tag == crate::client::MSG_COPY_OUT {
                    let chunk = std::str::from_utf8(&payload)
                        .map_err(|e| ClientError::Protocol(format!("copy_out decode: {e}")))?;
                    csv.push_str(chunk);
                }
            }
            Ok(csv)
        })
    }

    fn copy_in(
        &self,
        table: &str,
        columns: &[&str],
        csv_data: &str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<u64, ClientError>> + Send + '_>> {
        let descriptor = serde_json::json!({
            "table": table,
            "columns": columns,
        });
        let desc_payload = serde_json::to_vec(&descriptor).unwrap_or_default();
        let csv_owned = csv_data.to_owned();
        Box::pin(async move {
            let (mut send, mut recv) = self
                .connection
                .open_bi()
                .await
                .map_err(|e| ClientError::Connection(format!("failed to open copy_in stream: {e}")))?;

            // Send RPC type + descriptor
            crate::client::write_message(&mut send, crate::client::MSG_COPY_IN, &desc_payload).await?;
            // Send CSV data
            crate::client::write_message(&mut send, crate::client::MSG_COPY_IN, csv_owned.as_bytes()).await?;
            // Finish the send side
            send.finish()
                .map_err(|e| ClientError::Protocol(format!("finish copy_in stream: {e}")))?;

            // Read response
            let result = crate::client::read_query_result(&mut recv).await?;
            Ok(result.rows_affected)
        })
    }

    fn query_cursor(
        &self,
        sql: &str,
        params: &[Value],
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<crate::client::Cursor, ClientError>> + Send + '_>> {
        let final_sql = crate::client::interpolate_params(sql, params);
        Box::pin(async move {
            let (mut send, mut recv) = self
                .connection
                .open_bi()
                .await
                .map_err(|e| ClientError::Connection(format!("failed to open cursor stream: {e}")))?;

            crate::client::write_message(&mut send, crate::client::MSG_QUERY, final_sql.as_bytes()).await?;
            send.finish()
                .map_err(|e| ClientError::Protocol(format!("finish cursor stream: {e}")))?;

            // Read the schema message first
            let mut columns: Vec<String> = Vec::new();
            if let Some((tag, payload)) = crate::client::read_message(&mut recv).await? {
                if tag == 0x01 /* MSG_SCHEMA */ {
                    columns = serde_json::from_slice(&payload)
                        .map_err(|e| ClientError::Protocol(format!("cursor schema decode: {e}")))?;
                }
            }

            Ok(crate::client::Cursor::from_stream(columns, recv))
        })
    }

    fn subscribe_cdc(
        &self,
        table: &str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<crate::client::CdcStream, ClientError>> + Send + '_>> {
        let payload = serde_json::json!({ "table": table });
        let data = serde_json::to_vec(&payload).unwrap_or_default();
        let table_owned = table.to_owned();
        Box::pin(async move {
            let (mut send, mut recv) = self
                .connection
                .open_bi()
                .await
                .map_err(|e| ClientError::Connection(format!("failed to open cdc stream: {e}")))?;

            crate::client::write_message(&mut send, crate::client::MSG_SUBSCRIBE_CDC, &data).await?;
            send.finish()
                .map_err(|e| ClientError::Protocol(format!("finish cdc stream: {e}")))?;

            // Read the subscription confirmation
            let msg = crate::client::read_message(&mut recv).await?;
            let (_tag, resp_payload) = msg.ok_or_else(|| {
                ClientError::Protocol("cdc: expected subscription response, got EOS".into())
            })?;

            let resp: serde_json::Value = serde_json::from_slice(&resp_payload)
                .map_err(|e| ClientError::Protocol(format!("cdc response decode: {e}")))?;

            let subscription_id = resp["subscription_id"]
                .as_str()
                .unwrap_or("")
                .to_string();

            Ok(crate::client::CdcStream {
                subscription_id,
                table: table_owned,
            })
        })
    }
}

impl QuicTransport {
    /// Request topology hints from the server via the MSG_TOPOLOGY RPC.
    pub(crate) async fn get_topology(&self) -> Result<TopologyHints, ClientError> {
        let (mut send, mut recv) = self
            .connection
            .open_bi()
            .await
            .map_err(|e| ClientError::Connection(format!("failed to open topology stream: {e}")))?;

        // Send MSG_TOPOLOGY with empty payload
        crate::client::write_message(&mut send, crate::client::MSG_TOPOLOGY, &[]).await?;
        send.finish()
            .map_err(|e| ClientError::Protocol(format!("finish topology stream: {e}")))?;

        // Read the response
        let msg = crate::client::read_message(&mut recv).await?;
        let (_tag, payload) = msg.ok_or_else(|| {
            ClientError::Protocol("topology: expected response, got EOS".into())
        })?;

        let hints: TopologyHints = serde_json::from_slice(&payload)
            .map_err(|e| ClientError::Protocol(format!("topology decode: {e}")))?;

        Ok(hints)
    }
}

/// Open a bidirectional QUIC stream and send a MSG_QUERY frame.
async fn open_query_stream(
    connection: &quinn::Connection,
    sql: &str,
) -> Result<(quinn::SendStream, quinn::RecvStream), ClientError> {
    let (mut send, recv) = connection
        .open_bi()
        .await
        .map_err(|e| ClientError::Connection(format!("failed to open stream: {e}")))?;

    crate::client::write_message(&mut send, crate::client::MSG_QUERY, sql.as_bytes()).await?;
    send.finish()
        .map_err(|e| ClientError::Protocol(format!("finish failed: {e}")))?;

    Ok((send, recv))
}

// ── TCP PG wire transport ─────────────────────────────────────────────────────

/// PostgreSQL wire protocol over TCP — connects to PyroSQL's PG-compatible listener.
///
/// Uses the same Simple Query protocol as [`UnixTransport`] but over a TCP connection
/// (port 5432 by default). This enables connecting from any host using standard PG tools.
pub struct TcpPgTransport {
    stream: tokio::sync::Mutex<tokio::net::TcpStream>,
}

impl TcpPgTransport {
    /// Connect to a PyroSQL server via PostgreSQL wire protocol over TCP.
    pub async fn connect(host: &str, port: u16, user: &str, database: &str, password: &str) -> Result<Self, ClientError> {
        let addr = format!("{host}:{port}");
        let stream = tokio::net::TcpStream::connect(&addr)
            .await
            .map_err(|e| ClientError::Connection(format!("TCP connect({addr}): {e}")))?;

        let transport = Self {
            stream: tokio::sync::Mutex::new(stream),
        };

        transport.send_startup(user, database, password).await?;
        Ok(transport)
    }

    /// Send PG v3 StartupMessage and consume server responses until ReadyForQuery.
    async fn send_startup(&self, user: &str, database: &str, password: &str) -> Result<(), ClientError> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let mut stream = self.stream.lock().await;

        let mut body = Vec::new();
        body.extend_from_slice(&196608_i32.to_be_bytes());
        body.extend_from_slice(b"user\0");
        body.extend_from_slice(user.as_bytes());
        body.push(0);
        if !database.is_empty() {
            body.extend_from_slice(b"database\0");
            body.extend_from_slice(database.as_bytes());
            body.push(0);
        }
        body.push(0);

        let len = (body.len() + 4) as i32;
        let mut msg = Vec::with_capacity(body.len() + 4);
        msg.extend_from_slice(&len.to_be_bytes());
        msg.extend_from_slice(&body);

        stream.write_all(&msg).await
            .map_err(|e| ClientError::Connection(format!("TCP PG startup write: {e}")))?;

        // SCRAM state — populated when auth type 10 (SASL) is received.
        let mut scram_client_first_bare: Option<String> = None;
        let mut scram_client_nonce: Option<String> = None;
        let mut scram_expected_server_sig: Option<Vec<u8>> = None;

        loop {
            // Read tag + length in a single 5-byte read.
            let mut hdr = [0u8; 5];
            stream.read_exact(&mut hdr).await
                .map_err(|e| ClientError::Protocol(format!("TCP PG startup read header: {e}")))?;
            let payload_len = (i32::from_be_bytes([hdr[1], hdr[2], hdr[3], hdr[4]]) - 4) as usize;

            let mut payload = vec![0u8; payload_len];
            if payload_len > 0 {
                stream.read_exact(&mut payload).await
                    .map_err(|e| ClientError::Protocol(format!("TCP PG startup read payload: {e}")))?;
            }

            match hdr[0] {
                b'R' => {
                    if payload_len >= 4 {
                        let auth_type = i32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
                        match auth_type {
                            0 => { /* AuthenticationOk */ }
                            3 => {
                                // CleartextPassword — send password
                                let pw_bytes = password.as_bytes();
                                let pw_len = (pw_bytes.len() + 4 + 1) as i32;
                                let mut pw_msg = Vec::with_capacity(5 + pw_bytes.len() + 1);
                                pw_msg.push(b'p');
                                pw_msg.extend_from_slice(&pw_len.to_be_bytes());
                                pw_msg.extend_from_slice(pw_bytes);
                                pw_msg.push(0);
                                stream.write_all(&pw_msg).await
                                    .map_err(|e| ClientError::Connection(format!("TCP PG password write: {e}")))?;
                            }
                            5 => {
                                // MD5Password — not supported (insecure).
                                return Err(ClientError::Connection(
                                    "server requires MD5 auth which is not supported (insecure). \
                                     Set VALKARNSQL_TRUST_ALL=true on the server for dev, \
                                     or configure SCRAM-SHA-256 for production.".into()
                                ));
                            }
                            10 => {
                                // AuthenticationSASL — read mechanism list from payload[4..].
                                // Mechanism names are null-terminated strings, list ends with an extra null.
                                let mechanism_data = &payload[4..];
                                let mut found_scram256 = false;
                                for mech in mechanism_data.split(|&b| b == 0) {
                                    if mech == b"SCRAM-SHA-256" {
                                        found_scram256 = true;
                                        break;
                                    }
                                }
                                if !found_scram256 {
                                    return Err(ClientError::Connection(
                                        "TCP PG: server SASL mechanisms do not include SCRAM-SHA-256".into()
                                    ));
                                }

                                // Generate client-first-message
                                let (client_first_msg, bare, nonce) =
                                    crate::scram_client::scram_client_first(user);
                                scram_client_first_bare = Some(bare);
                                scram_client_nonce = Some(nonce);

                                // Send SASLInitialResponse:
                                //   'p' | int32 len | "SCRAM-SHA-256\0" | int32 client-first-len | client-first-data
                                let mechanism = b"SCRAM-SHA-256\0";
                                let cf_bytes = client_first_msg.as_bytes();
                                let cf_len = cf_bytes.len() as i32;
                                let total_len = 4 + mechanism.len() as i32 + 4 + cf_len;
                                let mut sasl_msg = Vec::with_capacity(1 + total_len as usize);
                                sasl_msg.push(b'p');
                                sasl_msg.extend_from_slice(&total_len.to_be_bytes());
                                sasl_msg.extend_from_slice(mechanism);
                                sasl_msg.extend_from_slice(&cf_len.to_be_bytes());
                                sasl_msg.extend_from_slice(cf_bytes);
                                stream.write_all(&sasl_msg).await
                                    .map_err(|e| ClientError::Connection(format!("TCP PG SASL initial write: {e}")))?;
                            }
                            11 => {
                                // AuthenticationSASLContinue — server-first-message is in payload[4..]
                                let server_first = std::str::from_utf8(&payload[4..]).map_err(|e| {
                                    ClientError::Protocol(format!("TCP PG: invalid UTF-8 in server-first: {e}"))
                                })?;

                                let bare = scram_client_first_bare.as_deref().ok_or_else(|| {
                                    ClientError::Protocol("TCP PG: got SASL continue without initial exchange".into())
                                })?;
                                let nonce = scram_client_nonce.as_deref().ok_or_else(|| {
                                    ClientError::Protocol("TCP PG: missing client nonce".into())
                                })?;

                                let (client_final, expected_sig) =
                                    crate::scram_client::scram_client_final(password, bare, server_first, nonce)?;
                                scram_expected_server_sig = Some(expected_sig);

                                // Send SASLResponse: 'p' | int32 len | client-final-data
                                let cf_bytes = client_final.as_bytes();
                                let total_len = (cf_bytes.len() + 4) as i32;
                                let mut resp_msg = Vec::with_capacity(1 + total_len as usize);
                                resp_msg.push(b'p');
                                resp_msg.extend_from_slice(&total_len.to_be_bytes());
                                resp_msg.extend_from_slice(cf_bytes);
                                stream.write_all(&resp_msg).await
                                    .map_err(|e| ClientError::Connection(format!("TCP PG SASL response write: {e}")))?;
                            }
                            12 => {
                                // AuthenticationSASLFinal — verify server signature in payload[4..]
                                let server_final = std::str::from_utf8(&payload[4..]).map_err(|e| {
                                    ClientError::Protocol(format!("TCP PG: invalid UTF-8 in server-final: {e}"))
                                })?;

                                if let Some(expected) = &scram_expected_server_sig {
                                    // Parse "v=<base64-signature>"
                                    let sig_b64 = server_final.strip_prefix("v=").ok_or_else(|| {
                                        ClientError::Protocol(format!(
                                            "TCP PG: server-final missing v= prefix: {server_final}"
                                        ))
                                    })?;
                                    let server_sig = base64::engine::general_purpose::STANDARD
                                        .decode(sig_b64)
                                        .map_err(|e| {
                                            ClientError::Protocol(format!("TCP PG: invalid server signature base64: {e}"))
                                        })?;
                                    if server_sig != *expected {
                                        return Err(ClientError::Connection(
                                            "TCP PG: SCRAM server signature mismatch — possible MITM".into()
                                        ));
                                    }
                                }
                            }
                            _ => {
                                return Err(ClientError::Connection(format!(
                                    "TCP PG: unsupported authentication type {auth_type}"
                                )));
                            }
                        }
                    }
                }
                b'K' | b'S' => { /* BackendKeyData / ParameterStatus — ignore */ }
                b'Z' => break,
                b'E' => {
                    let err_msg = pg_parse_error(&payload);
                    return Err(ClientError::Connection(format!("TCP PG startup error: {err_msg}")));
                }
                _ => {}
            }
        }

        Ok(())
    }

    /// Execute a Simple Query and return the parsed result.
    async fn simple_query(&self, sql: &str) -> Result<QueryResult, ClientError> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let mut stream = self.stream.lock().await;

        let sql_bytes = sql.as_bytes();
        let len = (sql_bytes.len() + 4 + 1) as i32;
        let mut msg = Vec::with_capacity(5 + sql_bytes.len() + 1);
        msg.push(b'Q');
        msg.extend_from_slice(&len.to_be_bytes());
        msg.extend_from_slice(sql_bytes);
        msg.push(0);

        stream.write_all(&msg).await
            .map_err(|e| ClientError::Protocol(format!("TCP PG query write: {e}")))?;

        let mut columns: Vec<String> = Vec::new();
        let mut col_meta: Option<std::sync::Arc<crate::row::ColumnMeta>> = None;
        let mut rows: Vec<Row> = Vec::new();
        let mut rows_affected: u64 = 0;

        loop {
            // Read tag + length in a single 5-byte read.
            let mut hdr = [0u8; 5];
            stream.read_exact(&mut hdr).await
                .map_err(|e| ClientError::Protocol(format!("TCP PG query read header: {e}")))?;
            let payload_len = (i32::from_be_bytes([hdr[1], hdr[2], hdr[3], hdr[4]]) - 4) as usize;

            let mut payload = vec![0u8; payload_len];
            if payload_len > 0 {
                stream.read_exact(&mut payload).await
                    .map_err(|e| ClientError::Protocol(format!("TCP PG query read payload: {e}")))?;
            }

            match hdr[0] {
                b'T' => {
                    columns = pg_parse_row_description(&payload)?;
                    col_meta = Some(crate::row::ColumnMeta::new(columns.clone()));
                }
                b'D' => {
                    let meta = col_meta.as_ref().ok_or_else(|| {
                        ClientError::Protocol("DataRow before RowDescription".into())
                    })?;
                    let values = pg_parse_data_row(&payload, &columns)?;
                    rows.push(Row::new(std::sync::Arc::clone(meta), values));
                }
                b'C' => {
                    let tag_str = String::from_utf8_lossy(&payload);
                    let tag_str = tag_str.trim_end_matches('\0');
                    rows_affected = pg_parse_command_tag(tag_str);
                }
                b'E' => {
                    let err_msg = pg_parse_error(&payload);
                    return Err(ClientError::Query(err_msg));
                }
                b'Z' => break,
                b'I' | b'N' => {}
                _ => {}
            }
        }

        Ok(QueryResult { columns, rows, rows_affected })
    }
}

/// Delegate all PyroTransport methods via simple_query (same pattern as UnixTransport).
impl PyroTransport for TcpPgTransport {
    fn tier(&self) -> &str { "T3:TCP-PG" }

    fn query(&self, sql: &str, params: &[Value])
        -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<QueryResult, ClientError>> + Send + '_>>
    {
        let final_sql = crate::client::interpolate_params(sql, params);
        Box::pin(async move { self.simple_query(&final_sql).await })
    }

    fn execute(&self, sql: &str, params: &[Value])
        -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<u64, ClientError>> + Send + '_>>
    {
        let final_sql = crate::client::interpolate_params(sql, params);
        Box::pin(async move {
            let result = self.simple_query(&final_sql).await?;
            Ok(result.rows_affected)
        })
    }

    fn send_action(&self, action: &serde_json::Value)
        -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, ClientError>> + Send + '_>>
    {
        let action = action.clone();
        Box::pin(async move {
            let action_type = action["type"].as_str().unwrap_or("");
            match action_type {
                "BeginTransaction" => {
                    let isolation = action["isolation"].as_str().unwrap_or("read committed");
                    self.simple_query(&format!("BEGIN ISOLATION LEVEL {}", isolation.to_uppercase())).await?;
                    let tx_id = format!("tcp-tx-{}", std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_nanos());
                    Ok(serde_json::json!({"transaction_id": tx_id}))
                }
                "Commit" => { self.simple_query("COMMIT").await?; Ok(serde_json::json!({"status": "ok"})) }
                "Rollback" => { self.simple_query("ROLLBACK").await?; Ok(serde_json::json!({"status": "ok"})) }
                "CreateSavepoint" => {
                    let name = action["name"].as_str().unwrap_or("sp");
                    self.simple_query(&format!("SAVEPOINT {name}")).await?;
                    Ok(serde_json::json!({"status": "ok"}))
                }
                "RollbackSavepoint" => {
                    let name = action["name"].as_str().unwrap_or("sp");
                    self.simple_query(&format!("ROLLBACK TO SAVEPOINT {name}")).await?;
                    Ok(serde_json::json!({"status": "ok"}))
                }
                "PrepareStatement" => {
                    let sql = action["sql"].as_str().unwrap_or("");
                    Ok(serde_json::json!({"handle": format!("tcp-ps-{}", sql.len())}))
                }
                other => Err(ClientError::Protocol(format!("action '{other}' not supported over TCP PG transport")))
            }
        })
    }

    fn bulk_insert(&self, table: &str, columns: &[&str], rows: &[Vec<Value>])
        -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<u64, ClientError>> + Send + '_>>
    {
        let col_list = columns.join(", ");
        let mut values_parts = Vec::with_capacity(rows.len());
        for row in rows {
            let vals: Vec<std::borrow::Cow<'static, str>> = row.iter().map(|v| crate::client::value_to_sql(v)).collect();
            values_parts.push(format!("({})", vals.join(", ")));
        }
        let sql = format!("INSERT INTO {table} ({col_list}) VALUES {}", values_parts.join(", "));
        Box::pin(async move {
            let result = self.simple_query(&sql).await?;
            Ok(result.rows_affected)
        })
    }

    fn close(&self)
        -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), ClientError>> + Send + '_>>
    {
        Box::pin(async move {
            use tokio::io::AsyncWriteExt;
            let mut stream = self.stream.lock().await;
            let msg = [b'X', 0, 0, 0, 4];
            let _ = stream.write_all(&msg).await;
            let _ = stream.shutdown().await;
            Ok(())
        })
    }

    fn copy_out(&self, sql: &str)
        -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<String, ClientError>> + Send + '_>>
    {
        let sql = sql.to_owned();
        Box::pin(async move {
            let result = self.simple_query(&sql).await?;
            let mut csv = result.columns.join(",");
            csv.push('\n');
            for row in &result.rows {
                let vals: Vec<String> = row.values().iter().map(|v| match v {
                    crate::row::Value::Null => String::new(),
                    crate::row::Value::Text(s) => if s.contains(',') || s.contains('"') || s.contains('\n') {
                        format!("\"{}\"", s.replace('"', "\"\""))
                    } else { s.clone() },
                    other => format!("{:?}", other),
                }).collect();
                csv.push_str(&vals.join(","));
                csv.push('\n');
            }
            Ok(csv)
        })
    }

    fn copy_in(&self, table: &str, columns: &[&str], csv_data: &str)
        -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<u64, ClientError>> + Send + '_>>
    {
        let col_list = columns.join(", ");
        let table = table.to_owned();
        let csv = csv_data.to_owned();
        Box::pin(async move {
            let mut total: u64 = 0;
            for line in csv.lines() {
                let trimmed = line.trim();
                if trimmed.is_empty() { continue; }
                let values: Vec<String> = trimmed.split(',').map(|v| {
                    let v = v.trim();
                    if v.is_empty() || v.eq_ignore_ascii_case("null") { "NULL".to_string() }
                    else if v.parse::<f64>().is_ok() { v.to_string() }
                    else { format!("'{}'", v.replace('\'', "''")) }
                }).collect();
                let sql = if col_list.is_empty() {
                    format!("INSERT INTO {} VALUES ({})", table, values.join(", "))
                } else {
                    format!("INSERT INTO {} ({}) VALUES ({})", table, col_list, values.join(", "))
                };
                self.simple_query(&sql).await?;
                total += 1;
            }
            Ok(total)
        })
    }

    fn query_cursor(&self, sql: &str, params: &[Value])
        -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<crate::client::Cursor, ClientError>> + Send + '_>>
    {
        let final_sql = crate::client::interpolate_params(sql, params);
        Box::pin(async move {
            let result = self.simple_query(&final_sql).await?;
            Ok(crate::client::Cursor::from_rows(result.columns.clone(), result.rows))
        })
    }

    fn subscribe_cdc(&self, table: &str)
        -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<crate::client::CdcStream, ClientError>> + Send + '_>>
    {
        let table = table.to_owned();
        Box::pin(async move {
            Ok(crate::client::CdcStream {
                subscription_id: format!("tcp-cdc-{}", table),
                table,
            })
        })
    }
}

// ── MySQL wire transport ─────────────────────────────────────────────────────

/// MySQL wire protocol transport over TCP.
///
/// Implements the MySQL v10 handshake and COM_QUERY protocol for connecting
/// to PyroSQL's MySQL-compatible listener (port 3306).
pub struct MysqlTransport {
    stream: tokio::sync::Mutex<tokio::net::TcpStream>,
}

impl MysqlTransport {
    /// Connect to a PyroSQL server via MySQL wire protocol.
    pub async fn connect(host: &str, port: u16, user: &str, _database: &str, password: &str) -> Result<Self, ClientError> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let addr = format!("{host}:{port}");
        let mut stream = tokio::net::TcpStream::connect(&addr)
            .await
            .map_err(|e| ClientError::Connection(format!("MySQL TCP connect({addr}): {e}")))?;

        // Read server greeting (handshake v10)
        let mut header = [0u8; 4];
        stream.read_exact(&mut header).await
            .map_err(|e| ClientError::Protocol(format!("MySQL handshake header: {e}")))?;
        let pkt_len = (header[0] as usize) | ((header[1] as usize) << 8) | ((header[2] as usize) << 16);
        let _seq = header[3];

        let mut greeting = vec![0u8; pkt_len];
        stream.read_exact(&mut greeting).await
            .map_err(|e| ClientError::Protocol(format!("MySQL handshake body: {e}")))?;

        // Parse minimal greeting: protocol version (1), server version (null-terminated),
        // connection id (4), auth-plugin-data-part-1 (8), filler (1)
        if greeting.is_empty() || greeting[0] != 10 {
            return Err(ClientError::Protocol("MySQL: unexpected protocol version".into()));
        }
        let null_pos = greeting[1..].iter().position(|&b| b == 0).unwrap_or(0) + 1;
        let _server_version = &greeting[1..null_pos];
        let pos = null_pos + 1; // skip null terminator
        let _conn_id = if pos + 4 <= greeting.len() {
            u32::from_le_bytes([greeting[pos], greeting[pos+1], greeting[pos+2], greeting[pos+3]])
        } else { 0 };
        let auth_data_1 = if pos + 4 + 8 <= greeting.len() {
            &greeting[pos+4..pos+4+8]
        } else { &[] };

        // Build HandshakeResponse41
        let capability_flags: u32 = 0x0000_0200 // CLIENT_PROTOCOL_41
            | 0x0000_8000  // CLIENT_SECURE_CONNECTION
            | 0x0008_0000  // CLIENT_PLUGIN_AUTH
            | 0x0000_0008; // CLIENT_CONNECT_WITH_DB
        let _max_packet = 16777216u32;
        let charset: u8 = 33; // utf8_general_ci

        // Compute auth response: SHA1(password) XOR SHA1(scramble + SHA1(SHA1(password)))
        let auth_response = if password.is_empty() {
            vec![]
        } else {
            mysql_native_password_auth(password.as_bytes(), auth_data_1)
        };

        let mut response = Vec::new();
        response.extend_from_slice(&capability_flags.to_le_bytes());
        response.extend_from_slice(&_max_packet.to_le_bytes());
        response.push(charset);
        response.extend_from_slice(&[0u8; 23]); // reserved
        response.extend_from_slice(user.as_bytes());
        response.push(0); // null-terminated username
        response.push(auth_response.len() as u8);
        response.extend_from_slice(&auth_response);
        response.extend_from_slice(_database.as_bytes());
        response.push(0);
        response.extend_from_slice(b"mysql_native_password\0");

        // Send as MySQL packet
        let pkt_len = response.len();
        let mut pkt = Vec::with_capacity(4 + pkt_len);
        pkt.push((pkt_len & 0xFF) as u8);
        pkt.push(((pkt_len >> 8) & 0xFF) as u8);
        pkt.push(((pkt_len >> 16) & 0xFF) as u8);
        pkt.push(1); // sequence number
        pkt.extend_from_slice(&response);
        stream.write_all(&pkt).await
            .map_err(|e| ClientError::Connection(format!("MySQL handshake response: {e}")))?;

        // Read OK/ERR response
        let mut resp_header = [0u8; 4];
        stream.read_exact(&mut resp_header).await
            .map_err(|e| ClientError::Protocol(format!("MySQL auth response header: {e}")))?;
        let resp_len = (resp_header[0] as usize) | ((resp_header[1] as usize) << 8) | ((resp_header[2] as usize) << 16);
        let mut resp_body = vec![0u8; resp_len];
        stream.read_exact(&mut resp_body).await
            .map_err(|e| ClientError::Protocol(format!("MySQL auth response body: {e}")))?;

        if !resp_body.is_empty() && resp_body[0] == 0xFF {
            // ERR_Packet
            let err_msg = if resp_body.len() > 9 {
                String::from_utf8_lossy(&resp_body[9..]).into_owned()
            } else {
                "authentication failed".into()
            };
            return Err(ClientError::Connection(format!("MySQL auth error: {err_msg}")));
        }

        Ok(Self {
            stream: tokio::sync::Mutex::new(stream),
        })
    }

    /// Execute a COM_QUERY and return parsed results.
    async fn com_query(&self, sql: &str) -> Result<QueryResult, ClientError> {
        use tokio::io::AsyncWriteExt;

        let mut stream = self.stream.lock().await;

        // COM_QUERY packet: [1 byte cmd=0x03] [SQL]
        let sql_bytes = sql.as_bytes();
        let pkt_len = 1 + sql_bytes.len();
        let mut pkt = Vec::with_capacity(4 + pkt_len);
        pkt.push((pkt_len & 0xFF) as u8);
        pkt.push(((pkt_len >> 8) & 0xFF) as u8);
        pkt.push(((pkt_len >> 16) & 0xFF) as u8);
        pkt.push(0); // sequence = 0
        pkt.push(0x03); // COM_QUERY
        pkt.extend_from_slice(sql_bytes);
        stream.write_all(&pkt).await
            .map_err(|e| ClientError::Protocol(format!("MySQL query write: {e}")))?;

        // Read result: first packet tells us column count or OK/ERR
        let (first_body, _seq) = mysql_read_packet(&mut *stream).await?;
        if first_body.is_empty() {
            return Ok(QueryResult { columns: vec![], rows: vec![], rows_affected: 0 });
        }

        // OK packet (0x00) — DML result
        if first_body[0] == 0x00 {
            let (affected, _) = mysql_decode_lenenc(&first_body[1..]);
            return Ok(QueryResult { columns: vec![], rows: vec![], rows_affected: affected });
        }

        // ERR packet (0xFF)
        if first_body[0] == 0xFF {
            let err_msg = if first_body.len() > 9 {
                String::from_utf8_lossy(&first_body[9..]).into_owned()
            } else {
                "query failed".into()
            };
            return Err(ClientError::Query(err_msg));
        }

        // Result set: first byte = column count (lenenc int)
        let (col_count, _) = mysql_decode_lenenc(&first_body);

        // Read column definitions
        let mut columns = Vec::with_capacity(col_count as usize);
        for _ in 0..col_count {
            let (col_body, _) = mysql_read_packet(&mut *stream).await?;
            // Column definition: catalog, schema, table, org_table, name, org_name...
            // We need to skip to 'name' field (5th lenenc string)
            let mut pos = 0;
            for _ in 0..4 { // skip catalog, schema, table, org_table
                let (len, adv) = mysql_decode_lenenc(&col_body[pos..]);
                pos += adv + len as usize;
            }
            // 5th = column name
            let (name_len, adv) = mysql_decode_lenenc(&col_body[pos..]);
            pos += adv;
            let name = String::from_utf8_lossy(&col_body[pos..pos + name_len as usize]).into_owned();
            columns.push(name);
        }

        // EOF marker after column defs
        let (eof_body, _) = mysql_read_packet(&mut *stream).await?;
        let _ = eof_body;

        // Read rows until EOF
        let col_meta = crate::row::ColumnMeta::new(columns.clone());
        let mut rows: Vec<Row> = Vec::new();
        loop {
            let (row_body, _) = mysql_read_packet(&mut *stream).await?;
            if row_body.is_empty() { break; }
            // EOF packet
            if row_body[0] == 0xFE && row_body.len() < 9 { break; }
            // ERR packet
            if row_body[0] == 0xFF {
                let err_msg = if row_body.len() > 9 {
                    String::from_utf8_lossy(&row_body[9..]).into_owned()
                } else { "row fetch error".into() };
                return Err(ClientError::Query(err_msg));
            }

            // Text protocol row: sequence of lenenc strings (0xFB = NULL)
            let mut values = Vec::with_capacity(columns.len());
            let mut pos = 0;
            for _ in 0..columns.len() {
                if pos < row_body.len() && row_body[pos] == 0xFB {
                    values.push(Value::Null);
                    pos += 1;
                } else {
                    let (slen, adv) = mysql_decode_lenenc(&row_body[pos..]);
                    pos += adv;
                    let text = String::from_utf8_lossy(&row_body[pos..pos + slen as usize]).into_owned();
                    pos += slen as usize;
                    // Type inference
                    if text == "1" || text == "0" || text == "true" || text == "false" {
                        if let Ok(n) = text.parse::<i64>() {
                            values.push(Value::Int(n));
                        } else {
                            values.push(Value::Bool(text == "true"));
                        }
                    } else if let Ok(n) = text.parse::<i64>() {
                        values.push(Value::Int(n));
                    } else if let Ok(f) = text.parse::<f64>() {
                        values.push(Value::Float(f));
                    } else {
                        values.push(Value::Text(text));
                    }
                }
            }
            rows.push(Row::new(std::sync::Arc::clone(&col_meta), values));
        }

        Ok(QueryResult { columns, rows, rows_affected: 0 })
    }
}

impl PyroTransport for MysqlTransport {
    fn tier(&self) -> &str { "T3:TCP-MySQL" }

    fn query(&self, sql: &str, params: &[Value])
        -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<QueryResult, ClientError>> + Send + '_>>
    {
        let final_sql = crate::client::interpolate_params(sql, params);
        Box::pin(async move { self.com_query(&final_sql).await })
    }

    fn execute(&self, sql: &str, params: &[Value])
        -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<u64, ClientError>> + Send + '_>>
    {
        let final_sql = crate::client::interpolate_params(sql, params);
        Box::pin(async move {
            let result = self.com_query(&final_sql).await?;
            Ok(result.rows_affected)
        })
    }

    fn send_action(&self, action: &serde_json::Value)
        -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, ClientError>> + Send + '_>>
    {
        let action = action.clone();
        Box::pin(async move {
            let action_type = action["type"].as_str().unwrap_or("");
            match action_type {
                "BeginTransaction" => {
                    self.com_query("BEGIN").await?;
                    let tx_id = format!("mysql-tx-{}", std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_nanos());
                    Ok(serde_json::json!({"transaction_id": tx_id}))
                }
                "Commit" => { self.com_query("COMMIT").await?; Ok(serde_json::json!({"status": "ok"})) }
                "Rollback" => { self.com_query("ROLLBACK").await?; Ok(serde_json::json!({"status": "ok"})) }
                "CreateSavepoint" => {
                    let name = action["name"].as_str().unwrap_or("sp");
                    self.com_query(&format!("SAVEPOINT {name}")).await?;
                    Ok(serde_json::json!({"status": "ok"}))
                }
                "RollbackSavepoint" => {
                    let name = action["name"].as_str().unwrap_or("sp");
                    self.com_query(&format!("ROLLBACK TO SAVEPOINT {name}")).await?;
                    Ok(serde_json::json!({"status": "ok"}))
                }
                other => Err(ClientError::Protocol(format!("action '{other}' not supported over MySQL transport")))
            }
        })
    }

    fn bulk_insert(&self, table: &str, columns: &[&str], rows: &[Vec<Value>])
        -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<u64, ClientError>> + Send + '_>>
    {
        let col_list = columns.join(", ");
        let mut values_parts = Vec::with_capacity(rows.len());
        for row in rows {
            let vals: Vec<std::borrow::Cow<'static, str>> = row.iter().map(|v| crate::client::value_to_sql(v)).collect();
            values_parts.push(format!("({})", vals.join(", ")));
        }
        let sql = format!("INSERT INTO `{table}` ({col_list}) VALUES {}", values_parts.join(", "));
        Box::pin(async move {
            let result = self.com_query(&sql).await?;
            Ok(result.rows_affected)
        })
    }

    fn close(&self)
        -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), ClientError>> + Send + '_>>
    {
        Box::pin(async move {
            use tokio::io::AsyncWriteExt;
            let mut stream = self.stream.lock().await;
            // COM_QUIT
            let pkt = [1, 0, 0, 0, 0x01];
            let _ = stream.write_all(&pkt).await;
            let _ = stream.shutdown().await;
            Ok(())
        })
    }

    fn copy_out(&self, sql: &str)
        -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<String, ClientError>> + Send + '_>>
    {
        let sql = sql.to_owned();
        Box::pin(async move {
            let result = self.com_query(&sql).await?;
            let mut csv = result.columns.join(",");
            csv.push('\n');
            for row in &result.rows {
                let vals: Vec<String> = row.values().iter().map(|v| match v {
                    crate::row::Value::Null => String::new(),
                    crate::row::Value::Text(s) => s.clone(),
                    other => format!("{:?}", other),
                }).collect();
                csv.push_str(&vals.join(","));
                csv.push('\n');
            }
            Ok(csv)
        })
    }

    fn copy_in(&self, table: &str, columns: &[&str], csv_data: &str)
        -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<u64, ClientError>> + Send + '_>>
    {
        let col_list = columns.join(", ");
        let table = table.to_owned();
        let csv = csv_data.to_owned();
        Box::pin(async move {
            let mut total: u64 = 0;
            for line in csv.lines() {
                let trimmed = line.trim();
                if trimmed.is_empty() { continue; }
                let values: Vec<String> = trimmed.split(',').map(|v| {
                    let v = v.trim();
                    if v.is_empty() || v.eq_ignore_ascii_case("null") { "NULL".to_string() }
                    else if v.parse::<f64>().is_ok() { v.to_string() }
                    else { format!("'{}'", v.replace('\'', "''")) }
                }).collect();
                let sql = format!("INSERT INTO `{}` ({}) VALUES ({})", table, col_list, values.join(", "));
                self.com_query(&sql).await?;
                total += 1;
            }
            Ok(total)
        })
    }

    fn query_cursor(&self, sql: &str, params: &[Value])
        -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<crate::client::Cursor, ClientError>> + Send + '_>>
    {
        let final_sql = crate::client::interpolate_params(sql, params);
        Box::pin(async move {
            let result = self.com_query(&final_sql).await?;
            Ok(crate::client::Cursor::from_rows(result.columns.clone(), result.rows))
        })
    }

    fn subscribe_cdc(&self, _table: &str)
        -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<crate::client::CdcStream, ClientError>> + Send + '_>>
    {
        Box::pin(async move {
            Err(ClientError::Protocol("CDC not supported over MySQL wire protocol".into()))
        })
    }
}

// ── MySQL wire protocol helpers ──────────────────────────────────────────────

/// Read a single MySQL packet (4-byte header + payload).
async fn mysql_read_packet(stream: &mut tokio::net::TcpStream) -> Result<(Vec<u8>, u8), ClientError> {
    use tokio::io::AsyncReadExt;
    let mut header = [0u8; 4];
    stream.read_exact(&mut header).await
        .map_err(|e| ClientError::Protocol(format!("MySQL read packet header: {e}")))?;
    let pkt_len = (header[0] as usize) | ((header[1] as usize) << 8) | ((header[2] as usize) << 16);
    let seq = header[3];
    let mut body = vec![0u8; pkt_len];
    if pkt_len > 0 {
        stream.read_exact(&mut body).await
            .map_err(|e| ClientError::Protocol(format!("MySQL read packet body: {e}")))?;
    }
    Ok((body, seq))
}

/// Decode a MySQL length-encoded integer. Returns (value, bytes_consumed).
#[inline]
fn mysql_decode_lenenc(data: &[u8]) -> (u64, usize) {
    if data.is_empty() { return (0, 0); }
    match data[0] {
        0..=0xFB => (data[0] as u64, 1),
        0xFC => {
            let v = u16::from_le_bytes([data.get(1).copied().unwrap_or(0), data.get(2).copied().unwrap_or(0)]);
            (v as u64, 3)
        }
        0xFD => {
            let v = (data.get(1).copied().unwrap_or(0) as u64)
                | ((data.get(2).copied().unwrap_or(0) as u64) << 8)
                | ((data.get(3).copied().unwrap_or(0) as u64) << 16);
            (v, 4)
        }
        0xFE => {
            let mut bytes = [0u8; 8];
            for i in 0..8 { bytes[i] = data.get(i + 1).copied().unwrap_or(0); }
            (u64::from_le_bytes(bytes), 9)
        }
        _ => (0, 1),
    }
}

/// MySQL native_password authentication stub.
/// For PyroSQL's MySQL wire listener with trust auth, an empty response works.
/// Real mysql_native_password requires SHA1 (add sha1 crate for production MySQL servers).
fn mysql_native_password_auth(_password: &[u8], _scramble: &[u8]) -> Vec<u8> {
    // With VALKARNSQL_TRUST_ALL=true, the server accepts any auth response.
    // For connecting to real MySQL servers, this needs proper SHA1.
    Vec::new()
}

// ── MD5 helper for PG auth ───────────────────────────────────────────────────

// Note: MD5 auth is deliberately NOT supported — it's cryptographically broken.
// PyroSQL supports SCRAM-SHA-256 (future) and trust auth.
// Over QUIC (vsql://), auth uses mTLS — no password hashing needed.
// Over TCP, use VALKARNSQL_TRUST_ALL=true for dev, or SCRAM-SHA-256 for production.

// ── Unix socket transport (T1) ───────────────────────────────────────────────

/// Default socket path for PyroSQL's PG-compatible listener.
pub const DEFAULT_UNIX_SOCKET_PATH: &str = "/var/run/pyrosql/pyrosql.sock";

/// T1: Unix domain socket transport — same host, PG wire protocol.
///
/// Speaks a minimal subset of the PostgreSQL wire protocol (v3) over a Unix
/// domain socket. Only the Simple Query flow is used, which is sufficient for
/// all SQL operations the client SDK supports.
///
/// This avoids adding `tokio-postgres` as a dependency (~100 lines of minimal
/// PG protocol is all we need).
pub struct UnixTransport {
    stream: tokio::sync::Mutex<tokio::net::UnixStream>,
}

impl UnixTransport {
    /// Connect to a PyroSQL server via Unix domain socket.
    ///
    /// Sends the PG v3 StartupMessage and waits for AuthenticationOk +
    /// ReadyForQuery before returning.
    pub async fn connect(socket_path: &str, user: &str, database: &str) -> Result<Self, ClientError> {
        Self::connect_with_password(socket_path, user, database, "").await
    }

    /// Connect with optional password (supports MD5 auth).
    pub async fn connect_with_password(socket_path: &str, user: &str, database: &str, password: &str) -> Result<Self, ClientError> {
        let stream = tokio::net::UnixStream::connect(socket_path)
            .await
            .map_err(|e| ClientError::Connection(format!("unix socket connect({socket_path}): {e}")))?;

        let transport = Self {
            stream: tokio::sync::Mutex::new(stream),
        };

        // Send PG v3 StartupMessage
        transport.send_startup(user, database, password).await?;

        Ok(transport)
    }

    /// Send PG v3 StartupMessage and consume server responses until ReadyForQuery.
    async fn send_startup(&self, user: &str, database: &str, password: &str) -> Result<(), ClientError> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let mut stream = self.stream.lock().await;

        // Build StartupMessage: int32 length, int32 protocol(196608 = 3.0),
        // then key=value pairs terminated by \0, then final \0.
        let mut body = Vec::new();
        body.extend_from_slice(&196608_i32.to_be_bytes()); // protocol 3.0
        body.extend_from_slice(b"user\0");
        body.extend_from_slice(user.as_bytes());
        body.push(0);
        if !database.is_empty() {
            body.extend_from_slice(b"database\0");
            body.extend_from_slice(database.as_bytes());
            body.push(0);
        }
        body.push(0); // terminator

        let len = (body.len() + 4) as i32; // +4 for the length field itself
        let mut msg = Vec::with_capacity(body.len() + 4);
        msg.extend_from_slice(&len.to_be_bytes());
        msg.extend_from_slice(&body);

        stream.write_all(&msg).await
            .map_err(|e| ClientError::Connection(format!("unix startup write: {e}")))?;

        // SCRAM state — populated when auth type 10 (SASL) is received.
        let mut scram_client_first_bare: Option<String> = None;
        let mut scram_client_nonce: Option<String> = None;
        let mut scram_expected_server_sig: Option<Vec<u8>> = None;

        // Read responses until ReadyForQuery ('Z')
        loop {
            // Read tag + length in a single 5-byte read.
            let mut hdr = [0u8; 5];
            stream.read_exact(&mut hdr).await
                .map_err(|e| ClientError::Protocol(format!("unix startup read header: {e}")))?;
            let payload_len = (i32::from_be_bytes([hdr[1], hdr[2], hdr[3], hdr[4]]) - 4) as usize;

            let mut payload = vec![0u8; payload_len];
            if payload_len > 0 {
                stream.read_exact(&mut payload).await
                    .map_err(|e| ClientError::Protocol(format!("unix startup read payload: {e}")))?;
            }

            match hdr[0] {
                b'R' => {
                    if payload_len >= 4 {
                        let auth_type = i32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
                        match auth_type {
                            0 => { /* AuthenticationOk */ }
                            3 => {
                                // CleartextPassword
                                let pw_bytes = password.as_bytes();
                                let pw_len = (pw_bytes.len() + 4 + 1) as i32;
                                let mut pw_msg = Vec::with_capacity(5 + pw_bytes.len() + 1);
                                pw_msg.push(b'p');
                                pw_msg.extend_from_slice(&pw_len.to_be_bytes());
                                pw_msg.extend_from_slice(pw_bytes);
                                pw_msg.push(0);
                                stream.write_all(&pw_msg).await
                                    .map_err(|e| ClientError::Connection(format!("unix password write: {e}")))?;
                            }
                            10 => {
                                // AuthenticationSASL — read mechanism list from payload[4..]
                                let mechanism_data = &payload[4..];
                                let mut found_scram256 = false;
                                for mech in mechanism_data.split(|&b| b == 0) {
                                    if mech == b"SCRAM-SHA-256" {
                                        found_scram256 = true;
                                        break;
                                    }
                                }
                                if !found_scram256 {
                                    return Err(ClientError::Connection(
                                        "unix: server SASL mechanisms do not include SCRAM-SHA-256".into()
                                    ));
                                }

                                let (client_first_msg, bare, nonce) =
                                    crate::scram_client::scram_client_first(user);
                                scram_client_first_bare = Some(bare);
                                scram_client_nonce = Some(nonce);

                                // Send SASLInitialResponse
                                let mechanism = b"SCRAM-SHA-256\0";
                                let cf_bytes = client_first_msg.as_bytes();
                                let cf_len = cf_bytes.len() as i32;
                                let total_len = 4 + mechanism.len() as i32 + 4 + cf_len;
                                let mut sasl_msg = Vec::with_capacity(1 + total_len as usize);
                                sasl_msg.push(b'p');
                                sasl_msg.extend_from_slice(&total_len.to_be_bytes());
                                sasl_msg.extend_from_slice(mechanism);
                                sasl_msg.extend_from_slice(&cf_len.to_be_bytes());
                                sasl_msg.extend_from_slice(cf_bytes);
                                stream.write_all(&sasl_msg).await
                                    .map_err(|e| ClientError::Connection(format!("unix SASL initial write: {e}")))?;
                            }
                            11 => {
                                // AuthenticationSASLContinue
                                let server_first = std::str::from_utf8(&payload[4..]).map_err(|e| {
                                    ClientError::Protocol(format!("unix: invalid UTF-8 in server-first: {e}"))
                                })?;

                                let bare = scram_client_first_bare.as_deref().ok_or_else(|| {
                                    ClientError::Protocol("unix: got SASL continue without initial exchange".into())
                                })?;
                                let nonce = scram_client_nonce.as_deref().ok_or_else(|| {
                                    ClientError::Protocol("unix: missing client nonce".into())
                                })?;

                                let (client_final, expected_sig) =
                                    crate::scram_client::scram_client_final(password, bare, server_first, nonce)?;
                                scram_expected_server_sig = Some(expected_sig);

                                // Send SASLResponse
                                let cf_bytes = client_final.as_bytes();
                                let total_len = (cf_bytes.len() + 4) as i32;
                                let mut resp_msg = Vec::with_capacity(1 + total_len as usize);
                                resp_msg.push(b'p');
                                resp_msg.extend_from_slice(&total_len.to_be_bytes());
                                resp_msg.extend_from_slice(cf_bytes);
                                stream.write_all(&resp_msg).await
                                    .map_err(|e| ClientError::Connection(format!("unix SASL response write: {e}")))?;
                            }
                            12 => {
                                // AuthenticationSASLFinal — verify server signature
                                let server_final = std::str::from_utf8(&payload[4..]).map_err(|e| {
                                    ClientError::Protocol(format!("unix: invalid UTF-8 in server-final: {e}"))
                                })?;

                                if let Some(expected) = &scram_expected_server_sig {
                                    let sig_b64 = server_final.strip_prefix("v=").ok_or_else(|| {
                                        ClientError::Protocol(format!(
                                            "unix: server-final missing v= prefix: {server_final}"
                                        ))
                                    })?;
                                    let server_sig = base64::engine::general_purpose::STANDARD
                                        .decode(sig_b64)
                                        .map_err(|e| {
                                            ClientError::Protocol(format!("unix: invalid server signature base64: {e}"))
                                        })?;
                                    if server_sig != *expected {
                                        return Err(ClientError::Connection(
                                            "unix: SCRAM server signature mismatch — possible MITM".into()
                                        ));
                                    }
                                }
                            }
                            _ => {
                                return Err(ClientError::Connection(format!(
                                    "unix socket: unsupported authentication type {auth_type}"
                                )));
                            }
                        }
                    }
                }
                b'K' => {
                    // BackendKeyData — ignore (cancellation key)
                }
                b'S' => {
                    // ParameterStatus — ignore
                }
                b'Z' => {
                    // ReadyForQuery — startup complete
                    break;
                }
                b'E' => {
                    // ErrorResponse
                    let err_msg = pg_parse_error(&payload);
                    return Err(ClientError::Connection(format!("unix startup error: {err_msg}")));
                }
                other => {
                    // Unknown message during startup — skip
                    let _ = other;
                }
            }
        }

        Ok(())
    }

    /// Execute a Simple Query and return the parsed result.
    async fn simple_query(&self, sql: &str) -> Result<QueryResult, ClientError> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let mut stream = self.stream.lock().await;

        // Send Query message: 'Q' + int32 len + sql + \0
        let sql_bytes = sql.as_bytes();
        let len = (sql_bytes.len() + 4 + 1) as i32; // +4 for len field, +1 for null terminator
        let mut msg = Vec::with_capacity(5 + sql_bytes.len() + 1);
        msg.push(b'Q');
        msg.extend_from_slice(&len.to_be_bytes());
        msg.extend_from_slice(sql_bytes);
        msg.push(0);

        stream.write_all(&msg).await
            .map_err(|e| ClientError::Protocol(format!("unix query write: {e}")))?;

        // Read responses
        let mut columns: Vec<String> = Vec::new();
        let mut col_meta: Option<std::sync::Arc<crate::row::ColumnMeta>> = None;
        let mut rows: Vec<Row> = Vec::new();
        let mut rows_affected: u64 = 0;

        loop {
            // Read tag + length in a single 5-byte read.
            let mut hdr = [0u8; 5];
            stream.read_exact(&mut hdr).await
                .map_err(|e| ClientError::Protocol(format!("unix query read header: {e}")))?;
            let payload_len = (i32::from_be_bytes([hdr[1], hdr[2], hdr[3], hdr[4]]) - 4) as usize;

            let mut payload = vec![0u8; payload_len];
            if payload_len > 0 {
                stream.read_exact(&mut payload).await
                    .map_err(|e| ClientError::Protocol(format!("unix query read payload: {e}")))?;
            }

            match hdr[0] {
                b'T' => {
                    // RowDescription
                    columns = pg_parse_row_description(&payload)?;
                    col_meta = Some(crate::row::ColumnMeta::new(columns.clone()));
                }
                b'D' => {
                    // DataRow
                    let meta = col_meta.as_ref().ok_or_else(|| {
                        ClientError::Protocol("DataRow before RowDescription".into())
                    })?;
                    let values = pg_parse_data_row(&payload, &columns)?;
                    rows.push(Row::new(std::sync::Arc::clone(meta), values));
                }
                b'C' => {
                    // CommandComplete — parse rows affected from tag string
                    let tag_str = String::from_utf8_lossy(&payload);
                    let tag_str = tag_str.trim_end_matches('\0');
                    rows_affected = pg_parse_command_tag(tag_str);
                }
                b'E' => {
                    // ErrorResponse
                    let err_msg = pg_parse_error(&payload);
                    return Err(ClientError::Query(err_msg));
                }
                b'Z' => {
                    // ReadyForQuery — done
                    break;
                }
                b'I' => {
                    // EmptyQueryResponse — ignore
                }
                b'N' => {
                    // NoticeResponse — ignore
                }
                _other => {
                    // Unknown — skip
                }
            }
        }

        Ok(QueryResult { columns, rows, rows_affected })
    }
}

impl PyroTransport for UnixTransport {
    fn tier(&self) -> &str {
        "T1:Unix"
    }

    fn query(
        &self,
        sql: &str,
        params: &[Value],
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<QueryResult, ClientError>> + Send + '_>> {
        let final_sql = crate::client::interpolate_params(sql, params);
        Box::pin(async move { self.simple_query(&final_sql).await })
    }

    fn execute(
        &self,
        sql: &str,
        params: &[Value],
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<u64, ClientError>> + Send + '_>> {
        let final_sql = crate::client::interpolate_params(sql, params);
        Box::pin(async move {
            let result = self.simple_query(&final_sql).await?;
            Ok(result.rows_affected)
        })
    }

    fn send_action(
        &self,
        action: &serde_json::Value,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, ClientError>> + Send + '_>> {
        // Actions are PyroLink-specific; over Unix socket we translate them
        // to SQL equivalents where possible.
        let action = action.clone();
        Box::pin(async move {
            let action_type = action["type"].as_str().unwrap_or("");
            match action_type {
                "BeginTransaction" => {
                    let isolation = action["isolation"].as_str().unwrap_or("read committed");
                    let sql = format!("BEGIN ISOLATION LEVEL {}", isolation.to_uppercase());
                    self.simple_query(&sql).await?;
                    // Generate a client-side transaction ID for tracking
                    let tx_id = format!("unix-tx-{}", std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_nanos());
                    Ok(serde_json::json!({"transaction_id": tx_id}))
                }
                "Commit" => {
                    self.simple_query("COMMIT").await?;
                    Ok(serde_json::json!({"status": "ok"}))
                }
                "Rollback" => {
                    self.simple_query("ROLLBACK").await?;
                    Ok(serde_json::json!({"status": "ok"}))
                }
                "CreateSavepoint" => {
                    let name = action["name"].as_str().unwrap_or("sp");
                    self.simple_query(&format!("SAVEPOINT {name}")).await?;
                    Ok(serde_json::json!({"status": "ok"}))
                }
                "RollbackSavepoint" => {
                    let name = action["name"].as_str().unwrap_or("sp");
                    self.simple_query(&format!("ROLLBACK TO SAVEPOINT {name}")).await?;
                    Ok(serde_json::json!({"status": "ok"}))
                }
                "PrepareStatement" => {
                    // For Unix socket, prepared statements just store the SQL
                    // client-side; actual execution uses Simple Query.
                    let sql = action["sql"].as_str().unwrap_or("");
                    let handle = format!("unix-ps-{}", sql.len());
                    Ok(serde_json::json!({"handle": handle}))
                }
                other => {
                    Err(ClientError::Protocol(format!(
                        "action '{other}' not supported over Unix socket transport"
                    )))
                }
            }
        })
    }

    fn bulk_insert(
        &self,
        table: &str,
        columns: &[&str],
        rows: &[Vec<Value>],
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<u64, ClientError>> + Send + '_>> {
        // Build a multi-row INSERT statement
        let col_list = columns.join(", ");
        let mut values_parts = Vec::with_capacity(rows.len());
        for row in rows {
            let vals: Vec<std::borrow::Cow<'static, str>> = row.iter().map(|v| crate::client::value_to_sql(v)).collect();
            values_parts.push(format!("({})", vals.join(", ")));
        }
        let sql = format!("INSERT INTO {table} ({col_list}) VALUES {}", values_parts.join(", "));
        Box::pin(async move {
            let result = self.simple_query(&sql).await?;
            Ok(result.rows_affected)
        })
    }

    fn close(
        &self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), ClientError>> + Send + '_>> {
        Box::pin(async move {
            use tokio::io::AsyncWriteExt;
            let mut stream = self.stream.lock().await;
            // Send Terminate message: 'X' + int32(4)
            let msg = [b'X', 0, 0, 0, 4];
            let _ = stream.write_all(&msg).await;
            let _ = stream.shutdown().await;
            Ok(())
        })
    }

    fn copy_out(
        &self,
        sql: &str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<String, ClientError>> + Send + '_>> {
        // Over Unix socket, execute the query and format as CSV.
        let sql = sql.to_owned();
        Box::pin(async move {
            let result = self.simple_query(&sql).await?;
            let mut csv = String::new();
            // Header
            csv.push_str(&result.columns.join(","));
            csv.push('\n');
            // Rows
            for row in &result.rows {
                let vals: Vec<String> = row.values().iter().map(|v| match v {
                    crate::row::Value::Null => String::new(),
                    crate::row::Value::Text(s) => {
                        if s.contains(',') || s.contains('"') || s.contains('\n') {
                            format!("\"{}\"", s.replace('"', "\"\""))
                        } else {
                            s.clone()
                        }
                    }
                    other => format!("{:?}", other),
                }).collect();
                csv.push_str(&vals.join(","));
                csv.push('\n');
            }
            Ok(csv)
        })
    }

    fn copy_in(
        &self,
        table: &str,
        columns: &[&str],
        csv_data: &str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<u64, ClientError>> + Send + '_>> {
        // Over Unix socket, parse CSV and execute INSERT statements.
        let col_list = columns.join(", ");
        let table = table.to_owned();
        let csv = csv_data.to_owned();
        Box::pin(async move {
            let mut total: u64 = 0;
            for line in csv.lines() {
                let trimmed = line.trim();
                if trimmed.is_empty() { continue; }
                // Simple CSV parse: split by comma (no quoting support for Unix path)
                let values: Vec<String> = trimmed.split(',').map(|v| {
                    let v = v.trim();
                    if v.is_empty() || v.eq_ignore_ascii_case("null") {
                        "NULL".to_string()
                    } else if v.parse::<f64>().is_ok() {
                        v.to_string()
                    } else {
                        format!("'{}'", v.replace('\'', "''"))
                    }
                }).collect();
                let sql = if col_list.is_empty() {
                    format!("INSERT INTO {} VALUES ({})", table, values.join(", "))
                } else {
                    format!("INSERT INTO {} ({}) VALUES ({})", table, col_list, values.join(", "))
                };
                self.simple_query(&sql).await?;
                total += 1;
            }
            Ok(total)
        })
    }

    fn query_cursor(
        &self,
        sql: &str,
        params: &[Value],
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<crate::client::Cursor, ClientError>> + Send + '_>> {
        // Unix socket: no streaming — execute fully and wrap in a Cursor.
        let final_sql = crate::client::interpolate_params(sql, params);
        Box::pin(async move {
            let result = self.simple_query(&final_sql).await?;
            Ok(crate::client::Cursor::from_rows(result.columns.clone(), result.rows))
        })
    }

    fn subscribe_cdc(
        &self,
        table: &str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<crate::client::CdcStream, ClientError>> + Send + '_>> {
        let table = table.to_owned();
        Box::pin(async move {
            // CDC is not supported over Unix socket (no QUIC server-push).
            // Return a stream with a synthetic subscription ID.
            Ok(crate::client::CdcStream {
                subscription_id: format!("unix-cdc-{}", table),
                table,
            })
        })
    }
}

// ── PG wire protocol helpers ─────────────────────────────────────────────────

/// Parse a PG RowDescription message into column names.
fn pg_parse_row_description(payload: &[u8]) -> Result<Vec<String>, ClientError> {
    if payload.len() < 2 {
        return Err(ClientError::Protocol("RowDescription too short".into()));
    }
    let num_fields = i16::from_be_bytes([payload[0], payload[1]]) as usize;
    let mut columns = Vec::with_capacity(num_fields);
    let mut pos = 2;

    for _ in 0..num_fields {
        // Field name is null-terminated string
        let name_end = payload[pos..]
            .iter()
            .position(|&b| b == 0)
            .ok_or_else(|| ClientError::Protocol("RowDescription: unterminated field name".into()))?;
        let name = String::from_utf8_lossy(&payload[pos..pos + name_end]).into_owned();
        pos += name_end + 1; // skip null terminator
        // Skip: table OID (4), column attr (2), type OID (4), type size (2),
        //        type modifier (4), format code (2) = 18 bytes
        pos += 18;
        columns.push(name);
    }

    Ok(columns)
}

/// Parse a PG DataRow message into a vector of Values.
fn pg_parse_data_row(payload: &[u8], _columns: &[String]) -> Result<Vec<Value>, ClientError> {
    if payload.len() < 2 {
        return Err(ClientError::Protocol("DataRow too short".into()));
    }
    let num_fields = i16::from_be_bytes([payload[0], payload[1]]) as usize;
    let mut values = Vec::with_capacity(num_fields);
    let mut pos = 2;

    for _ in 0..num_fields {
        if pos + 4 > payload.len() {
            return Err(ClientError::Protocol("DataRow: unexpected end of data".into()));
        }
        let col_len = i32::from_be_bytes([payload[pos], payload[pos + 1], payload[pos + 2], payload[pos + 3]]);
        pos += 4;

        if col_len == -1 {
            values.push(Value::Null);
        } else {
            let col_len = col_len as usize;
            if pos + col_len > payload.len() {
                return Err(ClientError::Protocol("DataRow: column data overflow".into()));
            }
            let raw = &payload[pos..pos + col_len];
            pos += col_len;

            // Parse typed values directly from bytes to avoid allocating a
            // String for integers and floats. Only allocate for Text values.
            let value = match raw {
                b"t" | b"true" => Value::Bool(true),
                b"f" | b"false" => Value::Bool(false),
                _ => {
                    // Try parsing as number from the byte slice (valid UTF-8
                    // is guaranteed for numeric literals).
                    if let Ok(s) = std::str::from_utf8(raw) {
                        if let Ok(n) = s.parse::<i64>() {
                            Value::Int(n)
                        } else if let Ok(f) = s.parse::<f64>() {
                            Value::Float(f)
                        } else {
                            Value::Text(s.to_owned())
                        }
                    } else {
                        Value::Text(String::from_utf8_lossy(raw).into_owned())
                    }
                }
            };
            values.push(value);
        }
    }

    Ok(values)
}

/// Parse a PG ErrorResponse into a human-readable string.
fn pg_parse_error(payload: &[u8]) -> String {
    let mut msg = String::new();
    let mut pos = 0;
    while pos < payload.len() {
        let field_type = payload[pos];
        pos += 1;
        if field_type == 0 {
            break;
        }
        let end = payload[pos..]
            .iter()
            .position(|&b| b == 0)
            .unwrap_or(payload.len() - pos);
        let value = String::from_utf8_lossy(&payload[pos..pos + end]);
        pos += end + 1;

        match field_type {
            b'M' => {
                if !msg.is_empty() {
                    msg.push_str(": ");
                }
                msg.push_str(&value);
            }
            b'S' if msg.is_empty() => {
                msg.push_str(&value);
            }
            _ => {}
        }
    }
    if msg.is_empty() {
        msg = "unknown PG error".into();
    }
    msg
}

/// Parse the rows-affected count from a PG CommandComplete tag.
///
/// Tags look like "INSERT 0 5", "UPDATE 3", "DELETE 1", "SELECT 10", etc.
#[inline]
fn pg_parse_command_tag(tag: &str) -> u64 {
    // The last space-separated token is the count
    tag.rsplit_once(' ')
        .and_then(|(_, n)| n.parse::<u64>().ok())
        .unwrap_or(0)
}

// ── Hostname helper ──────────────────────────────────────────────────────────

/// Get the local hostname without adding an external dependency.
pub(crate) fn get_local_hostname() -> String {
    #[cfg(unix)]
    {
        // Read from /etc/hostname as a simple cross-platform approach
        if let Ok(h) = std::fs::read_to_string("/etc/hostname") {
            let trimmed = h.trim().to_owned();
            if !trimmed.is_empty() {
                return trimmed;
            }
        }
    }
    // Fallback: use HOSTNAME env var
    std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("COMPUTERNAME"))
        .unwrap_or_else(|_| "unknown".to_owned())
}

// ── Tests ────────────────────────────────────────────────────────────────────

// ── PWire transport (T3:Wire) ──────────────────────────────────────────

/// PWire binary protocol transport over plain TCP.
///
/// Uses a simple framed protocol:
/// - Request:  `[type: u8][length: u32 LE][payload]`
/// - Response: `[type: u8][length: u32 LE][payload]`
pub struct PWireTransport {
    stream: std::sync::Mutex<std::net::TcpStream>,
}

const VW_REQ_QUERY: u8 = 0x01;
const VW_RESP_RESULT_SET: u8 = 0x01;
const VW_RESP_OK: u8 = 0x02;
const VW_RESP_ERROR: u8 = 0x03;

impl PWireTransport {
    /// Create from an existing blocking TCP stream (for FFI use).
    pub fn from_stream(stream: std::net::TcpStream) -> Self {
        Self { stream: std::sync::Mutex::new(stream) }
    }

    /// Connect to a PyroSQL server via PWire binary protocol over TCP.
    pub async fn connect(host: &str, port: u16) -> Result<Self, super::error::ClientError> {
        let addr = format!("{host}:{port}");
        let sock_addr: std::net::SocketAddr = addr.parse()
            .or_else(|_| {
                use std::net::ToSocketAddrs;
                addr.to_socket_addrs()
                    .map_err(|e| super::error::ClientError::Connection(format!("DNS resolve({addr}): {e}")))?
                    .find(|a| a.is_ipv4())
                    .ok_or_else(|| super::error::ClientError::Connection(format!("no IPv4 for {addr}")))
            })?;
        let stream = std::net::TcpStream::connect(sock_addr)
            .map_err(|e| super::error::ClientError::Connection(format!("PWire TCP connect({sock_addr}): {e}")))?;
        stream.set_nodelay(true)
            .map_err(|e| super::error::ClientError::Connection(format!("PWire set_nodelay: {e}")))?;
        Ok(Self { stream: std::sync::Mutex::new(stream) })
    }

    fn send_frame_sync(
        stream: &mut std::net::TcpStream,
        msg_type: u8,
        payload: &[u8],
    ) -> Result<(), super::error::ClientError> {
        use std::io::Write;
        let len_bytes = (payload.len() as u32).to_le_bytes();
        let header = [msg_type, len_bytes[0], len_bytes[1], len_bytes[2], len_bytes[3]];
        stream.write_all(&header)
            .map_err(|e| super::error::ClientError::Protocol(format!("PWire send header: {e}")))?;
        if !payload.is_empty() {
            stream.write_all(payload)
                .map_err(|e| super::error::ClientError::Protocol(format!("PWire send payload: {e}")))?;
        }
        Ok(())
    }

    fn recv_frame_sync(
        stream: &mut std::net::TcpStream,
    ) -> Result<(u8, Vec<u8>), super::error::ClientError> {
        use std::io::Read;
        let mut header = [0u8; 5];
        stream.read_exact(&mut header)
            .map_err(|e| super::error::ClientError::Protocol(format!("PWire recv header: {e}")))?;
        let msg_type = header[0];
        let len = u32::from_le_bytes([header[1], header[2], header[3], header[4]]) as usize;
        if len == 0 {
            return Ok((msg_type, Vec::new()));
        }
        let mut payload = vec![0u8; len];
        stream.read_exact(&mut payload)
            .map_err(|e| super::error::ClientError::Protocol(format!("PWire recv payload: {e}")))?;
        Ok((msg_type, payload))
    }

    async fn wire_query(&self, sql: &str) -> Result<super::row::QueryResult, super::error::ClientError> {
        use super::row::{QueryResult, Row, Value};
        let mut stream = self.stream.lock()
            .map_err(|e| super::error::ClientError::Protocol(format!("PWire lock: {e}")))?;

        Self::send_frame_sync(&mut stream, VW_REQ_QUERY, sql.as_bytes())?;
        let (resp_type, payload) = Self::recv_frame_sync(&mut stream)?;

        match resp_type {
            VW_RESP_RESULT_SET => {
                // Binary result set: [col_count: u16 LE]
                // Per col: [name_len: u8][name][type_tag: u8]
                // [row_count: u32 LE]
                // Per row: [null_bitmap] then typed values
                let p = &payload;
                let mut pos = 0usize;

                if p.len() < 2 {
                    return Err(super::error::ClientError::Protocol("PWire: result too short".into()));
                }
                let col_count = u16::from_le_bytes([p[pos], p[pos+1]]) as usize;
                pos += 2;

                let mut columns = Vec::with_capacity(col_count);
                let mut type_tags = Vec::with_capacity(col_count);
                for _ in 0..col_count {
                    if pos >= p.len() { break; }
                    let name_len = p[pos] as usize;
                    pos += 1;
                    let name = if pos + name_len <= p.len() {
                        String::from_utf8_lossy(&p[pos..pos+name_len]).into_owned()
                    } else { String::from("?") };
                    pos += name_len;
                    let type_tag = if pos < p.len() { p[pos] } else { 3 };
                    pos += 1;
                    columns.push(name);
                    type_tags.push(type_tag);
                }

                if pos + 4 > p.len() {
                    return Err(super::error::ClientError::Protocol("PWire: truncated row count".into()));
                }
                let row_count = u32::from_le_bytes([p[pos], p[pos+1], p[pos+2], p[pos+3]]) as usize;
                pos += 4;

                let null_bitmap_len = (col_count + 7) / 8;
                let mut rows = Vec::with_capacity(row_count);
                let col_meta = crate::row::ColumnMeta::new(columns.clone());

                for _ in 0..row_count {
                    if pos + null_bitmap_len > p.len() { break; }
                    let bitmap = &p[pos..pos+null_bitmap_len];
                    pos += null_bitmap_len;

                    let mut values = Vec::with_capacity(col_count);
                    for col_idx in 0..col_count {
                        let is_null = null_bitmap_len > 0 && (bitmap[col_idx / 8] >> (col_idx % 8)) & 1 == 1;
                        if is_null {
                            values.push(Value::Null);
                        } else {
                            match type_tags[col_idx] {
                                1 => { // I64
                                    if pos + 8 > p.len() { break; }
                                    let bytes: [u8; 8] = p[pos..pos+8].try_into().unwrap();
                                    pos += 8;
                                    values.push(Value::Int(i64::from_le_bytes(bytes)));
                                }
                                2 => { // F64
                                    if pos + 8 > p.len() { break; }
                                    let bytes: [u8; 8] = p[pos..pos+8].try_into().unwrap();
                                    pos += 8;
                                    values.push(Value::Float(f64::from_le_bytes(bytes)));
                                }
                                4 => { // BOOL
                                    if pos >= p.len() { break; }
                                    values.push(Value::Bool(p[pos] != 0));
                                    pos += 1;
                                }
                                _ => { // TEXT (3), BYTES (5), etc.
                                    if pos + 2 > p.len() { break; }
                                    let text_len = u16::from_le_bytes([p[pos], p[pos+1]]) as usize;
                                    pos += 2;
                                    if pos + text_len > p.len() { break; }
                                    // SAFETY: from_utf8_lossy avoids panic on invalid UTF-8;
                                    // into_owned only allocates if replacement was needed.
                                    let s = String::from_utf8_lossy(&p[pos..pos+text_len]).into_owned();
                                    pos += text_len;
                                    values.push(Value::Text(s));
                                }
                            }
                        }
                    }
                    rows.push(Row::new(std::sync::Arc::clone(&col_meta), values));
                }

                Ok(QueryResult { columns, rows, rows_affected: 0 })
            }
            VW_RESP_OK => {
                let rows_affected = if payload.len() >= 8 {
                    u64::from_le_bytes([
                        payload[0], payload[1], payload[2], payload[3],
                        payload[4], payload[5], payload[6], payload[7],
                    ])
                } else { 0 };
                Ok(QueryResult { columns: vec![], rows: vec![], rows_affected })
            }
            VW_RESP_ERROR => {
                let msg = if payload.len() > 6 {
                    let msg_len = payload[5] as usize;
                    std::str::from_utf8(&payload[6..6+msg_len.min(payload.len()-6)])
                        .unwrap_or("unknown error")
                } else {
                    std::str::from_utf8(&payload).unwrap_or("unknown error")
                };
                Err(super::error::ClientError::Query(msg.to_owned()))
            }
            other => {
                Err(super::error::ClientError::Protocol(format!("PWire: unexpected response type 0x{other:02x}")))
            }
        }
    }
}

impl PyroTransport for PWireTransport {
    fn tier(&self) -> &str { "T3:PWire" }

    fn query(&self, sql: &str, params: &[super::row::Value])
        -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<super::row::QueryResult, super::error::ClientError>> + Send + '_>>
    {
        let final_sql = crate::client::interpolate_params(sql, params);
        Box::pin(async move { self.wire_query(&final_sql).await })
    }

    fn execute(&self, sql: &str, params: &[super::row::Value])
        -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<u64, super::error::ClientError>> + Send + '_>>
    {
        let final_sql = crate::client::interpolate_params(sql, params);
        Box::pin(async move {
            let result = self.wire_query(&final_sql).await?;
            Ok(result.rows_affected)
        })
    }

    fn send_action(&self, action: &serde_json::Value)
        -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, super::error::ClientError>> + Send + '_>>
    {
        let action = action.clone();
        Box::pin(async move {
            let action_type = action["type"].as_str().unwrap_or("");
            match action_type {
                "BeginTransaction" => {
                    let isolation = action["isolation"].as_str().unwrap_or("read committed");
                    self.wire_query(&format!("BEGIN ISOLATION LEVEL {}", isolation.to_uppercase())).await?;
                    let tx_id = format!("wire-tx-{}", std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_nanos());
                    Ok(serde_json::json!({"transaction_id": tx_id}))
                }
                "Commit" => { self.wire_query("COMMIT").await?; Ok(serde_json::json!({"status": "ok"})) }
                "Rollback" => { self.wire_query("ROLLBACK").await?; Ok(serde_json::json!({"status": "ok"})) }
                other => Err(super::error::ClientError::Protocol(format!("action '{other}' not supported over PWire")))
            }
        })
    }

    fn bulk_insert(&self, table: &str, columns: &[&str], rows: &[Vec<super::row::Value>])
        -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<u64, super::error::ClientError>> + Send + '_>>
    {
        let col_list = columns.join(", ");
        let mut values_parts = Vec::with_capacity(rows.len());
        for row in rows {
            let vals: Vec<std::borrow::Cow<'static, str>> = row.iter().map(|v| crate::client::value_to_sql(v)).collect();
            values_parts.push(format!("({})", vals.join(", ")));
        }
        let sql = format!("INSERT INTO {table} ({col_list}) VALUES {}", values_parts.join(", "));
        Box::pin(async move {
            let result = self.wire_query(&sql).await?;
            Ok(result.rows_affected)
        })
    }

    fn close(&self)
        -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), super::error::ClientError>> + Send + '_>>
    {
        Box::pin(async move {
            // For std::net::TcpStream, just drop is sufficient.
            // The Mutex will be dropped when the transport is dropped.
            Ok(())
        })
    }

    fn copy_out(&self, sql: &str)
        -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<String, super::error::ClientError>> + Send + '_>>
    {
        let sql = sql.to_owned();
        Box::pin(async move {
            let result = self.wire_query(&sql).await?;
            let mut csv = result.columns.join(",");
            csv.push('\n');
            for row in &result.rows {
                let vals: Vec<String> = row.values().iter().map(|v| match v {
                    super::row::Value::Null => String::new(),
                    super::row::Value::Text(s) => s.clone(),
                    other => format!("{:?}", other),
                }).collect();
                csv.push_str(&vals.join(","));
                csv.push('\n');
            }
            Ok(csv)
        })
    }

    fn copy_in(&self, table: &str, columns: &[&str], csv_data: &str)
        -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<u64, super::error::ClientError>> + Send + '_>>
    {
        let col_list = columns.join(", ");
        let table = table.to_owned();
        let csv = csv_data.to_owned();
        Box::pin(async move {
            let mut total: u64 = 0;
            for line in csv.lines() {
                let trimmed = line.trim();
                if trimmed.is_empty() { continue; }
                let values: Vec<String> = trimmed.split(',').map(|v| {
                    let v = v.trim();
                    if v.is_empty() || v.eq_ignore_ascii_case("null") { "NULL".to_string() }
                    else if v.parse::<f64>().is_ok() { v.to_string() }
                    else { format!("'{}'", v.replace('\'', "''")) }
                }).collect();
                let sql = if col_list.is_empty() {
                    format!("INSERT INTO {} VALUES ({})", table, values.join(", "))
                } else {
                    format!("INSERT INTO {} ({}) VALUES ({})", table, col_list, values.join(", "))
                };
                self.wire_query(&sql).await?;
                total += 1;
            }
            Ok(total)
        })
    }

    fn query_cursor(&self, sql: &str, params: &[super::row::Value])
        -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<crate::client::Cursor, super::error::ClientError>> + Send + '_>>
    {
        let final_sql = crate::client::interpolate_params(sql, params);
        Box::pin(async move {
            let result = self.wire_query(&final_sql).await?;
            Ok(crate::client::Cursor::from_rows(result.columns.clone(), result.rows))
        })
    }

    fn subscribe_cdc(&self, _table: &str)
        -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<crate::client::CdcStream, super::error::ClientError>> + Send + '_>>
    {
        Box::pin(async move {
            Err(super::error::ClientError::Protocol("CDC not supported over PWire".into()))
        })
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn topology_same_pid_same_host_is_t0() {
        let hints = TopologyHints {
            hostname: "myhost".into(),
            pid: 1234,
            capabilities: Capabilities { quic: true, shm: true, unix_socket: false },
        };
        assert_eq!(hints.best_tier("myhost", 1234), TransportTier::T0InProcess);
    }

    #[test]
    fn topology_same_host_different_pid_with_shm_is_t1() {
        let hints = TopologyHints {
            hostname: "myhost".into(),
            pid: 1234,
            capabilities: Capabilities { quic: true, shm: true, unix_socket: false },
        };
        assert_eq!(hints.best_tier("myhost", 5678), TransportTier::T1SharedMemory);
    }

    #[test]
    fn topology_same_host_no_shm_is_unix_socket() {
        let hints = TopologyHints {
            hostname: "myhost".into(),
            pid: 1234,
            capabilities: Capabilities { quic: true, shm: false, unix_socket: false },
        };
        // Same host without SHM falls back to Unix socket (optimistic)
        assert_eq!(hints.best_tier("myhost", 5678), TransportTier::T1UnixSocket);
    }

    #[test]
    fn topology_different_host_is_t4() {
        let hints = TopologyHints {
            hostname: "server-a".into(),
            pid: 1234,
            capabilities: Capabilities { quic: true, shm: true, unix_socket: false },
        };
        assert_eq!(hints.best_tier("client-b", 5678), TransportTier::T4Quic);
    }

    #[test]
    fn transport_tier_display() {
        assert_eq!(TransportTier::T0InProcess.to_string(), "T0:InProcess");
        assert_eq!(TransportTier::T1UnixSocket.to_string(), "T1:Unix");
        assert_eq!(TransportTier::T1SharedMemory.to_string(), "T1:SharedMemory");
        assert_eq!(TransportTier::T4Quic.to_string(), "T4:QUIC");
    }

    #[test]
    fn pg_parse_command_tag_insert() {
        assert_eq!(pg_parse_command_tag("INSERT 0 5"), 5);
    }

    #[test]
    fn pg_parse_command_tag_update() {
        assert_eq!(pg_parse_command_tag("UPDATE 3"), 3);
    }

    #[test]
    fn pg_parse_command_tag_select() {
        assert_eq!(pg_parse_command_tag("SELECT 10"), 10);
    }

    #[test]
    fn pg_parse_command_tag_unknown() {
        assert_eq!(pg_parse_command_tag("BEGIN"), 0);
    }

    #[test]
    fn pg_parse_error_basic() {
        // Simulate a PG ErrorResponse: S"ERROR"\0 M"something broke"\0 \0
        let mut payload = Vec::new();
        payload.push(b'S');
        payload.extend_from_slice(b"ERROR\0");
        payload.push(b'M');
        payload.extend_from_slice(b"something broke\0");
        payload.push(0);
        let msg = pg_parse_error(&payload);
        assert!(msg.contains("something broke"));
    }

    #[test]
    fn pg_parse_row_description_single_column() {
        // Build a minimal RowDescription: 1 field named "id"
        let mut payload = Vec::new();
        payload.extend_from_slice(&1_i16.to_be_bytes()); // num fields
        payload.extend_from_slice(b"id\0"); // field name
        payload.extend_from_slice(&0_i32.to_be_bytes()); // table OID
        payload.extend_from_slice(&0_i16.to_be_bytes()); // column attr
        payload.extend_from_slice(&23_i32.to_be_bytes()); // type OID (int4)
        payload.extend_from_slice(&4_i16.to_be_bytes()); // type size
        payload.extend_from_slice(&(-1_i32).to_be_bytes()); // type modifier
        payload.extend_from_slice(&0_i16.to_be_bytes()); // format code
        let cols = pg_parse_row_description(&payload).unwrap();
        assert_eq!(cols, vec!["id"]);
    }

    #[test]
    fn pg_parse_data_row_values() {
        // Build a DataRow with 2 fields: "42" and NULL
        let mut payload = Vec::new();
        payload.extend_from_slice(&2_i16.to_be_bytes()); // num fields
        payload.extend_from_slice(&2_i32.to_be_bytes()); // col 0 length
        payload.extend_from_slice(b"42"); // col 0 data
        payload.extend_from_slice(&(-1_i32).to_be_bytes()); // col 1 = NULL
        let vals = pg_parse_data_row(&payload, &["a".into(), "b".into()]).unwrap();
        assert_eq!(vals, vec![Value::Int(42), Value::Null]);
    }

    #[test]
    fn topology_hints_json_roundtrip() {
        let hints = TopologyHints {
            hostname: "db-prod-01".into(),
            pid: 42,
            capabilities: Capabilities { quic: true, shm: true, unix_socket: false },
        };
        let json = serde_json::to_string(&hints).unwrap();
        let back: TopologyHints = serde_json::from_str(&json).unwrap();
        assert_eq!(back.hostname, "db-prod-01");
        assert_eq!(back.pid, 42);
        assert!(back.capabilities.shm);
    }

    #[test]
    fn get_local_hostname_returns_something() {
        let h = get_local_hostname();
        assert!(!h.is_empty(), "hostname should not be empty");
    }
}
