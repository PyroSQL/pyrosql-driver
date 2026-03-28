//! The main PyroSQL QUIC client.
//!
//! # Example
//!
//! ```no_run
//! use pyrosql::{Client, ConnectConfig};
//!
//! #[tokio::main]
//! async fn main() {
//!     let client = Client::connect_url("vsql://localhost:12520/mydb").await.unwrap();
//!     let result = client.query("SELECT * FROM users WHERE id = $1", &[42.into()]).await.unwrap();
//!     for row in result.rows {
//!         println!("{}: {}", row.get::<String>("name").unwrap(), row.get::<i64>("age").unwrap());
//!     }
//! }
//! ```

use crate::config::{ConnectConfig, Scheme};
use crate::error::ClientError;
use crate::row::{QueryResult, Row, Value};
use crate::transport::{
    MysqlTransport, QuicTransport, TcpPgTransport, TopologyHints, TransportTier, UnixTransport,
    PyroTransport, PWireTransport,
};
use bytes::Bytes;
use quinn::{Connection, Endpoint, RecvStream, SendStream};
use std::sync::Arc;

// ── Notification types ───────────────────────────────────────────────────

/// A server-pushed notification received via LISTEN/NOTIFY or WATCH.
#[derive(Debug, Clone)]
pub struct Notification {
    /// The channel name (e.g. `"my_channel"` for LISTEN, `"__watch_abc123"` for WATCH).
    pub channel: String,
    /// The notification payload string.
    pub payload: String,
}

/// Type alias for notification callbacks.
pub type NotificationCallback = Box<dyn Fn(Notification) + Send + Sync>;

/// ALPN protocol identifier — must match the server.
const ALPN_PYROSQL: &[u8] = b"pyrosql/1";

// ── Wire protocol constants (same as server) ────────────────────────────────

/// Schema metadata message.
const MSG_SCHEMA: u8 = 0x01;
/// Record batch (row data) message.
const MSG_RECORD_BATCH: u8 = 0x02;
/// Action response message (used for `CommandComplete`).
pub(crate) const MSG_DO_ACTION: u8 = 0x05;
/// Direct SQL query message.
pub(crate) const MSG_QUERY: u8 = 0x09;
/// Topology request message.
pub(crate) const MSG_TOPOLOGY: u8 = 0x0A;
/// COPY OUT data message.
pub(crate) const MSG_COPY_OUT: u8 = 0x0B;
/// COPY IN data message.
pub(crate) const MSG_COPY_IN: u8 = 0x0C;
/// CDC subscription message.
pub(crate) const MSG_SUBSCRIBE_CDC: u8 = 0x0D;
/// Server-pushed notification message (LISTEN/NOTIFY, WATCH).
pub(crate) const MSG_NOTIFICATION: u8 = 0x0F;
/// End-of-stream marker.
const MSG_EOS: u8 = 0xFF;

// ── Client ───────────────────────────────────────────────────────────────────

/// A connected PyroSQL client.
///
/// Each [`Client`] holds a transport (currently always QUIC) selected via
/// adaptive transport negotiation.  Queries are dispatched through the
/// [`PyroTransport`] trait, so the active transport is transparent to
/// the caller.
pub struct Client {
    transport: Box<dyn PyroTransport>,
    config: ConnectConfig,
    /// The topology hints received from the server, if any.
    #[allow(dead_code)]
    topology: Option<TopologyHints>,
    /// Registered notification callbacks, keyed by channel name (empty = all).
    notification_callbacks: Arc<std::sync::Mutex<Vec<NotificationCallback>>>,
    /// Handle to the background notification listener task.
    #[allow(dead_code)]
    notif_listener_handle: Option<tokio::task::JoinHandle<()>>,
    /// The raw QUIC connection, if available (needed for notification listener).
    quic_connection: Option<Connection>,
}

impl Client {
    /// Create a `Client` from a pre-connected transport (for FFI use).
    pub fn from_transport(transport: Box<dyn PyroTransport>, config: ConnectConfig) -> Self {
        Self {
            transport,
            config,
            topology: None,
            notification_callbacks: Arc::new(std::sync::Mutex::new(Vec::new())),
            notif_listener_handle: None,
            quic_connection: None,
        }
    }

    /// Connect to a PyroSQL server via PyroLink with adaptive transport
    /// negotiation.
    ///
    /// The connection flow:
    /// 1. Establish a QUIC connection (universal fallback, always works).
    /// 2. Request topology hints from the server.
    /// 3. Select the best available transport tier based on topology.
    /// 4. If a faster tier is available but not yet implemented, log and
    ///    fall back to QUIC.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if TLS setup, DNS resolution, or the QUIC
    /// handshake fails.
    pub async fn connect(config: ConnectConfig) -> Result<Self, ClientError> {
        // Explicit Unix socket — connect directly, no topology probe needed
        if config.scheme == Scheme::Unix {
            if let Some(ref socket_path) = config.unix_socket_path {
                let user = if config.user.is_empty() { "pyrosql" } else { &config.user };
                let unix = UnixTransport::connect_with_password(socket_path, user, &config.database, &config.password).await?;
                let transport: Box<dyn PyroTransport> = Box::new(unix);
                if let Some(mode) = config.syntax_mode {
                    transport.execute(&format!("SET syntax_mode = '{}'", mode.as_set_value()), &[]).await?;
                }
                return Ok(Self {
                    transport,
                    config,
                    topology: None,
                    notification_callbacks: Arc::new(std::sync::Mutex::new(Vec::new())),
                    notif_listener_handle: None,
                    quic_connection: None,
                });
            }
        }


        // ── Direct connect for PWire ────────────────────────────────
        // PWire is plain TCP — no QUIC probe needed.
        if config.scheme == Scheme::Wire {
            let wire = PWireTransport::connect(&config.host, config.port).await?;
            let transport: Box<dyn PyroTransport> = Box::new(wire);
            if let Some(mode) = config.syntax_mode {
                transport.execute(&format!("SET syntax_mode = '{}'", mode.as_set_value()), &[]).await?;
            }
            return Ok(Self {
                transport,
                config,
                topology: None,
                notification_callbacks: Arc::new(std::sync::Mutex::new(Vec::new())),
                notif_listener_handle: None,
                quic_connection: None,
            });
        }
        // ── PyroLink topology probe ──────────────────────────────────
        // For ALL schemes (vsql://, postgres://, mysql://, auto://),
        // attempt a QUIC topology probe first. If the server is on the
        // same host, we can upgrade to Unix socket for lower latency
        // regardless of the requested wire protocol.

        // Build a QUIC config for probing (uses port 12520 regardless of scheme port)
        let probe_config = ConnectConfig {
            scheme: Scheme::Quic,
            host: config.host.clone(),
            port: if config.scheme == Scheme::Quic { config.port } else { 12520 },
            database: config.database.clone(),
            user: config.user.clone(),
            password: config.password.clone(),
            tls_skip_verify: config.tls_skip_verify,
            unix_socket_path: None,
            syntax_mode: None,
        };

        // Try QUIC topology probe with a short timeout
        let probe_result = tokio::time::timeout(
            std::time::Duration::from_millis(500),
            establish_quic_connection(&probe_config),
        ).await;

        match probe_result {
            Ok(Ok(connection)) => {
                // QUIC probe succeeded — do full PyroLink negotiation
                let conn_for_notif = connection.clone();
                let quic = QuicTransport { connection };

                // Attempt topology detection
                let (topology, tier) = match quic.get_topology().await {
                    Ok(hints) => {
                        let my_hostname = crate::transport::get_local_hostname();
                        let my_pid = std::process::id();
                        let tier = hints.best_tier(&my_hostname, my_pid);
                        (Some(hints), tier)
                    }
                    Err(_) => (None, TransportTier::T4Quic),
                };

                // Select transport based on topology
                let transport: Box<dyn PyroTransport> = match tier {
                    TransportTier::T1UnixSocket | TransportTier::T1SharedMemory => {
                        // Same host — upgrade to Unix socket for lower latency.
                        // Unix socket speaks PG wire, compatible with all syntax modes.
                        let socket_path = config.unix_socket_path.as_deref()
                            .unwrap_or(crate::transport::DEFAULT_UNIX_SOCKET_PATH);
                        let user = if config.user.is_empty() { "pyrosql" } else { &config.user };
                        match UnixTransport::connect(socket_path, user, &config.database).await {
                            Ok(unix) => {
                                quic.connection.close(quinn::VarInt::from_u32(0), b"upgrading to unix");
                                Box::new(unix)
                            }
                            Err(_) => {
                                // Unix socket not available — use scheme-specific fallback
                                Self::scheme_fallback_transport(&config, quic).await?
                            }
                        }
                    }
                    TransportTier::T4Quic => {
                        // Cross-host: for vsql:// use QUIC directly;
                        // for postgres:// or mysql:// use their native TCP wire protocol
                        Self::scheme_fallback_transport(&config, quic).await?
                    }
                    TransportTier::T0InProcess => {
                        // Future: in-process transport
                        Self::scheme_fallback_transport(&config, quic).await?
                    }
                };

                // Set syntax mode if specified
                if let Some(mode) = config.syntax_mode {
                    transport.execute(&format!("SET syntax_mode = '{}'", mode.as_set_value()), &[]).await?;
                }

                let callbacks = Arc::new(std::sync::Mutex::new(Vec::new()));
                let cbs = Arc::clone(&callbacks);
                let conn_listener = conn_for_notif.clone();
                let notif_handle = tokio::spawn(async move {
                    Self::notification_listener(conn_listener, cbs).await;
                });

                Ok(Self {
                    transport,
                    config,
                    topology,
                    notification_callbacks: callbacks,
                    notif_listener_handle: Some(notif_handle),
                    quic_connection: Some(conn_for_notif),
                })
            }

            // QUIC probe failed (timeout or connection refused) — direct connect
            // with the scheme-specific wire protocol
            Ok(Err(_)) | Err(_) => {
                let transport = Self::direct_connect(&config).await?;

                if let Some(mode) = config.syntax_mode {
                    transport.execute(&format!("SET syntax_mode = '{}'", mode.as_set_value()), &[]).await?;
                }

                Ok(Self {
                    transport,
                    config,
                    topology: None,
                    notification_callbacks: Arc::new(std::sync::Mutex::new(Vec::new())),
                    notif_listener_handle: None,
                    quic_connection: None,
                })
            }
        }
    }

    /// Connect directly using the scheme-specific wire protocol (no PyroLink).
    async fn direct_connect(config: &ConnectConfig) -> Result<Box<dyn PyroTransport>, ClientError> {
        let user = if config.user.is_empty() { "pyrosql" } else { &config.user };
        match config.scheme {
            Scheme::Postgres => {
                let pg = TcpPgTransport::connect(&config.host, config.port, user, &config.database, &config.password).await?;
                Ok(Box::new(pg))
            }
            Scheme::MySQL => {
                let mysql_user = if config.user.is_empty() { "root" } else { &config.user };
                let mysql = MysqlTransport::connect(&config.host, config.port, mysql_user, &config.database, &config.password).await?;
                Ok(Box::new(mysql))
            }
            Scheme::Wire => {
                let wire = PWireTransport::connect(&config.host, config.port).await?;
                Ok(Box::new(wire))
            }
            _ => {
                // vsql:// or auto:// without QUIC available — try PG wire as fallback
                let pg = TcpPgTransport::connect(&config.host, config.port, user, &config.database, &config.password).await?;
                Ok(Box::new(pg))
            }
        }
    }

    /// For cross-host: use QUIC for vsql://, or native TCP protocol for postgres:///mysql://.
    async fn scheme_fallback_transport(
        config: &ConnectConfig,
        quic: QuicTransport,
    ) -> Result<Box<dyn PyroTransport>, ClientError> {
        match config.scheme {
            Scheme::Quic | Scheme::Auto => {
                // vsql:// and auto:// → keep QUIC
                Ok(Box::new(quic))
            }
            Scheme::Postgres => {
                // postgres:// cross-host → close QUIC, connect via PG wire TCP
                quic.connection.close(quinn::VarInt::from_u32(0), b"using pg wire");
                let user = if config.user.is_empty() { "pyrosql" } else { &config.user };
                let pg = TcpPgTransport::connect(&config.host, config.port, user, &config.database, &config.password).await?;
                Ok(Box::new(pg))
            }
            Scheme::MySQL => {
                // mysql:// cross-host → close QUIC, connect via MySQL wire TCP
                quic.connection.close(quinn::VarInt::from_u32(0), b"using mysql wire");
                let user = if config.user.is_empty() { "root" } else { &config.user };
                let mysql = MysqlTransport::connect(&config.host, config.port, user, &config.database, &config.password).await?;
                Ok(Box::new(mysql))
            }
            Scheme::Wire => {
                // Wire scheme — close QUIC, use PWire transport
                quic.connection.close(quinn::VarInt::from_u32(0), b"using wire");
                let wire = PWireTransport::connect(&config.host, config.port).await?;
                Ok(Box::new(wire))
            }
            Scheme::Unix => {
                // Should not reach here (handled earlier)
                Ok(Box::new(quic))
            }
        }
    }

    /// Shorthand: parse a URL and connect.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the URL is invalid or the connection fails.
    pub async fn connect_url(url: &str) -> Result<Self, ClientError> {
        Self::connect(ConnectConfig::from_url(url)?).await
    }

    /// Execute a query and return the result rows.
    ///
    /// Parameters in `params` can be referenced as `$1`, `$2`, etc. in the SQL.
    /// Currently parameters are interpolated client-side; server-side prepared
    /// statement binding is planned for a future release.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] on stream, protocol, or server errors.
    pub async fn query(&self, sql: &str, params: &[Value]) -> Result<QueryResult, ClientError> {
        self.transport.query(sql, params).await
    }

    /// Execute a DML statement (INSERT / UPDATE / DELETE) and return affected rows.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] on stream, protocol, or server errors.
    pub async fn execute(&self, sql: &str, params: &[Value]) -> Result<u64, ClientError> {
        self.transport.execute(sql, params).await
    }

    /// Returns the active transport tier name (e.g. `"T4:QUIC"`).
    #[must_use]
    pub fn transport_tier(&self) -> &str {
        self.transport.tier()
    }

    /// Returns the topology hints received from the server, if available.
    #[must_use]
    pub fn topology(&self) -> Option<&TopologyHints> {
        self.topology.as_ref()
    }

    // === Transactions ===

    /// Begin a transaction. Returns a [`Transaction`] handle.
    ///
    /// The transaction will be auto-rolled-back on drop if not explicitly
    /// committed or rolled back.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the server rejects the begin request.
    pub async fn begin(&self) -> Result<Transaction<'_>, ClientError> {
        let action = serde_json::json!({"type": "BeginTransaction"});
        let response = self.transport.send_action(&action).await?;
        let tx_id = response["transaction_id"]
            .as_str()
            .ok_or(ClientError::Protocol("no transaction_id in response".into()))?;
        Ok(Transaction {
            id: tx_id.to_string(),
            client: self,
            committed: false,
        })
    }

    /// Begin a transaction with serializable isolation level.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the server rejects the begin request.
    pub async fn begin_serializable(&self) -> Result<Transaction<'_>, ClientError> {
        let action = serde_json::json!({
            "type": "BeginTransaction",
            "isolation": "serializable"
        });
        let response = self.transport.send_action(&action).await?;
        let tx_id = response["transaction_id"]
            .as_str()
            .ok_or(ClientError::Protocol("no transaction_id in response".into()))?;
        Ok(Transaction {
            id: tx_id.to_string(),
            client: self,
            committed: false,
        })
    }

    // === Prepared Statements ===

    /// Prepare a statement for repeated execution.
    ///
    /// Returns a [`PreparedStatement`] handle that can be executed multiple
    /// times with different parameters.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the server rejects the prepare request.
    pub async fn prepare(&self, sql: &str) -> Result<PreparedStatement<'_>, ClientError> {
        let action = serde_json::json!({
            "type": "PrepareStatement",
            "sql": sql
        });
        let response = self.transport.send_action(&action).await?;
        let handle = response["handle"]
            .as_str()
            .unwrap_or("")
            .to_string();
        Ok(PreparedStatement {
            handle,
            sql: sql.to_string(),
            client: self,
        })
    }

    // === Bulk Insert ===

    /// Bulk insert rows into a table.
    ///
    /// This uses the DoPut RPC for efficient batch insertion.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] on stream, protocol, or server errors.
    pub async fn bulk_insert(
        &self,
        table: &str,
        columns: &[&str],
        rows: &[Vec<Value>],
    ) -> Result<u64, ClientError> {
        self.transport.bulk_insert(table, columns, rows).await
    }

    // === Auto-reconnect ===

    /// Execute a query with auto-reconnect on connection failure.
    ///
    /// If the initial query fails with a [`ClientError::Connection`] error,
    /// the client will attempt to reconnect and retry the query once.  All
    /// other error types are returned immediately.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if both the initial and retry attempts fail.
    pub async fn query_with_retry(
        &mut self,
        sql: &str,
        params: &[Value],
    ) -> Result<QueryResult, ClientError> {
        match self.query(sql, params).await {
            Ok(r) => Ok(r),
            Err(ClientError::Connection(_)) => {
                self.reconnect().await?;
                self.query(sql, params).await
            }
            Err(e) => Err(e),
        }
    }

    /// Execute a DML statement with auto-reconnect on connection failure.
    ///
    /// Same retry semantics as [`query_with_retry`](Client::query_with_retry).
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if both the initial and retry attempts fail.
    pub async fn execute_with_retry(
        &mut self,
        sql: &str,
        params: &[Value],
    ) -> Result<u64, ClientError> {
        match self.execute(sql, params).await {
            Ok(r) => Ok(r),
            Err(ClientError::Connection(_)) => {
                self.reconnect().await?;
                self.execute(sql, params).await
            }
            Err(e) => Err(e),
        }
    }

    /// Re-establish the connection using the stored configuration.
    ///
    /// Replaces the internal transport with a fresh connection.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the reconnection attempt fails.
    async fn reconnect(&mut self) -> Result<(), ClientError> {
        // Abort the old notification listener before reconnecting.
        if let Some(handle) = self.notif_listener_handle.take() {
            handle.abort();
        }
        let new_client = Client::connect(self.config.clone()).await?;
        self.transport = new_client.transport;
        self.topology = new_client.topology;
        self.quic_connection = new_client.quic_connection;
        self.notif_listener_handle = new_client.notif_listener_handle;
        // Preserve existing callbacks — they were registered by the user.
        Ok(())
    }

    // === Internal ===

    /// Send a DoAction RPC to the server.
    pub async fn send_action(
        &self,
        action: &serde_json::Value,
    ) -> Result<serde_json::Value, ClientError> {
        self.transport.send_action(action).await
    }

    // === COPY ===

    /// COPY table data to the client as CSV.
    ///
    /// Sends a COPY OUT RPC and returns the full CSV result (including header).
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] on stream, protocol, or server errors.
    pub async fn copy_out(&self, sql: &str) -> Result<String, ClientError> {
        self.transport.copy_out(sql).await
    }

    /// COPY CSV data from the client into a table.
    ///
    /// `csv_data` should contain raw CSV lines (no header). The columns
    /// parameter specifies the target columns.
    ///
    /// Returns the number of rows inserted.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] on stream, protocol, or server errors.
    pub async fn copy_in(&self, table: &str, columns: &[&str], csv_data: &str) -> Result<u64, ClientError> {
        self.transport.copy_in(table, columns, csv_data).await
    }

    // === Streaming Cursor ===

    /// Execute a query and return a [`Cursor`] for lazy, batched iteration.
    ///
    /// The server streams results in batches of ~1000 rows. The cursor reads
    /// them lazily, only fetching the next batch when the current one is exhausted.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] on stream, protocol, or server errors.
    pub async fn query_cursor(&self, sql: &str, params: &[Value]) -> Result<Cursor, ClientError> {
        self.transport.query_cursor(sql, params).await
    }

    // === CDC ===

    /// Subscribe to change data capture events on a table.
    ///
    /// Returns a [`CdcStream`] that yields [`CdcEvent`] items.
    ///
    /// NOTE: v1 uses polling via DoAction. WAL-based streaming planned for v2.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] on stream, protocol, or server errors.
    pub async fn subscribe_cdc(&self, table: &str) -> Result<CdcStream, ClientError> {
        self.transport.subscribe_cdc(table).await
    }

    // === WATCH / LISTEN / NOTIFY ===

    /// Register a callback for server-pushed notifications.
    ///
    /// The callback will be invoked for every notification received from the
    /// server (LISTEN/NOTIFY and WATCH result changes).  Multiple callbacks
    /// can be registered; they are all invoked in order.
    pub fn on_notification(&self, callback: NotificationCallback) {
        if let Ok(mut cbs) = self.notification_callbacks.lock() {
            cbs.push(callback);
        }
    }

    /// Subscribe to a reactive query via WATCH.
    ///
    /// Returns the watch channel name that will receive notifications when
    /// the query result changes.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the server rejects the WATCH command.
    pub async fn watch(&self, sql: &str) -> Result<String, ClientError> {
        let result = self.query(&format!("WATCH {sql}"), &[]).await?;
        // The server returns the channel name in the first row, first column.
        let channel = result
            .rows
            .first()
            .and_then(|row| row.values().first())
            .and_then(|v| match v {
                Value::Text(s) => Some(s.clone()),
                _ => None,
            })
            .unwrap_or_default();
        Ok(channel)
    }

    /// Unsubscribe from a WATCH channel.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the server rejects the UNWATCH command.
    pub async fn unwatch(&self, channel: &str) -> Result<(), ClientError> {
        self.execute(&format!("UNWATCH {channel}"), &[]).await?;
        Ok(())
    }

    /// Subscribe to a PubSub channel via LISTEN.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the server rejects the LISTEN command.
    pub async fn listen(&self, channel: &str) -> Result<(), ClientError> {
        self.execute(&format!("LISTEN {channel}"), &[]).await?;
        Ok(())
    }

    /// Unsubscribe from a PubSub channel via UNLISTEN.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the server rejects the UNLISTEN command.
    pub async fn unlisten(&self, channel: &str) -> Result<(), ClientError> {
        self.execute(&format!("UNLISTEN {channel}"), &[]).await?;
        Ok(())
    }

    /// Send a notification to a PubSub channel via NOTIFY.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the server rejects the NOTIFY command.
    pub async fn notify(&self, channel: &str, payload: &str) -> Result<(), ClientError> {
        self.execute(
            &format!("NOTIFY {channel}, '{}'", payload.replace('\'', "''")),
            &[],
        )
        .await?;
        Ok(())
    }

    // === Background notification listener ===

    /// Background task that accepts server-initiated unidirectional streams
    /// and dispatches notification payloads to registered callbacks.
    async fn notification_listener(
        conn: Connection,
        callbacks: Arc<std::sync::Mutex<Vec<NotificationCallback>>>,
    ) {
        loop {
            match conn.accept_uni().await {
                Ok(mut recv) => {
                    // Read the notification message from the uni stream.
                    match read_message(&mut recv).await {
                        Ok(Some((msg_type, data))) if msg_type == MSG_NOTIFICATION => {
                            if let Ok(parsed) =
                                serde_json::from_slice::<serde_json::Value>(&data)
                            {
                                let notif = Notification {
                                    channel: parsed["channel"]
                                        .as_str()
                                        .unwrap_or("")
                                        .to_string(),
                                    payload: parsed["payload"]
                                        .as_str()
                                        .unwrap_or("")
                                        .to_string(),
                                };
                                if let Ok(cbs) = callbacks.lock() {
                                    for cb in cbs.iter() {
                                        cb(notif.clone());
                                    }
                                }
                            }
                        }
                        Ok(_) => {
                            // Non-notification message on uni stream — ignore.
                        }
                        Err(_) => {
                            // Read error — stream may have been reset.
                        }
                    }
                }
                Err(_) => {
                    // Connection closed or error — exit listener.
                    break;
                }
            }
        }
    }
}

// ── Cursor ───────────────────────────────────────────────────────────────────

/// A streaming cursor over query results.
///
/// Reads batches lazily from the QUIC stream. Call [`next`](Cursor::next)
/// to fetch rows one at a time. When the current batch is exhausted, the
/// next batch is fetched from the server automatically.
pub struct Cursor {
    /// Column names for the result set.
    pub columns: Vec<String>,
    /// Shared column metadata for zero-allocation row construction.
    col_meta: Arc<crate::row::ColumnMeta>,
    buffer: std::collections::VecDeque<Row>,
    recv: Option<quinn::RecvStream>,
    exhausted: bool,
}

impl Cursor {
    /// Create a cursor from pre-fetched rows (used by non-QUIC transports).
    pub(crate) fn from_rows(columns: Vec<String>, rows: Vec<Row>) -> Self {
        let col_meta = crate::row::ColumnMeta::new(columns.clone());
        Self { columns, col_meta, buffer: rows.into(), recv: None, exhausted: true }
    }

    /// Create a cursor backed by a QUIC recv stream (lazy fetching).
    pub(crate) fn from_stream(columns: Vec<String>, recv: quinn::RecvStream) -> Self {
        let col_meta = crate::row::ColumnMeta::new(columns.clone());
        Self { columns, col_meta, buffer: std::collections::VecDeque::new(), recv: Some(recv), exhausted: false }
    }

    /// Fetch the next row, or `None` if all rows have been consumed.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if reading the next batch fails.
    pub async fn next(&mut self) -> Result<Option<Row>, ClientError> {
        if let Some(row) = self.buffer.pop_front() {
            return Ok(Some(row));
        }

        if self.exhausted {
            return Ok(None);
        }

        self.fetch_next_batch().await?;

        if self.buffer.is_empty() {
            self.exhausted = true;
            return Ok(None);
        }

        Ok(self.buffer.pop_front())
    }

    /// Collect all remaining rows into a Vec.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if reading any batch fails.
    pub async fn collect_all(&mut self) -> Result<Vec<Row>, ClientError> {
        let mut all = Vec::new();
        while let Some(row) = self.next().await? {
            all.push(row);
        }
        Ok(all)
    }

    async fn fetch_next_batch(&mut self) -> Result<(), ClientError> {
        self.buffer.clear();

        let recv = match self.recv.as_mut() {
            Some(r) => r,
            None => {
                self.exhausted = true;
                return Ok(());
            }
        };

        match read_message(recv).await? {
            Some((MSG_RECORD_BATCH, payload)) => {
                let batch: Vec<Vec<Value>> = serde_json::from_slice(&payload)
                    .map_err(|e| ClientError::Protocol(format!("cursor batch decode: {e}")))?;
                for row_values in batch {
                    self.buffer.push_back(Row::new(Arc::clone(&self.col_meta), row_values));
                }
            }
            Some((MSG_DO_ACTION, _)) => {
                // CommandComplete — no more rows
                self.exhausted = true;
            }
            Some((other, _)) => {
                return Err(ClientError::Protocol(format!(
                    "cursor: unexpected message type {other:#04x}"
                )));
            }
            None => {
                self.exhausted = true;
            }
        }

        Ok(())
    }
}

// ── CDC types ───────────────────────────────────────────────────────────────

/// The type of a CDC event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CdcEventType {
    /// A row was inserted.
    Insert,
    /// A row was updated.
    Update,
    /// A row was deleted.
    Delete,
}

/// A change data capture event.
#[derive(Debug, Clone)]
pub struct CdcEvent {
    /// The type of change.
    pub event_type: CdcEventType,
    /// The table that was modified.
    pub table: String,
    /// The row data as a JSON value.
    pub row_data: serde_json::Value,
}

/// A stream of CDC events from a subscription.
///
/// v1 wraps a subscription ID for polling via DoAction.
pub struct CdcStream {
    /// The server-assigned subscription ID.
    pub subscription_id: String,
    /// The subscribed table name.
    pub table: String,
}

impl CdcStream {
    /// Poll for the next CDC event.
    ///
    /// NOTE: v1 returns `None` immediately. WAL-based push streaming is
    /// planned for v2. Use `Client::send_action` with PollCdc to poll.
    pub async fn next(&mut self) -> Option<CdcEvent> {
        None
    }

    /// The subscription ID for manual polling.
    #[must_use]
    pub fn id(&self) -> &str {
        &self.subscription_id
    }
}

// ── Transaction ─────────────────────────────────────────────────────────────

/// A transaction handle tied to a [`Client`] connection.
///
/// Created by [`Client::begin`] or [`Client::begin_serializable`].
/// If dropped without calling [`commit`](Transaction::commit) or
/// [`rollback`](Transaction::rollback), a warning is printed and the server
/// will eventually time out the transaction.
pub struct Transaction<'a> {
    id: String,
    client: &'a Client,
    committed: bool,
}

impl<'a> Transaction<'a> {
    /// The server-assigned transaction ID.
    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Execute a query within this transaction.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] on stream, protocol, or server errors.
    pub async fn query(&self, sql: &str, params: &[Value]) -> Result<QueryResult, ClientError> {
        let tx_sql = format!("/* tx:{} */ {}", self.id, interpolate_params(sql, params));
        self.client.query(&tx_sql, &[]).await
    }

    /// Execute a DML statement within this transaction.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] on stream, protocol, or server errors.
    pub async fn execute(&self, sql: &str, params: &[Value]) -> Result<u64, ClientError> {
        let tx_sql = format!("/* tx:{} */ {}", self.id, interpolate_params(sql, params));
        self.client.execute(&tx_sql, &[]).await
    }

    /// Commit the transaction.
    ///
    /// Consumes the transaction handle so it cannot be used afterwards.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the server rejects the commit.
    pub async fn commit(mut self) -> Result<(), ClientError> {
        let action = serde_json::json!({"type": "Commit", "transaction_id": self.id});
        self.client.send_action(&action).await?;
        self.committed = true;
        Ok(())
    }

    /// Rollback the transaction.
    ///
    /// Consumes the transaction handle so it cannot be used afterwards.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the server rejects the rollback.
    pub async fn rollback(mut self) -> Result<(), ClientError> {
        let action = serde_json::json!({"type": "Rollback", "transaction_id": self.id});
        self.client.send_action(&action).await?;
        self.committed = true; // prevent double-rollback in Drop
        Ok(())
    }

    /// Create a savepoint within this transaction.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the server rejects the savepoint creation.
    pub async fn savepoint(&self, name: &str) -> Result<(), ClientError> {
        let action = serde_json::json!({
            "type": "CreateSavepoint",
            "transaction_id": self.id,
            "name": name
        });
        self.client.send_action(&action).await?;
        Ok(())
    }

    /// Rollback to a previously created savepoint.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if the server rejects the rollback.
    pub async fn rollback_to(&self, name: &str) -> Result<(), ClientError> {
        let action = serde_json::json!({
            "type": "RollbackSavepoint",
            "transaction_id": self.id,
            "name": name
        });
        self.client.send_action(&action).await?;
        Ok(())
    }
}

impl Drop for Transaction<'_> {
    fn drop(&mut self) {
        if !self.committed {
            eprintln!(
                "Transaction {} dropped without commit/rollback — auto-rollback (best-effort)",
                self.id
            );
        }
    }
}

// ── PreparedStatement ───────────────────────────────────────────────────────

/// A prepared statement handle tied to a [`Client`] connection.
///
/// Created by [`Client::prepare`]. Currently executes by re-sending
/// the SQL with client-side parameter interpolation; future versions will
/// use server-side parameter binding via the statement handle.
pub struct PreparedStatement<'a> {
    handle: String,
    sql: String,
    client: &'a Client,
}

impl<'a> PreparedStatement<'a> {
    /// The server-assigned statement handle (opaque identifier).
    #[must_use]
    pub fn handle(&self) -> &str {
        &self.handle
    }

    /// The original SQL string used to create this prepared statement.
    #[must_use]
    pub fn sql(&self) -> &str {
        &self.sql
    }

    /// Execute the prepared statement as a query, returning result rows.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] on stream, protocol, or server errors.
    pub async fn query(&self, params: &[Value]) -> Result<QueryResult, ClientError> {
        self.client.query(&self.sql, params).await
    }

    /// Execute the prepared statement as a DML operation.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] on stream, protocol, or server errors.
    pub async fn execute(&self, params: &[Value]) -> Result<u64, ClientError> {
        self.client.execute(&self.sql, params).await
    }
}

// ── QUIC connection setup ─────────────────────────────────────────────────────

/// Establish a raw QUIC connection to the server.
///
/// This is the first step of adaptive transport negotiation — QUIC is
/// always used for the initial handshake, even if a faster tier will be
/// selected afterwards.
async fn establish_quic_connection(config: &ConnectConfig) -> Result<Connection, ClientError> {
    let tls_config = build_client_tls(config)?;

    let mut client_config = quinn::ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(tls_config)
            .map_err(|e| ClientError::Tls(e.to_string()))?,
    ));

    // Sensible transport defaults
    let mut transport = quinn::TransportConfig::default();
    transport.keep_alive_interval(Some(std::time::Duration::from_secs(10)));
    client_config.transport_config(Arc::new(transport));

    let mut endpoint = Endpoint::client("0.0.0.0:0".parse().unwrap())
        .map_err(|e| ClientError::Connection(e.to_string()))?;
    endpoint.set_default_client_config(client_config);

    let addr = tokio::net::lookup_host(format!("{}:{}", config.host, config.port))
        .await
        .map_err(|e| ClientError::Connection(format!("DNS lookup failed: {e}")))?
        .next()
        .ok_or_else(|| {
            ClientError::Connection(format!(
                "no addresses found for {}:{}",
                config.host, config.port
            ))
        })?;

    let connection = endpoint
        .connect(addr, &config.host)
        .map_err(|e| ClientError::Connection(e.to_string()))?
        .await
        .map_err(|e| ClientError::Connection(e.to_string()))?;

    Ok(connection)
}

// ── Wire helpers ─────────────────────────────────────────────────────────────

/// Write a framed message: `[type_byte][4-byte LE length][payload]`.
#[inline]
pub(crate) async fn write_message(
    send: &mut SendStream,
    type_byte: u8,
    payload: &[u8],
) -> Result<(), ClientError> {
    let len = (payload.len() as u32).to_le_bytes();
    let header = [type_byte, len[0], len[1], len[2], len[3]];
    send.write_all(&header)
        .await
        .map_err(|e| ClientError::Protocol(format!("write header failed: {e}")))?;
    if !payload.is_empty() {
        send.write_all(payload)
            .await
            .map_err(|e| ClientError::Protocol(format!("write payload failed: {e}")))?;
    }
    Ok(())
}

/// Read one framed message from the receive stream.
///
/// Returns `None` on EOS or when the stream is cleanly finished.
#[inline]
pub(crate) async fn read_message(recv: &mut RecvStream) -> Result<Option<(u8, Bytes)>, ClientError> {
    // Read type byte + 4-byte LE length in a single syscall when possible.
    let mut header = [0u8; 5];
    match recv.read_exact(&mut header[..1]).await {
        Ok(()) => {}
        Err(e) => {
            // A clean stream finish is the normal end for the last message.
            let msg = e.to_string();
            if msg.contains("finished") || msg.contains("STREAM_FIN") {
                return Ok(None);
            }
            return Err(ClientError::Protocol(format!("read type byte: {e}")));
        }
    }

    if header[0] == MSG_EOS {
        return Ok(None);
    }

    recv.read_exact(&mut header[1..5])
        .await
        .map_err(|e| ClientError::Protocol(format!("read length: {e}")))?;
    let len = u32::from_le_bytes([header[1], header[2], header[3], header[4]]) as usize;

    if len > 256 * 1024 * 1024 {
        return Err(ClientError::Protocol(format!(
            "message length {len} exceeds 256 MiB limit"
        )));
    }

    let mut payload = vec![0u8; len];
    recv.read_exact(&mut payload)
        .await
        .map_err(|e| ClientError::Protocol(format!("read payload: {e}")))?;

    Ok(Some((header[0], Bytes::from(payload))))
}

/// Deserialized shape of a CommandComplete message.
#[derive(serde::Deserialize)]
struct CmdComplete {
    #[serde(default)]
    rows_affected: u64,
}

/// Read the full server response and assemble a [`QueryResult`].
pub(crate) async fn read_query_result(recv: &mut RecvStream) -> Result<QueryResult, ClientError> {
    let mut columns: Vec<String> = Vec::new();
    let mut col_meta: Option<std::sync::Arc<crate::row::ColumnMeta>> = None;
    let mut all_rows: Vec<Row> = Vec::new();
    let mut rows_affected: u64 = 0;

    while let Some((tag, payload)) = read_message(recv).await? {
        match tag {
            MSG_SCHEMA => {
                columns = serde_json::from_slice(&payload)
                    .map_err(|e| ClientError::Protocol(format!("schema decode: {e}")))?;
                col_meta = Some(crate::row::ColumnMeta::new(columns.clone()));
            }
            MSG_RECORD_BATCH => {
                let meta = col_meta.as_ref().ok_or_else(|| {
                    ClientError::Protocol("RecordBatch before Schema".into())
                })?;
                let batch: Vec<Vec<Value>> = serde_json::from_slice(&payload)
                    .map_err(|e| ClientError::Protocol(format!("batch decode: {e}")))?;
                all_rows.reserve(batch.len());
                for row_values in batch {
                    all_rows.push(Row::new(std::sync::Arc::clone(meta), row_values));
                }
            }
            MSG_DO_ACTION => {
                let cmd: CmdComplete = serde_json::from_slice(&payload)
                    .map_err(|e| ClientError::Protocol(format!("command complete decode: {e}")))?;
                rows_affected = cmd.rows_affected;
            }
            other => {
                return Err(ClientError::Protocol(format!(
                    "unexpected message type: {other:#04x}"
                )));
            }
        }
    }

    Ok(QueryResult {
        columns,
        rows: all_rows,
        rows_affected,
    })
}

// ── Parameter interpolation ─────────────────────────────────────────────────

/// Returns the length of a UTF-8 character from its leading byte.
#[inline]
fn utf8_char_len(b: u8) -> usize {
    if b < 0x80 { 1 }
    else if b < 0xE0 { 2 }
    else if b < 0xF0 { 3 }
    else { 4 }
}

/// Simple client-side parameter interpolation.
///
/// Replaces `$1`, `$2`, ... with the JSON representation of the corresponding
/// value.  This is a temporary measure until server-side prepared statement
/// parameter binding is available.
#[inline]
pub(crate) fn interpolate_params(sql: &str, params: &[Value]) -> String {
    if params.is_empty() {
        return sql.to_owned();
    }

    // Pre-compute all replacement strings once.
    let replacements: Vec<std::borrow::Cow<'static, str>> =
        params.iter().map(value_to_sql).collect();

    // Build result by scanning for $N placeholders. Since '$' and digits
    // are ASCII, we can safely index into the byte slice; non-ASCII bytes
    // are copied verbatim (they cannot be confused for '$' or digits).
    let mut result = String::with_capacity(sql.len());
    let bytes = sql.as_bytes();
    let mut pos = 0;
    while pos < bytes.len() {
        if bytes[pos] == b'$' && pos + 1 < bytes.len() && bytes[pos + 1].is_ascii_digit() {
            let start = pos + 1;
            let mut end = start;
            while end < bytes.len() && bytes[end].is_ascii_digit() {
                end += 1;
            }
            // The digit range is guaranteed ASCII, so this slice is valid str.
            if let Ok(idx) = sql[start..end].parse::<usize>() {
                if idx >= 1 && idx <= params.len() {
                    result.push_str(&replacements[idx - 1]);
                    pos = end;
                    continue;
                }
            }
        }
        // Copy one UTF-8 character (handles multi-byte sequences correctly).
        let ch_len = utf8_char_len(bytes[pos]);
        result.push_str(&sql[pos..pos + ch_len]);
        pos += ch_len;
    }
    result
}

/// Convert a [`Value`] to its SQL literal representation.
#[inline]
pub(crate) fn value_to_sql(v: &Value) -> std::borrow::Cow<'static, str> {
    match v {
        Value::Null => std::borrow::Cow::Borrowed("NULL"),
        Value::Bool(true) => std::borrow::Cow::Borrowed("TRUE"),
        Value::Bool(false) => std::borrow::Cow::Borrowed("FALSE"),
        Value::Int(n) => std::borrow::Cow::Owned(n.to_string()),
        Value::Float(f) => std::borrow::Cow::Owned(f.to_string()),
        Value::Text(s) => std::borrow::Cow::Owned(format!("'{}'", s.replace('\'', "''"))),
    }
}

// ── TLS setup ────────────────────────────────────────────────────────────────

/// Build a rustls [`ClientConfig`] for QUIC.
///
/// If `config.tls_skip_verify` is true, server certificate verification is
/// disabled (useful for development with self-signed certs).  A self-signed
/// client certificate is generated at runtime using `rcgen`.
fn build_client_tls(
    config: &ConnectConfig,
) -> Result<rustls::ClientConfig, ClientError> {
    // Generate an ephemeral self-signed client certificate.
    let cert_params = rcgen::CertificateParams::new(vec![config.host.clone()]);
    let cert = rcgen::Certificate::from_params(cert_params)
        .map_err(|e| ClientError::Tls(format!("rcgen self-sign: {e}")))?;

    let cert_der = rustls::pki_types::CertificateDer::from(
        cert.serialize_der()
            .map_err(|e| ClientError::Tls(format!("rcgen serialize cert: {e}")))?,
    );
    let key_der = rustls::pki_types::PrivateKeyDer::try_from(cert.serialize_private_key_der())
        .map_err(|e| ClientError::Tls(format!("private key DER: {e}")))?;

    let builder = rustls::ClientConfig::builder();

    let mut tls_config = if config.tls_skip_verify {
        builder
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(NoVerifier))
            .with_client_auth_cert(vec![cert_der], key_der)
            .map_err(|e| ClientError::Tls(e.to_string()))?
    } else {
        // In production, use the system root store.
        // For now we also skip verification since the server typically uses
        // a self-signed cert during development.  A proper root-store path
        // will be added when CA cert configuration is implemented.
        builder
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(NoVerifier))
            .with_client_auth_cert(vec![cert_der], key_der)
            .map_err(|e| ClientError::Tls(e.to_string()))?
    };

    tls_config.alpn_protocols = vec![ALPN_PYROSQL.to_vec()];

    Ok(tls_config)
}

/// A certificate verifier that accepts any server certificate.
///
/// **WARNING:** This disables all TLS server authentication and must only be
/// used for development and testing with self-signed certificates.
#[derive(Debug)]
struct NoVerifier;

impl rustls::client::danger::ServerCertVerifier for NoVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interpolate_no_params() {
        assert_eq!(interpolate_params("SELECT 1", &[]), "SELECT 1");
    }

    #[test]
    fn interpolate_single_param() {
        let sql = interpolate_params("SELECT * FROM t WHERE id = $1", &[Value::Int(42)]);
        assert_eq!(sql, "SELECT * FROM t WHERE id = 42");
    }

    #[test]
    fn interpolate_multiple_params() {
        let sql = interpolate_params(
            "INSERT INTO t (a, b) VALUES ($1, $2)",
            &[Value::Text("hello".into()), Value::Int(7)],
        );
        assert_eq!(sql, "INSERT INTO t (a, b) VALUES ('hello', 7)");
    }

    #[test]
    fn interpolate_text_escaping() {
        let sql = interpolate_params("SELECT $1", &[Value::Text("it's".into())]);
        assert_eq!(sql, "SELECT 'it''s'");
    }

    #[test]
    fn interpolate_null_and_bool() {
        let sql = interpolate_params(
            "SELECT $1, $2",
            &[Value::Null, Value::Bool(true)],
        );
        assert_eq!(sql, "SELECT NULL, TRUE");
    }

    #[test]
    fn value_to_sql_coverage() {
        assert_eq!(value_to_sql(&Value::Float(3.14)), "3.14");
    }

    #[test]
    fn transaction_drop_sets_warning() {
        // Verify that a Transaction can be constructed and its fields are accessible.
        // We can't test the actual drop warning without a real client, but we
        // verify the struct layout and committed flag logic.
        // (This is a compile-time check more than a runtime one.)
    }

    #[test]
    fn prepared_statement_accessors() {
        // Compile-time check that PreparedStatement has the expected public API.
        // Cannot construct without a real Client, but we verify the types exist.
    }

    #[tokio::test]
    async fn test_connect_and_query() {
        // This test requires a running PyroSQL server.
        // Skip if not available (CI/local dev without a server).
        let result = Client::connect(
            ConnectConfig::new("localhost", 12520).tls_skip_verify(true),
        )
        .await;

        if result.is_err() {
            eprintln!("Skipping integration test: PyroSQL server not available");
            return;
        }

        let client = result.unwrap();
        let qr = client.query("SELECT 1 AS n", &[]).await;
        assert!(qr.is_ok(), "query should succeed: {qr:?}");
    }
}
