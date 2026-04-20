//! PyroLink — high-performance QUIC client for PyroSQL.
//!
//! This crate provides a Rust client for connecting to PyroSQL via the
//! PyroLink QUIC protocol.  It is the foundation for Python, Node, Go, and
//! PHP bindings.
//!
//! # Quick start
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
//!
//! # Wire protocol
//!
//! The client speaks the same framed protocol as the PyroLink server:
//!
//! ```text
//! Send: [0x09 MSG_QUERY] [4-byte LE length] [SQL UTF-8 bytes]
//! Recv: [0x01 MSG_SCHEMA] [length] [column metadata JSON]
//!       [0x02 MSG_RECORD_BATCH] [length] [rows JSON batch] x N
//!       [0xFF MSG_EOS]
//! ```

#![deny(unsafe_code)]
#![warn(missing_docs)]

pub mod client;
pub mod config;
pub mod error;
pub mod mux;
pub mod pool;
pub mod pwire;
pub mod row;
pub mod scram_client;
pub mod transport;

pub use client::{
    CdcEvent, CdcEventType, CdcStream, Client, Cursor, Notification, NotificationCallback,
    PreparedStatement, Transaction,
};
pub use config::{ConnectConfig, Scheme, SyntaxMode};
pub use error::ClientError;
pub use pool::{Pool, PooledClient};
pub use pwire::{Pipeline, PipelineResponses, PyroWireConnection};
pub use row::{ColumnMeta, FromValue, QueryResult, Row, Value};
pub use transport::{
    Capabilities, MysqlTransport, TcpPgTransport, TopologyHints, TransportTier,
    UnixTransport, PyroTransport, PWireTransport, DEFAULT_UNIX_SOCKET_PATH,
};
