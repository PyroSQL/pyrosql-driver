//! PyroSQL — official Rust driver speaking the native **PWire** binary
//! protocol over TCP.
//!
//! This crate is deliberately single-transport: PWire only.  Consumers
//! wanting to talk PostgreSQL wire, MySQL wire, Unix-socket, or QUIC
//! should use a transport-specific driver directly against the
//! PyroSQL server's matching port.
//!
//! # Quick start
//!
//! ```no_run
//! use pyrosql::{Client, ConnectConfig};
//!
//! # async fn demo() -> Result<(), pyrosql::ClientError> {
//! let client = Client::connect_url("vsql://localhost:12520/mydb").await?;
//! let result = client.query("SELECT * FROM users WHERE id = $1", &[42.into()]).await?;
//! for row in result.rows {
//!     println!("{}: {}", row.get::<String>("name").unwrap_or_default(), row.get::<i64>("age").unwrap_or(0));
//! }
//! # Ok(())
//! # }
//! ```
//!
//! The futures produced by the `Client` API are runtime-agnostic — they
//! resolve on `futures::executor::block_on`, `pyro_runtime`, or any other
//! executor that polls `Pin<Box<dyn Future>>`.
//!
//! # Wire protocol
//!
//! PWire framing:
//!
//! ```text
//! Request:  [type:u8][len:u32 LE][payload]
//!   0x01 VW_REQ_QUERY    payload = UTF-8 SQL
//!   0x02 VW_REQ_PREPARE  payload = UTF-8 SQL
//!   0x03 VW_REQ_EXECUTE  payload = [handle:u32 LE][count:u16 LE] + params
//!
//! Response: [type:u8][len:u32 LE][payload]
//!   0x01 VW_RESP_RESULT_SET
//!   0x02 VW_RESP_OK
//!   0x03 VW_RESP_ERROR
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

pub use client::{Client, PreparedStatement, Transaction};
pub use config::{ConnectConfig, Scheme, SyntaxMode};
pub use error::ClientError;
pub use pool::{Pool, PooledClient};
pub use pwire::{Pipeline, PipelineResponses, PyroWireConnection};
pub use row::{ColumnMeta, FromValue, QueryResult, Row, Value};
