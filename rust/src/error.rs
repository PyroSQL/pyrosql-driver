//! Error types for the PyroSQL client SDK.

/// Errors returned by the PyroSQL client.
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    /// Failed to establish a QUIC connection.
    #[error("connection failed: {0}")]
    Connection(String),

    /// Server returned a query-level error.
    #[error("query error: {0}")]
    Query(String),

    /// Wire protocol framing or message-type mismatch.
    #[error("protocol error: {0}")]
    Protocol(String),

    /// Invalid connection URL syntax.
    #[error("invalid URL: {0}")]
    InvalidUrl(String),

    /// TLS configuration or handshake failure.
    #[error("TLS error: {0}")]
    Tls(String),

    /// I/O error on the underlying QUIC stream.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}
