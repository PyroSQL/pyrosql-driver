//! Error types for the PyroLink QUIC transport.

use thiserror::Error;

/// Errors that can occur in the PyroLink layer.
#[derive(Debug, Error)]
pub enum PyroLinkError {
    /// A QUIC connection-level error reported by quinn.
    #[error("QUIC connection error: {0}")]
    Connection(#[from] quinn::ConnectionError),

    /// Failed to accept or open a QUIC stream.
    #[error("QUIC stream error: {0}")]
    Stream(String),

    /// An I/O error on the underlying QUIC stream.
    #[error("stream I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// The wire frame received from the client was malformed.
    #[error("framing error: {0}")]
    Framing(String),

    /// The RPC type byte was not recognised.
    #[error("unknown RPC type byte: {0:#04x}")]
    UnknownRpcType(u8),

    /// TLS configuration was invalid.
    #[error("TLS configuration error: {0}")]
    Tls(String),

    /// The server endpoint could not be bound to the requested address.
    #[error("bind error: {0}")]
    Bind(String),

    /// An error propagated from the Flight SQL layer.
    #[error("Flight SQL error: {0}")]
    Flight(String),

    /// An internal, unexpected error.
    #[error("internal error: {0}")]
    Internal(String),
}

impl From<quinn::ReadExactError> for PyroLinkError {
    fn from(e: quinn::ReadExactError) -> Self {
        match e {
            quinn::ReadExactError::FinishedEarly(n) => {
                Self::Framing(format!("stream ended {n} bytes early"))
            }
            quinn::ReadExactError::ReadError(re) => Self::Stream(re.to_string()),
        }
    }
}
