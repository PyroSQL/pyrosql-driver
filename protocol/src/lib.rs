//! PyroLink wire protocol — shared types for client and server.
//!
//! This crate contains the wire protocol constants and types shared between
//! PyroLink client SDKs and the PyroSQL server:
//!
//! - Message type constants (`MSG_QUERY`, `MSG_SCHEMA`, etc.)
//! - [`RpcType`] enum
//! - Frame reading/writing helpers
//! - [`PyroLinkError`] error types

#![deny(unsafe_code)]
#![warn(missing_docs)]

pub mod codec;
pub mod error;

pub use codec::*;
pub use error::PyroLinkError;
