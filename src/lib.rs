//! # Model Context Protocol (MCP) Rust SDK
//!
//! This SDK provides a Rust implementation of the Model Context Protocol (MCP), a protocol designed
//! for communication between AI models and their runtime environments. The SDK supports both client
//! and server implementations with multiple transport layers.
//!
//! ## Features
//!
//! - Full implementation of MCP protocol specification
//! - Multiple transport layers (WebSocket, stdio)
//! - Async/await support using Tokio
//! - Type-safe message handling
//! - Comprehensive error handling
//!
//! ## Example
//!
//! ```no_run
//! use std::sync::Arc;
//! use mcp_rust_sdk::client::Client;
//! use mcp_rust_sdk::transport::websocket::WebSocketTransport;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Create a WebSocket transport
//!     let transport = WebSocketTransport::new("ws://localhost:8080").await?;
//!
//!     // Create the client with Arc-wrapped transport
//!     let client = Client::new(Arc::new(transport));
//!
//!     // Use the client...
//!
//!     Ok(())
//! }
//! ```

/// Client module provides the MCP client implementation
pub mod client;
/// Error types and handling for the SDK
pub mod error;
/// Protocol-specific types and implementations
pub mod protocol;
/// Server module provides the MCP server implementation
pub mod server;
/// Transport layer implementations (WebSocket, stdio)
pub mod transport;
/// Common types used throughout the SDK
pub mod types;

// Re-export commonly used types for convenience
pub use error::Error;
pub use protocol::{Notification, Request, Response};
pub use types::*;

/// The latest supported protocol version of MCP
///
/// This version represents the most recent protocol specification that this SDK supports.
/// It is used during client-server handshake to ensure compatibility.
pub const LATEST_PROTOCOL_VERSION: &str = "2024-11-05";

/// List of all protocol versions supported by this SDK
///
/// This list is used during version negotiation to determine compatibility between
/// client and server. The versions are listed in order of preference, with the
/// most recent version first.
pub const SUPPORTED_PROTOCOL_VERSIONS: &[&str] = &[LATEST_PROTOCOL_VERSION, "2024-10-07"];

/// JSON-RPC version used by the MCP protocol
///
/// MCP uses JSON-RPC 2.0 for its message format. This constant is used to ensure
/// all messages conform to the correct specification.
pub const JSONRPC_VERSION: &str = "2.0";
