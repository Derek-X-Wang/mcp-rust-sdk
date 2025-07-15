# ⚠️ ⚠️ ⚠️ Please use the offical [MCP Rust SDK](https://github.com/modelcontextprotocol/rust-sdk)

# Model Context Protocol (MCP) Rust SDK

> ⚠️ **Warning**: This SDK is currently a work in progress and is not ready for production use.

A Rust implementation of the Model Context Protocol (MCP), designed for seamless communication between AI models and their runtime environments.

[![Rust CI/CD](https://github.com/Derek-X-Wang/mcp-rust-sdk/actions/workflows/rust.yml/badge.svg)](https://github.com/Derek-X-Wang/mcp-rust-sdk/actions/workflows/rust.yml)
[![crates.io](https://img.shields.io/crates/v/mcp_rust_sdk.svg)](https://crates.io/crates/mcp_rust_sdk)
[![Documentation](https://docs.rs/mcp_rust_sdk/badge.svg)](https://docs.rs/mcp_rust_sdk)

## Features

- 🚀 Full implementation of MCP protocol specification
- 🔄 Multiple transport layers (WebSocket, stdio)
- ⚡ Async/await support using Tokio
- 🛡️ Type-safe message handling
- 🔍 Comprehensive error handling
- 📦 Zero-copy serialization/deserialization

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
mcp_rust_sdk = "0.1.0"
```

## Quick Start

### Client Example

```rust
use mcp_rust_sdk::{Client, transport::WebSocketTransport};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a WebSocket transport
    let transport = WebSocketTransport::new("ws://localhost:8080").await?;
    
    // Create and connect the client
    let client = Client::new(transport);
    client.connect().await?;
    
    // Make requests
    let response = client.request("method_name", Some(params)).await?;
    
    Ok(())
}
```

### Server Example

```rust
use mcp_rust_sdk::{Server, transport::StdioTransport};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a stdio transport
    let (transport, _) = StdioTransport::new();
    
    // Create and start the server
    let server = Server::new(transport);
    server.start().await?;
    
    Ok(())
}
```

## Transport Layers

The SDK supports multiple transport layers:

### WebSocket Transport
- Ideal for network-based communication
- Supports both secure (WSS) and standard (WS) connections
- Built-in reconnection handling

### stdio Transport
- Perfect for local process communication
- Lightweight and efficient
- Great for command-line tools and local development

## Error Handling

The SDK provides comprehensive error handling through the `Error` type:

```rust
use mcp_rust_sdk::Error;

match result {
    Ok(value) => println!("Success: {:?}", value),
    Err(Error::Protocol(code, msg)) => println!("Protocol error {}: {}", code, msg),
    Err(Error::Transport(e)) => println!("Transport error: {}", e),
    Err(e) => println!("Other error: {}", e),
}
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
