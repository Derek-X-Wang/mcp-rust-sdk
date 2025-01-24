use async_trait::async_trait;
use serde_json::{json, Value};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio_tungstenite::accept_async;

use mcp_rust_sdk::{
    error::Error,
    server::{Server, ServerHandler},
    transport::websocket::WebSocketTransport,
    types::{ClientCapabilities, Implementation, ServerCapabilities},
};

/// Example server handler that implements basic MCP server functionality
struct ExampleHandler;

#[async_trait]
impl ServerHandler for ExampleHandler {
    async fn initialize(
        &self,
        implementation: Implementation,
        _capabilities: ClientCapabilities,
    ) -> Result<ServerCapabilities, Error> {
        log::debug!(
            "Client connected: {} v{}",
            implementation.name,
            implementation.version
        );
        Ok(ServerCapabilities::default())
    }

    async fn handle_method(&self, method: &str, params: Option<Value>) -> Result<Value, Error> {
        log::debug!("Received method call: {} with params: {:?}", method, params);
        Ok(json!({
            "status": "success",
            "message": "Hello from Rust MCP server!"
        }))
    }

    async fn shutdown(&self) -> Result<(), Error> {
        log::debug!("Server shutting down");
        Ok(())
    }
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "127.0.0.1:8780";
    let listener = TcpListener::bind(addr).await?;
    log::info!("WebSocket server listening on ws://{}", addr);

    // Accept and handle incoming connections
    while let Ok((stream, addr)) = listener.accept().await {
        log::debug!("New connection from: {}", addr);

        // Create WebSocket stream from TCP connection
        let ws_stream = accept_async(stream).await.map_err(|e| {
            log::debug!("Error during WebSocket handshake: {}", e);
            e
        })?;

        // Create WebSocket transport from the accepted stream
        let transport = WebSocketTransport::from_stream(ws_stream);

        // Create MCP server with the transport and handler
        let server = Server::new(Arc::new(transport), Arc::new(ExampleHandler));

        // Handle this connection in a new task
        tokio::spawn(async move {
            log::debug!("Starting server for connection from {}", addr);
            if let Err(e) = server.start().await {
                log::debug!("Error in WebSocket connection from {}: {}", addr, e);
            }
            log::debug!("Connection from {} closed", addr);
        });
    }

    Ok(())
}
