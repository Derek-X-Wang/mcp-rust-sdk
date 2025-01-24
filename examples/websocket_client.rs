use mcp_rust_sdk::{
    client::Client,
    transport::websocket::WebSocketTransport,
    types::{ClientCapabilities, Implementation},
};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create WebSocket transport
    let transport = WebSocketTransport::new("ws://127.0.0.1:8780").await?;

    // Create MCP client
    let client = Client::new(Arc::new(transport));

    // Initialize client
    let implementation = Implementation {
        name: "example-client".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
    };
    let capabilities = ClientCapabilities::default();

    log::debug!("Initializing MCP client...");
    let server_capabilities = client.initialize(implementation, capabilities).await?;
    log::debug!("Server capabilities: {:?}", server_capabilities);

    // Send a request
    log::debug!("Sending test request...");
    let response = client
        .request(
            "test/method",
            Some(serde_json::json!({
                "message": "Hello from Rust MCP client!"
            })),
        )
        .await?;
    log::debug!("Received response: {:?}", response);

    // Shutdown client
    log::debug!("Shutting down...");
    client.shutdown().await?;

    Ok(())
}
