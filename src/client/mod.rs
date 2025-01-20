use futures::StreamExt;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

use crate::{
    error::{Error, ErrorCode},
    protocol::{Notification, Request, RequestId},
    transport::{Message, Transport},
    types::{ClientCapabilities, Implementation, ServerCapabilities},
};

/// MCP client state
pub struct Client {
    transport: Arc<dyn Transport>,
    server_capabilities: Arc<RwLock<Option<ServerCapabilities>>>,
    request_counter: Arc<RwLock<i64>>,
    response_receiver: Arc<Mutex<tokio::sync::mpsc::UnboundedReceiver<Message>>>,
    #[allow(dead_code)]
    response_sender: tokio::sync::mpsc::UnboundedSender<Message>,
}

impl Client {
    /// Create a new MCP client with the given transport
    pub fn new(transport: Arc<dyn Transport>) -> Self {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let client = Self {
            transport: transport.clone(),
            server_capabilities: Arc::new(RwLock::new(None)),
            request_counter: Arc::new(RwLock::new(0)),
            response_receiver: Arc::new(Mutex::new(rx)),
            response_sender: tx.clone(),
        };

        // Start response handler task
        let transport_clone = transport.clone();
        let tx_clone = tx.clone();
        tokio::spawn(async move {
            let mut stream = transport_clone.receive();
            while let Some(result) = stream.next().await {
                match result {
                    Ok(message) => {
                        if tx_clone.send(message).is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        });

        client
    }

    /// Initialize the client
    pub async fn initialize(
        &self,
        implementation: Implementation,
        capabilities: ClientCapabilities,
    ) -> Result<ServerCapabilities, Error> {
        let params = serde_json::json!({
            "clientInfo": implementation,
            "capabilities": capabilities,
            "protocolVersion": crate::LATEST_PROTOCOL_VERSION,
        });

        let response = self.request("initialize", Some(params)).await?;

        let server_capabilities: ServerCapabilities = serde_json::from_value(response)?;
        *self.server_capabilities.write().await = Some(server_capabilities.clone());

        // Send initialized notification
        self.notify("initialized", None).await?;

        Ok(server_capabilities)
    }

    /// Send a request to the server and wait for the response.
    ///
    /// This method will block until a response is received from the server.
    /// If the server returns an error, it will be propagated as an `Error`.
    pub async fn request(
        &self,
        method: &str,
        params: Option<serde_json::Value>,
    ) -> Result<serde_json::Value, Error> {
        let mut counter = self.request_counter.write().await;
        *counter += 1;
        let id = RequestId::Number(*counter);

        let request = Request::new(method, params, id.clone());
        self.transport.send(Message::Request(request)).await?;

        // Wait for matching response
        let mut receiver = self.response_receiver.lock().await;
        while let Some(message) = receiver.recv().await {
            if let Message::Response(response) = message {
                if response.id == id {
                    if let Some(error) = response.error {
                        return Err(Error::protocol(
                            match error.code {
                                -32700 => ErrorCode::ParseError,
                                -32600 => ErrorCode::InvalidRequest,
                                -32601 => ErrorCode::MethodNotFound,
                                -32602 => ErrorCode::InvalidParams,
                                -32603 => ErrorCode::InternalError,
                                -32002 => ErrorCode::ServerNotInitialized,
                                -32001 => ErrorCode::UnknownErrorCode,
                                -32000 => ErrorCode::RequestFailed,
                                _ => ErrorCode::UnknownErrorCode,
                            },
                            &error.message,
                        ));
                    }
                    return response.result.ok_or_else(|| {
                        Error::protocol(ErrorCode::InternalError, "Response missing result")
                    });
                }
            }
        }

        Err(Error::protocol(
            ErrorCode::InternalError,
            "Connection closed while waiting for response",
        ))
    }

    /// Send a notification to the server
    pub async fn notify(
        &self,
        method: &str,
        params: Option<serde_json::Value>,
    ) -> Result<(), Error> {
        let notification = Notification::new(method, params);
        self.transport
            .send(Message::Notification(notification))
            .await
    }

    /// Get the server capabilities
    pub async fn capabilities(&self) -> Option<ServerCapabilities> {
        self.server_capabilities.read().await.clone()
    }

    /// Close the client connection
    pub async fn shutdown(&self) -> Result<(), Error> {
        // Send shutdown request
        self.request("shutdown", None).await?;

        // Send exit notification
        self.notify("exit", None).await?;

        // Close transport
        self.transport.close().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use futures::Stream;
    use std::{pin::Pin, time::Duration};
    use tokio::sync::broadcast;

    struct MockTransport {
        tx: broadcast::Sender<Result<Message, Error>>,
        send_delay: Duration,
    }

    impl MockTransport {
        fn new(send_delay: Duration) -> (Self, broadcast::Sender<Result<Message, Error>>) {
            let (tx, _) = broadcast::channel(10);
            let tx_clone = tx.clone();
            (Self { tx, send_delay }, tx_clone)
        }
    }

    #[async_trait]
    impl Transport for MockTransport {
        async fn send(&self, message: Message) -> Result<(), Error> {
            tokio::time::sleep(self.send_delay).await;
            self.tx.send(Ok(message)).map(|_| ()).map_err(|_| {
                Error::protocol(
                    crate::error::ErrorCode::InternalError,
                    "Failed to send message",
                )
            })
        }

        fn receive(&self) -> Pin<Box<dyn Stream<Item = Result<Message, Error>> + Send>> {
            let mut rx = self.tx.subscribe();
            Box::pin(async_stream::stream! {
                while let Ok(msg) = rx.recv().await {
                    yield msg;
                }
            })
        }

        async fn close(&self) -> Result<(), Error> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_client_initialization_timeout() {
        // Create a mock transport with 6 second delay (longer than our timeout)
        let (transport, _tx) = MockTransport::new(Duration::from_secs(6));
        let client = Client::new(Arc::new(transport));

        // Try to initialize with 5 second timeout
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            client.initialize(
                Implementation {
                    name: "test".to_string(),
                    version: "1.0".to_string(),
                },
                ClientCapabilities::default(),
            ),
        )
        .await;

        // Should timeout
        assert!(result.is_err(), "Expected timeout error");
    }

    #[tokio::test]
    async fn test_client_request_timeout() {
        // Create a mock transport with 6 second delay
        let (transport, _tx) = MockTransport::new(Duration::from_secs(6));
        let client = Client::new(Arc::new(transport));

        // Try to send request with 5 second timeout
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            client.request("test", Some(serde_json::json!({"key": "value"}))),
        )
        .await;

        // Should timeout
        assert!(result.is_err(), "Expected timeout error");
    }

    #[tokio::test]
    async fn test_client_notification_timeout() {
        // Create a mock transport with 6 second delay
        let (transport, _tx) = MockTransport::new(Duration::from_secs(6));
        let client = Client::new(Arc::new(transport));

        // Try to send notification with 5 second timeout
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            client.notify("test", Some(serde_json::json!({"key": "value"}))),
        )
        .await;

        // Should timeout
        assert!(result.is_err(), "Expected timeout error");
    }

    #[tokio::test]
    async fn test_client_fast_operation() {
        // Create a mock transport with 1 second delay (shorter than timeout)
        let (transport, _tx) = MockTransport::new(Duration::from_secs(1));
        let client = Client::new(Arc::new(transport));

        // Try to send notification with 5 second timeout
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            client.notify("test", Some(serde_json::json!({"key": "value"}))),
        )
        .await;

        // Should complete before timeout
        assert!(result.is_ok(), "Operation should complete before timeout");
    }
}
