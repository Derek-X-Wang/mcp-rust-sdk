use async_trait::async_trait;
use futures::StreamExt;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::{
    error::{Error, ErrorCode},
    protocol::{Request, Response, ResponseError},
    transport::{Message, Transport},
    types::{ClientCapabilities, Implementation, ServerCapabilities},
};

/// Trait for implementing MCP server handlers
#[async_trait]
pub trait ServerHandler: Send + Sync {
    /// Handle initialization
    async fn initialize(
        &self,
        implementation: Implementation,
        capabilities: ClientCapabilities,
    ) -> Result<ServerCapabilities, Error>;

    /// Handle shutdown request
    async fn shutdown(&self) -> Result<(), Error>;

    /// Handle custom method calls
    async fn handle_method(
        &self,
        method: &str,
        params: Option<serde_json::Value>,
    ) -> Result<serde_json::Value, Error>;
}

/// Server state
pub struct Server {
    transport: Arc<dyn Transport>,
    handler: Arc<dyn ServerHandler>,
    initialized: Arc<RwLock<bool>>,
}

impl Server {
    /// Create a new MCP server
    pub fn new(transport: Arc<dyn Transport>, handler: Arc<dyn ServerHandler>) -> Self {
        Self {
            transport,
            handler,
            initialized: Arc::new(RwLock::new(false)),
        }
    }

    /// Start the server
    pub async fn start(&self) -> Result<(), Error> {
        let mut stream = self.transport.receive();

        while let Some(message) = stream.next().await {
            match message? {
                Message::Request(request) => {
                    let response = match self.handle_request(request.clone()).await {
                        Ok(response) => response,
                        Err(err) => Response::error(request.id, ResponseError::from(err)),
                    };
                    self.transport.send(Message::Response(response)).await?;
                }
                Message::Notification(notification) => {
                    match notification.method.as_str() {
                        "exit" => break,
                        "initialized" => {
                            *self.initialized.write().await = true;
                        }
                        _ => {
                            // Handle other notifications
                        }
                    }
                }
                Message::Response(_) => {
                    // Server shouldn't receive responses
                    return Err(Error::protocol(
                        ErrorCode::InvalidRequest,
                        "Server received unexpected response",
                    ));
                }
            }
        }

        Ok(())
    }

    async fn handle_request(&self, request: Request) -> Result<Response, Error> {
        let initialized = *self.initialized.read().await;

        match request.method.as_str() {
            "initialize" => {
                if initialized {
                    return Err(Error::protocol(
                        ErrorCode::InvalidRequest,
                        "Server already initialized",
                    ));
                }

                let params: serde_json::Value = request.params.unwrap_or(serde_json::json!({}));
                let implementation: Implementation = serde_json::from_value(
                    params.get("implementation").cloned().unwrap_or_default(),
                )?;
                let capabilities: ClientCapabilities = serde_json::from_value(
                    params.get("capabilities").cloned().unwrap_or_default(),
                )?;

                let result = self
                    .handler
                    .initialize(implementation, capabilities)
                    .await?;
                Ok(Response::success(
                    request.id,
                    Some(serde_json::to_value(result)?),
                ))
            }
            "shutdown" => {
                if !initialized {
                    return Err(Error::protocol(
                        ErrorCode::ServerNotInitialized,
                        "Server not initialized",
                    ));
                }

                self.handler.shutdown().await?;
                Ok(Response::success(request.id, None))
            }
            _ => {
                if !initialized {
                    return Err(Error::protocol(
                        ErrorCode::ServerNotInitialized,
                        "Server not initialized",
                    ));
                }

                let result = self
                    .handler
                    .handle_method(&request.method, request.params)
                    .await?;
                Ok(Response::success(request.id, Some(result)))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{Notification, RequestId};
    use async_trait::async_trait;
    use futures::{Stream, StreamExt};
    use std::{pin::Pin, time::Duration};
    use tokio::sync::{broadcast, mpsc};

    struct TestHandler {
        init_delay: Duration,
        shutdown_delay: Duration,
        method_delay: Duration,
    }

    impl TestHandler {
        fn new(init_delay: Duration, shutdown_delay: Duration, method_delay: Duration) -> Self {
            Self {
                init_delay,
                shutdown_delay,
                method_delay,
            }
        }
    }

    #[async_trait]
    impl ServerHandler for TestHandler {
        async fn initialize(
            &self,
            _implementation: Implementation,
            _capabilities: ClientCapabilities,
        ) -> Result<ServerCapabilities, Error> {
            tokio::time::sleep(self.init_delay).await;
            Ok(ServerCapabilities::default())
        }

        async fn shutdown(&self) -> Result<(), Error> {
            tokio::time::sleep(self.shutdown_delay).await;
            Ok(())
        }

        async fn handle_method(
            &self,
            _method: &str,
            _params: Option<serde_json::Value>,
        ) -> Result<serde_json::Value, Error> {
            tokio::time::sleep(self.method_delay).await;
            Ok(serde_json::json!({"status": "ok"}))
        }
    }

    struct MockTransport {
        client_to_server: Arc<tokio::sync::Mutex<mpsc::UnboundedReceiver<Result<Message, Error>>>>,
        server_to_client: broadcast::Sender<Result<Message, Error>>,
    }

    impl MockTransport {
        fn new() -> (
            Self,
            mpsc::UnboundedSender<Result<Message, Error>>,
            broadcast::Receiver<Result<Message, Error>>,
        ) {
            let (tx1, rx1) = mpsc::unbounded_channel();
            let (tx2, rx2) = broadcast::channel(100);
            (
                Self {
                    client_to_server: Arc::new(tokio::sync::Mutex::new(rx1)),
                    server_to_client: tx2.clone(),
                },
                tx1,
                rx2,
            )
        }
    }

    #[async_trait]
    impl Transport for MockTransport {
        async fn send(&self, message: Message) -> Result<(), Error> {
            self.server_to_client
                .send(Ok(message))
                .map(|_| ())
                .map_err(|_| Error::protocol(ErrorCode::InternalError, "Failed to send message"))
        }

        fn receive(&self) -> Pin<Box<dyn Stream<Item = Result<Message, Error>> + Send>> {
            let rx = self.client_to_server.clone();
            Box::pin(async_stream::stream! {
                let mut rx = rx.lock().await;
                while let Some(msg) = rx.recv().await {
                    yield msg;
                }
            })
        }

        async fn close(&self) -> Result<(), Error> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_server_initialization_timeout() {
        // Create transport and server with slow initialization
        let (transport, client_tx, mut client_rx) = MockTransport::new();
        let handler = TestHandler::new(
            Duration::from_secs(6), // Initialization takes 6 seconds
            Duration::from_millis(100),
            Duration::from_millis(100),
        );
        let server = Server::new(Arc::new(transport), Arc::new(handler));

        // Start server in background
        let server_handle = tokio::spawn(async move {
            if let Err(e) = server.start().await {
                log::error!("Server error: {}", e);
            }
        });

        // Give server time to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Send initialize request
        let init_request = Request::new(
            "initialize",
            Some(serde_json::json!({
                "implementation": {
                    "name": "test-client",
                    "version": "0.1.0"
                },
                "capabilities": {},
                "protocolVersion": "2024-11-05"
            })),
            RequestId::Number(1),
        );

        let _ = client_tx.send(Ok(Message::Request(init_request)));

        // Try to receive response with 5 second timeout
        let result = tokio::time::timeout(Duration::from_secs(5), client_rx.recv()).await;

        // Should timeout
        assert!(result.is_err(), "Expected timeout error");

        // Cleanup
        let _ = client_tx.send(Ok(Message::Notification(Notification::new("exit", None))));
        let _ = tokio::time::timeout(Duration::from_secs(1), server_handle).await;
    }

    #[tokio::test]
    async fn test_server_fast_operation() {
        // Create transport and server with fast initialization
        let (transport, client_tx, mut client_rx) = MockTransport::new();
        let handler = TestHandler::new(
            Duration::from_millis(100), // Fast initialization
            Duration::from_millis(100),
            Duration::from_millis(100),
        );
        let server = Server::new(Arc::new(transport), Arc::new(handler));

        // Start server in background
        let server_handle = tokio::spawn(async move {
            if let Err(e) = server.start().await {
                log::error!("Server error: {}", e);
            }
        });

        // Give server time to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Send initialize request
        let init_request = Request::new(
            "initialize",
            Some(serde_json::json!({
                "implementation": {
                    "name": "test-client",
                    "version": "0.1.0"
                },
                "capabilities": {},
                "protocolVersion": "2024-11-05"
            })),
            RequestId::Number(1),
        );

        let _ = client_tx.send(Ok(Message::Request(init_request)));

        // Try to receive response with 5 second timeout
        let result = tokio::time::timeout(Duration::from_secs(5), client_rx.recv()).await;

        // Should succeed
        assert!(result.is_ok(), "Operation should complete before timeout");
        if let Ok(Ok(Ok(Message::Response(response)))) = result {
            assert!(
                response.error.is_none(),
                "Response should not contain error"
            );
            assert!(response.result.is_some(), "Response should contain result");
        } else {
            panic!("Expected successful response");
        }

        // Send initialized notification
        let _ = client_tx.send(Ok(Message::Notification(Notification::new(
            "initialized",
            None,
        ))));

        // Give server time to process notification
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Try a custom method
        let method_request = Request::new(
            "test_method",
            Some(serde_json::json!({"key": "value"})),
            RequestId::Number(2),
        );

        let _ = client_tx.send(Ok(Message::Request(method_request)));

        // Try to receive response with 5 second timeout
        let result = tokio::time::timeout(Duration::from_secs(5), client_rx.recv()).await;

        // Should succeed
        assert!(result.is_ok(), "Operation should complete before timeout");
        if let Ok(Ok(Ok(Message::Response(response)))) = result {
            assert!(
                response.error.is_none(),
                "Response should not contain error"
            );
            assert_eq!(
                response.result,
                Some(serde_json::json!({"status": "ok"})),
                "Response should match handler result"
            );
        } else {
            panic!("Expected successful response");
        }

        // Cleanup
        let _ = client_tx.send(Ok(Message::Notification(Notification::new("exit", None))));
        let _ = tokio::time::timeout(Duration::from_secs(1), server_handle).await;
    }

    #[tokio::test]
    async fn test_server_error_handling() {
        // Create transport and server
        let (transport, client_tx, mut client_rx) = MockTransport::new();
        let handler = TestHandler::new(
            Duration::from_millis(100),
            Duration::from_millis(100),
            Duration::from_millis(100),
        );
        let server = Server::new(Arc::new(transport), Arc::new(handler));

        // Start server in background
        let server_handle = tokio::spawn(async move {
            if let Err(e) = server.start().await {
                log::error!("Server error: {}", e);
            }
        });

        // Give server time to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Try a method before initialization
        let method_request = Request::new(
            "test_method",
            Some(serde_json::json!({"key": "value"})),
            RequestId::Number(1),
        );

        let _ = client_tx.send(Ok(Message::Request(method_request)));

        // Should receive error response
        let result = tokio::time::timeout(Duration::from_secs(5), client_rx.recv()).await;

        assert!(result.is_ok(), "Should receive error response");
        if let Ok(Ok(Ok(Message::Response(response)))) = result {
            assert!(response.error.is_some(), "Response should contain error");
            assert_eq!(
                response.error.as_ref().unwrap().code,
                ErrorCode::ServerNotInitialized as i32,
                "Should receive server not initialized error"
            );
        } else {
            panic!("Expected error response");
        }

        // Cleanup
        let _ = client_tx.send(Ok(Message::Notification(Notification::new("exit", None))));
        let _ = tokio::time::timeout(Duration::from_secs(1), server_handle).await;
    }

    #[tokio::test]
    async fn test_double_initialization() {
        // Create transport and server
        let (transport, client_tx, mut client_rx) = MockTransport::new();
        let handler = TestHandler::new(
            Duration::from_millis(100),
            Duration::from_millis(100),
            Duration::from_millis(100),
        );
        let server = Server::new(Arc::new(transport), Arc::new(handler));

        // Start server in background
        let server_handle = tokio::spawn(async move {
            if let Err(e) = server.start().await {
                log::error!("Server error: {}", e);
            }
        });

        // Give server time to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Send first initialize request
        let init_request = Request::new(
            "initialize",
            Some(serde_json::json!({
                "implementation": {
                    "name": "test-client",
                    "version": "0.1.0"
                },
                "capabilities": {},
                "protocolVersion": "2024-11-05"
            })),
            RequestId::Number(1),
        );

        let _ = client_tx.send(Ok(Message::Request(init_request.clone())));

        // Wait for first response
        let _ = client_rx.recv().await;

        // Send initialized notification
        let _ = client_tx.send(Ok(Message::Notification(Notification::new(
            "initialized",
            None,
        ))));

        // Give server time to process notification
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Send second initialize request
        let init_request2 = Request::new(
            "initialize",
            Some(serde_json::json!({
                "implementation": {
                    "name": "test-client",
                    "version": "0.1.0"
                },
                "capabilities": {},
                "protocolVersion": "2024-11-05"
            })),
            RequestId::Number(2),
        );

        let _ = client_tx.send(Ok(Message::Request(init_request2)));

        // Try to receive response
        let result = tokio::time::timeout(Duration::from_secs(5), client_rx.recv()).await;

        // Should receive error response
        assert!(result.is_ok(), "Should receive response");
        if let Ok(Ok(Ok(Message::Response(response)))) = result {
            assert!(response.error.is_some(), "Response should contain error");
            assert_eq!(
                response.error.as_ref().unwrap().code,
                ErrorCode::InvalidRequest as i32,
                "Should receive invalid request error"
            );
        } else {
            panic!("Expected error response");
        }

        // Cleanup
        let _ = client_tx.send(Ok(Message::Notification(Notification::new("exit", None))));
        let _ = tokio::time::timeout(Duration::from_secs(1), server_handle).await;
    }

    #[tokio::test]
    async fn test_shutdown_handling() {
        // Create transport and server
        let (transport, client_tx, mut client_rx) = MockTransport::new();
        let handler = TestHandler::new(
            Duration::from_millis(100),
            Duration::from_millis(100),
            Duration::from_millis(100),
        );
        let server = Server::new(Arc::new(transport), Arc::new(handler));

        // Start server in background
        let server_handle = tokio::spawn(async move {
            if let Err(e) = server.start().await {
                log::error!("Server error: {}", e);
            }
        });

        // Give server time to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Send initialize request
        let init_request = Request::new(
            "initialize",
            Some(serde_json::json!({
                "implementation": {
                    "name": "test-client",
                    "version": "0.1.0"
                },
                "capabilities": {},
                "protocolVersion": "2024-11-05"
            })),
            RequestId::Number(1),
        );

        let _ = client_tx.send(Ok(Message::Request(init_request)));

        // Wait for response
        let _ = client_rx.recv().await;

        // Send initialized notification
        let _ = client_tx.send(Ok(Message::Notification(Notification::new(
            "initialized",
            None,
        ))));

        // Give server time to process notification
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Send shutdown request
        let shutdown_request = Request::new("shutdown", None, RequestId::Number(2));

        let _ = client_tx.send(Ok(Message::Request(shutdown_request)));

        // Try to receive response
        let result = tokio::time::timeout(Duration::from_secs(5), client_rx.recv()).await;

        // Should receive success response
        assert!(result.is_ok(), "Should receive response");
        if let Ok(Ok(Ok(Message::Response(response)))) = result {
            assert!(
                response.error.is_none(),
                "Response should not contain error"
            );
            assert!(
                response.result.is_none(),
                "Response should not contain result"
            );
        } else {
            panic!("Expected success response");
        }

        // Send exit notification
        let _ = client_tx.send(Ok(Message::Notification(Notification::new("exit", None))));

        // Server should exit
        let _ = tokio::time::timeout(Duration::from_secs(1), server_handle).await;
    }

    #[tokio::test]
    async fn test_invalid_message_handling() {
        // Create transport and server
        let (transport, client_tx, mut client_rx) = MockTransport::new();
        let handler = TestHandler::new(
            Duration::from_millis(100),
            Duration::from_millis(100),
            Duration::from_millis(100),
        );
        let server = Server::new(Arc::new(transport), Arc::new(handler));

        // Start server in background
        let server_handle = tokio::spawn(async move {
            if let Err(e) = server.start().await {
                log::error!("Server error: {}", e);
            }
        });

        // Give server time to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Send a response message (which server should not receive)
        let response = Response::success(RequestId::Number(1), None);
        let _ = client_tx.send(Ok(Message::Response(response)));

        // Give server time to process
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Server should have exited with error
        let result = tokio::time::timeout(Duration::from_secs(1), server_handle).await;
        assert!(result.is_ok(), "Server should exit");

        // Cleanup
        let _ = client_tx.send(Ok(Message::Notification(Notification::new("exit", None))));
    }
}
