use crate::{
    error::{Error, ErrorCode},
    protocol::{Notification, Request, RequestId, Response},
    transport::{Message, Transport},
    types::{ClientCapabilities, Implementation, ServerCapabilities},
};
use async_trait::async_trait;
use futures::StreamExt;
use serde_json::{json, Value};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::{
    mpsc::{UnboundedReceiver, UnboundedSender},
    Mutex, RwLock,
};

/// Trait for implementing MCP client handlers
#[async_trait]
pub trait ClientHandler: Send + Sync {
    /// Start the handler and listen for messages
    async fn start(self) -> Result<(), Error>;

    /// Handle shutdown request
    async fn shutdown(&self) -> Result<(), Error>;

    /// Handle custom method calls
    async fn handle_method(
        &self,
        method: String,
        params: Option<serde_json::Value>,
    ) -> Result<serde_json::Value, Error>;

    /// Handle notifications
    async fn handle_notification(
        &self,
        method: String,
        params: Option<serde_json::Value>,
    ) -> Result<(), Error>;
}

#[derive(Clone)]
pub struct DefaultClientHandler {
    transport: Arc<dyn Transport>,
    receiver: Arc<Mutex<UnboundedReceiver<Message>>>,
    sender: Arc<UnboundedSender<Message>>,
}
impl DefaultClientHandler {
    pub fn new(
        transport: Arc<dyn Transport>,
        sender: UnboundedSender<Message>,
        receiver: UnboundedReceiver<Message>,
    ) -> Self {
        Self {
            transport,
            sender: Arc::new(sender),
            receiver: Arc::new(Mutex::new(receiver)),
        }
    }
}
#[async_trait]
impl ClientHandler for DefaultClientHandler {
    async fn start(self) -> Result<(), Error> {
        let cl = self.clone();
        // listen for messages from the server
        tokio::spawn(async move {
            let mut stream = cl.transport.receive();
            while let Some(result) = stream.next().await {
                match result {
                    Ok(message) => match &message {
                        Message::Request(r) => {
                            let res = cl.handle_method(r.method.clone(), r.params.clone()).await;
                            if cl
                                .transport
                                .send(Message::Response(Response::success(
                                    r.id.clone(),
                                    Some(res.unwrap()),
                                )))
                                .await
                                .is_err()
                            {
                                break;
                            }
                        }
                        Message::Response(_) => {
                            if cl.sender.send(message).is_err() {
                                break;
                            }
                        }
                        Message::Notification(n) => {
                            if cl
                                .handle_notification(n.method.clone(), n.params.clone())
                                .await
                                .is_err()
                            {
                                break;
                            }
                        }
                    },
                    Err(_) => break,
                }
            }
        });
        // listen for requests from the client
        let rx_clone = self.receiver.clone();
        let tx_clone = self.transport.clone();
        tokio::spawn(async move {
            let mut stream = rx_clone.lock().await;
            while let Some(message) = stream.recv().await {
                tx_clone.send(message).await.unwrap();
            }
        });
        Ok(())
    }

    async fn handle_method(&self, method: String, _params: Option<Value>) -> Result<Value, Error> {
        match method.as_str() {
            // TODO: allow a commune client to impl this
            "sampling/createMessage" => {
                println!("Got sampling/createMessage");
                Ok(json!({}))
            }
            _ => Err(Error::Other("unknown method".to_string())),
        }
    }

    async fn handle_notification(
        &self,
        method: String,
        params: Option<Value>,
    ) -> Result<(), Error> {
        match method.as_str() {
            "notifications/resources/updated" => {
                if let Some(p) = params {
                    let update_params: HashMap<String, Value> = serde_json::from_value(p)?;
                    if let Some(uri_val) = update_params.get("uri") {
                        let uri = uri_val.as_str().ok_or("some file").unwrap();
                        println!("Resource {uri} was updated");
                    }
                }
                Ok(())
            }
            "notifications/resources/list_changed" => {
                println!("Got list changed notification!");
                Ok(())
            }
            _ => Err(Error::Other("unknown notification".to_string())),
        }
    }

    async fn shutdown(&self) -> Result<(), Error> {
        println!("Client shutting down");
        Ok(())
    }
}

/// MCP client state
#[derive(Clone)]
pub struct Client {
    server_capabilities: Arc<RwLock<Option<ServerCapabilities>>>,
    request_counter: Arc<RwLock<i64>>,
    #[allow(dead_code)]
    sender: Arc<UnboundedSender<Message>>,
    receiver: Arc<Mutex<UnboundedReceiver<Message>>>,
}

impl Client {
    /// Create a new MCP client
    pub fn new(sender: UnboundedSender<Message>, receiver: UnboundedReceiver<Message>) -> Self {
        Self {
            server_capabilities: Arc::new(RwLock::new(None)),
            request_counter: Arc::new(RwLock::new(0)),
            sender: Arc::new(sender),
            receiver: Arc::new(Mutex::new(receiver)),
        }
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
        let mut caps = ServerCapabilities::default();
        if let Some(resp_obj) = response.as_object() {
            if let Some(server_caps) = resp_obj.get("capabilities") {
                if let Some(exp) = server_caps.get("experimental") {
                    caps.experimental = Some(exp.clone());
                }
                if let Some(logging) = server_caps.get("logging") {
                    caps.logging = Some(logging.clone());
                }
                if let Some(prompts) = server_caps.get("prompts") {
                    caps.prompts = Some(prompts.clone());
                }
                if let Some(resources) = server_caps.get("resources") {
                    caps.resources = Some(resources.clone());
                }
                if let Some(tools) = server_caps.get("tools") {
                    caps.tools = Some(tools.clone());
                }
            }
        }
        *self.server_capabilities.write().await = Some(caps.clone());
        // Send initialized notification
        self.notify("initialized", None).await?;
        Ok(caps)
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
        self.sender
            .send(Message::Request(request))
            .map_err(|_| Error::Transport("failed to send request message".to_string()))?;
        // Wait for matching response
        let mut receiver = self.receiver.lock().await;
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

    pub async fn subscribe(&self, uri: &str) -> Result<(), Error> {
        let mut counter = self.request_counter.write().await;
        *counter += 1;
        let id = RequestId::Number(*counter);

        let request = Request::new("resources/subscribe", Some(json!({"uri": uri})), id.clone());
        self.sender.send(Message::Request(request)).map_err(|_| {
            Error::Transport("failed to send subscribe request message".to_string())
        })?;
        Ok(())
    }

    /// Send a notification to the server
    pub async fn notify(
        &self,
        method: &str,
        params: Option<serde_json::Value>,
    ) -> Result<(), Error> {
        let notification = Notification::new(method, params);
        self.sender
            .send(Message::Notification(notification))
            .map_err(|_| Error::Transport("failed to send notification message".to_string()))
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
        Ok(())
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
