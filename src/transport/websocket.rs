//! WebSocket Transport Implementation
//!
//! This module provides a transport implementation that uses WebSocket protocol
//! for communication. This transport is ideal for:
//! - Network-based client-server communication
//! - Real-time bidirectional messaging
//! - Web-based applications
//! - Scenarios requiring secure communication (WSS)
//!
//! The implementation uses tokio-tungstenite for WebSocket functionality and
//! provides thread-safe connection management through Arc and Mutex.

use async_trait::async_trait;
use futures::{
    stream::{SplitSink, SplitStream},
    Sink, SinkExt, Stream, StreamExt,
};
use std::{fmt::Display, pin::Pin, sync::Arc};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::Mutex;
use tokio_tungstenite::{
    connect_async,
    tungstenite::{error::Error as WsError, protocol::CloseFrame, protocol::Message as WsMessage},
    WebSocketStream,
};
use url::Url;

use crate::{
    error::Error,
    transport::{Message, Transport},
};

/// Type alias for the WebSocket connection stream
type WebSocketConnection<S> = WebSocketStream<S>;

/// A transport implementation that uses WebSocket protocol for communication.
///
/// This transport provides bidirectional communication over WebSocket protocol,
/// supporting both secure (WSS) and standard (WS) connections.
pub struct WebSocketTransport<S> {
    read_connection: Arc<Mutex<SplitStream<WebSocketConnection<S>>>>,
    write_connection: Arc<Mutex<SplitSink<WebSocketConnection<S>, WsMessage>>>,
}

impl<S> WebSocketTransport<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static,
{
    /// Creates a new WebSocket transport from an existing WebSocket stream.
    /// This is typically used on the server side when accepting a new connection.
    ///
    /// # Arguments
    ///
    /// * `stream` - The WebSocket stream from an accepted connection

    pub fn from_stream(stream: WebSocketConnection<S>) -> Self {
        let (w, r) = stream.split();
        Self {
            read_connection: Arc::new(Mutex::new(r)),
            write_connection: Arc::new(Mutex::new(w)),
        }
    }

    /// Converts an MCP message to a WebSocket message.
    fn convert_to_ws_message(message: &Message) -> Result<WsMessage, Error> {
        let json =
            serde_json::to_string(message).map_err(|e| Error::Serialization(e.to_string()))?;
        Ok(WsMessage::Text(json))
    }

    /// Parses a WebSocket message into an MCP message.
    fn parse_ws_message(ws_message: WsMessage) -> Result<Message, Error> {
        match ws_message {
            WsMessage::Text(text) => {
                serde_json::from_str(&text).map_err(|e| Error::Serialization(e.to_string()))
            }
            WsMessage::Binary(_) => Err(Error::Transport(
                "Binary messages not supported".to_string(),
            )),
            WsMessage::Ping(_) => Ok(Message::Notification(crate::protocol::Notification {
                jsonrpc: crate::protocol::JSONRPC_VERSION.to_string(),
                method: "ping".to_string(),
                params: None,
            })),
            WsMessage::Pong(_) => Ok(Message::Notification(crate::protocol::Notification {
                jsonrpc: crate::protocol::JSONRPC_VERSION.to_string(),
                method: "pong".to_string(),
                params: None,
            })),
            WsMessage::Close(_) => Err(Error::Transport("Connection closed".to_string())),
            WsMessage::Frame(_) => Err(Error::Transport("Raw frames not supported".to_string())),
        }
    }

    /// Handle a WebSocket message, including control messages
    async fn handle_ws_message<T, E>(
        connection: &mut T,
        message: WsMessage,
    ) -> Result<Option<Message>, Error>
    where
        T: Sink<WsMessage, Error = E> + Unpin,
        E: Display,
    {
        match message {
            WsMessage::Ping(data) => {
                // Automatically respond to ping with pong
                connection
                    .send(WsMessage::Pong(data))
                    .await
                    .map_err(|e| Error::Transport(e.to_string()))?;
                Ok(None)
            }
            WsMessage::Pong(_) => {
                // Ignore pong messages
                Ok(None)
            }
            _ => Self::parse_ws_message(message).map(Some),
        }
    }
}

#[async_trait]
impl<S> Transport for WebSocketTransport<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static,
{
    async fn send(&self, message: Message) -> Result<(), Error> {
        let ws_message = Self::convert_to_ws_message(&message)?;
        let mut connection = self.write_connection.lock().await;
        connection
            .send(ws_message)
            .await
            .map_err(|e| Error::Transport(e.to_string()))
    }

    fn receive(&self) -> Pin<Box<dyn Stream<Item = Result<Message, Error>> + Send>> {
        let read_connection = self.read_connection.clone();
        let write_connection = self.write_connection.clone();

        Box::pin(futures::stream::unfold(
            read_connection,
            move |read_connection| {
                let read_connection = read_connection.clone();
                let write_connection = write_connection.clone();
                async move {
                    loop {
                        let mut guard = read_connection.lock().await;
                        match guard.next().await {
                            Some(Ok(ws_message)) => {
                                drop(guard);
                                let mut guard = write_connection.lock().await;
                                match Self::handle_ws_message(&mut *guard, ws_message).await {
                                    Ok(Some(message)) => {
                                        return Some((Ok(message), read_connection.clone()))
                                    }
                                    Ok(None) => continue, // Control message handled, continue to next message
                                    Err(e) => return Some((Err(e), read_connection.clone())),
                                }
                            }
                            Some(Err(e)) => {
                                return Some((
                                    Err(Error::Transport(e.to_string())),
                                    read_connection.clone(),
                                ))
                            }
                            None => return None,
                        }
                    }
                }
            },
        ))
    }

    async fn close(&self) -> Result<(), Error> {
        let mut connection = self.write_connection.lock().await;
        // Send close frame with normal closure status
        connection
            .send(WsMessage::Close(Some(CloseFrame {
                code: 1000u16.into(), // Normal closure
                reason: "Client initiated close".into(),
            })))
            .await
            .map_err(|e| Error::Transport(e.to_string()))?;
        drop(connection);

        let mut connection = self.read_connection.lock().await;
        // Wait for the close frame from the server
        while let Some(msg) = connection.next().await {
            match msg {
                Ok(WsMessage::Close(_)) => break,
                Ok(_) => continue,
                Err(e) => {
                    if matches!(e, WsError::ConnectionClosed) {
                        break;
                    }
                    return Err(Error::Transport(e.to_string()));
                }
            }
        }

        Ok(())
    }
}

// Client-specific implementation
impl WebSocketTransport<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>> {
    /// Creates a new WebSocket transport as a client by connecting to the specified URL.
    ///
    /// # Arguments
    ///
    /// * `url` - The WebSocket URL to connect to (ws:// or wss://)
    ///
    /// # Returns
    ///
    /// Returns a Result containing:
    /// - Ok: The new WebSocketTransport instance
    /// - Err: An error if connection fails
    pub async fn new(url: &str) -> Result<Self, Error> {
        let url = Url::parse(url).map_err(|e| Error::Transport(e.to_string()))?;

        let (ws_stream, _) = connect_async(url)
            .await
            .map_err(|e| Error::Transport(e.to_string()))?;
        let (w, r) = ws_stream.split();
        Ok(Self {
            read_connection: Arc::new(Mutex::new(r)),
            write_connection: Arc::new(Mutex::new(w)),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{Notification, JSONRPC_VERSION};

    #[tokio::test]
    async fn test_message_conversion() {
        let message = Message::Notification(Notification {
            jsonrpc: JSONRPC_VERSION.to_string(),
            method: "test/method".to_string(),
            params: Some(serde_json::json!({
                "key": "value"
            })),
        });

        // Convert to WebSocket message
        let ws_message = WebSocketTransport::<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >::convert_to_ws_message(&message)
        .unwrap();
        assert!(matches!(ws_message, WsMessage::Text(_)));

        // Parse back to MCP message
        let parsed_message = WebSocketTransport::<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >::parse_ws_message(ws_message)
        .unwrap();
        assert!(matches!(parsed_message, Message::Notification(_)));

        if let Message::Notification(notification) = parsed_message {
            assert_eq!(notification.jsonrpc, JSONRPC_VERSION);
            assert_eq!(notification.method, "test/method");
            assert!(notification.params.is_some());
        }
    }
}
