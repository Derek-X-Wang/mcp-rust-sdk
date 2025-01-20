use async_trait::async_trait;
use futures::Stream;
use serde::{Deserialize, Serialize};
use std::pin::Pin;

use crate::protocol::{Notification, Request, Response};
use crate::Error;

/// A message that can be sent over a transport
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Message {
    #[serde(rename = "request")]
    Request(Request),
    #[serde(rename = "response")]
    Response(Response),
    #[serde(rename = "notification")]
    Notification(Notification),
}

/// Trait for implementing MCP transports
#[async_trait]
pub trait Transport: Send + Sync + 'static {
    /// Send a message over the transport
    async fn send(&self, message: Message) -> Result<(), Error>;

    /// Receive messages from the transport
    fn receive(&self) -> Pin<Box<dyn Stream<Item = Result<Message, Error>> + Send>>;

    /// Close the transport
    async fn close(&self) -> Result<(), Error>;
}

pub mod stdio;
pub mod websocket;
