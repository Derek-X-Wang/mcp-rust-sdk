//! Standard I/O Transport Implementation
//!
//! This module provides a transport implementation that uses standard input/output (stdio)
//! for communication. This is particularly useful for:
//! - Command-line tools that need to communicate with an MCP server
//! - Local development and testing
//! - Situations where network transport is not desired or available
//!
//! The implementation uses Tokio for asynchronous I/O operations and provides thread-safe
//! access to stdin/stdout through Arc and Mutex.

use async_trait::async_trait;
use futures::Stream;
use std::{
    io::Write,
    pin::Pin,
    sync::{Arc, Mutex},
};
use tokio::{
    io::{AsyncBufReadExt, BufReader as TokioBufReader},
    sync::broadcast,
};

use crate::{
    error::{Error, ErrorCode},
    transport::{Message, Transport},
};

/// A transport implementation that uses standard input/output for communication.
///
/// This transport is suitable for scenarios where the client and server communicate
/// through stdin/stdout, such as command-line applications or local development.
///
/// # Thread Safety
///
/// The implementation is thread-safe, using Arc and Mutex to protect shared access
/// to stdin/stdout. This allows the transport to be used safely across multiple
/// threads in an async context.
///
/// # Message Flow
///
/// - Input: Messages are read line by line from stdin, parsed as JSON-RPC messages
/// - Output: Messages are serialized to JSON and written to stdout
///
/// # Example
///
/// ```rust
/// use mcp_rust_sdk::transport::stdio::StdioTransport;
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let transport = StdioTransport::new();
///     Ok(())
/// }
/// ```
pub struct StdioTransport {
    /// Thread-safe handle to output
    stdout: Arc<Mutex<std::io::Stdout>>,
    /// Receiver for messages read from stdin
    receiver: broadcast::Receiver<Result<Message, Error>>,
}

impl StdioTransport {
    /// Creates a new stdio transport instance using actual stdin/stdout.
    pub fn new() -> (Self, broadcast::Sender<Result<Message, Error>>) {
        let (sender, receiver) = broadcast::channel(100);
        let transport = Self {
            stdout: Arc::new(Mutex::new(std::io::stdout())),
            receiver,
        };

        // Start reading from stdin in a separate task
        let stdin = tokio::io::stdin();
        let mut reader = TokioBufReader::new(stdin);
        let sender_clone = sender.clone();
        tokio::spawn(async move {
            let mut line = String::new();
            loop {
                line.clear();
                match reader.read_line(&mut line).await {
                    Ok(0) => break, // EOF
                    Ok(_) => {
                        let message = match serde_json::from_str(&line) {
                            Ok(message) => Ok(message),
                            Err(err) => Err(Error::Serialization(err.to_string())),
                        };

                        if sender_clone.send(message).is_err() {
                            break;
                        }
                    }
                    Err(err) => {
                        let _ = sender_clone.send(Err(Error::Io(err.to_string())));
                        break;
                    }
                }
            }
        });

        (transport, sender)
    }
}

#[async_trait]
impl Transport for StdioTransport {
    /// Sends a message by writing it to stdout.
    ///
    /// # Arguments
    ///
    /// * `message` - The message to send
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if the message was successfully written to stdout,
    /// or an error if the write failed or stdout was locked.
    ///
    /// # Errors
    ///
    /// This function will return an error if:
    /// - Failed to acquire the stdout lock
    /// - Failed to serialize the message to JSON
    /// - Failed to write to stdout
    /// - Failed to flush stdout
    async fn send(&self, message: Message) -> Result<(), Error> {
        let mut stdout = self.stdout.lock().map_err(|_e| {
            Error::protocol(ErrorCode::InternalError, "Failed to acquire stdout lock")
        })?;

        let json = serde_json::to_string(&message)?;
        writeln!(stdout, "{}", json).map_err(|e| Error::Io(e.to_string()))?;
        stdout.flush().map_err(|e| Error::Io(e.to_string()))?;

        Ok(())
    }

    /// Creates a stream of messages received from stdin.
    ///
    /// # Returns
    ///
    /// Returns a pinned box containing a stream that yields Result<Message, Error>.
    /// The stream will continue until stdin is closed or an error occurs.
    ///
    /// # Message Flow
    ///
    /// 1. Messages are read from stdin in the background task created in `new()`
    /// 2. Each message is sent through the broadcast channel
    /// 3. This stream receives messages from the broadcast channel
    fn receive(&self) -> Pin<Box<dyn Stream<Item = Result<Message, Error>> + Send>> {
        let rx = self.receiver.resubscribe();
        Box::pin(futures::stream::unfold(rx, |mut rx| async move {
            match rx.recv().await {
                Ok(msg) => Some((msg, rx)),
                Err(_) => None,
            }
        }))
    }

    /// Closes the transport.
    ///
    /// For stdio transport, this is a no-op as we don't own stdin/stdout.
    ///
    /// # Returns
    ///
    /// Always returns `Ok(())`.
    async fn close(&self) -> Result<(), Error> {
        // Nothing to do for stdio transport
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{Request, RequestId};
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;
    use tokio::runtime::Runtime;
    use tokio::sync::broadcast;

    #[test]
    fn test_stdio_transport() {
        // Create a oneshot channel to verify message sending
        let (verify_tx, verify_rx) = mpsc::channel();

        // Spawn a thread to handle the transport operations
        thread::spawn(move || {
            // Create a tokio runtime for async operations
            let rt = Runtime::new().unwrap();

            // Create test message
            let request = Request::new(
                "test_method",
                Some(serde_json::json!({"key": "value"})),
                RequestId::Number(1),
            );
            let message = Message::Request(request);

            // Create transport with broadcast channel
            let (_, receiver) = broadcast::channel(100);
            let transport = StdioTransport {
                stdout: Arc::new(Mutex::new(std::io::stdout())),
                receiver,
            };

            // Test sending a message
            let send_result = rt.block_on(transport.send(message.clone()));
            verify_tx.send(send_result.is_ok()).unwrap();
        });

        // Wait for the result with timeout
        match verify_rx.recv_timeout(Duration::from_secs(1)) {
            Ok(true) => (), // Test passed
            Ok(false) => panic!("Failed to send message"),
            Err(_) => panic!("Test timed out"),
        }
    }
}
