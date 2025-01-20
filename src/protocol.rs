use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt;

use crate::error::{Error, ErrorCode};

pub const LATEST_PROTOCOL_VERSION: &str = "2024-11-05";
pub const SUPPORTED_PROTOCOL_VERSIONS: &[&str] = &[LATEST_PROTOCOL_VERSION, "2024-10-07"];
pub const JSONRPC_VERSION: &str = "2.0";

/// A unique identifier for a request
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RequestId {
    String(String),
    Number(i64),
}

/// Base JSON-RPC request structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Request {
    pub jsonrpc: String,
    pub method: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub params: Option<Value>,
    pub id: RequestId,
}

/// Base JSON-RPC notification structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Notification {
    pub jsonrpc: String,
    pub method: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub params: Option<Value>,
}

/// Base JSON-RPC response structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Response {
    pub jsonrpc: String,
    pub id: RequestId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ResponseError>,
}

/// JSON-RPC error object
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResponseError {
    pub code: i32,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

impl Request {
    pub fn new(method: impl Into<String>, params: Option<Value>, id: RequestId) -> Self {
        Self {
            jsonrpc: crate::JSONRPC_VERSION.to_string(),
            method: method.into(),
            params,
            id,
        }
    }
}

impl Notification {
    pub fn new(method: impl Into<String>, params: Option<Value>) -> Self {
        Self {
            jsonrpc: crate::JSONRPC_VERSION.to_string(),
            method: method.into(),
            params,
        }
    }
}

impl Response {
    pub fn success(id: RequestId, result: Option<Value>) -> Self {
        Self {
            jsonrpc: crate::JSONRPC_VERSION.to_string(),
            id,
            result,
            error: None,
        }
    }

    pub fn error(id: RequestId, error: ResponseError) -> Self {
        Self {
            jsonrpc: crate::JSONRPC_VERSION.to_string(),
            id,
            result: None,
            error: Some(error),
        }
    }
}

impl From<Error> for ResponseError {
    fn from(err: Error) -> Self {
        match err {
            Error::Protocol {
                code,
                message,
                data,
            } => ResponseError {
                code: code.into(),
                message,
                data,
            },
            Error::Transport(msg) => ResponseError {
                code: ErrorCode::InternalError.into(),
                message: format!("Transport error: {}", msg),
                data: None,
            },
            Error::Serialization(err) => ResponseError {
                code: ErrorCode::ParseError.into(),
                message: err.to_string(),
                data: None,
            },
            Error::Io(err) => ResponseError {
                code: ErrorCode::InternalError.into(),
                message: err.to_string(),
                data: None,
            },
            Error::Other(msg) => ResponseError {
                code: ErrorCode::InternalError.into(),
                message: msg,
                data: None,
            },
        }
    }
}

impl fmt::Display for RequestId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RequestId::String(s) => write!(f, "{}", s),
            RequestId::Number(n) => write!(f, "{}", n),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_request_creation() {
        let id = RequestId::Number(1);
        let params = Some(json!({"key": "value"}));
        let request = Request::new("test_method", params.clone(), id.clone());

        assert_eq!(request.jsonrpc, JSONRPC_VERSION);
        assert_eq!(request.method, "test_method");
        assert_eq!(request.params, params);
        assert_eq!(request.id, id);
    }

    #[test]
    fn test_notification_creation() {
        let params = Some(json!({"event": "update"}));
        let notification = Notification::new("test_event", params.clone());

        assert_eq!(notification.jsonrpc, JSONRPC_VERSION);
        assert_eq!(notification.method, "test_event");
        assert_eq!(notification.params, params);
    }

    #[test]
    fn test_response_success() {
        let id = RequestId::String("test-1".to_string());
        let result = Some(json!({"status": "ok"}));
        let response = Response::success(id.clone(), result.clone());

        assert_eq!(response.jsonrpc, JSONRPC_VERSION);
        assert_eq!(response.id, id);
        assert_eq!(response.result, result);
        assert!(response.error.is_none());
    }

    #[test]
    fn test_response_error() {
        let id = RequestId::Number(123);
        let error = ResponseError {
            code: -32600,
            message: "Invalid Request".to_string(),
            data: Some(json!({"details": "missing method"})),
        };
        let response = Response::error(id.clone(), error.clone());

        assert_eq!(response.jsonrpc, JSONRPC_VERSION);
        assert_eq!(response.id, id);
        assert!(response.result.is_none());

        let response_error = response.error.unwrap();
        assert_eq!(response_error.code, error.code);
        assert_eq!(response_error.message, error.message);
    }

    #[test]
    fn test_request_id_display() {
        let num_id = RequestId::Number(42);
        let str_id = RequestId::String("test-id".to_string());

        assert_eq!(num_id.to_string(), "42");
        assert_eq!(str_id.to_string(), "test-id");
    }

    #[test]
    fn test_protocol_versions() {
        assert!(SUPPORTED_PROTOCOL_VERSIONS.contains(&LATEST_PROTOCOL_VERSION));
        assert_eq!(JSONRPC_VERSION, "2.0");
    }
}
