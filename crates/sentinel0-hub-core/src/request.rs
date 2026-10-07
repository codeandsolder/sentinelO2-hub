//! Runtime-independent direct-request coordination.
//!
//! HTTP, WebSocket, persistence and clocks stay in the adapters. This module
//! owns the semantics that must be identical across them: operation parsing,
//! host selection, payload normalization, request-ID validation/fingerprinting,
//! wire request construction and agent-response normalization.

use crate::{HostRegistry, HostResolutionError, invocation_fingerprint, parse_op};
use sentinel0_proto::{Message, Op, ResponseError};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use thiserror::Error;

pub const MAX_CLIENT_REQUEST_ID_BYTES: usize = 256;

#[derive(Debug, Clone)]
pub struct DirectRequestInput {
    pub op_name: String,
    pub host_selector: Option<String>,
    pub payload: Value,
    pub client_request_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PreparedDirectRequest {
    pub op: Op,
    pub host_id: String,
    pub payload: BTreeMap<String, Value>,
    pub client_request_id: Option<String>,
    pub invocation_fingerprint: Option<String>,
}

impl PreparedDirectRequest {
    #[must_use]
    pub fn background_requested(&self) -> bool {
        self.payload
            .get("background")
            .and_then(Value::as_bool)
            .unwrap_or(false)
    }

    pub fn assign_background_job_id(&mut self, job_id: &str) {
        self.payload
            .insert("job_id".to_owned(), Value::String(job_id.to_owned()));
    }

    #[must_use]
    pub fn wire_message(&self, request_id: String) -> Message {
        Message::Request {
            id: request_id,
            op: self.op,
            payload: self.payload.clone(),
            deadline: None,
            opaque_ref: None,
        }
    }
}

#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum DirectRequestError {
    #[error("unsupported op {0:?}")]
    UnsupportedOp(String),
    #[error("payload must be an object, got {0}")]
    InvalidPayload(String),
    #[error("client_request_id must contain 1..=256 bytes")]
    InvalidClientRequestId,
    #[error(transparent)]
    Host(#[from] HostResolutionError),
}

/// Resolve and normalize one direct Hub invocation before adapter-specific I/O.
///
/// # Errors
///
/// Returns a stable semantic error for unsupported operations, invalid payloads,
/// invalid request IDs or host-selection failures.
pub fn prepare_direct_request(
    registry: &HostRegistry,
    input: DirectRequestInput,
) -> Result<PreparedDirectRequest, DirectRequestError> {
    let DirectRequestInput {
        op_name,
        host_selector,
        payload,
        client_request_id,
    } = input;

    let op = parse_op(&op_name).ok_or(DirectRequestError::UnsupportedOp(op_name))?;
    if client_request_id
        .as_deref()
        .is_some_and(|id| id.is_empty() || id.len() > MAX_CLIENT_REQUEST_ID_BYTES)
    {
        return Err(DirectRequestError::InvalidClientRequestId);
    }

    let host_id = registry.resolve(host_selector.as_deref())?.host_id.clone();
    let values = match payload {
        Value::Object(values) => values,
        Value::Null => serde_json::Map::new(),
        other => return Err(DirectRequestError::InvalidPayload(other.to_string())),
    };
    let payload_value = Value::Object(values.clone());
    let payload = values.into_iter().collect::<BTreeMap<String, Value>>();
    let fingerprint = client_request_id
        .as_ref()
        .map(|_| invocation_fingerprint(op, &host_id, &payload_value));

    Ok(PreparedDirectRequest {
        op,
        host_id,
        payload,
        client_request_id,
        invocation_fingerprint: fingerprint,
    })
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DirectResponse {
    pub ok: bool,
    pub result: Option<BTreeMap<String, Value>>,
    pub error: Option<ResponseError>,
    pub hub_request_id: String,
    pub client_request_id: Option<String>,
    pub replayed: bool,
}

impl DirectResponse {
    #[must_use]
    pub fn replayed(mut self) -> Self {
        self.replayed = true;
        self
    }

    #[must_use]
    pub fn running_job_id(&self) -> Option<&str> {
        let result = self.result.as_ref()?;
        (self.ok && result.get("status").and_then(Value::as_str) == Some("running"))
            .then(|| result.get("job_id").and_then(Value::as_str))
            .flatten()
    }
}

#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum DirectResponseError {
    #[error("unexpected agent message; expected response")]
    UnexpectedMessage,
}

/// Convert a protocol response into the caller-visible direct-response shape.
/// Internal `_sx_timing` diagnostics are stripped here for every adapter.
///
/// # Errors
///
/// Returns [`DirectResponseError::UnexpectedMessage`] when the message is not
/// an agent response.
pub fn normalize_agent_response(
    message: &Message,
    request_id: &str,
    client_request_id: Option<&str>,
    replayed: bool,
) -> Result<DirectResponse, DirectResponseError> {
    let Message::Response {
        ok, result, error, ..
    } = message
    else {
        return Err(DirectResponseError::UnexpectedMessage);
    };
    let mut result = result.clone();
    if let Some(result) = result.as_mut() {
        result.remove("_sx_timing");
    }

    Ok(DirectResponse {
        ok: *ok,
        result,
        error: error.clone(),
        hub_request_id: request_id.to_owned(),
        client_request_id: client_request_id.map(str::to_owned),
        replayed,
    })
}
