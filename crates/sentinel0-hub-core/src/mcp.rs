//! Stateless MCP 2026-07-28 wire helpers shared by Hub adapters.

use crate::{DirectResponse, DirectTool, direct_tool_by_public_name, direct_tool_catalog};
use serde::Deserialize;
use serde_json::{Map, Value, json};
use thiserror::Error;

pub const MCP_PROTOCOL_VERSION: &str = "2026-07-28";
pub const MCP_HEADER_MISMATCH: i64 = -32_020;
pub const JSONRPC_INVALID_REQUEST: i64 = -32_600;
pub const JSONRPC_METHOD_NOT_FOUND: i64 = -32_601;
pub const JSONRPC_INVALID_PARAMS: i64 = -32_602;

#[derive(Debug, Clone, Deserialize)]
pub struct McpRequest {
    pub jsonrpc: String,
    pub id: Value,
    pub method: String,
    #[serde(default)]
    pub params: Value,
}

#[derive(Debug, Clone, PartialEq)]
pub struct McpDirectCall {
    pub tool_name: String,
    pub op_name: String,
    pub host_selector: Option<String>,
    pub payload: Value,
}

#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum McpRequestError {
    #[error("jsonrpc must be exactly 2.0")]
    InvalidJsonRpc,
    #[error("MCP-Protocol-Version must be 2026-07-28")]
    ProtocolVersion,
    #[error("mcp-method header does not match the request body's method")]
    MethodHeaderMismatch,
    #[error("mcp-name header does not match the request body's name")]
    NameHeaderMismatch,
    #[error("request _meta protocol version does not match 2026-07-28")]
    MetaProtocolVersion,
    #[error("method not supported: {0}")]
    MethodNotFound(String),
    #[error("tools/call params must be an object")]
    InvalidCallParams,
    #[error("tools/call requires a string name")]
    MissingToolName,
    #[error("unknown tool: {0}")]
    UnknownTool(String),
    #[error("tools/call arguments must be an object")]
    InvalidToolArguments,
    #[error("host_id must be a string or null")]
    InvalidHostSelector,
}

impl McpRequestError {
    #[must_use]
    pub const fn jsonrpc_code(&self) -> i64 {
        match self {
            Self::MethodHeaderMismatch | Self::NameHeaderMismatch => MCP_HEADER_MISMATCH,
            Self::MethodNotFound(_) => JSONRPC_METHOD_NOT_FOUND,
            Self::InvalidCallParams
            | Self::MissingToolName
            | Self::UnknownTool(_)
            | Self::InvalidToolArguments
            | Self::InvalidHostSelector
            | Self::MetaProtocolVersion
            | Self::ProtocolVersion => JSONRPC_INVALID_PARAMS,
            Self::InvalidJsonRpc => JSONRPC_INVALID_REQUEST,
        }
    }
}

#[must_use]
pub fn mcp_server_info() -> Value {
    json!({
        "name": "sentinel0-hub",
        "version": env!("CARGO_PKG_VERSION")
    })
}

#[must_use]
pub fn mcp_response_meta() -> Value {
    json!({
        "io.modelcontextprotocol/serverInfo": mcp_server_info()
    })
}

#[must_use]
pub fn mcp_jsonrpc_result(id: &Value, mut result: Value) -> Value {
    if let Value::Object(values) = &mut result {
        values
            .entry("resultType".to_owned())
            .or_insert_with(|| Value::String("complete".to_owned()));
        values
            .entry("_meta".to_owned())
            .or_insert_with(mcp_response_meta);
    }
    json!({"jsonrpc": "2.0", "id": id, "result": result})
}

#[must_use]
pub fn mcp_jsonrpc_error(id: &Value, code: i64, message: impl Into<String>) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": {"code": code, "message": message.into()}
    })
}

#[must_use]
pub fn mcp_discover_response(id: &Value) -> Value {
    mcp_jsonrpc_result(
        id,
        json!({
            "supportedVersions": [MCP_PROTOCOL_VERSION],
            "capabilities": {"tools": {"listChanged": false}},
            "instructions": "Sentinel0² exposes policy-gated host operations. Host policy and OS permissions remain independent enforcement boundaries.",
            "ttlMs": 0,
            "cacheScope": "private"
        }),
    )
}

#[must_use]
pub fn mcp_tools_list_response(id: &Value, tools: &[DirectTool]) -> Value {
    mcp_jsonrpc_result(
        id,
        json!({
            "tools": direct_tool_catalog(tools),
            "ttlMs": 0,
            "cacheScope": "private"
        }),
    )
}

/// Validate the 2026-07-28 per-request envelope and routable headers.
///
/// # Errors
/// Returns a stable MCP request error when the body or routing headers disagree.
pub fn validate_modern_mcp_request(
    request: &McpRequest,
    protocol_header: Option<&str>,
    method_header: Option<&str>,
    name_header: Option<&str>,
) -> Result<(), McpRequestError> {
    if request.jsonrpc != "2.0" {
        return Err(McpRequestError::InvalidJsonRpc);
    }
    if protocol_header != Some(MCP_PROTOCOL_VERSION) {
        return Err(McpRequestError::ProtocolVersion);
    }
    if method_header != Some(request.method.as_str()) {
        return Err(McpRequestError::MethodHeaderMismatch);
    }
    let meta_version = request
        .params
        .as_object()
        .and_then(|params| params.get("_meta"))
        .and_then(Value::as_object)
        .and_then(|meta| meta.get("io.modelcontextprotocol/protocolVersion"))
        .and_then(Value::as_str);
    if meta_version != Some(MCP_PROTOCOL_VERSION) {
        return Err(McpRequestError::MetaProtocolVersion);
    }
    if request.method == "tools/call" {
        let body_name = request
            .params
            .as_object()
            .and_then(|params| params.get("name"))
            .and_then(Value::as_str);
        if name_header != body_name {
            return Err(McpRequestError::NameHeaderMismatch);
        }
    }
    Ok(())
}

/// Convert one model-facing direct tool call into the existing REST/direct request shape.
///
/// # Errors
/// Returns an MCP parameter/tool error before any host operation is dispatched.
pub fn parse_mcp_direct_call(request: &McpRequest) -> Result<McpDirectCall, McpRequestError> {
    if request.method != "tools/call" {
        return Err(McpRequestError::MethodNotFound(request.method.clone()));
    }
    let params = request
        .params
        .as_object()
        .ok_or(McpRequestError::InvalidCallParams)?;
    let tool_name = params
        .get("name")
        .and_then(Value::as_str)
        .ok_or(McpRequestError::MissingToolName)?;
    let tool = direct_tool_by_public_name(tool_name)
        .ok_or_else(|| McpRequestError::UnknownTool(tool_name.to_owned()))?;
    let mut arguments = match params.get("arguments") {
        None | Some(Value::Null) => Map::new(),
        Some(Value::Object(values)) => values.clone(),
        Some(_) => return Err(McpRequestError::InvalidToolArguments),
    };
    let host_selector = match arguments.remove("host_id") {
        None | Some(Value::Null) => None,
        Some(Value::String(value)) => Some(value),
        Some(_) => return Err(McpRequestError::InvalidHostSelector),
    };
    Ok(McpDirectCall {
        tool_name: tool_name.to_owned(),
        op_name: tool.op.as_str().to_owned(),
        host_selector,
        payload: Value::Object(arguments),
    })
}

#[must_use]
pub fn mcp_tool_success(result: &Value) -> Value {
    let text = serde_json::to_string(result).unwrap_or_else(|_| "null".to_owned());
    json!({
        "content": [{"type": "text", "text": text}],
        "structuredContent": result,
        "isError": false
    })
}

#[must_use]
pub fn mcp_tool_error(message: impl Into<String>, structured: Option<Value>) -> Value {
    let message = message.into();
    let mut result = json!({
        "content": [{"type": "text", "text": message}],
        "isError": true
    });
    if let (Value::Object(values), Some(structured)) = (&mut result, structured) {
        values.insert("structuredContent".to_owned(), structured);
    }
    result
}

#[must_use]
pub fn mcp_tool_result_from_direct(response: &DirectResponse) -> Value {
    if response.ok {
        let result = response.result.as_ref().map_or_else(
            || json!({"ok": true}),
            |values| {
                Value::Object(
                    values
                        .iter()
                        .map(|(key, value)| (key.clone(), value.clone()))
                        .collect(),
                )
            },
        );
        return mcp_tool_success(&result);
    }
    let error = response.error.as_ref();
    let message = error.map_or_else(
        || "agent operation failed".to_owned(),
        |error| format!("{}: {}", error.code, error.message),
    );
    let structured = error.map(|error| {
        json!({
            "ok": false,
            "error": {
                "code": error.code,
                "message": error.message,
                "details": error.details
            }
        })
    });
    mcp_tool_error(message, structured)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DIRECT_TOOLS;
    use sentinel0_proto::ResponseError;
    use std::collections::BTreeMap;

    fn request(method: &str, params: Value) -> McpRequest {
        McpRequest {
            jsonrpc: "2.0".to_owned(),
            id: json!(1),
            method: method.to_owned(),
            params,
        }
    }

    #[test]
    fn modern_headers_are_cross_checked() {
        let req = request(
            "tools/call",
            json!({
                "name": "sentinel_ping",
                "arguments": {},
                "_meta": {
                    "io.modelcontextprotocol/protocolVersion": MCP_PROTOCOL_VERSION
                }
            }),
        );
        assert_eq!(
            validate_modern_mcp_request(
                &req,
                Some(MCP_PROTOCOL_VERSION),
                Some("tools/call"),
                Some("sentinel_ping")
            ),
            Ok(())
        );
        assert_eq!(
            validate_modern_mcp_request(
                &req,
                Some(MCP_PROTOCOL_VERSION),
                Some("tools/list"),
                Some("sentinel_ping")
            ),
            Err(McpRequestError::MethodHeaderMismatch)
        );
    }

    #[test]
    fn direct_call_lifts_host_but_keeps_opaque_ref_in_payload() -> Result<(), McpRequestError> {
        let req = request(
            "tools/call",
            json!({
                "name": "sentinel_exec",
                "arguments": {
                    "host_id": "build",
                    "command": "true",
                    "opaque_ref": "trace"
                },
                "_meta": {
                    "io.modelcontextprotocol/protocolVersion": MCP_PROTOCOL_VERSION
                }
            }),
        );
        let call = parse_mcp_direct_call(&req)?;
        assert_eq!(call.op_name, "exec");
        assert_eq!(call.host_selector.as_deref(), Some("build"));
        assert_eq!(call.payload["command"], "true");
        assert_eq!(call.payload["opaque_ref"], "trace");
        assert!(call.payload.get("host_id").is_none());
        Ok(())
    }

    #[test]
    fn tools_list_is_modern_cacheable_result() {
        let id = json!(7);
        let response = mcp_tools_list_response(&id, &DIRECT_TOOLS);
        assert_eq!(response["jsonrpc"], "2.0");
        assert_eq!(response["result"]["resultType"], "complete");
        assert_eq!(response["result"]["ttlMs"], 0);
        assert_eq!(response["result"]["cacheScope"], "private");
        assert_eq!(
            response["result"]["tools"].as_array().map(Vec::len),
            Some(25)
        );
    }

    #[test]
    fn agent_failure_becomes_tool_error_result() {
        let response = DirectResponse {
            ok: false,
            result: None,
            error: Some(ResponseError {
                code: "denied".to_owned(),
                message: "nope".to_owned(),
                details: Some(BTreeMap::from([("retryable".to_owned(), json!(false))])),
            }),
            hub_request_id: "hreq".to_owned(),
            client_request_id: None,
            replayed: false,
        };
        let result = mcp_tool_result_from_direct(&response);
        assert_eq!(result["isError"], true);
        assert_eq!(result["structuredContent"]["error"]["code"], "denied");
    }
}
