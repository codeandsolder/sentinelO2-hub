//! Stateless MCP 2026-07-28 HTTP adapter for the native Hub.

use super::{AppState, OpRequest, v1_op};
use axum::{
    Json,
    body::{Bytes, to_bytes},
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use sentinel0_hub_core::{
    DIRECT_TOOLS, DirectResponse, JSONRPC_METHOD_NOT_FOUND, McpRequest, McpRequestError,
    mcp_discover_response, mcp_jsonrpc_error, mcp_jsonrpc_result, mcp_tool_error,
    mcp_tool_result_from_direct, mcp_tools_list_response, parse_mcp_direct_call,
    validate_modern_mcp_request,
};
use serde_json::Value;

const MCP_BODY_LIMIT: usize = 4 * 1024 * 1024;

pub(super) async fn mcp(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let request = match serde_json::from_slice::<McpRequest>(&body) {
        Ok(request) => request,
        Err(error) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(mcp_jsonrpc_error(
                    &Value::Null,
                    -32_700,
                    format!("invalid JSON-RPC request: {error}"),
                )),
            )
                .into_response();
        }
    };

    let protocol_header = header_str(&headers, "mcp-protocol-version");
    let method_header = header_str(&headers, "mcp-method");
    let name_header = header_str(&headers, "mcp-name");
    if let Err(error) =
        validate_modern_mcp_request(&request, protocol_header, method_header, name_header)
    {
        return modern_request_error(&request.id, &error);
    }

    match request.method.as_str() {
        "server/discover" => Json(mcp_discover_response(&request.id)).into_response(),
        "tools/list" => Json(mcp_tools_list_response(&request.id, &DIRECT_TOOLS)).into_response(),
        "tools/call" => mcp_call(state, request).await,
        method => Json(mcp_jsonrpc_error(
            &request.id,
            JSONRPC_METHOD_NOT_FOUND,
            format!("method not found: {method}"),
        ))
        .into_response(),
    }
}

fn header_str<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).and_then(|value| value.to_str().ok())
}

fn modern_request_error(id: &Value, error: &McpRequestError) -> Response {
    let status = match error {
        McpRequestError::MethodHeaderMismatch
        | McpRequestError::NameHeaderMismatch
        | McpRequestError::ProtocolVersion
        | McpRequestError::MetaProtocolVersion
        | McpRequestError::InvalidJsonRpc => StatusCode::BAD_REQUEST,
        _ => StatusCode::OK,
    };
    (
        status,
        Json(mcp_jsonrpc_error(
            id,
            error.jsonrpc_code(),
            error.to_string(),
        )),
    )
        .into_response()
}

async fn mcp_call(state: AppState, request: McpRequest) -> Response {
    let call = match parse_mcp_direct_call(&request) {
        Ok(call) => call,
        Err(error) => {
            let result = mcp_tool_error(error.to_string(), None);
            return Json(mcp_jsonrpc_result(&request.id, result)).into_response();
        }
    };

    let direct = v1_op(
        State(state),
        Json(OpRequest {
            op: call.op_name,
            host_id: call.host_selector,
            payload: call.payload,
            client_request_id: None,
        }),
    )
    .await;
    let result = direct_response_to_tool_result(direct).await;
    Json(mcp_jsonrpc_result(&request.id, result)).into_response()
}

async fn direct_response_to_tool_result(response: Response) -> Value {
    let status = response.status();
    let bytes = match to_bytes(response.into_body(), MCP_BODY_LIMIT).await {
        Ok(bytes) => bytes,
        Err(error) => {
            return mcp_tool_error(
                format!("Hub response body could not be read: {error}"),
                None,
            );
        }
    };
    let value = match serde_json::from_slice::<Value>(&bytes) {
        Ok(value) => value,
        Err(error) => {
            return mcp_tool_error(
                format!("Hub returned a non-JSON direct response: {error}"),
                None,
            );
        }
    };

    if status.is_success() {
        return match serde_json::from_value::<DirectResponse>(value.clone()) {
            Ok(response) => mcp_tool_result_from_direct(&response),
            Err(error) => mcp_tool_error(
                format!("Hub returned an invalid direct response: {error}"),
                Some(value),
            ),
        };
    }

    let code = value
        .get("error")
        .and_then(Value::as_str)
        .unwrap_or("hub_error");
    let message = value
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("Hub operation failed");
    mcp_tool_error(format!("{code}: {message}"), Some(value))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_mismatch_is_http_bad_request() {
        let response = modern_request_error(
            &serde_json::json!(1),
            &McpRequestError::MethodHeaderMismatch,
        );
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn tool_parameter_error_stays_jsonrpc_success_path() {
        let response = modern_request_error(
            &serde_json::json!(1),
            &McpRequestError::UnknownTool("missing".to_owned()),
        );
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[test]
    fn protocol_constant_matches_modern_revision() {
        assert_eq!(sentinel0_hub_core::MCP_PROTOCOL_VERSION, "2026-07-28");
        assert_eq!(sentinel0_hub_core::JSONRPC_INVALID_REQUEST, -32_600);
    }
}
