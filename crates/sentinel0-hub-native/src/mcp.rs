//! Stateless MCP 2026-07-28 HTTP adapter for the native Hub.

use super::{
    AppState, NotificationsRequest, OpRequest, SetDefaultRequest, SetLabelRequest,
    v1_clear_default_host, v1_get_default_host, v1_hosts, v1_notifications, v1_op,
    v1_set_default_host, v1_set_host_label,
};
use axum::{
    Json,
    body::{Bytes, to_bytes},
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use sentinel0_hub_core::{
    DirectResponse, HubToolKind, JSONRPC_METHOD_NOT_FOUND, McpHubCall, McpRequest, McpRequestError,
    McpToolCall, mcp_discover_response, mcp_jsonrpc_error, mcp_jsonrpc_result, mcp_tool_error,
    mcp_tool_result_from_direct, mcp_tool_success, mcp_tools_list_response, parse_mcp_tool_call,
    validate_modern_mcp_request,
};
use serde_json::{Value, json};

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
        "tools/list" => Json(mcp_tools_list_response(&request.id)).into_response(),
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
    let call = match parse_mcp_tool_call(&request) {
        Ok(call) => call,
        Err(error) => {
            let result = mcp_tool_error(error.to_string(), None);
            return Json(mcp_jsonrpc_result(&request.id, result)).into_response();
        }
    };

    let result = match call {
        McpToolCall::Direct(call) => {
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
            direct_response_to_tool_result(direct).await
        }
        McpToolCall::Hub(call) => mcp_hub_call(state, call).await,
    };
    Json(mcp_jsonrpc_result(&request.id, result)).into_response()
}

async fn mcp_hub_call(state: AppState, call: McpHubCall) -> Value {
    let response = match call.kind {
        HubToolKind::ListHosts => v1_hosts(State(state)).await,
        HubToolKind::GetDefaultHost => v1_get_default_host(State(state)).await,
        HubToolKind::ClearDefaultHost => v1_clear_default_host(State(state)).await,
        HubToolKind::SetDefaultHost => {
            let host_id = match required_string(&call.arguments, "host_id") {
                Ok(value) => value,
                Err(error) => return error,
            };
            v1_set_default_host(State(state), Json(SetDefaultRequest { host_id })).await
        }
        HubToolKind::SetHostLabel => {
            let host_id = match required_string(&call.arguments, "host_id") {
                Ok(value) => value,
                Err(error) => return error,
            };
            let label = match required_string(&call.arguments, "label") {
                Ok(value) => value,
                Err(error) => return error,
            };
            v1_set_host_label(
                State(state),
                Json(SetLabelRequest {
                    host_id,
                    label: Some(label),
                }),
            )
            .await
        }
        HubToolKind::RemoveHostLabel => {
            return remove_host_label(state, &call.arguments).await;
        }
        HubToolKind::NotificationsCheck => {
            v1_notifications(
                State(state),
                Json(NotificationsRequest {
                    operation: "check".to_owned(),
                    job_id: None,
                }),
            )
            .await
        }
        HubToolKind::NotificationsGet | HubToolKind::NotificationsAck => {
            let job_id = match required_string(&call.arguments, "job_id") {
                Ok(value) => value,
                Err(error) => return error,
            };
            let operation = if call.kind == HubToolKind::NotificationsGet {
                "get"
            } else {
                "ack"
            };
            v1_notifications(
                State(state),
                Json(NotificationsRequest {
                    operation: operation.to_owned(),
                    job_id: Some(job_id),
                }),
            )
            .await
        }
        HubToolKind::TransferFile => {
            let request = match serde_json::from_value::<super::transfer::TransferFileRequest>(
                call.arguments,
            ) {
                Ok(request) => request,
                Err(error) => {
                    return mcp_tool_error(format!("invalid transfer arguments: {error}"), None);
                }
            };
            super::transfer::v1_transfer_file(State(state), Json(request)).await
        }
    };
    hub_response_to_tool_result(response).await
}

async fn remove_host_label(state: AppState, arguments: &Value) -> Value {
    let host_id = match required_string(arguments, "host_id") {
        Ok(value) => value,
        Err(error) => return error,
    };
    let removed = {
        let registry = state.hub.registry.read().await;
        let Some(host) = registry.hosts().find(|host| host.host_id == host_id) else {
            return mcp_tool_error("host_not_found: unknown host_id", None);
        };
        host.label.is_some()
    };
    let response = v1_set_host_label(
        State(state),
        Json(SetLabelRequest {
            host_id,
            label: None,
        }),
    )
    .await;
    if !response.status().is_success() {
        return hub_response_to_tool_result(response).await;
    }
    mcp_tool_success(&json!({"ok": true, "removed": removed}))
}

fn required_string(arguments: &Value, name: &str) -> Result<String, Value> {
    arguments
        .get(name)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| mcp_tool_error(format!("{name} must be a string"), None))
}

async fn direct_response_to_tool_result(response: Response) -> Value {
    let status = response.status();
    let value = match response_json(response).await {
        Ok(value) => value,
        Err(error) => return error,
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
    http_error_to_tool_result(value)
}

async fn hub_response_to_tool_result(response: Response) -> Value {
    let status = response.status();
    let value = match response_json(response).await {
        Ok(value) => value,
        Err(error) => return error,
    };
    if status.is_success() {
        return mcp_tool_success(&value);
    }
    http_error_to_tool_result(value)
}

async fn response_json(response: Response) -> Result<Value, Value> {
    let bytes = to_bytes(response.into_body(), MCP_BODY_LIMIT)
        .await
        .map_err(|error| {
            mcp_tool_error(
                format!("Hub response body could not be read: {error}"),
                None,
            )
        })?;
    serde_json::from_slice::<Value>(&bytes)
        .map_err(|error| mcp_tool_error(format!("Hub returned a non-JSON response: {error}"), None))
}

fn http_error_to_tool_result(value: Value) -> Value {
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
