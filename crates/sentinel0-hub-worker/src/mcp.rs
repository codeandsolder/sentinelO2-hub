//! Stateless MCP 2026-07-28 HTTP adapter for the Cloudflare Hub.

use super::{OpRequest, TenantHub};
use sentinel0_hub_core::{
    DIRECT_TOOLS, DirectResponse, JSONRPC_METHOD_NOT_FOUND, McpRequest, McpRequestError,
    mcp_discover_response, mcp_jsonrpc_error, mcp_jsonrpc_result, mcp_tool_error,
    mcp_tool_result_from_direct, mcp_tools_list_response, parse_mcp_direct_call,
    validate_modern_mcp_request,
};
use serde_json::Value;
use worker::{Request, Response, Result};

impl TenantHub {
    pub(super) async fn mcp(&self, mut req: Request) -> Result<Response> {
        let protocol_header = req.headers().get("mcp-protocol-version")?;
        let method_header = req.headers().get("mcp-method")?;
        let name_header = req.headers().get("mcp-name")?;
        let raw = req.text().await?;
        let request = match serde_json::from_str::<McpRequest>(&raw) {
            Ok(request) => request,
            Err(error) => {
                return Ok(Response::from_json(&mcp_jsonrpc_error(
                    &Value::Null,
                    -32_700,
                    format!("invalid JSON-RPC request: {error}"),
                ))?
                .with_status(400));
            }
        };

        if let Err(error) = validate_modern_mcp_request(
            &request,
            protocol_header.as_deref(),
            method_header.as_deref(),
            name_header.as_deref(),
        ) {
            return modern_request_error(&request.id, &error);
        }

        match request.method.as_str() {
            "server/discover" => Response::from_json(&mcp_discover_response(&request.id)),
            "tools/list" => {
                Response::from_json(&mcp_tools_list_response(&request.id, &DIRECT_TOOLS))
            }
            "tools/call" => self.mcp_call(request).await,
            method => Response::from_json(&mcp_jsonrpc_error(
                &request.id,
                JSONRPC_METHOD_NOT_FOUND,
                format!("method not found: {method}"),
            )),
        }
    }

    async fn mcp_call(&self, request: McpRequest) -> Result<Response> {
        let call = match parse_mcp_direct_call(&request) {
            Ok(call) => call,
            Err(error) => {
                return Response::from_json(&mcp_jsonrpc_result(
                    &request.id,
                    mcp_tool_error(error.to_string(), None),
                ));
            }
        };
        let response = self
            .dispatch_op(OpRequest {
                op: call.op_name,
                host_id: call.host_selector,
                payload: call.payload,
                client_request_id: None,
            })
            .await?;
        let result = direct_response_to_tool_result(response).await;
        Response::from_json(&mcp_jsonrpc_result(&request.id, result))
    }
}

fn modern_request_error(id: &Value, error: &McpRequestError) -> Result<Response> {
    let status = match error {
        McpRequestError::MethodHeaderMismatch
        | McpRequestError::NameHeaderMismatch
        | McpRequestError::ProtocolVersion
        | McpRequestError::MetaProtocolVersion
        | McpRequestError::InvalidJsonRpc => 400,
        _ => 200,
    };
    Ok(Response::from_json(&mcp_jsonrpc_error(
        id,
        error.jsonrpc_code(),
        error.to_string(),
    ))?
    .with_status(status))
}

async fn direct_response_to_tool_result(mut response: Response) -> Value {
    let status = response.status_code();
    let raw = match response.text().await {
        Ok(raw) => raw,
        Err(error) => {
            return mcp_tool_error(
                format!("Hub response body could not be read: {error}"),
                None,
            );
        }
    };
    let value = match serde_json::from_str::<Value>(&raw) {
        Ok(value) => value,
        Err(error) => {
            return mcp_tool_error(
                format!("Hub returned a non-JSON direct response: {error}"),
                None,
            );
        }
    };

    if (200..300).contains(&status) {
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
