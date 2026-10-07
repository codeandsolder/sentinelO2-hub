//! Stateless MCP 2026-07-28 HTTP adapter for the Cloudflare Hub.

use super::{NotificationsRequest, OpRequest, TenantHub};
use sentinel0_hub_core::{
    DirectResponse, HubToolKind, JSONRPC_METHOD_NOT_FOUND, McpHubCall, McpRequest, McpRequestError,
    McpToolCall, hub_protocol_call, mcp_discover_response, mcp_jsonrpc_error, mcp_jsonrpc_result,
    mcp_tool_error, mcp_tool_result_from_direct, mcp_tool_success, mcp_tools_list_response,
    parse_mcp_tool_call, validate_modern_mcp_request,
};
use sentinel0_proto::Op;
use serde_json::{Map, Value, json};
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
            "tools/list" => Response::from_json(&mcp_tools_list_response(&request.id)),
            "tools/call" => self.mcp_call(request).await,
            method => Response::from_json(&mcp_jsonrpc_error(
                &request.id,
                JSONRPC_METHOD_NOT_FOUND,
                format!("method not found: {method}"),
            )),
        }
    }

    async fn mcp_call(&self, request: McpRequest) -> Result<Response> {
        let call = match parse_mcp_tool_call(&request) {
            Ok(call) => call,
            Err(error) => {
                return Response::from_json(&mcp_jsonrpc_result(
                    &request.id,
                    mcp_tool_error(error.to_string(), None),
                ));
            }
        };
        let result = match call {
            McpToolCall::Direct(call) => {
                let response = self
                    .dispatch_op(OpRequest {
                        op: call.op_name,
                        host_id: call.host_selector,
                        payload: call.payload,
                        client_request_id: None,
                    })
                    .await?;
                direct_response_to_tool_result(response).await
            }
            McpToolCall::Hub(call) => self.mcp_hub_call(call).await?,
        };
        Response::from_json(&mcp_jsonrpc_result(&request.id, result))
    }

    async fn mcp_hub_call(&self, call: McpHubCall) -> Result<Value> {
        match hub_protocol_call(call.kind, &call.arguments) {
            Ok(Some(protocol)) => {
                let response = self
                    .dispatch_protocol_op(protocol.op, protocol.host_selector, protocol.payload)
                    .await?;
                return Ok(direct_response_to_tool_result(response).await);
            }
            Ok(None) => {}
            Err(error) => return Ok(mcp_tool_error(error.to_string(), None)),
        }

        let response = match call.kind {
            HubToolKind::ListHosts => self.list_hosts_response()?,
            HubToolKind::GetDefaultHost => self.default_host_response()?,
            HubToolKind::ClearDefaultHost => self.clear_default_host()?,
            HubToolKind::SetDefaultHost => {
                let host_id = match required_string(&call.arguments, "host_id") {
                    Ok(value) => value,
                    Err(error) => return Ok(error),
                };
                self.set_default_host(&host_id)?
            }
            HubToolKind::SetHostLabel => {
                let host_id = match required_string(&call.arguments, "host_id") {
                    Ok(value) => value,
                    Err(error) => return Ok(error),
                };
                let label = match required_string(&call.arguments, "label") {
                    Ok(value) => value,
                    Err(error) => return Ok(error),
                };
                self.set_host_label(&host_id, Some(label))?
            }
            HubToolKind::RemoveHostLabel => {
                return self.mcp_remove_host_label(&call.arguments).await;
            }
            HubToolKind::ServiceStatus => {
                return self.mcp_service_status(&call.arguments).await;
            }
            HubToolKind::GitDiff
            | HubToolKind::GitApplyPatch
            | HubToolKind::GitLsRemote
            | HubToolKind::GitFetch
            | HubToolKind::GitClone
            | HubToolKind::GitPush
            | HubToolKind::ServiceStart
            | HubToolKind::ServiceStop
            | HubToolKind::ServiceReload => {
                return Ok(mcp_tool_error(
                    "internal Hub protocol wrapper routing failure",
                    None,
                ));
            }
            HubToolKind::NotificationsCheck => self.notifications(&NotificationsRequest {
                operation: "check".to_owned(),
                job_id: None,
            })?,
            HubToolKind::NotificationsGet | HubToolKind::NotificationsAck => {
                let job_id = match required_string(&call.arguments, "job_id") {
                    Ok(value) => value,
                    Err(error) => return Ok(error),
                };
                let operation = if call.kind == HubToolKind::NotificationsGet {
                    "get"
                } else {
                    "ack"
                };
                self.notifications(&NotificationsRequest {
                    operation: operation.to_owned(),
                    job_id: Some(job_id),
                })?
            }
            HubToolKind::TransferFile => {
                let request = match serde_json::from_value::<super::transfer::TransferFileRequest>(
                    call.arguments,
                ) {
                    Ok(request) => request,
                    Err(error) => {
                        return Ok(mcp_tool_error(
                            format!("invalid transfer arguments: {error}"),
                            None,
                        ));
                    }
                };
                self.transfer_file(request).await?
            }
        };
        Ok(hub_response_to_tool_result(response).await)
    }

    async fn mcp_service_status(&self, arguments: &Value) -> Result<Value> {
        let service = match required_string(arguments, "service") {
            Ok(value) => value,
            Err(error) => return Ok(error),
        };
        let host_selector = match optional_string(arguments, "host_id") {
            Ok(value) => value,
            Err(error) => return Ok(error),
        };
        let opaque_ref = match optional_string(arguments, "opaque_ref") {
            Ok(value) => value,
            Err(error) => return Ok(error),
        };

        let mut base = Map::new();
        base.insert("service".to_owned(), Value::String(service.clone()));
        if let Some(opaque_ref) = opaque_ref {
            base.insert("opaque_ref".to_owned(), Value::String(opaque_ref));
        }

        let status = self
            .service_status_part(host_selector.clone(), &base, "status")
            .await?;
        let is_active = self
            .service_status_part(host_selector.clone(), &base, "is-active")
            .await?;
        let is_enabled = self
            .service_status_part(host_selector, &base, "is-enabled")
            .await?;
        Ok(mcp_tool_success(&json!({
            "ok": true,
            "service": service,
            "status": status,
            "is_active": is_active,
            "is_enabled": is_enabled,
        })))
    }

    async fn service_status_part(
        &self,
        host_selector: Option<String>,
        base: &Map<String, Value>,
        action: &str,
    ) -> Result<Value> {
        let mut payload = base.clone();
        payload.insert("action".to_owned(), Value::String(action.to_owned()));
        let response = self
            .dispatch_protocol_op(Op::Service, host_selector, Value::Object(payload))
            .await?;
        Ok(direct_response_to_part(response).await)
    }

    async fn mcp_remove_host_label(&self, arguments: &Value) -> Result<Value> {
        let host_id = match required_string(arguments, "host_id") {
            Ok(value) => value,
            Err(error) => return Ok(error),
        };
        let live = self.live_sockets()?;
        let registry = self.load_registry(&live)?;
        let Some(host) = registry.hosts().find(|host| host.host_id == host_id) else {
            return Ok(mcp_tool_error("host_not_found: unknown host_id", None));
        };
        let removed = host.label.is_some();
        let response = self.set_host_label(&host_id, None)?;
        if !(200..300).contains(&response.status_code()) {
            return Ok(hub_response_to_tool_result(response).await);
        }
        Ok(mcp_tool_success(&json!({"ok": true, "removed": removed})))
    }
}

fn required_string(arguments: &Value, name: &str) -> std::result::Result<String, Value> {
    arguments
        .get(name)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| mcp_tool_error(format!("{name} must be a string"), None))
}

fn optional_string(arguments: &Value, name: &str) -> std::result::Result<Option<String>, Value> {
    match arguments.get(name) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) => Ok(Some(value.clone())),
        Some(_) => Err(mcp_tool_error(
            format!("{name} must be a string or null"),
            None,
        )),
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

async fn direct_response_to_tool_result(response: Response) -> Value {
    let status = response.status_code();
    let value = match response_json(response).await {
        Ok(value) => value,
        Err(error) => return error,
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
    http_error_to_tool_result(value)
}

async fn direct_response_to_part(response: Response) -> Value {
    let status = response.status_code();
    let value = match response_json(response).await {
        Ok(value) => value,
        Err(error) => return error,
    };
    if !(200..300).contains(&status) {
        return value;
    }
    let response = match serde_json::from_value::<DirectResponse>(value.clone()) {
        Ok(response) => response,
        Err(error) => {
            return json!({
                "ok": false,
                "error": "invalid_hub_response",
                "message": error.to_string(),
            });
        }
    };
    if response.ok {
        return response.result.map_or_else(
            || json!({"ok": true}),
            |result| {
                let mut result = result.into_iter().collect::<Map<String, Value>>();
                result.entry("ok".to_owned()).or_insert(Value::Bool(true));
                Value::Object(result)
            },
        );
    }
    match response.error {
        Some(error) => json!({
            "ok": false,
            "error": error.code,
            "message": error.message,
            "details": error.details,
        }),
        None => json!({"ok": false, "error": "agent_error", "message": "agent operation failed"}),
    }
}

async fn hub_response_to_tool_result(response: Response) -> Value {
    let status = response.status_code();
    let value = match response_json(response).await {
        Ok(value) => value,
        Err(error) => return error,
    };
    if (200..300).contains(&status) {
        return mcp_tool_success(&value);
    }
    http_error_to_tool_result(value)
}

async fn response_json(mut response: Response) -> std::result::Result<Value, Value> {
    let raw = response.text().await.map_err(|error| {
        mcp_tool_error(
            format!("Hub response body could not be read: {error}"),
            None,
        )
    })?;
    serde_json::from_str::<Value>(&raw)
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
