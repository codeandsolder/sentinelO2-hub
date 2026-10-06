#![forbid(unsafe_code)]

//! Cloudflare Workers + Durable Objects adapter for the Sentinel0² Hub.

use futures_channel::oneshot;
use futures_util::future::{Either, select};
use sentinel0_hub_core::parse_op;
use sentinel0_proto::{HEARTBEAT_INTERVAL_SECS, Message};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    cell::RefCell,
    collections::{BTreeMap, HashMap},
    time::Duration,
};
use worker::{
    Context, Date, Delay, DurableObject, Env, Error, Method, Request, Response, Result, State,
    WebSocket, WebSocketIncomingMessage, WebSocketPair, console_log, console_warn, durable_object,
    event, wasm_bindgen,
};

const TENANT_HUB_BINDING: &str = "TENANT_HUB";
const DEFAULT_TENANT: &str = "default";
const AGENT_TOKEN_SECRET: &str = "SENTINEL0_ENROLLMENT_TOKEN";

#[event(fetch, respond_with_errors)]
/// Route one public Worker request to this deployment's tenant Durable Object.
///
/// # Errors
///
/// Returns a Worker runtime error when the Durable Object binding or internal
/// fetch cannot be resolved.
pub async fn main(req: Request, env: Env, _ctx: Context) -> Result<Response> {
    let namespace = env.durable_object(TENANT_HUB_BINDING)?;
    let stub = namespace.id_from_name(DEFAULT_TENANT)?.get_stub()?;
    stub.fetch_with_request(req).await
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct SocketAttachment {
    host_id: Option<String>,
    session_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct OpRequest {
    op: String,
    host_id: Option<String>,
    #[serde(default)]
    payload: Value,
    client_request_id: Option<String>,
}

#[durable_object]
pub struct TenantHub {
    state: State,
    env: Env,
    pending: RefCell<HashMap<String, oneshot::Sender<Message>>>,
    request_counter: RefCell<u64>,
}

impl DurableObject for TenantHub {
    fn new(state: State, env: Env) -> Self {
        Self {
            state,
            env,
            pending: RefCell::new(HashMap::new()),
            request_counter: RefCell::new(0),
        }
    }

    async fn fetch(&self, mut req: Request) -> Result<Response> {
        let url = req.url()?;
        match (req.method(), url.path()) {
            (Method::Get, "/healthz") => {
                Response::from_json(&json!({"ok": true, "service": "sentinel0-hub-worker"}))
            }
            (Method::Get, "/agent/connect") => self.upgrade_agent(&req),
            (Method::Post, "/v1/op") => {
                let request = req.json::<OpRequest>().await?;
                self.dispatch_op(request).await
            }
            _ => Response::error("not found", 404),
        }
    }

    #[allow(clippy::unused_async_trait_impl)] // DurableObject requires async event handlers.
    async fn websocket_message(
        &self,
        ws: WebSocket,
        incoming: WebSocketIncomingMessage,
    ) -> Result<()> {
        let WebSocketIncomingMessage::String(raw) = incoming else {
            console_warn!("binary frame received before transfer coordinator exists");
            return Ok(());
        };

        let message = match serde_json::from_str::<Message>(&raw) {
            Ok(message) => message,
            Err(error) => {
                console_warn!("malformed agent JSON: {error}");
                return Ok(());
            }
        };

        let attachment = ws
            .deserialize_attachment::<SocketAttachment>()?
            .unwrap_or_default();

        if attachment.host_id.is_none() {
            return Self::accept_hello(&ws, message);
        }

        match message {
            Message::Response { ref id, .. } => {
                if let Some(waiter) = self.pending.borrow_mut().remove(id) {
                    let _ = waiter.send(message);
                }
            }
            Message::Ping { timestamp } => {
                ws.send(&Message::Pong { timestamp })?;
            }
            Message::Event { kind, .. } => {
                console_log!("agent event: {kind}");
            }
            _ => {}
        }
        Ok(())
    }

    #[allow(clippy::unused_async_trait_impl)] // DurableObject requires async event handlers.
    async fn websocket_close(
        &self,
        ws: WebSocket,
        code: usize,
        reason: String,
        was_clean: bool,
    ) -> Result<()> {
        let attachment = ws
            .deserialize_attachment::<SocketAttachment>()?
            .unwrap_or_default();
        console_log!(
            "agent socket closed host={:?} code={} clean={} reason={}",
            attachment.host_id,
            code,
            was_clean,
            reason
        );
        Ok(())
    }

    #[allow(clippy::unused_async_trait_impl)] // DurableObject requires async event handlers.
    async fn websocket_error(&self, ws: WebSocket, error: Error) -> Result<()> {
        let attachment = ws
            .deserialize_attachment::<SocketAttachment>()?
            .unwrap_or_default();
        console_warn!("agent socket error host={:?}: {error}", attachment.host_id);
        Ok(())
    }
}

impl TenantHub {
    fn upgrade_agent(&self, req: &Request) -> Result<Response> {
        let supplied = req.headers().get("authorization")?;
        let expected = format!("Bearer {}", self.env.secret(AGENT_TOKEN_SECRET)?);
        if supplied.as_deref() != Some(expected.as_str()) {
            return json_error(401, "unauthorized", "invalid agent enrollment token");
        }

        let pair = WebSocketPair::new()?;
        pair.server
            .serialize_attachment(SocketAttachment::default())?;
        self.state.accept_web_socket(&pair.server);
        Response::from_websocket(pair.client)
    }

    fn accept_hello(ws: &WebSocket, message: Message) -> Result<()> {
        let Message::Hello {
            protocol_version,
            host,
            ..
        } = message
        else {
            ws.close(Some(1008), Some("first message must be hello"))?;
            return Ok(());
        };

        if !protocol_version.starts_with("1.") {
            ws.close(Some(1008), Some("incompatible protocol"))?;
            return Ok(());
        }

        let session_id = format!("sess_{}_{}", host.id, Date::now().as_millis());
        ws.serialize_attachment(SocketAttachment {
            host_id: Some(host.id),
            session_id: Some(session_id.clone()),
        })?;
        ws.send(&Message::Welcome {
            session_id,
            server_time: chrono::Utc::now(),
            heartbeat_interval_seconds: HEARTBEAT_INTERVAL_SECS,
        })?;
        Ok(())
    }

    async fn dispatch_op(&self, request: OpRequest) -> Result<Response> {
        let Some(op) = parse_op(&request.op) else {
            return json_error(
                400,
                "unsupported_op",
                &format!("unsupported op {:?}", request.op),
            );
        };

        let Some(socket) = self.resolve_socket(request.host_id.as_deref())? else {
            return json_error(404, "agent_offline", "no eligible agent is connected");
        };

        let payload = match request.payload {
            Value::Object(values) => values.into_iter().collect(),
            Value::Null => BTreeMap::default(),
            other => {
                return json_error(
                    400,
                    "invalid_payload",
                    &format!("payload must be an object, got {other}"),
                );
            }
        };

        let request_id = self.next_request_id();
        let wire = Message::Request {
            id: request_id.clone(),
            op,
            payload,
            deadline: None,
            opaque_ref: None,
        };
        let (tx, rx) = oneshot::channel();
        self.pending.borrow_mut().insert(request_id.clone(), tx);
        if let Err(error) = socket.send(&wire) {
            self.pending.borrow_mut().remove(&request_id);
            return json_error(502, "agent_disconnected", &error.to_string());
        }

        let delay = Delay::from(Duration::from_secs(65));
        let message = match select(rx, delay).await {
            Either::Left((Ok(message), _)) => message,
            Either::Left((Err(_), _)) => {
                self.pending.borrow_mut().remove(&request_id);
                return json_error(502, "agent_disconnected", "agent response channel closed");
            }
            Either::Right(((), _)) => {
                self.pending.borrow_mut().remove(&request_id);
                return json_error(504, "timeout", "agent response deadline exceeded");
            }
        };

        let Message::Response {
            ok,
            mut result,
            error,
            ..
        } = message
        else {
            return json_error(502, "invalid_agent_response", "unexpected agent message");
        };
        if let Some(result) = result.as_mut() {
            result.remove("_sx_timing");
        }

        Response::from_json(&json!({
            "ok": ok,
            "result": result,
            "error": error,
            "hub_request_id": request_id,
            "client_request_id": request.client_request_id,
            "replayed": false
        }))
    }

    fn resolve_socket(&self, selector: Option<&str>) -> Result<Option<WebSocket>> {
        let mut matching = Vec::new();
        for socket in self.state.get_websockets() {
            let Some(attachment) = socket.deserialize_attachment::<SocketAttachment>()? else {
                continue;
            };
            let Some(host_id) = attachment.host_id.as_deref() else {
                continue;
            };
            if selector.is_none() || selector == Some(host_id) {
                matching.push(socket);
            }
        }

        match matching.len() {
            0 => Ok(None),
            1 => Ok(matching.pop()),
            _ => Err(Error::RustError(
                "ambiguous_host: multiple connected hosts match".to_owned(),
            )),
        }
    }

    fn next_request_id(&self) -> String {
        let mut counter = self.request_counter.borrow_mut();
        *counter = counter.wrapping_add(1);
        format!("hreq_{}_{}", Date::now().as_millis(), *counter)
    }
}

fn json_error(status: u16, code: &str, message: &str) -> Result<Response> {
    Ok(Response::from_json(&json!({
        "ok": false,
        "error": code,
        "message": message
    }))?
    .with_status(status))
}
