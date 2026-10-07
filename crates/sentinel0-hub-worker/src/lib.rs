#![forbid(unsafe_code)]

//! Cloudflare Workers + Durable Objects adapter for the Sentinel0² Hub.

use futures_channel::oneshot;
use futures_util::future::{Either, select};
use sentinel0_hub_core::{HostRecord, HostRegistry, HostResolutionError, parse_op};
use sentinel0_proto::{HEARTBEAT_INTERVAL_SECS, Message};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    cell::RefCell,
    collections::{BTreeMap, HashMap},
    time::Duration,
};
use worker::{
    Context, Date, Delay, DurableObject, Env, Error, Method, Request, Response, Result,
    SqlStorageValue, State, WebSocket, WebSocketIncomingMessage, WebSocketPair, console_log,
    console_warn, durable_object, event, wasm_bindgen,
};

const TENANT_HUB_BINDING: &str = "TENANT_HUB";
const DEFAULT_TENANT: &str = "default";
const AGENT_TOKEN_SECRET: &str = "SENTINEL0_ENROLLMENT_TOKEN";
const API_TOKEN_SECRET: &str = "SENTINEL0_API_TOKEN";
const DEFAULT_HOST_KEY: &str = "default_host_id";

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

#[derive(Debug, Deserialize)]
struct SetDefaultRequest {
    host_id: String,
}

#[derive(Debug, Deserialize)]
struct SetLabelRequest {
    host_id: String,
    label: Option<String>,
}

#[derive(Debug, Deserialize)]
struct SetDisabledRequest {
    host_id: String,
    disabled: bool,
}

#[derive(Debug, Deserialize)]
struct DbHostRow {
    host_id: String,
    hostname: String,
    label: Option<String>,
    disabled: i64,
}

#[derive(Debug, Deserialize)]
struct SettingRow {
    value: String,
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
        let hub = Self {
            state,
            env,
            pending: RefCell::new(HashMap::new()),
            request_counter: RefCell::new(0),
        };
        if let Err(error) = hub.initialize_schema() {
            console_warn!("failed to initialize TenantHub SQLite schema: {error}");
        }
        hub
    }

    async fn fetch(&self, mut req: Request) -> Result<Response> {
        let url = req.url()?;
        if url.path().starts_with("/v1/")
            && let Some(response) = self.api_auth_error(&req)?
        {
            return Ok(response);
        }
        match (req.method(), url.path()) {
            (Method::Get, "/healthz") => {
                Response::from_json(&json!({"ok": true, "service": "sentinel0-hub-worker"}))
            }
            (Method::Get, "/agent/connect") => self.upgrade_agent(&req),
            (Method::Post, "/v1/op") => {
                let request = req.json::<OpRequest>().await?;
                self.dispatch_op(request).await
            }
            (Method::Get, "/v1/hosts") => self.list_hosts_response(),
            (Method::Get, "/v1/default-host") => self.default_host_response(),
            (Method::Put, "/v1/default-host") => {
                let request = req.json::<SetDefaultRequest>().await?;
                self.set_default_host(&request.host_id)
            }
            (Method::Delete, "/v1/default-host") => self.clear_default_host(),
            (Method::Put, "/v1/hosts/label") => {
                let request = req.json::<SetLabelRequest>().await?;
                self.set_host_label(&request.host_id, request.label)
            }
            (Method::Put, "/v1/hosts/disabled") => {
                let request = req.json::<SetDisabledRequest>().await?;
                self.set_host_disabled(&request.host_id, request.disabled)
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
            return self.accept_hello(&ws, message);
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
        if let Some(host_id) = attachment.host_id.as_deref() {
            self.mark_disconnected(host_id)?;
        }
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
    fn api_auth_error(&self, req: &Request) -> Result<Option<Response>> {
        let token = match self.env.secret(API_TOKEN_SECRET) {
            Ok(value) => value.to_string(),
            Err(_) => {
                return Ok(Some(json_error(
                    503,
                    "api_auth_not_configured",
                    "SENTINEL0_API_TOKEN is not configured",
                )?));
            }
        };
        let supplied = req.headers().get("authorization")?;
        let expected = format!("Bearer {token}");
        if supplied.as_deref() != Some(expected.as_str()) {
            return Ok(Some(json_error(
                401,
                "unauthorized",
                "invalid Hub API token",
            )?));
        }
        Ok(None)
    }

    fn initialize_schema(&self) -> Result<()> {
        let sql = self.state.storage().sql();
        sql.exec(
            "CREATE TABLE IF NOT EXISTS hosts (\
                host_id TEXT PRIMARY KEY,\
                hostname TEXT NOT NULL,\
                label TEXT,\
                disabled INTEGER NOT NULL DEFAULT 0,\
                agent_version TEXT NOT NULL DEFAULT '',\
                protocol_version TEXT NOT NULL DEFAULT '',\
                last_connected_ms INTEGER NOT NULL DEFAULT 0,\
                last_disconnected_ms INTEGER\
            )",
            None::<Vec<SqlStorageValue>>,
        )?;
        sql.exec(
            "CREATE UNIQUE INDEX IF NOT EXISTS hosts_label_unique \
             ON hosts(label) WHERE label IS NOT NULL",
            None::<Vec<SqlStorageValue>>,
        )?;
        sql.exec(
            "CREATE TABLE IF NOT EXISTS tenant_settings (\
                key TEXT PRIMARY KEY,\
                value TEXT NOT NULL\
            )",
            None::<Vec<SqlStorageValue>>,
        )?;
        Ok(())
    }

    fn upgrade_agent(&self, req: &Request) -> Result<Response> {
        let supplied = req.headers().get("authorization")?;
        let token = match self.env.secret(AGENT_TOKEN_SECRET) {
            Ok(value) => value.to_string(),
            Err(_) => {
                return json_error(
                    503,
                    "enrollment_not_configured",
                    "SENTINEL0_ENROLLMENT_TOKEN is not configured",
                );
            }
        };
        let expected = format!("Bearer {token}");
        if supplied.as_deref() != Some(expected.as_str()) {
            return json_error(401, "unauthorized", "invalid agent enrollment token");
        }

        let pair = WebSocketPair::new()?;
        pair.server
            .serialize_attachment(SocketAttachment::default())?;
        self.state.accept_web_socket(&pair.server);
        Response::from_websocket(pair.client)
    }

    fn accept_hello(&self, ws: &WebSocket, message: Message) -> Result<()> {
        let Message::Hello {
            protocol_version,
            agent_version,
            host,
            capabilities: _,
            preferred_profile: _,
            agent_name: _,
        } = message
        else {
            ws.close(Some(1008), Some("first message must be hello"))?;
            return Ok(());
        };

        if !protocol_version.starts_with("1.") {
            ws.close(Some(1008), Some("incompatible protocol"))?;
            return Ok(());
        }
        if self.host_disabled(&host.id)? {
            ws.close(Some(1008), Some("host disabled"))?;
            return Ok(());
        }

        self.close_superseded_socket(&host.id)?;
        let session_id = format!("sess_{}_{}", host.id, Date::now().as_millis());
        self.persist_hello(&host.id, &host.hostname, &agent_version, &protocol_version)?;
        ws.serialize_attachment(SocketAttachment {
            host_id: Some(host.id.clone()),
            session_id: Some(session_id.clone()),
        })?;
        ws.send(&Message::Welcome {
            session_id,
            server_time: chrono::Utc::now(),
            heartbeat_interval_seconds: HEARTBEAT_INTERVAL_SECS,
        })?;
        Ok(())
    }

    fn persist_hello(
        &self,
        host_id: &str,
        hostname: &str,
        agent_version: &str,
        protocol_version: &str,
    ) -> Result<()> {
        self.state.storage().sql().exec(
            "INSERT INTO hosts (\
                host_id, hostname, agent_version, protocol_version, last_connected_ms, last_disconnected_ms\
             ) VALUES (?, ?, ?, ?, ?, NULL) \
             ON CONFLICT(host_id) DO UPDATE SET \
                hostname = excluded.hostname, \
                agent_version = excluded.agent_version, \
                protocol_version = excluded.protocol_version, \
                last_connected_ms = excluded.last_connected_ms, \
                last_disconnected_ms = NULL",
            vec![
                host_id.into(),
                hostname.into(),
                agent_version.into(),
                protocol_version.into(),
                chrono::Utc::now().timestamp_millis().into(),
            ],
        )?;
        Ok(())
    }

    fn mark_disconnected(&self, host_id: &str) -> Result<()> {
        self.state.storage().sql().exec(
            "UPDATE hosts SET last_disconnected_ms = ? WHERE host_id = ?",
            vec![chrono::Utc::now().timestamp_millis().into(), host_id.into()],
        )?;
        Ok(())
    }

    fn host_disabled(&self, host_id: &str) -> Result<bool> {
        #[derive(Deserialize)]
        struct DisabledRow {
            disabled: i64,
        }

        let rows = self
            .state
            .storage()
            .sql()
            .exec(
                "SELECT disabled FROM hosts WHERE host_id = ? LIMIT 1",
                vec![host_id.into()],
            )?
            .to_array::<DisabledRow>()?;
        Ok(rows.first().is_some_and(|row| row.disabled != 0))
    }

    fn close_superseded_socket(&self, host_id: &str) -> Result<()> {
        for socket in self.state.get_websockets() {
            let Some(attachment) = socket.deserialize_attachment::<SocketAttachment>()? else {
                continue;
            };
            if attachment.host_id.as_deref() == Some(host_id) {
                socket.close(Some(1000), Some("superseded by newer session"))?;
            }
        }
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

        let socket = match self.resolve_socket(request.host_id.as_deref())? {
            Ok(socket) => socket,
            Err(error) => return host_resolution_error(&error),
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

    fn live_sockets(&self) -> Result<HashMap<String, WebSocket>> {
        let mut sockets = HashMap::new();
        for socket in self.state.get_websockets() {
            let Some(attachment) = socket.deserialize_attachment::<SocketAttachment>()? else {
                continue;
            };
            let Some(host_id) = attachment.host_id else {
                continue;
            };
            sockets.insert(host_id, socket);
        }
        Ok(sockets)
    }

    fn load_registry(&self, live: &HashMap<String, WebSocket>) -> Result<HostRegistry> {
        let rows = self
            .state
            .storage()
            .sql()
            .exec(
                "SELECT host_id, hostname, label, disabled FROM hosts ORDER BY host_id",
                None::<Vec<SqlStorageValue>>,
            )?
            .to_array::<DbHostRow>()?;
        let hosts = rows.into_iter().map(|row| HostRecord {
            connected: live.contains_key(&row.host_id),
            disabled: row.disabled != 0,
            host_id: row.host_id,
            hostname: row.hostname,
            label: row.label,
        });
        Ok(HostRegistry::from_records(
            hosts,
            self.load_default_host_id()?,
        ))
    }

    fn load_default_host_id(&self) -> Result<Option<String>> {
        let rows = self
            .state
            .storage()
            .sql()
            .exec(
                "SELECT value FROM tenant_settings WHERE key = ? LIMIT 1",
                vec![DEFAULT_HOST_KEY.into()],
            )?
            .to_array::<SettingRow>()?;
        Ok(rows.first().map(|row| row.value.clone()))
    }

    fn resolve_socket(
        &self,
        selector: Option<&str>,
    ) -> Result<std::result::Result<WebSocket, HostResolutionError>> {
        let live = self.live_sockets()?;
        let registry = self.load_registry(&live)?;
        let host_id = match registry.resolve(selector) {
            Ok(host) => host.host_id.clone(),
            Err(error) => return Ok(Err(error)),
        };
        match live.get(&host_id) {
            Some(socket) => Ok(Ok(socket.clone())),
            None => Ok(Err(HostResolutionError::NotFound(host_id))),
        }
    }

    fn list_hosts_response(&self) -> Result<Response> {
        let live = self.live_sockets()?;
        let registry = self.load_registry(&live)?;
        let hosts = registry.hosts().cloned().collect::<Vec<_>>();
        Response::from_json(&json!({
            "ok": true,
            "hosts": hosts,
            "default_host_id": registry.default_host_id(),
        }))
    }

    fn default_host_response(&self) -> Result<Response> {
        let live = self.live_sockets()?;
        let registry = self.load_registry(&live)?;
        let default_host_id = registry.default_host_id();
        let is_connected = default_host_id.and_then(|host_id| {
            registry
                .hosts()
                .find(|host| host.host_id == host_id)
                .map(HostRecord::eligible)
        });
        Response::from_json(&json!({
            "ok": true,
            "default_host_id": default_host_id,
            "is_connected": is_connected,
        }))
    }

    fn set_default_host(&self, host_id: &str) -> Result<Response> {
        let live = self.live_sockets()?;
        let registry = self.load_registry(&live)?;
        if !registry.hosts().any(|host| host.host_id == host_id) {
            return json_error(404, "host_not_found", "unknown host_id");
        }
        self.state.storage().sql().exec(
            "INSERT INTO tenant_settings (key, value) VALUES (?, ?) \
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            vec![DEFAULT_HOST_KEY.into(), host_id.into()],
        )?;
        Response::from_json(&json!({
            "ok": true,
            "host_id": host_id,
            "is_connected": live.contains_key(host_id),
        }))
    }

    fn clear_default_host(&self) -> Result<Response> {
        let cursor = self.state.storage().sql().exec(
            "DELETE FROM tenant_settings WHERE key = ?",
            vec![DEFAULT_HOST_KEY.into()],
        )?;
        Response::from_json(&json!({
            "ok": true,
            "removed": cursor.rows_written() > 0,
        }))
    }

    fn set_host_label(&self, host_id: &str, label: Option<String>) -> Result<Response> {
        let live = self.live_sockets()?;
        let registry = self.load_registry(&live)?;
        if !registry.hosts().any(|host| host.host_id == host_id) {
            return json_error(404, "host_not_found", "unknown host_id");
        }
        let label = label
            .map(|value| value.trim().to_owned())
            .filter(|value| !value.is_empty());
        if let Some(label) = label.as_deref() {
            #[derive(Deserialize)]
            struct LabelOwnerRow {
                host_id: String,
            }
            let owners = self
                .state
                .storage()
                .sql()
                .exec(
                    "SELECT host_id FROM hosts WHERE label = ? AND host_id <> ? LIMIT 1",
                    vec![label.into(), host_id.into()],
                )?
                .to_array::<LabelOwnerRow>()?;
            if let Some(owner) = owners.first() {
                return json_error(
                    409,
                    "label_conflict",
                    &format!("label is already assigned to {}", owner.host_id),
                );
            }
        }
        self.state.storage().sql().exec(
            "UPDATE hosts SET label = ? WHERE host_id = ?",
            vec![label.clone().into(), host_id.into()],
        )?;
        Response::from_json(&json!({"ok": true, "host_id": host_id, "label": label}))
    }

    fn set_host_disabled(&self, host_id: &str, disabled: bool) -> Result<Response> {
        let live = self.live_sockets()?;
        let registry = self.load_registry(&live)?;
        if !registry.hosts().any(|host| host.host_id == host_id) {
            return json_error(404, "host_not_found", "unknown host_id");
        }
        self.state.storage().sql().exec(
            "UPDATE hosts SET disabled = ? WHERE host_id = ?",
            vec![disabled.into(), host_id.into()],
        )?;
        if disabled && let Some(socket) = live.get(host_id) {
            socket.close(Some(1008), Some("host disabled"))?;
        }
        Response::from_json(&json!({"ok": true, "host_id": host_id, "disabled": disabled}))
    }

    fn next_request_id(&self) -> String {
        let mut counter = self.request_counter.borrow_mut();
        *counter = counter.wrapping_add(1);
        format!("hreq_{}_{}", Date::now().as_millis(), *counter)
    }
}

fn host_resolution_error(error: &HostResolutionError) -> Result<Response> {
    let (status, code) = match error {
        HostResolutionError::NotFound(_) | HostResolutionError::NoEligibleHost => {
            (404, "agent_offline")
        }
        HostResolutionError::Ambiguous(_) | HostResolutionError::AmbiguousDefault => {
            (409, "ambiguous_host")
        }
        HostResolutionError::DefaultOffline(_) => (409, "default_host_offline"),
    };
    json_error(status, code, &error.to_string())
}

fn json_error(status: u16, code: &str, message: &str) -> Result<Response> {
    Ok(Response::from_json(&json!({
        "ok": false,
        "error": code,
        "message": message
    }))?
    .with_status(status))
}
