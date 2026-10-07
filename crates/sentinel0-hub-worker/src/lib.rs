#![forbid(unsafe_code)]

//! Cloudflare Workers + Durable Objects adapter for the Sentinel0² Hub.

mod mcp;
mod transfer;

use futures_channel::oneshot;
use futures_util::future::{Either, select};
use sentinel0_hub_core::{
    DIRECT_TOOLS, DirectRequestError, DirectRequestInput, DirectResponse, HostRecord, HostRegistry,
    HostResolutionError, JobCompletion, direct_rest_openapi, direct_tool_by_op,
    direct_tool_mcp_entry, model_tool_catalog, normalize_agent_response, parse_job_completion,
    prepare_direct_request, prepare_protocol_request,
};
use sentinel0_proto::{HEARTBEAT_INTERVAL_SECS, Message, Op};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{cell::RefCell, collections::HashMap, time::Duration};
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
const IDEMPOTENCY_TTL_MS: i64 = 24 * 60 * 60 * 1_000;

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
#[allow(clippy::struct_field_names)] // IDs are distinct protocol/session concepts.
struct SocketAttachment {
    connection_id: Option<String>,
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
struct NotificationsRequest {
    operation: String,
    job_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ActiveSessionRow {
    host_id: String,
    session_id: String,
}

#[derive(Debug, Deserialize)]
struct CurrentSessionRow {
    present: i64,
}

#[derive(Debug, Deserialize)]
struct JobOwnerRow {
    host_id: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct RunningJobRow {
    job_id: String,
    #[serde(rename = "host")]
    host_id: String,
    tool: String,
    status: String,
}

#[derive(Debug, Deserialize)]
struct NotificationRow {
    notification_id: String,
    summary_json: String,
}

#[derive(Debug, Deserialize)]
struct JobRow {
    job_id: String,
    host_id: String,
    tool: String,
    status: String,
    completion_json: Option<String>,
    created_ms: i64,
    updated_ms: i64,
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

#[derive(Debug, Deserialize)]
struct IdempotencyRow {
    fingerprint: String,
    state: String,
    hub_request_id: String,
    response_json: Option<String>,
    http_status: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct IdempotencyRequestRow {
    client_request_id: String,
}

struct PendingRequest {
    waiter: oneshot::Sender<Message>,
    client_request_id: Option<String>,
}

#[durable_object]
pub struct TenantHub {
    state: State,
    env: Env,
    pending: RefCell<HashMap<String, PendingRequest>>,
    transfer_binary_waiters: RefCell<transfer::BinaryWaiters>,
    transfer_ack_waiters: RefCell<transfer::AckWaiters>,
    request_counter: RefCell<u64>,
}

impl DurableObject for TenantHub {
    fn new(state: State, env: Env) -> Self {
        let hub = Self {
            state,
            env,
            pending: RefCell::new(HashMap::new()),
            transfer_binary_waiters: RefCell::new(HashMap::new()),
            transfer_ack_waiters: RefCell::new(HashMap::new()),
            request_counter: RefCell::new(0),
        };
        if let Err(error) = hub.initialize_schema() {
            console_warn!("failed to initialize TenantHub SQLite schema: {error}");
        }
        hub
    }

    async fn fetch(&self, mut req: Request) -> Result<Response> {
        let url = req.url()?;
        if (url.path().starts_with("/v1/") || url.path() == "/mcp")
            && let Some(response) = self.api_auth_error(&req)?
        {
            return Ok(response);
        }
        if req.method() == Method::Get
            && let Some(op_name) = url.path().strip_prefix("/v1/ops/")
            && !op_name.is_empty()
        {
            return Self::direct_op_info_response(op_name);
        }
        match (req.method(), url.path()) {
            (Method::Get, "/healthz") => {
                Response::from_json(&json!({"ok": true, "service": "sentinel0-hub-worker"}))
            }
            (Method::Get, "/agent/connect") => self.upgrade_agent(&req),
            (Method::Post, "/mcp") => self.mcp(req).await,
            (Method::Post, "/v1/op") => {
                let request = req.json::<OpRequest>().await?;
                self.dispatch_op(request).await
            }
            (Method::Get, "/v1/ops") => Self::direct_ops_response(),
            (Method::Get, "/v1/tools") => Self::direct_tools_response(),
            (Method::Get, "/v1/openapi.json") => Response::from_json(&direct_rest_openapi()),
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
            (Method::Post, "/v1/notifications") => {
                let request = req.json::<NotificationsRequest>().await?;
                self.notifications(&request)
            }
            (Method::Post, "/v1/transfer-file") => {
                let request = req.json::<transfer::TransferFileRequest>().await?;
                self.transfer_file(request).await
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
        let attachment = self.resolve_attachment(&ws)?;
        let raw = match incoming {
            WebSocketIncomingMessage::String(raw) => raw,
            WebSocketIncomingMessage::Binary(raw) => {
                if let Some(host_id) = attachment.host_id.as_deref() {
                    self.handle_transfer_binary(host_id, raw);
                } else {
                    console_warn!("binary frame received before agent hello");
                }
                return Ok(());
            }
        };

        let message = match serde_json::from_str::<Message>(&raw) {
            Ok(message) => message,
            Err(error) => {
                console_warn!("malformed agent JSON: {error}");
                return Ok(());
            }
        };

        if attachment.host_id.is_none() {
            return self.accept_hello(&ws, message);
        }

        match message {
            Message::Response { ref id, .. } => {
                if let Some(pending) = self.pending.borrow_mut().remove(id) {
                    if let Some(client_request_id) = pending.client_request_id.as_deref()
                        && let Err(error) = self.persist_idempotent_agent_response_for_client(
                            id,
                            client_request_id,
                            &message,
                        )
                    {
                        console_warn!("failed to persist idempotent response {id}: {error}");
                    }
                    let _ = pending.waiter.send(message);
                } else if let Err(error) = self.persist_idempotent_agent_response(id, &message) {
                    console_warn!("failed to persist late idempotent response {id}: {error}");
                }
            }
            Message::Ping { timestamp } => {
                ws.send(&Message::Pong { timestamp })?;
            }
            Message::Event { ref kind, .. } => {
                if let Some(host_id) = attachment.host_id.as_deref() {
                    self.handle_transfer_event(host_id, &message);
                }
                match parse_job_completion(&message) {
                    Ok(Some(completion)) => {
                        if attachment.host_id.as_deref() != Some(completion.host_id.as_str()) {
                            console_warn!(
                                "discarding job completion whose host does not match its socket"
                            );
                        } else if let Err(error) = self.persist_job_completion(&completion) {
                            console_warn!(
                                "failed to persist job completion {}: {error}",
                                completion.job_id
                            );
                        }
                    }
                    Ok(None) => {}
                    Err(error) => console_warn!("invalid job_completed event: {error}"),
                }
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
        let attachment = self.resolve_attachment(&ws)?;
        if let Some(host_id) = attachment.host_id.as_deref() {
            let was_current = match attachment.connection_id.as_deref() {
                Some(connection_id) => self.clear_active_session(host_id, connection_id)?,
                None => true,
            };
            if was_current {
                self.mark_disconnected(host_id)?;
                self.mark_running_jobs_orphaned(host_id)?;
            }
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
        let attachment = self.resolve_attachment(&ws)?;
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
        sql.exec(
            "CREATE TABLE IF NOT EXISTS idempotency (\
                client_request_id TEXT PRIMARY KEY,\
                fingerprint TEXT NOT NULL,\
                state TEXT NOT NULL CHECK (state IN ('pending', 'complete')),\
                hub_request_id TEXT NOT NULL UNIQUE,\
                response_json TEXT,\
                http_status INTEGER,\
                created_ms INTEGER NOT NULL,\
                expires_ms INTEGER NOT NULL\
            )",
            None::<Vec<SqlStorageValue>>,
        )?;
        sql.exec(
            "CREATE INDEX IF NOT EXISTS idempotency_expires_idx ON idempotency(expires_ms)",
            None::<Vec<SqlStorageValue>>,
        )?;
        sql.exec(
            "CREATE TABLE IF NOT EXISTS active_sessions (\
                host_id TEXT PRIMARY KEY,\
                connection_id TEXT NOT NULL UNIQUE,\
                session_id TEXT NOT NULL,\
                connected_ms INTEGER NOT NULL\
            )",
            None::<Vec<SqlStorageValue>>,
        )?;
        sql.exec(
            "CREATE TABLE IF NOT EXISTS jobs (\
                job_id TEXT PRIMARY KEY,\
                host_id TEXT NOT NULL,\
                tool TEXT NOT NULL,\
                status TEXT NOT NULL,\
                completion_json TEXT,\
                created_ms INTEGER NOT NULL,\
                updated_ms INTEGER NOT NULL\
            )",
            None::<Vec<SqlStorageValue>>,
        )?;
        sql.exec(
            "CREATE INDEX IF NOT EXISTS jobs_status_idx ON jobs(status)",
            None::<Vec<SqlStorageValue>>,
        )?;
        sql.exec(
            "CREATE TABLE IF NOT EXISTS notifications (\
                notification_id TEXT PRIMARY KEY,\
                kind TEXT NOT NULL,\
                ref_id TEXT NOT NULL UNIQUE,\
                summary_json TEXT NOT NULL,\
                created_ms INTEGER NOT NULL,\
                read_ms INTEGER,\
                acked_ms INTEGER\
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
        let connection_id = self.next_connection_id();
        pair.server.serialize_attachment(SocketAttachment {
            connection_id: Some(connection_id),
            ..SocketAttachment::default()
        })?;
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

        let mut attachment = ws
            .deserialize_attachment::<SocketAttachment>()?
            .unwrap_or_default();
        let connection_id = attachment
            .connection_id
            .clone()
            .unwrap_or_else(|| self.next_connection_id());

        self.close_superseded_socket(&host.id)?;
        let session_id = format!("sess_{}_{}", host.id, Date::now().as_millis());
        self.persist_hello(&host.id, &host.hostname, &agent_version, &protocol_version)?;
        self.persist_active_session(&host.id, &connection_id, &session_id)?;

        attachment.connection_id = Some(connection_id);
        attachment.host_id = Some(host.id.clone());
        attachment.session_id = Some(session_id.clone());
        ws.serialize_attachment(attachment)?;
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

    fn persist_active_session(
        &self,
        host_id: &str,
        connection_id: &str,
        session_id: &str,
    ) -> Result<()> {
        self.state.storage().sql().exec(
            "INSERT INTO active_sessions (host_id, connection_id, session_id, connected_ms) \
             VALUES (?, ?, ?, ?) ON CONFLICT(host_id) DO UPDATE SET \
                connection_id = excluded.connection_id, \
                session_id = excluded.session_id, \
                connected_ms = excluded.connected_ms",
            vec![
                host_id.into(),
                connection_id.into(),
                session_id.into(),
                chrono::Utc::now().timestamp_millis().into(),
            ],
        )?;
        Ok(())
    }

    fn clear_active_session(&self, host_id: &str, connection_id: &str) -> Result<bool> {
        let cursor = self.state.storage().sql().exec(
            "DELETE FROM active_sessions WHERE host_id = ? AND connection_id = ?",
            vec![host_id.into(), connection_id.into()],
        )?;
        Ok(cursor.rows_written() > 0)
    }

    fn resolve_attachment(&self, ws: &WebSocket) -> Result<SocketAttachment> {
        let mut attachment = ws
            .deserialize_attachment::<SocketAttachment>()?
            .unwrap_or_default();
        if attachment.host_id.is_some() {
            return Ok(attachment);
        }
        let Some(connection_id) = attachment.connection_id.as_deref() else {
            return Ok(attachment);
        };
        let rows = self
            .state
            .storage()
            .sql()
            .exec(
                "SELECT host_id, session_id FROM active_sessions \
                 WHERE connection_id = ? LIMIT 1",
                vec![connection_id.into()],
            )?
            .to_array::<ActiveSessionRow>()?;
        let Some(row) = rows.first() else {
            return Ok(attachment);
        };
        attachment.host_id = Some(row.host_id.clone());
        attachment.session_id = Some(row.session_id.clone());
        ws.serialize_attachment(attachment.clone())?;
        Ok(attachment)
    }

    fn socket_is_current(&self, attachment: &SocketAttachment) -> Result<bool> {
        let (Some(host_id), Some(connection_id)) = (
            attachment.host_id.as_deref(),
            attachment.connection_id.as_deref(),
        ) else {
            return Ok(attachment.host_id.is_some());
        };
        let rows = self
            .state
            .storage()
            .sql()
            .exec(
                "SELECT 1 AS present FROM active_sessions \
                 WHERE host_id = ? AND connection_id = ? LIMIT 1",
                vec![host_id.into(), connection_id.into()],
            )?
            .to_array::<CurrentSessionRow>()?;
        Ok(rows.first().is_some_and(|row| row.present == 1))
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
            let attachment = self.resolve_attachment(&socket)?;
            if attachment.host_id.as_deref() == Some(host_id)
                && self.socket_is_current(&attachment)?
            {
                socket.close(Some(1000), Some("superseded by newer session"))?;
            }
        }
        Ok(())
    }

    async fn dispatch_op(&self, request: OpRequest) -> Result<Response> {
        let live = self.live_sockets()?;
        let registry = self.load_registry(&live)?;
        let prepared = match prepare_direct_request(
            &registry,
            DirectRequestInput {
                op_name: request.op,
                host_selector: request.host_id,
                payload: request.payload,
                client_request_id: request.client_request_id,
            },
        ) {
            Ok(prepared) => prepared,
            Err(error) => return direct_request_error(&error),
        };
        self.dispatch_prepared_request(prepared, &live).await
    }

    async fn dispatch_protocol_op(
        &self,
        op: Op,
        host_selector: Option<String>,
        payload: Value,
    ) -> Result<Response> {
        let live = self.live_sockets()?;
        let registry = self.load_registry(&live)?;
        let prepared = match prepare_protocol_request(
            &registry,
            op,
            host_selector.as_deref(),
            payload,
            None,
        ) {
            Ok(prepared) => prepared,
            Err(error) => return direct_request_error(&error),
        };
        self.dispatch_prepared_request(prepared, &live).await
    }

    async fn dispatch_prepared_request(
        &self,
        mut prepared: sentinel0_hub_core::PreparedDirectRequest,
        live: &HashMap<String, WebSocket>,
    ) -> Result<Response> {
        let Some(socket) = live.get(&prepared.host_id).cloned() else {
            return json_error(
                502,
                "agent_disconnected",
                "resolved agent has no live socket",
            );
        };

        let request_id = self.next_request_id();
        if let (Some(client_request_id), Some(fingerprint)) = (
            prepared.client_request_id.as_deref(),
            prepared.invocation_fingerprint.as_deref(),
        ) && let Some(response) =
            self.begin_idempotency(client_request_id, fingerprint, &request_id)?
        {
            return Ok(response);
        }

        let background_job_id = prepared.background_requested().then(|| self.next_job_id());
        if let Some(job_id) = background_job_id.as_deref() {
            prepared.assign_background_job_id(job_id);
        }
        let wire = prepared.wire_message(request_id.clone());
        let (tx, rx) = oneshot::channel();
        self.pending.borrow_mut().insert(
            request_id.clone(),
            PendingRequest {
                waiter: tx,
                client_request_id: prepared.client_request_id.clone(),
            },
        );
        if let Err(error) = socket.send(&wire) {
            self.pending.borrow_mut().remove(&request_id);
            if let Some(client_request_id) = prepared.client_request_id.as_deref()
                && let Err(persist_error) = self.complete_idempotency_http_error(
                    client_request_id,
                    &request_id,
                    502,
                    "agent_disconnected",
                    &error.to_string(),
                )
            {
                console_warn!(
                    "failed to persist pre-dispatch transport failure {request_id}: {persist_error}"
                );
            }
            return json_error(502, "agent_disconnected", &error.to_string());
        }

        let body = match self
            .await_agent_response(&request_id, prepared.client_request_id.as_deref(), rx)
            .await?
        {
            Ok(body) => body,
            Err(response) => return Ok(response),
        };
        if let Some(job_id) = background_job_id.as_deref()
            && body.running_job_id() == Some(job_id)
        {
            self.persist_job_started(job_id, &prepared.host_id, prepared.op.as_str())?;
        }
        Response::from_json(&body)
    }

    async fn await_agent_response(
        &self,
        request_id: &str,
        client_request_id: Option<&str>,
        rx: oneshot::Receiver<Message>,
    ) -> Result<std::result::Result<DirectResponse, Response>> {
        let delay = Delay::from(Duration::from_secs(65));
        let message = match select(rx, delay).await {
            Either::Left((Ok(message), _)) => message,
            Either::Left((Err(_), _)) => {
                self.pending.borrow_mut().remove(request_id);
                self.persist_terminal_request_error(
                    client_request_id,
                    request_id,
                    502,
                    "agent_disconnected",
                    "agent response channel closed",
                );
                return Ok(Err(json_error(
                    502,
                    "agent_disconnected",
                    "agent response channel closed",
                )?));
            }
            Either::Right(((), _)) => {
                self.pending.borrow_mut().remove(request_id);
                self.persist_terminal_request_error(
                    client_request_id,
                    request_id,
                    504,
                    "timeout",
                    "agent response deadline exceeded",
                );
                return Ok(Err(json_error(
                    504,
                    "timeout",
                    "agent response deadline exceeded",
                )?));
            }
        };

        if let Ok(body) = normalize_agent_response(&message, request_id, client_request_id, false) {
            Ok(Ok(body))
        } else {
            self.persist_terminal_request_error(
                client_request_id,
                request_id,
                502,
                "invalid_agent_response",
                "unexpected agent message",
            );
            Ok(Err(json_error(
                502,
                "invalid_agent_response",
                "unexpected agent message",
            )?))
        }
    }

    fn persist_terminal_request_error(
        &self,
        client_request_id: Option<&str>,
        request_id: &str,
        status: u16,
        code: &str,
        message: &str,
    ) {
        let Some(client_request_id) = client_request_id else {
            return;
        };
        if let Err(error) = self.complete_idempotency_http_error(
            client_request_id,
            request_id,
            status,
            code,
            message,
        ) {
            console_warn!("failed to persist terminal request error {request_id}: {error}");
        }
    }

    fn begin_idempotency(
        &self,
        client_request_id: &str,
        fingerprint: &str,
        request_id: &str,
    ) -> Result<Option<Response>> {
        match self.begin_idempotency_inner(client_request_id, fingerprint, request_id) {
            Ok(response) => Ok(response),
            Err(error) => {
                console_warn!("idempotency storage unavailable: {error}");
                Ok(Some(json_error(
                    503,
                    "idempotency_unavailable",
                    "idempotency storage is unavailable; request was not dispatched",
                )?))
            }
        }
    }

    fn begin_idempotency_inner(
        &self,
        client_request_id: &str,
        fingerprint: &str,
        request_id: &str,
    ) -> Result<Option<Response>> {
        let now_ms = chrono::Utc::now().timestamp_millis();
        let sql = self.state.storage().sql();
        sql.exec(
            "DELETE FROM idempotency WHERE expires_ms <= ?",
            vec![now_ms.into()],
        )?;

        let rows = sql
            .exec(
                "SELECT fingerprint, state, hub_request_id, response_json, http_status \
                 FROM idempotency WHERE client_request_id = ? LIMIT 1",
                vec![client_request_id.into()],
            )?
            .to_array::<IdempotencyRow>()?;

        if let Some(row) = rows.first() {
            if row.fingerprint != fingerprint {
                return Ok(Some(json_error(
                    409,
                    "request_id_conflict",
                    "client_request_id was already used for a different invocation",
                )?));
            }

            return match row.state.as_str() {
                "pending" => Ok(Some(
                    Response::from_json(&json!({
                        "ok": false,
                        "error": "request_in_progress",
                        "message": "the original invocation is still in progress",
                        "client_request_id": client_request_id,
                        "hub_request_id": row.hub_request_id,
                        "replayed": false,
                    }))?
                    .with_status(409),
                )),
                "complete" => {
                    let response_json = row.response_json.as_deref().ok_or_else(|| {
                        Error::RustError("complete idempotency row has no response_json".to_owned())
                    })?;
                    let mut body = serde_json::from_str::<Value>(response_json)
                        .map_err(|error| Error::RustError(error.to_string()))?;
                    if let Value::Object(values) = &mut body {
                        values.insert("replayed".to_owned(), Value::Bool(true));
                    }
                    let status = row
                        .http_status
                        .and_then(|status| u16::try_from(status).ok())
                        .filter(|status| (100..=599).contains(status))
                        .ok_or_else(|| {
                            Error::RustError(
                                "complete idempotency row has invalid http_status".to_owned(),
                            )
                        })?;
                    Ok(Some(Response::from_json(&body)?.with_status(status)))
                }
                other => Err(Error::RustError(format!(
                    "unknown idempotency state {other:?}"
                ))),
            };
        }

        sql.exec(
            "INSERT INTO idempotency (\
                client_request_id, fingerprint, state, hub_request_id, \
                response_json, http_status, created_ms, expires_ms\
             ) VALUES (?, ?, 'pending', ?, NULL, NULL, ?, ?)",
            vec![
                client_request_id.into(),
                fingerprint.into(),
                request_id.into(),
                now_ms.into(),
                (now_ms + IDEMPOTENCY_TTL_MS).into(),
            ],
        )?;
        Ok(None)
    }

    fn persist_idempotent_agent_response_for_client(
        &self,
        request_id: &str,
        client_request_id: &str,
        message: &Message,
    ) -> Result<()> {
        let Ok(body) =
            normalize_agent_response(message, request_id, Some(client_request_id), false)
        else {
            return Ok(());
        };
        self.complete_idempotency(client_request_id, request_id, 200, &body)
    }

    fn persist_idempotent_agent_response(&self, request_id: &str, message: &Message) -> Result<()> {
        let rows = self
            .state
            .storage()
            .sql()
            .exec(
                "SELECT client_request_id FROM idempotency \
                 WHERE hub_request_id = ? AND state = 'pending' LIMIT 1",
                vec![request_id.into()],
            )?
            .to_array::<IdempotencyRequestRow>()?;
        let Some(row) = rows.first() else {
            return Ok(());
        };
        let Ok(body) =
            normalize_agent_response(message, request_id, Some(&row.client_request_id), false)
        else {
            return Ok(());
        };
        self.complete_idempotency(&row.client_request_id, request_id, 200, &body)
    }

    fn complete_idempotency_http_error(
        &self,
        client_request_id: &str,
        request_id: &str,
        status: u16,
        code: &str,
        message: &str,
    ) -> Result<()> {
        let body = json!({
            "ok": false,
            "error": code,
            "message": message,
            "client_request_id": client_request_id,
            "hub_request_id": request_id,
            "replayed": false,
        });
        self.complete_idempotency(client_request_id, request_id, status, &body)
    }

    fn complete_idempotency<T: Serialize>(
        &self,
        client_request_id: &str,
        request_id: &str,
        status: u16,
        body: &T,
    ) -> Result<()> {
        let response_json =
            serde_json::to_string(body).map_err(|error| Error::RustError(error.to_string()))?;
        self.state.storage().sql().exec(
            "UPDATE idempotency SET \
                state = 'complete', response_json = ?, http_status = ? \
             WHERE client_request_id = ? AND hub_request_id = ? AND state = 'pending'",
            vec![
                response_json.into(),
                i64::from(status).into(),
                client_request_id.into(),
                request_id.into(),
            ],
        )?;
        Ok(())
    }

    fn mark_running_jobs_orphaned(&self, host_id: &str) -> Result<()> {
        let now_ms = chrono::Utc::now().timestamp_millis();
        self.state.storage().sql().exec(
            "UPDATE jobs SET status = 'orphaned', updated_ms = ? \
             WHERE host_id = ? AND status = 'running'",
            vec![now_ms.into(), host_id.into()],
        )?;
        Ok(())
    }

    fn persist_job_started(&self, job_id: &str, host_id: &str, tool: &str) -> Result<()> {
        let now_ms = chrono::Utc::now().timestamp_millis();
        self.state.storage().sql().exec(
            "INSERT INTO jobs (job_id, host_id, tool, status, completion_json, created_ms, updated_ms) \
             VALUES (?, ?, ?, 'running', NULL, ?, ?) ON CONFLICT(job_id) DO NOTHING",
            vec![
                job_id.into(),
                host_id.into(),
                tool.into(),
                now_ms.into(),
                now_ms.into(),
            ],
        )?;
        Ok(())
    }

    fn persist_job_completion(&self, completion: &JobCompletion) -> Result<()> {
        let sql = self.state.storage().sql();
        let owners = sql
            .exec(
                "SELECT host_id FROM jobs WHERE job_id = ? LIMIT 1",
                vec![completion.job_id.as_str().into()],
            )?
            .to_array::<JobOwnerRow>()?;
        if owners
            .first()
            .is_some_and(|row| row.host_id != completion.host_id)
        {
            return Err(Error::RustError(
                "job completion host does not match existing job owner".to_owned(),
            ));
        }

        let now_ms = chrono::Utc::now().timestamp_millis();
        let completion_json = serde_json::to_string(&completion.data)
            .map_err(|error| Error::RustError(error.to_string()))?;
        let summary_json = serde_json::to_string(&completion.summary())
            .map_err(|error| Error::RustError(error.to_string()))?;
        sql.exec(
            "INSERT INTO jobs (job_id, host_id, tool, status, completion_json, created_ms, updated_ms) \
             VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT(job_id) DO UPDATE SET \
                tool = excluded.tool, status = excluded.status, \
                completion_json = excluded.completion_json, updated_ms = excluded.updated_ms \
             WHERE jobs.host_id = excluded.host_id",
            vec![
                completion.job_id.as_str().into(),
                completion.host_id.as_str().into(),
                completion.tool.as_str().into(),
                completion.status.as_str().into(),
                completion_json.into(),
                now_ms.into(),
                now_ms.into(),
            ],
        )?;
        let notification_id = format!("job:{}", completion.job_id);
        sql.exec(
            "INSERT INTO notifications (notification_id, kind, ref_id, summary_json, created_ms, read_ms, acked_ms) \
             VALUES (?, 'job_completed', ?, ?, ?, NULL, NULL) ON CONFLICT(notification_id) DO UPDATE SET \
                summary_json = excluded.summary_json",
            vec![
                notification_id.into(),
                completion.job_id.as_str().into(),
                summary_json.into(),
                now_ms.into(),
            ],
        )?;
        Ok(())
    }

    fn notifications(&self, request: &NotificationsRequest) -> Result<Response> {
        match request.operation.as_str() {
            "check" => self.notifications_check(),
            "get" => {
                let Some(job_id) = request.job_id.as_deref() else {
                    return json_error(400, "missing_job_id", "notifications get requires job_id");
                };
                self.notifications_get(job_id)
            }
            "ack" => self.notifications_ack(request.job_id.as_deref()),
            _ => json_error(
                400,
                "invalid_operation",
                "notifications operation must be check, get or ack",
            ),
        }
    }

    fn notifications_check(&self) -> Result<Response> {
        let sql = self.state.storage().sql();
        let unread = sql
            .exec(
                "SELECT notification_id, summary_json FROM notifications \
                 WHERE read_ms IS NULL AND acked_ms IS NULL ORDER BY created_ms LIMIT 100",
                None::<Vec<SqlStorageValue>>,
            )?
            .to_array::<NotificationRow>()?;
        let mut completed = Vec::with_capacity(unread.len());
        let now_ms = chrono::Utc::now().timestamp_millis();
        for row in unread {
            let summary = serde_json::from_str::<Value>(&row.summary_json)
                .map_err(|error| Error::RustError(error.to_string()))?;
            completed.push(summary);
            sql.exec(
                "UPDATE notifications SET read_ms = ? \
                 WHERE notification_id = ? AND read_ms IS NULL AND acked_ms IS NULL",
                vec![now_ms.into(), row.notification_id.into()],
            )?;
        }
        let running = sql
            .exec(
                "SELECT job_id, host_id AS host, tool, status FROM jobs \
                 WHERE status = 'running' ORDER BY created_ms LIMIT 100",
                None::<Vec<SqlStorageValue>>,
            )?
            .to_array::<RunningJobRow>()?;
        let orphaned = sql
            .exec(
                "SELECT job_id, host_id AS host, tool, status FROM jobs \
                 WHERE status = 'orphaned' ORDER BY updated_ms LIMIT 100",
                None::<Vec<SqlStorageValue>>,
            )?
            .to_array::<RunningJobRow>()?;
        Response::from_json(&json!({
            "ok": true,
            "completed": completed,
            "running": running,
            "orphaned": orphaned,
            "broadcasts": [],
            "answered_reports": [],
        }))
    }

    fn notifications_get(&self, job_id: &str) -> Result<Response> {
        let rows = self
            .state
            .storage()
            .sql()
            .exec(
                "SELECT job_id, host_id, tool, status, completion_json, created_ms, updated_ms \
                 FROM jobs WHERE job_id = ? LIMIT 1",
                vec![job_id.into()],
            )?
            .to_array::<JobRow>()?;
        let Some(row) = rows.first() else {
            return json_error(404, "job_not_found", "unknown job_id");
        };
        let completion = row
            .completion_json
            .as_deref()
            .map(serde_json::from_str::<Value>)
            .transpose()
            .map_err(|error| Error::RustError(error.to_string()))?;
        Response::from_json(&json!({
            "ok": true,
            "job_id": row.job_id,
            "host": row.host_id,
            "tool": row.tool,
            "status": row.status,
            "completion": completion,
            "created_ms": row.created_ms,
            "updated_ms": row.updated_ms,
        }))
    }

    fn notifications_ack(&self, job_id: Option<&str>) -> Result<Response> {
        let sql = self.state.storage().sql();
        let now_ms = chrono::Utc::now().timestamp_millis();
        let cursor = match job_id {
            None | Some("all") => sql.exec(
                "UPDATE notifications SET acked_ms = ? WHERE acked_ms IS NULL",
                vec![now_ms.into()],
            )?,
            Some(job_id) => sql.exec(
                "UPDATE notifications SET acked_ms = ? \
                 WHERE ref_id = ? AND acked_ms IS NULL",
                vec![now_ms.into(), job_id.into()],
            )?,
        };
        Response::from_json(&json!({"ok": true, "acked": cursor.rows_written()}))
    }

    fn live_sockets(&self) -> Result<HashMap<String, WebSocket>> {
        let mut sockets = HashMap::new();
        for socket in self.state.get_websockets() {
            let attachment = self.resolve_attachment(&socket)?;
            if !self.socket_is_current(&attachment)? {
                continue;
            }
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

    fn direct_ops_response() -> Result<Response> {
        Response::from_json(&json!({"ok": true, "ops": DIRECT_TOOLS}))
    }

    fn direct_tools_response() -> Result<Response> {
        Response::from_json(&json!({"ok": true, "tools": model_tool_catalog()}))
    }

    fn direct_op_info_response(op_name: &str) -> Result<Response> {
        let Some(tool) = direct_tool_by_op(op_name) else {
            return json_error(
                404,
                "unsupported_op",
                &format!("unsupported op {op_name:?}"),
            );
        };
        Response::from_json(&json!({"ok": true, "op": tool, "tool": direct_tool_mcp_entry(tool)}))
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

    fn next_job_id(&self) -> String {
        let mut counter = self.request_counter.borrow_mut();
        *counter = counter.wrapping_add(1);
        format!("job_{:x}_{:x}", Date::now().as_millis(), *counter)
    }

    fn next_connection_id(&self) -> String {
        let mut counter = self.request_counter.borrow_mut();
        *counter = counter.wrapping_add(1);
        format!("conn_{:x}_{:x}", Date::now().as_millis(), *counter)
    }
}

fn direct_request_error(error: &DirectRequestError) -> Result<Response> {
    match error {
        DirectRequestError::UnsupportedOp(_)
        | DirectRequestError::InvalidPayload(_)
        | DirectRequestError::InvalidClientRequestId
        | DirectRequestError::InvalidOpaqueRef => {
            let code = match error {
                DirectRequestError::UnsupportedOp(_) => "unsupported_op",
                DirectRequestError::InvalidPayload(_) => "invalid_payload",
                DirectRequestError::InvalidClientRequestId => "invalid_client_request_id",
                DirectRequestError::InvalidOpaqueRef => "invalid_opaque_ref",
                DirectRequestError::Host(_) => unreachable!("host errors handled separately"),
            };
            json_error(400, code, &error.to_string())
        }
        DirectRequestError::Host(host) => host_resolution_error(host),
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
