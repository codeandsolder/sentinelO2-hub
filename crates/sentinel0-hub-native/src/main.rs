#![forbid(unsafe_code)]

mod store;
mod transfer;

use axum::{
    Json, Router,
    extract::{
        Path, Request, State, WebSocketUpgrade,
        ws::{Message as WsMessage, WebSocket},
    },
    http::{HeaderMap, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post, put},
};
use chrono::Utc;
use futures_util::{SinkExt as _, StreamExt as _};
use sentinel0_hub_core::{
    DIRECT_TOOLS, DirectRequestError, DirectRequestInput, HostRegistry, HostResolutionError,
    PreparedDirectRequest, direct_rest_openapi, direct_tool_by_op, direct_tool_catalog,
    direct_tool_mcp_entry, normalize_agent_response, parse_job_completion, prepare_direct_request,
};
use sentinel0_proto::{HEARTBEAT_INTERVAL_SECS, Message};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    env,
    sync::{Arc, Mutex as StdMutex},
    time::Duration,
};
use tokio::sync::{Mutex, RwLock, mpsc, oneshot};
use tracing::{info, warn};
use uuid::Uuid;

use store::{BeginIdempotency, NativeStore, StoreError};

#[derive(Clone)]
struct AppState {
    hub: Arc<Hub>,
}

struct Hub {
    enrollment_token: String,
    api_token: String,
    registry: RwLock<HostRegistry>,
    sessions: RwLock<HashMap<String, AgentSession>>,
    pending: Mutex<HashMap<String, PendingRequest>>,
    transfer_binary_waiters: Mutex<transfer::BinaryWaiters>,
    transfer_ack_waiters: Mutex<transfer::AckWaiters>,
    store: StdMutex<NativeStore>,
}

#[derive(Clone)]
struct AgentSession {
    session_id: String,
    tx: mpsc::Sender<WsMessage>,
}

struct PendingRequest {
    waiter: oneshot::Sender<Message>,
    client_request_id: Option<String>,
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
struct NotificationsRequest {
    #[serde(default = "default_notifications_operation")]
    operation: String,
    job_id: Option<String>,
}

fn default_notifications_operation() -> String {
    "check".to_owned()
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

#[derive(Debug, Serialize)]
struct ErrorBody {
    ok: bool,
    error: &'static str,
    message: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "sentinel0_hub_native=info".into()),
        )
        .init();

    let enrollment_token = env::var("SENTINEL0_ENROLLMENT_TOKEN")?;
    let api_token = env::var("SENTINEL0_API_TOKEN")?;
    let listen = env::var("SENTINEL0_LISTEN").unwrap_or_else(|_| "127.0.0.1:8788".to_owned());
    let database_path =
        env::var("SENTINEL0_DB_PATH").unwrap_or_else(|_| "sentinel0-hub.sqlite3".to_owned());
    let store = NativeStore::open(&database_path)?;
    let registry = store.load_host_registry()?;

    let state = AppState {
        hub: Arc::new(Hub {
            enrollment_token,
            api_token,
            registry: RwLock::new(registry),
            sessions: RwLock::new(HashMap::new()),
            pending: Mutex::new(HashMap::new()),
            transfer_binary_waiters: Mutex::new(HashMap::new()),
            transfer_ack_waiters: Mutex::new(HashMap::new()),
            store: StdMutex::new(store),
        }),
    };

    let v1 = Router::new()
        .route("/op", post(v1_op))
        .route("/ops", get(v1_ops))
        .route("/ops/{op}", get(v1_op_info))
        .route("/tools", get(v1_tools))
        .route("/openapi.json", get(v1_openapi))
        .route("/hosts", get(v1_hosts))
        .route(
            "/default-host",
            get(v1_get_default_host)
                .put(v1_set_default_host)
                .delete(v1_clear_default_host),
        )
        .route("/hosts/label", put(v1_set_host_label))
        .route("/hosts/disabled", put(v1_set_host_disabled))
        .route("/notifications", post(v1_notifications))
        .route("/transfer-file", post(transfer::v1_transfer_file))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            require_api_auth,
        ));

    let app = Router::new()
        .route("/healthz", get(healthz))
        .route("/agent/connect", get(agent_connect))
        .nest("/v1", v1)
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(&listen).await?;
    info!(%listen, "Sentinel0² native Hub listening");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn healthz() -> Json<Value> {
    Json(json!({"ok": true, "service": "sentinel0-hub-native"}))
}

async fn require_api_auth(State(state): State<AppState>, request: Request, next: Next) -> Response {
    let expected = format!("Bearer {}", state.hub.api_token);
    let authorized = request
        .headers()
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value == expected);
    if !authorized {
        return api_error(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "invalid Hub API token".to_owned(),
        );
    }
    next.run(request).await
}

async fn v1_ops() -> Json<Value> {
    Json(json!({"ok": true, "ops": DIRECT_TOOLS}))
}

async fn v1_tools() -> Json<Value> {
    Json(json!({"ok": true, "tools": direct_tool_catalog(&DIRECT_TOOLS)}))
}

async fn v1_openapi() -> Json<Value> {
    Json(direct_rest_openapi())
}

async fn v1_op_info(Path(op): Path<String>) -> Response {
    let Some(tool) = direct_tool_by_op(&op) else {
        return api_error(
            StatusCode::NOT_FOUND,
            "unsupported_op",
            format!("unsupported op {op:?}"),
        );
    };
    (
        StatusCode::OK,
        Json(json!({"ok": true, "op": tool, "tool": direct_tool_mcp_entry(tool)})),
    )
        .into_response()
}

async fn agent_connect(
    State(state): State<AppState>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> Response {
    let expected = format!("Bearer {}", state.hub.enrollment_token);
    let authorized = headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value == expected);

    if !authorized {
        return (
            StatusCode::UNAUTHORIZED,
            Json(ErrorBody {
                ok: false,
                error: "unauthorized",
                message: "invalid agent enrollment token".to_owned(),
            }),
        )
            .into_response();
    }

    ws.on_upgrade(move |socket| serve_agent(state, socket))
}

async fn serve_agent(state: AppState, socket: WebSocket) {
    let (mut sink, mut stream) = socket.split();
    let hello = tokio::time::timeout(Duration::from_secs(10), stream.next()).await;
    let Ok(Some(Ok(WsMessage::Text(raw)))) = hello else {
        warn!("agent did not provide a text hello in time");
        return;
    };

    let Ok(message) = serde_json::from_str::<Message>(&raw) else {
        warn!("agent sent invalid hello JSON");
        return;
    };
    let Some(host) = validate_and_persist_hello(&state, message) else {
        return;
    };
    let host_id = host.id.clone();
    let session_id = format!("sess_{}", Uuid::now_v7().simple());
    let welcome = Message::Welcome {
        session_id: session_id.clone(),
        server_time: Utc::now(),
        heartbeat_interval_seconds: HEARTBEAT_INTERVAL_SECS,
    };
    let Ok(welcome_json) = serde_json::to_string(&welcome) else {
        return;
    };
    if sink
        .send(WsMessage::Text(welcome_json.into()))
        .await
        .is_err()
    {
        return;
    }

    state.hub.registry.write().await.register_hello(&host);
    let (tx, mut rx) = mpsc::channel::<WsMessage>(256);
    let superseded = state.hub.sessions.write().await.insert(
        host_id.clone(),
        AgentSession {
            session_id: session_id.clone(),
            tx,
        },
    );
    if let Some(superseded) = superseded {
        let _ = superseded.tx.send(WsMessage::Close(None)).await;
    }

    info!(%host_id, %session_id, "agent connected");

    loop {
        tokio::select! {
            outbound = rx.recv() => {
                let Some(outbound) = outbound else {
                    break;
                };
                if sink.send(outbound).await.is_err() {
                    break;
                }
            }
            inbound = stream.next() => {
                match inbound {
                    Some(Ok(WsMessage::Text(raw))) => {
                        handle_agent_text(&state, &host_id, &raw).await;
                    }
                    Some(Ok(WsMessage::Binary(raw))) => {
                        transfer::handle_binary_frame(&state, &host_id, raw.to_vec()).await;
                    }
                    Some(Ok(WsMessage::Close(_)) | Err(_)) | None => break,
                    Some(Ok(_)) => {}
                }
            }
        }
    }

    let mut sessions = state.hub.sessions.write().await;
    let is_current = sessions
        .get(&host_id)
        .is_some_and(|session| session.session_id == session_id);
    if is_current {
        sessions.remove(&host_id);
        state.hub.registry.write().await.disconnect(&host_id);
        let store = state
            .hub
            .store
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Err(error) = store.mark_host_disconnected(&host_id) {
            warn!(%host_id, %error, "failed to persist host disconnect");
        }
        if let Err(error) = store.mark_running_jobs_orphaned(&host_id) {
            warn!(%host_id, %error, "failed to mark disconnected jobs orphaned");
        }
    }
    info!(%host_id, %session_id, "agent disconnected");
}

fn validate_and_persist_hello(
    state: &AppState,
    message: Message,
) -> Option<sentinel0_proto::HostInfo> {
    let Message::Hello {
        protocol_version,
        agent_version,
        host,
        ..
    } = message
    else {
        warn!("agent first message was not hello");
        return None;
    };
    if !protocol_version.starts_with("1.") {
        warn!(%protocol_version, "incompatible protocol");
        return None;
    }
    let store = state
        .hub
        .store
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    match store.host_disabled(&host.id) {
        Ok(true) => {
            warn!(host_id = %host.id, "disabled host attempted to connect");
            return None;
        }
        Ok(false) => {}
        Err(error) => {
            warn!(host_id = %host.id, %error, "failed to inspect host policy");
            return None;
        }
    }
    if let Err(error) =
        store.persist_hello(&host.id, &host.hostname, &agent_version, &protocol_version)
    {
        warn!(host_id = %host.id, %error, "failed to persist agent hello");
        return None;
    }
    drop(store);
    Some(*host)
}

async fn handle_agent_text(state: &AppState, host_id: &str, raw: &str) {
    let Ok(message) = serde_json::from_str::<Message>(raw) else {
        warn!(%host_id, "agent sent malformed JSON message");
        return;
    };

    match message {
        Message::Response { ref id, .. } => {
            let pending = state.hub.pending.lock().await.remove(id);
            if let Some(pending) = pending {
                if let Some(client_request_id) = pending.client_request_id.as_deref()
                    && let Ok(body) =
                        normalize_agent_response(&message, id, Some(client_request_id), false)
                    && let Err(error) = state
                        .hub
                        .store
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .complete_idempotency(client_request_id, id, 200, &body)
                {
                    warn!(%id, %error, "failed to persist idempotent agent response");
                }
                let _ = pending.waiter.send(message);
            } else {
                let store = state
                    .hub
                    .store
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                match store.pending_client_request_id(id) {
                    Ok(Some(client_request_id)) => {
                        if let Ok(body) =
                            normalize_agent_response(&message, id, Some(&client_request_id), false)
                            && let Err(error) =
                                store.complete_idempotency(&client_request_id, id, 200, &body)
                        {
                            warn!(%id, %error, "failed to persist late idempotent response");
                        }
                    }
                    Ok(None) => {}
                    Err(error) => warn!(%id, %error, "failed to inspect late agent response"),
                }
            }
        }
        Message::Ping { timestamp } => {
            let pong = Message::Pong { timestamp };
            if let Ok(text) = serde_json::to_string(&pong) {
                send_text_to_host(state, host_id, &text).await;
            }
        }
        Message::Event { ref kind, .. } => {
            transfer::handle_transfer_event(state, host_id, &message).await;
            match parse_job_completion(&message) {
                Ok(Some(completion)) => {
                    if completion.host_id != host_id {
                        warn!(
                            %host_id,
                            completion_host = %completion.host_id,
                            job_id = %completion.job_id,
                            "discarding job completion whose host does not match its socket"
                        );
                    } else if let Err(error) = state
                        .hub
                        .store
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .job_completed(&completion)
                    {
                        warn!(
                            job_id = %completion.job_id,
                            %error,
                            "failed to persist job completion"
                        );
                    }
                }
                Ok(None) => {}
                Err(error) => warn!(%host_id, %error, "invalid job_completed event"),
            }
            info!(%host_id, %kind, "agent event received");
        }
        _ => {}
    }
}

async fn send_text_to_host(state: &AppState, host_id: &str, text: &str) {
    let session = state.hub.sessions.read().await.get(host_id).cloned();
    if let Some(session) = session {
        let _ = session
            .tx
            .send(WsMessage::Text(text.to_owned().into()))
            .await;
    }
}

async fn v1_hosts(State(state): State<AppState>) -> Response {
    let registry = state.hub.registry.read().await;
    let hosts = registry.hosts().cloned().collect::<Vec<_>>();
    (
        StatusCode::OK,
        Json(json!({
            "ok": true,
            "hosts": hosts,
            "default_host_id": registry.default_host_id(),
        })),
    )
        .into_response()
}

async fn v1_get_default_host(State(state): State<AppState>) -> Response {
    let registry = state.hub.registry.read().await;
    let default_host_id = registry.default_host_id();
    let is_connected = default_host_id.and_then(|host_id| {
        registry
            .hosts()
            .find(|host| host.host_id == host_id)
            .map(sentinel0_hub_core::HostRecord::eligible)
    });
    (
        StatusCode::OK,
        Json(json!({
            "ok": true,
            "default_host_id": default_host_id,
            "is_connected": is_connected,
        })),
    )
        .into_response()
}

async fn v1_set_default_host(
    State(state): State<AppState>,
    Json(request): Json<SetDefaultRequest>,
) -> Response {
    let is_connected = {
        let registry = state.hub.registry.read().await;
        let Some(host) = registry
            .hosts()
            .find(|host| host.host_id == request.host_id)
        else {
            return api_error(
                StatusCode::NOT_FOUND,
                "host_not_found",
                "unknown host_id".to_owned(),
            );
        };
        host.connected
    };
    if let Err(error) = state
        .hub
        .store
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .set_default_host(&request.host_id)
    {
        return store_error_response(&error);
    }
    if let Err(error) = state
        .hub
        .registry
        .write()
        .await
        .set_default(&request.host_id)
    {
        return host_mutation_error(&error);
    }
    (
        StatusCode::OK,
        Json(json!({
            "ok": true,
            "host_id": request.host_id,
            "is_connected": is_connected,
        })),
    )
        .into_response()
}

async fn v1_clear_default_host(State(state): State<AppState>) -> Response {
    let removed = match state
        .hub
        .store
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clear_default_host()
    {
        Ok(removed) => removed,
        Err(error) => return store_error_response(&error),
    };
    state.hub.registry.write().await.clear_default();
    (
        StatusCode::OK,
        Json(json!({"ok": true, "removed": removed})),
    )
        .into_response()
}

async fn v1_set_host_label(
    State(state): State<AppState>,
    Json(request): Json<SetLabelRequest>,
) -> Response {
    let label = request
        .label
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty());
    {
        let registry = state.hub.registry.read().await;
        if !registry.hosts().any(|host| host.host_id == request.host_id) {
            return api_error(
                StatusCode::NOT_FOUND,
                "host_not_found",
                "unknown host_id".to_owned(),
            );
        }
    }
    if let Err(error) = state
        .hub
        .store
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .set_host_label(&request.host_id, label.as_deref())
    {
        return match error {
            StoreError::LabelConflict(owner) => api_error(
                StatusCode::CONFLICT,
                "label_conflict",
                format!("label is already assigned to {owner}"),
            ),
            other => store_error_response(&other),
        };
    }
    if let Err(error) = state
        .hub
        .registry
        .write()
        .await
        .set_label(&request.host_id, label.clone())
    {
        return host_mutation_error(&error);
    }
    (
        StatusCode::OK,
        Json(json!({"ok": true, "host_id": request.host_id, "label": label})),
    )
        .into_response()
}

async fn v1_set_host_disabled(
    State(state): State<AppState>,
    Json(request): Json<SetDisabledRequest>,
) -> Response {
    {
        let registry = state.hub.registry.read().await;
        if !registry.hosts().any(|host| host.host_id == request.host_id) {
            return api_error(
                StatusCode::NOT_FOUND,
                "host_not_found",
                "unknown host_id".to_owned(),
            );
        }
    }
    if let Err(error) = state
        .hub
        .store
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .set_host_disabled(&request.host_id, request.disabled)
    {
        return store_error_response(&error);
    }
    if let Err(error) = state
        .hub
        .registry
        .write()
        .await
        .set_disabled(&request.host_id, request.disabled)
    {
        return host_mutation_error(&error);
    }
    if request.disabled {
        state
            .hub
            .registry
            .write()
            .await
            .disconnect(&request.host_id);
        let session = state.hub.sessions.write().await.remove(&request.host_id);
        if let Some(session) = session {
            let _ = session.tx.send(WsMessage::Close(None)).await;
        }
        let store = state
            .hub
            .store
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Err(error) = store.mark_host_disconnected(&request.host_id) {
            warn!(host_id = %request.host_id, %error, "failed to persist disabled host disconnect");
        }
        if let Err(error) = store.mark_running_jobs_orphaned(&request.host_id) {
            warn!(host_id = %request.host_id, %error, "failed to orphan disabled host jobs");
        }
    }
    (
        StatusCode::OK,
        Json(json!({
            "ok": true,
            "host_id": request.host_id,
            "disabled": request.disabled,
        })),
    )
        .into_response()
}

async fn v1_op(State(state): State<AppState>, Json(request): Json<OpRequest>) -> Response {
    let mut prepared = {
        let registry = state.hub.registry.read().await;
        match prepare_direct_request(
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
        }
    };

    let session = state
        .hub
        .sessions
        .read()
        .await
        .get(&prepared.host_id)
        .cloned();
    let Some(session) = session else {
        return api_error(
            StatusCode::BAD_GATEWAY,
            "agent_disconnected",
            format!("host {:?} has no active agent session", prepared.host_id),
        );
    };

    let request_id = format!("hreq_{}", Uuid::now_v7().simple());
    if let Some(response) = begin_native_idempotency(&state, &prepared, &request_id) {
        return response;
    }

    let background_job_id = prepared
        .background_requested()
        .then(|| format!("job_{}", Uuid::now_v7().simple()));
    if let Some(job_id) = background_job_id.as_deref() {
        prepared.assign_background_job_id(job_id);
    }

    let wire = prepared.wire_message(request_id.clone());
    let Ok(text) = serde_json::to_string(&wire) else {
        persist_terminal_request_error(
            &state,
            prepared.client_request_id.as_deref(),
            &request_id,
            StatusCode::INTERNAL_SERVER_ERROR,
            "serialization_error",
            "could not encode agent request",
        );
        return api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "serialization_error",
            "could not encode agent request".to_owned(),
        );
    };

    let message = match dispatch_and_wait(&state, &session, &prepared, &request_id, text).await {
        Ok(message) => message,
        Err(error) => {
            return api_error(error.status, error.code, error.message.to_owned());
        }
    };
    let Ok(response) = normalize_agent_response(
        &message,
        &request_id,
        prepared.client_request_id.as_deref(),
        false,
    ) else {
        persist_terminal_request_error(
            &state,
            prepared.client_request_id.as_deref(),
            &request_id,
            StatusCode::BAD_GATEWAY,
            "invalid_agent_response",
            "unexpected agent message",
        );
        return api_error(
            StatusCode::BAD_GATEWAY,
            "invalid_agent_response",
            "unexpected agent message".to_owned(),
        );
    };

    if let Some(job_id) = background_job_id.as_deref()
        && response.running_job_id() == Some(job_id)
        && let Err(error) = state
            .hub
            .store
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .job_started(job_id, &prepared.host_id, prepared.op.as_str())
    {
        return store_error_response(&error);
    }

    (StatusCode::OK, Json(response)).into_response()
}

fn begin_native_idempotency(
    state: &AppState,
    prepared: &PreparedDirectRequest,
    request_id: &str,
) -> Option<Response> {
    let (Some(client_request_id), Some(fingerprint)) = (
        prepared.client_request_id.as_deref(),
        prepared.invocation_fingerprint.as_deref(),
    ) else {
        return None;
    };
    let begin = state
        .hub
        .store
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .begin_idempotency(client_request_id, fingerprint, request_id);
    match begin {
        Ok(BeginIdempotency::Start) => None,
        Ok(BeginIdempotency::Conflict) => Some(api_error(
            StatusCode::CONFLICT,
            "request_id_conflict",
            "client_request_id was already used for a different invocation".to_owned(),
        )),
        Ok(BeginIdempotency::Pending { hub_request_id }) => Some(
            (
                StatusCode::CONFLICT,
                Json(json!({
                    "ok": false,
                    "error": "request_in_progress",
                    "message": "the original invocation is still in progress",
                    "client_request_id": client_request_id,
                    "hub_request_id": hub_request_id,
                    "replayed": false,
                })),
            )
                .into_response(),
        ),
        Ok(BeginIdempotency::Replay { status, body }) => Some(value_response(status, body)),
        Err(error) => {
            warn!(%error, "idempotency storage unavailable");
            Some(api_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "idempotency_unavailable",
                "idempotency storage is unavailable; request was not dispatched".to_owned(),
            ))
        }
    }
}

struct DispatchFailure {
    status: StatusCode,
    code: &'static str,
    message: &'static str,
}

async fn dispatch_and_wait(
    state: &AppState,
    session: &AgentSession,
    prepared: &PreparedDirectRequest,
    request_id: &str,
    text: String,
) -> Result<Message, DispatchFailure> {
    let (tx, rx) = oneshot::channel();
    state.hub.pending.lock().await.insert(
        request_id.to_owned(),
        PendingRequest {
            waiter: tx,
            client_request_id: prepared.client_request_id.clone(),
        },
    );
    if session.tx.send(WsMessage::Text(text.into())).await.is_err() {
        state.hub.pending.lock().await.remove(request_id);
        persist_terminal_request_error(
            state,
            prepared.client_request_id.as_deref(),
            request_id,
            StatusCode::BAD_GATEWAY,
            "agent_disconnected",
            "agent disconnected while dispatching",
        );
        return Err(DispatchFailure {
            status: StatusCode::BAD_GATEWAY,
            code: "agent_disconnected",
            message: "agent disconnected while dispatching",
        });
    }

    match tokio::time::timeout(Duration::from_secs(65), rx).await {
        Ok(Ok(message)) => Ok(message),
        Ok(Err(_)) => {
            state.hub.pending.lock().await.remove(request_id);
            persist_terminal_request_error(
                state,
                prepared.client_request_id.as_deref(),
                request_id,
                StatusCode::BAD_GATEWAY,
                "agent_disconnected",
                "agent response channel closed",
            );
            Err(DispatchFailure {
                status: StatusCode::BAD_GATEWAY,
                code: "agent_disconnected",
                message: "agent response channel closed",
            })
        }
        Err(_) => {
            state.hub.pending.lock().await.remove(request_id);
            persist_terminal_request_error(
                state,
                prepared.client_request_id.as_deref(),
                request_id,
                StatusCode::GATEWAY_TIMEOUT,
                "timeout",
                "agent response deadline exceeded",
            );
            Err(DispatchFailure {
                status: StatusCode::GATEWAY_TIMEOUT,
                code: "timeout",
                message: "agent response deadline exceeded",
            })
        }
    }
}

async fn v1_notifications(
    State(state): State<AppState>,
    Json(request): Json<NotificationsRequest>,
) -> Response {
    let mut store = state
        .hub
        .store
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    match request.operation.as_str() {
        "check" => match store.notifications_check() {
            Ok(check) => (
                StatusCode::OK,
                Json(json!({
                    "ok": true,
                    "completed": check.completed,
                    "running": check.running,
                    "orphaned": check.orphaned,
                    "broadcasts": [],
                    "answered_reports": [],
                })),
            )
                .into_response(),
            Err(error) => store_error_response(&error),
        },
        "get" => {
            let Some(job_id) = request.job_id.as_deref() else {
                return api_error(
                    StatusCode::BAD_REQUEST,
                    "missing_job_id",
                    "notifications get requires job_id".to_owned(),
                );
            };
            match store.notifications_get(job_id) {
                Ok(Some(job)) => (
                    StatusCode::OK,
                    Json(json!({
                        "ok": true,
                        "job_id": job.job_id,
                        "host": job.host_id,
                        "tool": job.tool,
                        "status": job.status,
                        "completion": job.completion,
                        "created_ms": job.created_ms,
                        "updated_ms": job.updated_ms,
                    })),
                )
                    .into_response(),
                Ok(None) => api_error(
                    StatusCode::NOT_FOUND,
                    "job_not_found",
                    "unknown job_id".to_owned(),
                ),
                Err(error) => store_error_response(&error),
            }
        }
        "ack" => match store.notifications_ack(request.job_id.as_deref()) {
            Ok(acked) => {
                (StatusCode::OK, Json(json!({"ok": true, "acked": acked}))).into_response()
            }
            Err(error) => store_error_response(&error),
        },
        _ => api_error(
            StatusCode::BAD_REQUEST,
            "invalid_operation",
            "notifications operation must be check, get or ack".to_owned(),
        ),
    }
}

fn persist_terminal_request_error(
    state: &AppState,
    client_request_id: Option<&str>,
    hub_request_id: &str,
    status: StatusCode,
    code: &str,
    message: &str,
) {
    let Some(client_request_id) = client_request_id else {
        return;
    };
    let body = json!({
        "ok": false,
        "error": code,
        "message": message,
        "client_request_id": client_request_id,
        "hub_request_id": hub_request_id,
        "replayed": false,
    });
    if let Err(error) = state
        .hub
        .store
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .complete_idempotency(client_request_id, hub_request_id, status.as_u16(), &body)
    {
        warn!(%hub_request_id, %error, "failed to persist terminal request error");
    }
}

fn value_response(status: u16, body: Value) -> Response {
    match StatusCode::from_u16(status) {
        Ok(status) => (status, Json(body)).into_response(),
        Err(_) => api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "store_error",
            "stored idempotency response has invalid HTTP status".to_owned(),
        ),
    }
}

fn host_mutation_error(error: &HostResolutionError) -> Response {
    match error {
        HostResolutionError::NotFound(_) => api_error(
            StatusCode::NOT_FOUND,
            "host_not_found",
            "unknown host_id".to_owned(),
        ),
        other => api_error(StatusCode::BAD_REQUEST, "invalid_host", other.to_string()),
    }
}

fn direct_request_error(error: &DirectRequestError) -> Response {
    match error {
        DirectRequestError::UnsupportedOp(_)
        | DirectRequestError::InvalidPayload(_)
        | DirectRequestError::InvalidClientRequestId => {
            let code = match error {
                DirectRequestError::UnsupportedOp(_) => "unsupported_op",
                DirectRequestError::InvalidPayload(_) => "invalid_payload",
                DirectRequestError::InvalidClientRequestId => "invalid_client_request_id",
                DirectRequestError::Host(_) => unreachable!("host errors handled separately"),
            };
            api_error(StatusCode::BAD_REQUEST, code, error.to_string())
        }
        DirectRequestError::Host(host) => host_error(host),
    }
}

fn host_error(error: &HostResolutionError) -> Response {
    let (status, code) = match error {
        HostResolutionError::NotFound(_) | HostResolutionError::NoEligibleHost => {
            (StatusCode::NOT_FOUND, "agent_offline")
        }
        HostResolutionError::Ambiguous(_) | HostResolutionError::AmbiguousDefault => {
            (StatusCode::BAD_REQUEST, "ambiguous_host")
        }
        HostResolutionError::DefaultOffline(_) => (StatusCode::CONFLICT, "default_host_offline"),
    };
    api_error(status, code, error.to_string())
}

fn store_error_response(error: &StoreError) -> Response {
    api_error(
        StatusCode::INTERNAL_SERVER_ERROR,
        "store_error",
        error.to_string(),
    )
}

fn api_error(status: StatusCode, code: &'static str, message: String) -> Response {
    (
        status,
        Json(ErrorBody {
            ok: false,
            error: code,
            message,
        }),
    )
        .into_response()
}
