#![forbid(unsafe_code)]

use axum::{
    Json, Router,
    extract::{
        State, WebSocketUpgrade,
        ws::{Message as WsMessage, WebSocket},
    },
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use chrono::Utc;
use futures_util::{SinkExt as _, StreamExt as _};
use sentinel0_hub_core::{HostRegistry, HostResolutionError, parse_op};
use sentinel0_proto::{HEARTBEAT_INTERVAL_SECS, Message};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, HashMap},
    env,
    sync::Arc,
    time::Duration,
};
use tokio::sync::{Mutex, RwLock, mpsc, oneshot};
use tracing::{info, warn};
use uuid::Uuid;

#[derive(Clone)]
struct AppState {
    hub: Arc<Hub>,
}

struct Hub {
    enrollment_token: String,
    registry: RwLock<HostRegistry>,
    sessions: RwLock<HashMap<String, AgentSession>>,
    pending: Mutex<HashMap<String, oneshot::Sender<Message>>>,
}

#[derive(Clone)]
struct AgentSession {
    session_id: String,
    tx: mpsc::Sender<WsMessage>,
}

#[derive(Debug, Deserialize)]
struct OpRequest {
    op: String,
    host_id: Option<String>,
    #[serde(default)]
    payload: Value,
    client_request_id: Option<String>,
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
    let listen = env::var("SENTINEL0_LISTEN").unwrap_or_else(|_| "127.0.0.1:8788".to_owned());

    let state = AppState {
        hub: Arc::new(Hub {
            enrollment_token,
            registry: RwLock::new(HostRegistry::default()),
            sessions: RwLock::new(HashMap::new()),
            pending: Mutex::new(HashMap::new()),
        }),
    };

    let app = Router::new()
        .route("/healthz", get(healthz))
        .route("/agent/connect", get(agent_connect))
        .route("/v1/op", post(v1_op))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(&listen).await?;
    info!(%listen, "Sentinel0² native Hub listening");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn healthz() -> Json<Value> {
    Json(json!({"ok": true, "service": "sentinel0-hub-native"}))
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
    let Message::Hello {
        protocol_version,
        host,
        ..
    } = message
    else {
        warn!("agent first message was not hello");
        return;
    };

    if !protocol_version.starts_with("1.") {
        warn!(%protocol_version, "incompatible protocol");
        return;
    }

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
    state.hub.sessions.write().await.insert(
        host_id.clone(),
        AgentSession {
            session_id: session_id.clone(),
            tx,
        },
    );

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
                    Some(Ok(WsMessage::Binary(_))) => {
                        warn!(%host_id, "binary frame received before transfer coordinator exists");
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
    }
    info!(%host_id, %session_id, "agent disconnected");
}

async fn handle_agent_text(state: &AppState, host_id: &str, raw: &str) {
    let Ok(message) = serde_json::from_str::<Message>(raw) else {
        warn!(%host_id, "agent sent malformed JSON message");
        return;
    };

    match message {
        Message::Response { ref id, .. } => {
            if let Some(waiter) = state.hub.pending.lock().await.remove(id) {
                let _ = waiter.send(message);
            }
        }
        Message::Ping { timestamp } => {
            let pong = Message::Pong { timestamp };
            if let Ok(text) = serde_json::to_string(&pong) {
                send_text_to_host(state, host_id, &text).await;
            }
        }
        Message::Event { kind, .. } => {
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

async fn v1_op(State(state): State<AppState>, Json(request): Json<OpRequest>) -> Response {
    let Some(op) = parse_op(&request.op) else {
        return api_error(
            StatusCode::BAD_REQUEST,
            "unsupported_op",
            format!("unsupported op {:?}", request.op),
        );
    };

    let host_id = {
        let registry = state.hub.registry.read().await;
        match registry.resolve(request.host_id.as_deref()) {
            Ok(host) => host.host_id.clone(),
            Err(error) => return host_error(&error),
        }
    };

    let session = state.hub.sessions.read().await.get(&host_id).cloned();
    let Some(session) = session else {
        return api_error(
            StatusCode::BAD_GATEWAY,
            "agent_disconnected",
            format!("host {host_id:?} has no active agent session"),
        );
    };

    let request_id = format!("hreq_{}", Uuid::now_v7().simple());
    let payload = match request.payload {
        Value::Object(values) => values.into_iter().collect::<BTreeMap<_, _>>(),
        Value::Null => BTreeMap::new(),
        other => {
            return api_error(
                StatusCode::BAD_REQUEST,
                "invalid_payload",
                format!("payload must be an object, got {other}"),
            );
        }
    };

    let wire = Message::Request {
        id: request_id.clone(),
        op,
        payload,
        deadline: None,
        opaque_ref: None,
    };
    let Ok(text) = serde_json::to_string(&wire) else {
        return api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "serialization_error",
            "could not encode agent request".to_owned(),
        );
    };

    let (tx, rx) = oneshot::channel();
    state
        .hub
        .pending
        .lock()
        .await
        .insert(request_id.clone(), tx);
    if session.tx.send(WsMessage::Text(text.into())).await.is_err() {
        state.hub.pending.lock().await.remove(&request_id);
        return api_error(
            StatusCode::BAD_GATEWAY,
            "agent_disconnected",
            "agent disconnected while dispatching".to_owned(),
        );
    }

    let response = tokio::time::timeout(Duration::from_secs(65), rx).await;
    let Ok(Ok(Message::Response {
        ok, result, error, ..
    })) = response
    else {
        state.hub.pending.lock().await.remove(&request_id);
        return api_error(
            StatusCode::GATEWAY_TIMEOUT,
            "timeout",
            "agent response deadline exceeded".to_owned(),
        );
    };

    let mut result = result;
    if let Some(result) = result.as_mut() {
        result.remove("_sx_timing");
    }

    (
        StatusCode::OK,
        Json(json!({
            "ok": ok,
            "result": result,
            "error": error,
            "hub_request_id": request_id,
            "client_request_id": request.client_request_id,
            "replayed": false
        })),
    )
        .into_response()
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
