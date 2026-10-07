//! Native adapter for bounded host-to-host file transfer.

use super::{AgentSession, AppState, PendingRequest, api_error};
use axum::{
    Json, extract::State, http::StatusCode, response::IntoResponse as _, response::Response,
};
use sentinel0_hub_core::{
    ExportChunkResult, ExportPlan, HostRegistry, HostResolutionError, TransferChunkAck,
    TransferProtocolError, parse_export_chunk_result, parse_export_digest, parse_export_plan,
    parse_transfer_chunk_ack, parse_upload_result, validate_upload_init,
};
use sentinel0_proto::{Message, Op, TRANSFER_CHUNK_BYTES, decode_binary_frame};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{collections::BTreeMap, fmt::Write as _, time::Duration};
use tokio::sync::oneshot;
use tracing::warn;
use uuid::Uuid;

pub(super) type TransferChunkKey = (String, String, u32);
pub(super) type BinaryWaiters =
    std::collections::HashMap<TransferChunkKey, oneshot::Sender<Vec<u8>>>;
pub(super) type AckWaiters =
    std::collections::HashMap<TransferChunkKey, oneshot::Sender<TransferChunkAck>>;

#[derive(Debug, Deserialize)]
pub(super) struct TransferFileRequest {
    source_host_id: String,
    source_path: String,
    destination_host_id: String,
    destination_path: String,
    #[serde(default)]
    overwrite: bool,
    #[serde(default)]
    land_in_place: bool,
}

struct TransferFailure {
    status: StatusCode,
    code: &'static str,
    message: String,
}

impl TransferFailure {
    fn response(self) -> Response {
        api_error(self.status, self.code, self.message)
    }

    fn transport(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            code,
            message: message.into(),
        }
    }

    fn protocol(error: TransferProtocolError) -> Self {
        match error {
            TransferProtocolError::TooLarge(size) => Self::transport(
                StatusCode::PAYLOAD_TOO_LARGE,
                "file_too_large",
                format!("source file is {size} bytes; Hub transfer limit is 512 MiB"),
            ),
            TransferProtocolError::AgentRejected(error) => Self::transport(
                StatusCode::BAD_REQUEST,
                "agent_rejected",
                format!("{}: {}", error.code, error.message),
            ),
            other => Self::transport(
                StatusCode::BAD_GATEWAY,
                "transfer_protocol_error",
                other.to_string(),
            ),
        }
    }
}

pub(super) async fn v1_transfer_file(
    State(state): State<AppState>,
    Json(request): Json<TransferFileRequest>,
) -> Response {
    match transfer_file(&state, &request).await {
        Ok(result) => (StatusCode::OK, Json(result)).into_response(),
        Err(error) => error.response(),
    }
}

async fn transfer_file(
    state: &AppState,
    request: &TransferFileRequest,
) -> Result<Value, TransferFailure> {
    let (source_host_id, destination_host_id) = {
        let registry = state.hub.registry.read().await;
        let source_host_id = resolve_host(&registry, &request.source_host_id)?;
        let destination_host_id = resolve_host(&registry, &request.destination_host_id)?;
        (source_host_id, destination_host_id)
    };

    let (source, destination) = {
        let sessions = state.hub.sessions.read().await;
        let source = sessions.get(&source_host_id).cloned().ok_or_else(|| {
            TransferFailure::transport(
                StatusCode::BAD_GATEWAY,
                "agent_disconnected",
                "source host has no active agent session",
            )
        })?;
        let destination = sessions.get(&destination_host_id).cloned().ok_or_else(|| {
            TransferFailure::transport(
                StatusCode::BAD_GATEWAY,
                "agent_disconnected",
                "destination host has no active agent session",
            )
        })?;
        (source, destination)
    };

    let transfer_id = Uuid::now_v7().simple().to_string();
    let init = internal_request(
        state,
        &source,
        Op::FileExportInit,
        BTreeMap::from([
            ("transfer_id".to_owned(), Value::String(transfer_id.clone())),
            (
                "source_path".to_owned(),
                Value::String(request.source_path.clone()),
            ),
            (
                "chunk_size".to_owned(),
                Value::from(u64::try_from(TRANSFER_CHUNK_BYTES).unwrap_or(u64::MAX)),
            ),
        ]),
    )
    .await?;
    let plan = parse_export_plan(&init, &transfer_id).map_err(TransferFailure::protocol)?;

    let result = transfer_after_source_init(
        state,
        request,
        &source_host_id,
        &destination_host_id,
        &source,
        &destination,
        &plan,
    )
    .await;
    if result.is_err() {
        best_effort_export_cleanup(state, &source, &transfer_id).await;
    }
    result
}

fn resolve_host(registry: &HostRegistry, selector: &str) -> Result<String, TransferFailure> {
    registry
        .resolve(Some(selector))
        .map(|host| host.host_id.clone())
        .map_err(|error| match error {
            HostResolutionError::NotFound(_) | HostResolutionError::NoEligibleHost => {
                TransferFailure::transport(
                    StatusCode::NOT_FOUND,
                    "agent_offline",
                    error.to_string(),
                )
            }
            HostResolutionError::Ambiguous(_) | HostResolutionError::AmbiguousDefault => {
                TransferFailure::transport(
                    StatusCode::BAD_REQUEST,
                    "ambiguous_host",
                    error.to_string(),
                )
            }
            HostResolutionError::DefaultOffline(_) => TransferFailure::transport(
                StatusCode::CONFLICT,
                "default_host_offline",
                error.to_string(),
            ),
        })
}

async fn transfer_after_source_init(
    state: &AppState,
    request: &TransferFileRequest,
    source_host_id: &str,
    destination_host_id: &str,
    source: &AgentSession,
    destination: &AgentSession,
    plan: &ExportPlan,
) -> Result<Value, TransferFailure> {
    let destination_init = internal_request(
        state,
        destination,
        Op::UploadInit,
        BTreeMap::from([
            (
                "upload_id".to_owned(),
                Value::String(plan.transfer_id.clone()),
            ),
            (
                "target_path".to_owned(),
                Value::String(request.destination_path.clone()),
            ),
            ("total_size".to_owned(), Value::from(plan.size)),
            ("overwrite".to_owned(), Value::Bool(request.overwrite)),
            (
                "land_in_place".to_owned(),
                Value::Bool(request.land_in_place),
            ),
            ("filename".to_owned(), Value::String(plan.filename.clone())),
        ]),
    )
    .await?;
    validate_upload_init(&destination_init, &plan.transfer_id)
        .map_err(TransferFailure::protocol)?;

    for index_u64 in 0..plan.num_chunks {
        let index = u32::try_from(index_u64).map_err(|_| {
            TransferFailure::transport(
                StatusCode::BAD_GATEWAY,
                "transfer_protocol_error",
                "source chunk index exceeds protocol range",
            )
        })?;
        transfer_one_chunk(
            state,
            source_host_id,
            destination_host_id,
            source,
            destination,
            plan,
            index,
        )
        .await?;
    }

    let source_complete = internal_request(
        state,
        source,
        Op::FileExportComplete,
        BTreeMap::from([(
            "transfer_id".to_owned(),
            Value::String(plan.transfer_id.clone()),
        )]),
    )
    .await?;
    let digest = parse_export_digest(&source_complete, plan).map_err(TransferFailure::protocol)?;

    let destination_complete = internal_request(
        state,
        destination,
        Op::UploadComplete,
        BTreeMap::from([
            (
                "upload_id".to_owned(),
                Value::String(plan.transfer_id.clone()),
            ),
            ("sha256".to_owned(), Value::String(digest.sha256.clone())),
        ]),
    )
    .await?;
    let upload = parse_upload_result(
        &destination_complete,
        &plan.transfer_id,
        plan.size,
        &digest.sha256,
    )
    .map_err(TransferFailure::protocol)?;

    Ok(json!({
        "ok": true,
        "transfer_id": plan.transfer_id,
        "source_host_id": source_host_id,
        "destination_host_id": destination_host_id,
        "source_path": request.source_path,
        "destination_path": upload.target_path,
        "size": upload.size,
        "sha256": upload.sha256,
        "chunks": plan.num_chunks,
    }))
}

#[allow(clippy::too_many_arguments)]
async fn transfer_one_chunk(
    state: &AppState,
    source_host_id: &str,
    destination_host_id: &str,
    source: &AgentSession,
    destination: &AgentSession,
    plan: &ExportPlan,
    index: u32,
) -> Result<(), TransferFailure> {
    let (binary, chunk) = receive_source_chunk(state, source_host_id, source, plan, index).await?;
    forward_destination_chunk(
        state,
        destination_host_id,
        destination,
        plan,
        index,
        binary,
        chunk.bytes,
    )
    .await
}

async fn receive_source_chunk(
    state: &AppState,
    source_host_id: &str,
    source: &AgentSession,
    plan: &ExportPlan,
    index: u32,
) -> Result<(Vec<u8>, ExportChunkResult), TransferFailure> {
    let key = (source_host_id.to_owned(), plan.transfer_id.clone(), index);
    let (binary_tx, binary_rx) = oneshot::channel();
    state
        .hub
        .transfer_binary_waiters
        .lock()
        .await
        .insert(key.clone(), binary_tx);

    let response = internal_request(
        state,
        source,
        Op::FileExportChunk,
        BTreeMap::from([
            (
                "transfer_id".to_owned(),
                Value::String(plan.transfer_id.clone()),
            ),
            ("chunk_index".to_owned(), Value::from(u64::from(index))),
        ]),
    )
    .await;
    let response = match response {
        Ok(response) => response,
        Err(error) => {
            state.hub.transfer_binary_waiters.lock().await.remove(&key);
            return Err(error);
        }
    };
    let chunk = match parse_export_chunk_result(&response, &plan.transfer_id, index) {
        Ok(chunk) => chunk,
        Err(error) => {
            state.hub.transfer_binary_waiters.lock().await.remove(&key);
            return Err(TransferFailure::protocol(error));
        }
    };

    let binary = match tokio::time::timeout(Duration::from_secs(65), binary_rx).await {
        Ok(Ok(binary)) => binary,
        Ok(Err(_)) => {
            return Err(TransferFailure::transport(
                StatusCode::BAD_GATEWAY,
                "agent_disconnected",
                "source binary transfer channel closed",
            ));
        }
        Err(_) => {
            state.hub.transfer_binary_waiters.lock().await.remove(&key);
            return Err(TransferFailure::transport(
                StatusCode::GATEWAY_TIMEOUT,
                "timeout",
                "source binary chunk deadline exceeded",
            ));
        }
    };
    validate_source_binary(&binary, plan, index, &chunk)?;
    Ok((binary, chunk))
}

fn validate_source_binary(
    binary: &[u8],
    plan: &ExportPlan,
    index: u32,
    chunk: &ExportChunkResult,
) -> Result<(), TransferFailure> {
    let frame = decode_binary_frame(binary).map_err(|error| {
        TransferFailure::transport(
            StatusCode::BAD_GATEWAY,
            "transfer_protocol_error",
            error.to_string(),
        )
    })?;
    let payload_bytes = u64::try_from(frame.payload.len()).unwrap_or(u64::MAX);
    if frame.chunk_index != index || payload_bytes != chunk.bytes {
        return Err(TransferFailure::transport(
            StatusCode::BAD_GATEWAY,
            "transfer_protocol_error",
            format!(
                "source binary metadata mismatch at chunk {index}: frame_index={}, payload_bytes={}, response_bytes={}",
                frame.chunk_index, payload_bytes, chunk.bytes
            ),
        ));
    }
    let should_eof = u64::from(index) + 1 == plan.num_chunks;
    if chunk.eof != should_eof {
        return Err(TransferFailure::transport(
            StatusCode::BAD_GATEWAY,
            "transfer_protocol_error",
            format!("source eof mismatch at chunk {index}"),
        ));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn forward_destination_chunk(
    state: &AppState,
    destination_host_id: &str,
    destination: &AgentSession,
    plan: &ExportPlan,
    index: u32,
    binary: Vec<u8>,
    expected_bytes: u64,
) -> Result<(), TransferFailure> {
    let key = (
        destination_host_id.to_owned(),
        plan.transfer_id.clone(),
        index,
    );
    let (ack_tx, ack_rx) = oneshot::channel();
    state
        .hub
        .transfer_ack_waiters
        .lock()
        .await
        .insert(key.clone(), ack_tx);
    if destination
        .tx
        .send(axum::extract::ws::Message::Binary(binary.into()))
        .await
        .is_err()
    {
        state.hub.transfer_ack_waiters.lock().await.remove(&key);
        return Err(TransferFailure::transport(
            StatusCode::BAD_GATEWAY,
            "agent_disconnected",
            "destination agent disconnected while forwarding chunk",
        ));
    }
    let ack = match tokio::time::timeout(Duration::from_secs(65), ack_rx).await {
        Ok(Ok(ack)) => ack,
        Ok(Err(_)) => {
            return Err(TransferFailure::transport(
                StatusCode::BAD_GATEWAY,
                "agent_disconnected",
                "destination transfer acknowledgement channel closed",
            ));
        }
        Err(_) => {
            state.hub.transfer_ack_waiters.lock().await.remove(&key);
            return Err(TransferFailure::transport(
                StatusCode::GATEWAY_TIMEOUT,
                "timeout",
                "destination transfer acknowledgement deadline exceeded",
            ));
        }
    };
    if !ack.ok {
        return Err(TransferFailure::transport(
            StatusCode::BAD_GATEWAY,
            "destination_chunk_rejected",
            ack.error
                .unwrap_or_else(|| "destination rejected chunk".to_owned()),
        ));
    }
    if ack.bytes != Some(expected_bytes) {
        return Err(TransferFailure::transport(
            StatusCode::BAD_GATEWAY,
            "transfer_protocol_error",
            format!(
                "destination acknowledged {:?} bytes for chunk {index}, expected {expected_bytes}",
                ack.bytes
            ),
        ));
    }
    Ok(())
}

async fn internal_request(
    state: &AppState,
    session: &AgentSession,
    op: Op,
    payload: BTreeMap<String, Value>,
) -> Result<Message, TransferFailure> {
    let request_id = format!("hreq_{}", Uuid::now_v7().simple());
    let wire = Message::Request {
        id: request_id.clone(),
        op,
        payload,
        deadline: None,
        opaque_ref: None,
    };
    let text = serde_json::to_string(&wire).map_err(|error| {
        TransferFailure::transport(
            StatusCode::INTERNAL_SERVER_ERROR,
            "serialization_error",
            error.to_string(),
        )
    })?;
    let (tx, rx) = oneshot::channel();
    state.hub.pending.lock().await.insert(
        request_id.clone(),
        PendingRequest {
            waiter: tx,
            client_request_id: None,
        },
    );
    if session
        .tx
        .send(axum::extract::ws::Message::Text(text.into()))
        .await
        .is_err()
    {
        state.hub.pending.lock().await.remove(&request_id);
        return Err(TransferFailure::transport(
            StatusCode::BAD_GATEWAY,
            "agent_disconnected",
            "agent disconnected while dispatching transfer operation",
        ));
    }
    match tokio::time::timeout(Duration::from_secs(65), rx).await {
        Ok(Ok(message)) => Ok(message),
        Ok(Err(_)) => Err(TransferFailure::transport(
            StatusCode::BAD_GATEWAY,
            "agent_disconnected",
            "agent response channel closed",
        )),
        Err(_) => {
            state.hub.pending.lock().await.remove(&request_id);
            Err(TransferFailure::transport(
                StatusCode::GATEWAY_TIMEOUT,
                "timeout",
                "agent transfer operation deadline exceeded",
            ))
        }
    }
}

async fn best_effort_export_cleanup(state: &AppState, source: &AgentSession, transfer_id: &str) {
    if let Err(error) = internal_request(
        state,
        source,
        Op::FileExportComplete,
        BTreeMap::from([(
            "transfer_id".to_owned(),
            Value::String(transfer_id.to_owned()),
        )]),
    )
    .await
    {
        warn!(%transfer_id, message = %error.message, "failed to clean source export session");
    }
}

pub(super) async fn handle_binary_frame(state: &AppState, host_id: &str, raw: Vec<u8>) {
    let frame = match decode_binary_frame(&raw) {
        Ok(frame) => frame,
        Err(error) => {
            warn!(%host_id, %error, "agent sent malformed binary transfer frame");
            return;
        }
    };
    let transfer_id = hex_transfer_id(&frame.transfer_id);
    let key = (host_id.to_owned(), transfer_id.clone(), frame.chunk_index);
    let waiter = state.hub.transfer_binary_waiters.lock().await.remove(&key);
    if let Some(waiter) = waiter {
        let _ = waiter.send(raw);
    } else {
        warn!(%host_id, %transfer_id, chunk_index = frame.chunk_index, "unexpected binary transfer frame");
    }
}

pub(super) async fn handle_transfer_event(state: &AppState, host_id: &str, message: &Message) {
    let ack = match parse_transfer_chunk_ack(message) {
        Ok(Some(ack)) => ack,
        Ok(None) => return,
        Err(error) => {
            warn!(%host_id, %error, "invalid transfer_chunk_ack event");
            return;
        }
    };
    let key = (host_id.to_owned(), ack.transfer_id.clone(), ack.chunk_index);
    let waiter = state.hub.transfer_ack_waiters.lock().await.remove(&key);
    if let Some(waiter) = waiter {
        let _ = waiter.send(ack);
    } else {
        warn!(%host_id, transfer_id = %ack.transfer_id, chunk_index = ack.chunk_index, "unexpected transfer_chunk_ack event");
    }
}

fn hex_transfer_id(bytes: &[u8; 16]) -> String {
    let mut out = String::with_capacity(32);
    for byte in bytes {
        let _ = write!(&mut out, "{byte:02x}");
    }
    out
}
