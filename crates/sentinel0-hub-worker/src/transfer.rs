//! Cloudflare Durable Object adapter for bounded host-to-host file transfer.

use super::{PendingRequest, TenantHub, json_error};
use futures_channel::oneshot;
use futures_util::future::{Either, select};
use sentinel0_hub_core::{
    ExportChunkResult, ExportPlan, HostRegistry, HostResolutionError, TransferChunkAck,
    TransferProtocolError, parse_export_chunk_result, parse_export_digest, parse_export_plan,
    parse_transfer_chunk_ack, parse_upload_result, validate_upload_init,
};
use sentinel0_proto::{Message, Op, TRANSFER_CHUNK_BYTES, decode_binary_frame};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{collections::BTreeMap, fmt::Write as _, time::Duration};
use worker::{Delay, Response, Result, WebSocket, console_warn};

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
    status: u16,
    code: &'static str,
    message: String,
}

impl TransferFailure {
    fn new(status: u16, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            code,
            message: message.into(),
        }
    }

    fn protocol(error: TransferProtocolError) -> Self {
        match error {
            TransferProtocolError::TooLarge(size) => Self::new(
                413,
                "file_too_large",
                format!("source file is {size} bytes; Hub transfer limit is 512 MiB"),
            ),
            TransferProtocolError::AgentRejected(error) => Self::new(
                400,
                "agent_rejected",
                format!("{}: {}", error.code, error.message),
            ),
            other => Self::new(502, "transfer_protocol_error", other.to_string()),
        }
    }

    fn response(self) -> Result<Response> {
        json_error(self.status, self.code, &self.message)
    }
}

impl TenantHub {
    pub(super) async fn transfer_file(&self, request: TransferFileRequest) -> Result<Response> {
        match self.transfer_file_inner(&request).await {
            Ok(body) => Response::from_json(&body),
            Err(error) => error.response(),
        }
    }

    async fn transfer_file_inner(
        &self,
        request: &TransferFileRequest,
    ) -> std::result::Result<Value, TransferFailure> {
        let live = self.live_sockets().map_err(|error| {
            TransferFailure::new(503, "hub_state_unavailable", error.to_string())
        })?;
        let registry = self.load_registry(&live).map_err(|error| {
            TransferFailure::new(503, "hub_state_unavailable", error.to_string())
        })?;
        let source_host_id = resolve_host(&registry, &request.source_host_id)?;
        let destination_host_id = resolve_host(&registry, &request.destination_host_id)?;
        let source = live.get(&source_host_id).cloned().ok_or_else(|| {
            TransferFailure::new(
                502,
                "agent_disconnected",
                "source host has no active agent socket",
            )
        })?;
        let destination = live.get(&destination_host_id).cloned().ok_or_else(|| {
            TransferFailure::new(
                502,
                "agent_disconnected",
                "destination host has no active agent socket",
            )
        })?;

        let transfer_id = format!(
            "{:016x}{:016x}",
            worker::Date::now().as_millis(),
            self.next_transfer_nonce()
        );
        let source_init = self
            .transfer_internal_request(
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
        let plan =
            parse_export_plan(&source_init, &transfer_id).map_err(TransferFailure::protocol)?;

        let result = self
            .transfer_after_source_init(
                request,
                &source_host_id,
                &destination_host_id,
                &source,
                &destination,
                &plan,
            )
            .await;
        if result.is_err() {
            self.best_effort_export_cleanup(&source, &plan.transfer_id)
                .await;
        }
        result
    }

    #[allow(clippy::too_many_arguments)]
    async fn transfer_after_source_init(
        &self,
        request: &TransferFileRequest,
        source_host_id: &str,
        destination_host_id: &str,
        source: &WebSocket,
        destination: &WebSocket,
        plan: &ExportPlan,
    ) -> std::result::Result<Value, TransferFailure> {
        let destination_init = self
            .transfer_internal_request(
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
                TransferFailure::new(
                    502,
                    "transfer_protocol_error",
                    "source chunk index exceeds protocol range",
                )
            })?;
            self.transfer_one_chunk(
                source_host_id,
                destination_host_id,
                source,
                destination,
                plan,
                index,
            )
            .await?;
        }

        let source_complete = self
            .transfer_internal_request(
                source,
                Op::FileExportComplete,
                BTreeMap::from([(
                    "transfer_id".to_owned(),
                    Value::String(plan.transfer_id.clone()),
                )]),
            )
            .await?;
        let digest =
            parse_export_digest(&source_complete, plan).map_err(TransferFailure::protocol)?;

        let destination_complete = self
            .transfer_internal_request(
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
        &self,
        source_host_id: &str,
        destination_host_id: &str,
        source: &WebSocket,
        destination: &WebSocket,
        plan: &ExportPlan,
        index: u32,
    ) -> std::result::Result<(), TransferFailure> {
        let (binary, chunk) = self
            .receive_source_chunk(source_host_id, source, plan, index)
            .await?;
        self.forward_destination_chunk(
            destination_host_id,
            destination,
            plan,
            index,
            &binary,
            chunk.bytes,
        )
        .await
    }

    async fn receive_source_chunk(
        &self,
        source_host_id: &str,
        source: &WebSocket,
        plan: &ExportPlan,
        index: u32,
    ) -> std::result::Result<(Vec<u8>, ExportChunkResult), TransferFailure> {
        let key = (source_host_id.to_owned(), plan.transfer_id.clone(), index);
        let (binary_tx, binary_rx) = oneshot::channel();
        self.transfer_binary_waiters
            .borrow_mut()
            .insert(key.clone(), binary_tx);
        let response = self
            .transfer_internal_request(
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
                self.transfer_binary_waiters.borrow_mut().remove(&key);
                return Err(error);
            }
        };
        let chunk = match parse_export_chunk_result(&response, &plan.transfer_id, index) {
            Ok(chunk) => chunk,
            Err(error) => {
                self.transfer_binary_waiters.borrow_mut().remove(&key);
                return Err(TransferFailure::protocol(error));
            }
        };
        let binary = match select(binary_rx, Delay::from(Duration::from_secs(65))).await {
            Either::Left((Ok(binary), _)) => binary,
            Either::Left((Err(_), _)) => {
                return Err(TransferFailure::new(
                    502,
                    "agent_disconnected",
                    "source binary transfer channel closed",
                ));
            }
            Either::Right(((), _)) => {
                self.transfer_binary_waiters.borrow_mut().remove(&key);
                return Err(TransferFailure::new(
                    504,
                    "timeout",
                    "source binary chunk deadline exceeded",
                ));
            }
        };
        validate_source_binary(&binary, plan, index, &chunk)?;
        Ok((binary, chunk))
    }

    #[allow(clippy::too_many_arguments)]
    async fn forward_destination_chunk(
        &self,
        destination_host_id: &str,
        destination: &WebSocket,
        plan: &ExportPlan,
        index: u32,
        binary: &[u8],
        expected_bytes: u64,
    ) -> std::result::Result<(), TransferFailure> {
        let key = (
            destination_host_id.to_owned(),
            plan.transfer_id.clone(),
            index,
        );
        let (ack_tx, ack_rx) = oneshot::channel();
        self.transfer_ack_waiters
            .borrow_mut()
            .insert(key.clone(), ack_tx);
        if let Err(error) = destination.send_with_bytes(binary) {
            self.transfer_ack_waiters.borrow_mut().remove(&key);
            return Err(TransferFailure::new(
                502,
                "agent_disconnected",
                format!("destination send failed: {error}"),
            ));
        }
        let ack = match select(ack_rx, Delay::from(Duration::from_secs(65))).await {
            Either::Left((Ok(ack), _)) => ack,
            Either::Left((Err(_), _)) => {
                return Err(TransferFailure::new(
                    502,
                    "agent_disconnected",
                    "destination transfer acknowledgement channel closed",
                ));
            }
            Either::Right(((), _)) => {
                self.transfer_ack_waiters.borrow_mut().remove(&key);
                return Err(TransferFailure::new(
                    504,
                    "timeout",
                    "destination transfer acknowledgement deadline exceeded",
                ));
            }
        };
        if !ack.ok {
            return Err(TransferFailure::new(
                502,
                "destination_chunk_rejected",
                ack.error
                    .unwrap_or_else(|| "destination rejected chunk".to_owned()),
            ));
        }
        if ack.bytes != Some(expected_bytes) {
            return Err(TransferFailure::new(
                502,
                "transfer_protocol_error",
                format!(
                    "destination acknowledged {:?} bytes for chunk {index}, expected {expected_bytes}",
                    ack.bytes
                ),
            ));
        }
        Ok(())
    }

    async fn transfer_internal_request(
        &self,
        socket: &WebSocket,
        op: Op,
        payload: BTreeMap<String, Value>,
    ) -> std::result::Result<Message, TransferFailure> {
        let request_id = self.next_request_id();
        let wire = Message::Request {
            id: request_id.clone(),
            op,
            payload,
            deadline: None,
            opaque_ref: None,
        };
        let (tx, rx) = oneshot::channel();
        self.pending.borrow_mut().insert(
            request_id.clone(),
            PendingRequest {
                waiter: tx,
                client_request_id: None,
            },
        );
        if let Err(error) = socket.send(&wire) {
            self.pending.borrow_mut().remove(&request_id);
            return Err(TransferFailure::new(
                502,
                "agent_disconnected",
                format!("agent send failed: {error}"),
            ));
        }
        match select(rx, Delay::from(Duration::from_secs(65))).await {
            Either::Left((Ok(message), _)) => Ok(message),
            Either::Left((Err(_), _)) => Err(TransferFailure::new(
                502,
                "agent_disconnected",
                "agent response channel closed",
            )),
            Either::Right(((), _)) => {
                self.pending.borrow_mut().remove(&request_id);
                Err(TransferFailure::new(
                    504,
                    "timeout",
                    "agent transfer operation deadline exceeded",
                ))
            }
        }
    }

    async fn best_effort_export_cleanup(&self, source: &WebSocket, transfer_id: &str) {
        if let Err(error) = self
            .transfer_internal_request(
                source,
                Op::FileExportComplete,
                BTreeMap::from([(
                    "transfer_id".to_owned(),
                    Value::String(transfer_id.to_owned()),
                )]),
            )
            .await
        {
            console_warn!(
                "failed to clean source export session {transfer_id}: {}",
                error.message
            );
        }
    }

    pub(super) fn handle_transfer_binary(&self, host_id: &str, raw: Vec<u8>) {
        let frame = match decode_binary_frame(&raw) {
            Ok(frame) => frame,
            Err(error) => {
                console_warn!("agent {host_id} sent malformed binary frame: {error}");
                return;
            }
        };
        let transfer_id = hex_transfer_id(&frame.transfer_id);
        let key = (host_id.to_owned(), transfer_id.clone(), frame.chunk_index);
        if let Some(waiter) = self.transfer_binary_waiters.borrow_mut().remove(&key) {
            let _ = waiter.send(raw);
        } else {
            console_warn!(
                "unexpected binary transfer frame host={host_id} transfer={transfer_id} chunk={}",
                frame.chunk_index
            );
        }
    }

    pub(super) fn handle_transfer_event(&self, host_id: &str, message: &Message) {
        let ack = match parse_transfer_chunk_ack(message) {
            Ok(Some(ack)) => ack,
            Ok(None) => return,
            Err(error) => {
                console_warn!("invalid transfer_chunk_ack from {host_id}: {error}");
                return;
            }
        };
        let key = (host_id.to_owned(), ack.transfer_id.clone(), ack.chunk_index);
        if let Some(waiter) = self.transfer_ack_waiters.borrow_mut().remove(&key) {
            let _ = waiter.send(ack);
        } else {
            console_warn!(
                "unexpected transfer ack host={host_id} transfer={} chunk={}",
                ack.transfer_id,
                ack.chunk_index
            );
        }
    }

    fn next_transfer_nonce(&self) -> u64 {
        let mut counter = self.request_counter.borrow_mut();
        *counter = counter.wrapping_add(1);
        *counter
    }
}

fn resolve_host(
    registry: &HostRegistry,
    selector: &str,
) -> std::result::Result<String, TransferFailure> {
    registry
        .resolve(Some(selector))
        .map(|host| host.host_id.clone())
        .map_err(|error| match error {
            HostResolutionError::NotFound(_) | HostResolutionError::NoEligibleHost => {
                TransferFailure::new(404, "agent_offline", error.to_string())
            }
            HostResolutionError::Ambiguous(_) | HostResolutionError::AmbiguousDefault => {
                TransferFailure::new(400, "ambiguous_host", error.to_string())
            }
            HostResolutionError::DefaultOffline(_) => {
                TransferFailure::new(409, "default_host_offline", error.to_string())
            }
        })
}

fn validate_source_binary(
    binary: &[u8],
    plan: &ExportPlan,
    index: u32,
    chunk: &ExportChunkResult,
) -> std::result::Result<(), TransferFailure> {
    let frame = decode_binary_frame(binary)
        .map_err(|error| TransferFailure::new(502, "transfer_protocol_error", error.to_string()))?;
    let payload_bytes = u64::try_from(frame.payload.len()).unwrap_or(u64::MAX);
    if frame.chunk_index != index || payload_bytes != chunk.bytes {
        return Err(TransferFailure::new(
            502,
            "transfer_protocol_error",
            format!(
                "source binary metadata mismatch at chunk {index}: frame_index={}, payload_bytes={}, response_bytes={}",
                frame.chunk_index, payload_bytes, chunk.bytes
            ),
        ));
    }
    let should_eof = u64::from(index) + 1 == plan.num_chunks;
    if chunk.eof != should_eof {
        return Err(TransferFailure::new(
            502,
            "transfer_protocol_error",
            format!("source eof mismatch at chunk {index}"),
        ));
    }
    Ok(())
}

fn hex_transfer_id(bytes: &[u8; 16]) -> String {
    let mut out = String::with_capacity(32);
    for byte in bytes {
        let _ = write!(&mut out, "{byte:02x}");
    }
    out
}
