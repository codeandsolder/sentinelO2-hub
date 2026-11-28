//! Real WebSocket agents exercising the native Hub transfer endpoint.

use super::*;
use axum::body::Body;
use sentinel0_proto::{TRANSFER_CHUNK_BYTES, decode_binary_frame, encode_binary_frame};
use sha2::{Digest as _, Sha256};
use std::{collections::BTreeMap, convert::Infallible, fmt::Write as _};
use tokio::task::JoinHandle;
use tower::ServiceExt as _;

const SOURCE_PATH: &str = "/virtual/source.bin";
const DESTINATION_PATH: &str = "/virtual/destination.bin";

struct TransferHarness {
    state: AppState,
    source: AgentSocket,
    destination: AgentSocket,
    request: JoinHandle<Result<Response, Infallible>>,
    server: JoinHandle<()>,
}

impl Drop for TransferHarness {
    fn drop(&mut self) {
        self.request.abort();
        self.server.abort();
    }
}

async fn start_transfer() -> TestResult<TransferHarness> {
    let state = state()?;
    let (address, server) = server(state.clone()).await?;
    let (source, source_session) = connect_agent(address, "source").await?;
    let (destination, destination_session) = connect_agent(address, "destination").await?;
    wait_for_session(&state, "source", &source_session).await?;
    wait_for_session(&state, "destination", &destination_session).await?;

    let api = Router::new()
        .route("/v1/transfer-file", post(crate::transfer::v1_transfer_file))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            require_api_auth,
        ))
        .with_state(state.clone());
    let request = Request::builder()
        .method("POST")
        .uri("/v1/transfer-file")
        .header("authorization", "Bearer api-test")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&json!({
            "source_host_id": "source",
            "source_path": SOURCE_PATH,
            "destination_host_id": "destination",
            "destination_path": DESTINATION_PATH,
            "overwrite": true,
            "land_in_place": false,
        }))?))?;
    let request = tokio::spawn(async move { api.oneshot(request).await });
    Ok(TransferHarness {
        state,
        source,
        destination,
        request,
        server,
    })
}

async fn expect_op(
    socket: &mut AgentSocket,
    expected: Op,
) -> TestResult<(String, BTreeMap<String, Value>)> {
    let Message::Request {
        id, op, payload, ..
    } = read_agent(socket).await?
    else {
        return Err("expected agent request".into());
    };
    if op != expected {
        return Err(format!("expected {expected}, got {op}").into());
    }
    Ok((id, payload))
}

async fn reply(socket: &mut AgentSocket, request_id: String, result: Value) -> TestResult {
    let result = serde_json::from_value::<BTreeMap<String, Value>>(result)?;
    send_agent(
        socket,
        &Message::Response {
            id: request_id,
            ok: true,
            result: Some(result),
            error: None,
        },
    )
    .await
}

async fn acknowledge(
    socket: &mut AgentSocket,
    transfer_id: &str,
    index: u32,
    acknowledged: Result<u64, &str>,
) -> TestResult {
    let data = match acknowledged {
        Ok(bytes) => json!({
            "transfer_id": transfer_id,
            "chunk_index": index,
            "ok": true,
            "bytes": bytes,
        }),
        Err(error) => json!({
            "transfer_id": transfer_id,
            "chunk_index": index,
            "ok": false,
            "error": error,
        }),
    };
    send_agent(
        socket,
        &Message::Event {
            kind: "transfer_chunk_ack".to_owned(),
            data: serde_json::from_value(data)?,
            timestamp: UtcDateTime::now(),
        },
    )
    .await
}

async fn read_binary(socket: &mut AgentSocket) -> TestResult<Vec<u8>> {
    let frame = tokio::time::timeout(Duration::from_secs(5), socket.next())
        .await?
        .ok_or("destination WebSocket closed")??;
    let ClientMessage::Binary(data) = frame else {
        return Err(format!("expected binary frame, got {frame:?}").into());
    };
    Ok(data.to_vec())
}

async fn export_init(
    harness: &mut TransferHarness,
    size: usize,
    chunk_size: usize,
) -> TestResult<String> {
    let (request_id, request) = expect_op(&mut harness.source, Op::FileExportInit).await?;
    assert_eq!(request["source_path"], SOURCE_PATH);
    assert_eq!(request["chunk_size"], u64::try_from(TRANSFER_CHUNK_BYTES)?);
    let transfer_id = request["transfer_id"]
        .as_str()
        .ok_or("missing transfer_id")?
        .to_owned();
    assert_eq!(transfer_id.len(), 32);
    reply(
        &mut harness.source,
        request_id,
        json!({
            "transfer_id": transfer_id,
            "filename": "source.bin",
            "size": size,
            "chunk_size": chunk_size,
            "num_chunks": size.div_ceil(chunk_size).max(1),
        }),
    )
    .await?;
    Ok(transfer_id)
}

async fn upload_init(harness: &mut TransferHarness, transfer_id: &str, size: usize) -> TestResult {
    let (request_id, request) = expect_op(&mut harness.destination, Op::UploadInit).await?;
    assert_eq!(request["upload_id"], transfer_id);
    assert_eq!(request["target_path"], DESTINATION_PATH);
    assert_eq!(request["total_size"], size);
    assert_eq!(request["overwrite"], true);
    reply(
        &mut harness.destination,
        request_id,
        json!({"upload_id": transfer_id}),
    )
    .await
}

async fn source_chunk(
    harness: &mut TransferHarness,
    transfer_id: &str,
    index: u32,
    payload: &[u8],
    binary_first: bool,
) -> TestResult {
    let (request_id, request) = expect_op(&mut harness.source, Op::FileExportChunk).await?;
    assert_eq!(request["transfer_id"], transfer_id);
    assert_eq!(request["chunk_index"], index);
    let binary = encode_binary_frame(*Uuid::parse_str(transfer_id)?.as_bytes(), index, payload);
    let result = json!({
        "transfer_id": transfer_id,
        "chunk_index": index,
        "bytes": payload.len(),
        "eof": index == 2,
    });
    if binary_first {
        harness
            .source
            .send(ClientMessage::Binary(binary.into()))
            .await?;
        reply(&mut harness.source, request_id, result).await?;
    } else {
        reply(&mut harness.source, request_id, result).await?;
        harness
            .source
            .send(ClientMessage::Binary(binary.into()))
            .await?;
    }
    Ok(())
}

/// Model the first source frame independently from the metadata's byte
/// count, including intentionally inconsistent replies in negative cases.
async fn first_source_chunk(
    harness: &mut TransferHarness,
    transfer_id: &str,
    payload: &[u8],
    reported_bytes: u64,
) -> TestResult<Vec<u8>> {
    let (request_id, request) = expect_op(&mut harness.source, Op::FileExportChunk).await?;
    assert_eq!(request["transfer_id"], transfer_id);
    assert_eq!(request["chunk_index"], 0);
    let binary = encode_binary_frame(*Uuid::parse_str(transfer_id)?.as_bytes(), 0, payload);
    harness
        .source
        .send(ClientMessage::Binary(binary.clone().into()))
        .await?;
    reply(
        &mut harness.source,
        request_id,
        json!({
            "transfer_id": transfer_id,
            "chunk_index": 0,
            "bytes": reported_bytes,
            "eof": true,
        }),
    )
    .await?;
    Ok(binary)
}

async fn assert_first_chunk_forwarded(
    harness: &mut TransferHarness,
    transfer_id: &str,
    payload: &[u8],
    reported_bytes: u64,
) -> TestResult {
    let expected = first_source_chunk(harness, transfer_id, payload, reported_bytes).await?;
    assert_eq!(read_binary(&mut harness.destination).await?, expected);
    Ok(())
}

/// An aborted transfer still has to close its source export session.
async fn expect_export_cleanup(harness: &mut TransferHarness, transfer_id: &str) -> TestResult {
    let (request_id, request) = expect_op(&mut harness.source, Op::FileExportComplete).await?;
    assert_eq!(request["transfer_id"], transfer_id);
    reply(
        &mut harness.source,
        request_id,
        json!({"transfer_id":transfer_id}),
    )
    .await
}

/// A ping/pong proves no binary frame or upload-completion request was sent
/// on the destination socket before the failure was reported.
async fn assert_destination_responsive(harness: &mut TransferHarness) -> TestResult {
    let timestamp = UtcDateTime::now();
    send_agent(&mut harness.destination, &Message::Ping { timestamp }).await?;
    assert!(matches!(
        read_agent(&mut harness.destination).await?,
        Message::Pong { .. }
    ));
    Ok(())
}

async fn finish_source(
    harness: &mut TransferHarness,
    transfer_id: &str,
    size: usize,
    chunks: usize,
    sha256: &str,
) -> TestResult {
    let (request_id, request) = expect_op(&mut harness.source, Op::FileExportComplete).await?;
    assert_eq!(request["transfer_id"], transfer_id);
    reply(
        &mut harness.source,
        request_id,
        json!({
            "transfer_id": transfer_id,
            "size": size,
            "chunks_read": chunks,
            "num_chunks": chunks,
            "sha256_complete": true,
            "sha256": sha256,
        }),
    )
    .await
}

async fn finish_http(
    harness: &mut TransferHarness,
    expected_status: StatusCode,
) -> TestResult<Value> {
    let response = tokio::time::timeout(Duration::from_secs(5), &mut harness.request).await???;
    assert_eq!(response.status(), expected_status);
    let bytes = axum::body::to_bytes(response.into_body(), 64 * 1024).await?;
    let result = serde_json::from_slice::<Value>(&bytes)?;
    assert!(harness.state.hub.pending.lock().await.is_empty());
    assert!(
        harness
            .state
            .hub
            .transfer_binary_waiters
            .lock()
            .await
            .is_empty()
    );
    assert!(
        harness
            .state
            .hub
            .transfer_ack_waiters
            .lock()
            .await
            .is_empty()
    );
    Ok(result)
}

#[tokio::test]
async fn multi_chunk_transfer_forwards_exact_binary_and_digest() -> TestResult {
    let bytes = b"abcdefghi";
    let mut sha256 = String::with_capacity(64);
    for byte in Sha256::digest(bytes) {
        write!(&mut sha256, "{byte:02x}")?;
    }
    let mut harness = start_transfer().await?;
    let transfer_id = export_init(&mut harness, bytes.len(), 4).await?;
    upload_init(&mut harness, &transfer_id, bytes.len()).await?;

    let mut received = Vec::new();
    for (index, chunk) in bytes.chunks(4).enumerate() {
        let index = u32::try_from(index)?;
        source_chunk(&mut harness, &transfer_id, index, chunk, index % 2 == 0).await?;
        let binary = read_binary(&mut harness.destination).await?;
        let frame = decode_binary_frame(&binary)?;
        assert_eq!(
            frame.transfer_id,
            *Uuid::parse_str(&transfer_id)?.as_bytes()
        );
        assert_eq!(frame.chunk_index, index);
        assert_eq!(frame.payload, chunk);
        received.extend_from_slice(frame.payload);

        // An event from the source must not fulfill the destination's waiter.
        if index == 1 {
            acknowledge(
                &mut harness.source,
                &transfer_id,
                index,
                Ok(chunk.len() as u64),
            )
            .await?;
            let timestamp = UtcDateTime::now();
            send_agent(&mut harness.source, &Message::Ping { timestamp }).await?;
            assert!(matches!(
                read_agent(&mut harness.source).await?,
                Message::Pong { .. }
            ));
            assert!(
                harness
                    .state
                    .hub
                    .transfer_ack_waiters
                    .lock()
                    .await
                    .contains_key(&("destination".to_owned(), transfer_id.clone(), index,))
            );
        }
        acknowledge(
            &mut harness.destination,
            &transfer_id,
            index,
            Ok(u64::try_from(chunk.len())?),
        )
        .await?;
    }
    assert_eq!(received, bytes);
    finish_source(&mut harness, &transfer_id, bytes.len(), 3, &sha256).await?;

    let (request_id, request) = expect_op(&mut harness.destination, Op::UploadComplete).await?;
    assert_eq!(request["upload_id"], transfer_id);
    assert_eq!(request["sha256"], sha256);
    reply(
        &mut harness.destination,
        request_id,
        json!({
            "upload_id": transfer_id,
            "target_path": DESTINATION_PATH,
            "size": bytes.len(),
            "sha256": sha256,
        }),
    )
    .await?;

    let result = finish_http(&mut harness, StatusCode::OK).await?;
    assert_eq!(result["ok"], true);
    assert_eq!(result["transfer_id"], transfer_id);
    assert_eq!(result["source_host_id"], "source");
    assert_eq!(result["destination_host_id"], "destination");
    assert_eq!(result["size"], bytes.len());
    assert_eq!(result["chunks"], 3);
    assert_eq!(result["sha256"], sha256);
    Ok(())
}

/// Invalid source metadata must abort the transfer, release the source
/// session, and leave the destination WebSocket free of transfer data.
async fn assert_invalid_first_chunk_rejected(reported_bytes: u64) -> TestResult {
    let mut harness = start_transfer().await?;
    let transfer_id = export_init(&mut harness, 4, 4).await?;
    upload_init(&mut harness, &transfer_id, 4).await?;
    first_source_chunk(&mut harness, &transfer_id, b"bad", reported_bytes).await?;
    expect_export_cleanup(&mut harness, &transfer_id).await?;

    let result = finish_http(&mut harness, StatusCode::BAD_GATEWAY).await?;
    assert_eq!(result["error"], "transfer_protocol_error");
    assert_destination_responsive(&mut harness).await
}

#[tokio::test]
async fn destination_rejection_triggers_source_cleanup() -> TestResult {
    let mut harness = start_transfer().await?;
    let transfer_id = export_init(&mut harness, 4, 4).await?;
    upload_init(&mut harness, &transfer_id, 4).await?;

    assert_first_chunk_forwarded(&mut harness, &transfer_id, b"data", 4).await?;
    acknowledge(&mut harness.destination, &transfer_id, 0, Err("disk_full")).await?;

    // Failed destination ingest must not cause source finalization or more chunks.
    expect_export_cleanup(&mut harness, &transfer_id).await?;
    let result = finish_http(&mut harness, StatusCode::BAD_GATEWAY).await?;
    assert_eq!(result["error"], "destination_chunk_rejected");
    assert_eq!(result["message"], "disk_full");
    Ok(())
}

#[tokio::test]
async fn malformed_source_binary_is_not_forwarded_and_export_is_closed() -> TestResult {
    // The frame contains three bytes while its response declares four.
    assert_invalid_first_chunk_rejected(4).await
}

#[tokio::test]
async fn oversized_export_plan_is_rejected_with_source_cleanup() -> TestResult {
    let mut harness = start_transfer().await?;
    let (request_id, request) = expect_op(&mut harness.source, Op::FileExportInit).await?;
    let transfer_id = request["transfer_id"]
        .as_str()
        .ok_or("missing transfer_id")?
        .to_owned();
    let size = sentinel0_hub_core::HUB_TRANSFER_MAX_BYTES + 1;
    reply(
        &mut harness.source,
        request_id,
        json!({
            "transfer_id": transfer_id,
            "filename": "huge.bin",
            "size": size,
            "chunk_size": TRANSFER_CHUNK_BYTES,
            "num_chunks": size.div_ceil(TRANSFER_CHUNK_BYTES as u64),
        }),
    )
    .await?;
    expect_export_cleanup(&mut harness, &transfer_id).await?;
    let result = finish_http(&mut harness, StatusCode::PAYLOAD_TOO_LARGE).await?;
    assert_eq!(result["error"], "file_too_large");
    Ok(())
}

#[tokio::test]
async fn self_consistent_but_wrong_sized_chunk_is_rejected() -> TestResult {
    // The frame and response agree on three bytes, contradicting the plan.
    assert_invalid_first_chunk_rejected(3).await
}

#[tokio::test]
async fn empty_file_uses_one_zero_byte_chunk() -> TestResult {
    let mut harness = start_transfer().await?;
    let transfer_id = export_init(&mut harness, 0, 4).await?;
    upload_init(&mut harness, &transfer_id, 0).await?;
    assert_first_chunk_forwarded(&mut harness, &transfer_id, b"", 0).await?;
    acknowledge(&mut harness.destination, &transfer_id, 0, Ok(0)).await?;

    let sha256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    finish_source(&mut harness, &transfer_id, 0, 1, sha256).await?;
    let (request_id, request) = expect_op(&mut harness.destination, Op::UploadComplete).await?;
    assert_eq!(request["sha256"], sha256);
    reply(
        &mut harness.destination,
        request_id,
        json!({
            "upload_id": transfer_id,
            "target_path": DESTINATION_PATH,
            "size": 0,
            "sha256": sha256,
        }),
    )
    .await?;
    let result = finish_http(&mut harness, StatusCode::OK).await?;
    assert_eq!(result["size"], 0);
    assert_eq!(result["chunks"], 1);
    assert_eq!(result["sha256"], sha256);
    Ok(())
}
