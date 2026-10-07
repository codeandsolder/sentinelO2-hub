//! Runtime-independent file-transfer protocol validation.

use sentinel0_proto::{Message, ResponseError, TRANSFER_CHUNK_BYTES};
use serde_json::Value;
use std::collections::BTreeMap;
use thiserror::Error;

pub const HUB_TRANSFER_MAX_BYTES: u64 = 512 * 1024 * 1024;
pub const TRANSFER_CHUNK_ACK_EVENT: &str = "transfer_chunk_ack";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExportPlan {
    pub transfer_id: String,
    pub filename: String,
    pub size: u64,
    pub chunk_size: u64,
    pub num_chunks: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExportChunkResult {
    pub bytes: u64,
    pub eof: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransferChunkAck {
    pub transfer_id: String,
    pub chunk_index: u32,
    pub ok: bool,
    pub bytes: Option<u64>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExportDigest {
    pub size: u64,
    pub chunks_read: u64,
    pub num_chunks: u64,
    pub sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UploadResult {
    pub size: u64,
    pub sha256: String,
    pub target_path: String,
}

#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum TransferProtocolError {
    #[error("unexpected agent message; expected response")]
    UnexpectedMessage,
    #[error("agent rejected transfer operation: {0:?}")]
    AgentRejected(ResponseError),
    #[error("successful transfer response is missing result")]
    MissingResult,
    #[error("transfer field {0:?} is missing or has the wrong type")]
    InvalidField(&'static str),
    #[error("transfer field {field:?} mismatch: expected {expected:?}, got {actual:?}")]
    FieldMismatch {
        field: &'static str,
        expected: String,
        actual: String,
    },
    #[error("transfer size {0} exceeds Hub limit")]
    TooLarge(u64),
    #[error("invalid transfer plan: {0}")]
    InvalidPlan(String),
}

fn successful_result(message: &Message) -> Result<&BTreeMap<String, Value>, TransferProtocolError> {
    let Message::Response {
        ok, result, error, ..
    } = message
    else {
        return Err(TransferProtocolError::UnexpectedMessage);
    };
    if !ok {
        return Err(error.clone().map_or_else(
            || {
                TransferProtocolError::InvalidPlan(
                    "agent rejected request without error".to_owned(),
                )
            },
            TransferProtocolError::AgentRejected,
        ));
    }
    result.as_ref().ok_or(TransferProtocolError::MissingResult)
}

fn string_field<'a>(
    result: &'a BTreeMap<String, Value>,
    field: &'static str,
) -> Result<&'a str, TransferProtocolError> {
    result
        .get(field)
        .and_then(Value::as_str)
        .ok_or(TransferProtocolError::InvalidField(field))
}

fn u64_field(
    result: &BTreeMap<String, Value>,
    field: &'static str,
) -> Result<u64, TransferProtocolError> {
    result
        .get(field)
        .and_then(Value::as_u64)
        .ok_or(TransferProtocolError::InvalidField(field))
}

fn bool_field(
    result: &BTreeMap<String, Value>,
    field: &'static str,
) -> Result<bool, TransferProtocolError> {
    result
        .get(field)
        .and_then(Value::as_bool)
        .ok_or(TransferProtocolError::InvalidField(field))
}

fn require_string_match(
    result: &BTreeMap<String, Value>,
    field: &'static str,
    expected: &str,
) -> Result<(), TransferProtocolError> {
    let actual = string_field(result, field)?;
    if actual == expected {
        return Ok(());
    }
    Err(TransferProtocolError::FieldMismatch {
        field,
        expected: expected.to_owned(),
        actual: actual.to_owned(),
    })
}

/// Validate the source agent's `file_export_init` response.
///
/// # Errors
/// Returns an error for rejected, malformed, inconsistent, or oversized exports.
pub fn parse_export_plan(
    message: &Message,
    expected_transfer_id: &str,
) -> Result<ExportPlan, TransferProtocolError> {
    let result = successful_result(message)?;
    require_string_match(result, "transfer_id", expected_transfer_id)?;
    let filename = string_field(result, "filename")?.to_owned();
    let size = u64_field(result, "size")?;
    if size > HUB_TRANSFER_MAX_BYTES {
        return Err(TransferProtocolError::TooLarge(size));
    }
    let chunk_size = u64_field(result, "chunk_size")?;
    let num_chunks = u64_field(result, "num_chunks")?;
    let max_chunk = u64::try_from(TRANSFER_CHUNK_BYTES).unwrap_or(u64::MAX);
    if chunk_size == 0 || chunk_size > max_chunk || num_chunks == 0 {
        return Err(TransferProtocolError::InvalidPlan(format!(
            "chunk_size={chunk_size}, num_chunks={num_chunks}"
        )));
    }
    let expected_chunks = size.div_ceil(chunk_size).max(1);
    if num_chunks != expected_chunks {
        return Err(TransferProtocolError::InvalidPlan(format!(
            "num_chunks={num_chunks}, expected={expected_chunks}"
        )));
    }
    Ok(ExportPlan {
        transfer_id: expected_transfer_id.to_owned(),
        filename,
        size,
        chunk_size,
        num_chunks,
    })
}

/// Validate the JSON response paired with one source binary chunk.
///
/// # Errors
/// Returns an error for rejected, malformed, or mismatched responses.
pub fn parse_export_chunk_result(
    message: &Message,
    expected_transfer_id: &str,
    expected_index: u32,
) -> Result<ExportChunkResult, TransferProtocolError> {
    let result = successful_result(message)?;
    require_string_match(result, "transfer_id", expected_transfer_id)?;
    let index = u64_field(result, "chunk_index")?;
    if index != u64::from(expected_index) {
        return Err(TransferProtocolError::FieldMismatch {
            field: "chunk_index",
            expected: expected_index.to_string(),
            actual: index.to_string(),
        });
    }
    Ok(ExportChunkResult {
        bytes: u64_field(result, "bytes")?,
        eof: bool_field(result, "eof")?,
    })
}

/// Parse a destination agent's asynchronous binary-ingest acknowledgement.
/// Unrelated messages return `Ok(None)`.
///
/// # Errors
/// Returns an error when a transfer acknowledgement is structurally invalid.
pub fn parse_transfer_chunk_ack(
    message: &Message,
) -> Result<Option<TransferChunkAck>, TransferProtocolError> {
    let Message::Event { kind, data, .. } = message else {
        return Ok(None);
    };
    if kind != TRANSFER_CHUNK_ACK_EVENT {
        return Ok(None);
    }
    let transfer_id = string_field(data, "transfer_id")?.to_owned();
    if transfer_id.len() != 32 || !transfer_id.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(TransferProtocolError::InvalidField("transfer_id"));
    }
    let index = u64_field(data, "chunk_index")?;
    let chunk_index =
        u32::try_from(index).map_err(|_| TransferProtocolError::InvalidField("chunk_index"))?;
    let ok = bool_field(data, "ok")?;
    let bytes = data.get("bytes").and_then(Value::as_u64);
    let error = data.get("error").and_then(Value::as_str).map(str::to_owned);
    if ok && bytes.is_none() {
        return Err(TransferProtocolError::InvalidField("bytes"));
    }
    if !ok && error.is_none() {
        return Err(TransferProtocolError::InvalidField("error"));
    }
    Ok(Some(TransferChunkAck {
        transfer_id: transfer_id.to_ascii_lowercase(),
        chunk_index,
        ok,
        bytes,
        error,
    }))
}

/// Validate the source agent's final digest.
///
/// # Errors
/// Returns an error when the source did not hash every chunk or returned inconsistent metadata.
pub fn parse_export_digest(
    message: &Message,
    plan: &ExportPlan,
) -> Result<ExportDigest, TransferProtocolError> {
    let result = successful_result(message)?;
    require_string_match(result, "transfer_id", &plan.transfer_id)?;
    let size = u64_field(result, "size")?;
    let chunks_read = u64_field(result, "chunks_read")?;
    let num_chunks = u64_field(result, "num_chunks")?;
    if size != plan.size || chunks_read != plan.num_chunks || num_chunks != plan.num_chunks {
        return Err(TransferProtocolError::InvalidPlan(format!(
            "source completion size={size}, chunks_read={chunks_read}, num_chunks={num_chunks}"
        )));
    }
    if !bool_field(result, "sha256_complete")? {
        return Err(TransferProtocolError::InvalidPlan(
            "source SHA-256 is incomplete".to_owned(),
        ));
    }
    let sha256 = string_field(result, "sha256")?.to_ascii_lowercase();
    if sha256.len() != 64 || !sha256.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(TransferProtocolError::InvalidField("sha256"));
    }
    Ok(ExportDigest {
        size,
        chunks_read,
        num_chunks,
        sha256,
    })
}

/// Validate destination upload initialization.
///
/// # Errors
/// Returns an error for a rejected or mismatched upload session.
pub fn validate_upload_init(
    message: &Message,
    expected_transfer_id: &str,
) -> Result<(), TransferProtocolError> {
    let result = successful_result(message)?;
    require_string_match(result, "upload_id", expected_transfer_id)
}

/// Validate destination upload completion and checksum.
///
/// # Errors
/// Returns an error for rejected, malformed, or checksum-mismatched completion.
pub fn parse_upload_result(
    message: &Message,
    expected_transfer_id: &str,
    expected_size: u64,
    expected_sha256: &str,
) -> Result<UploadResult, TransferProtocolError> {
    let result = successful_result(message)?;
    require_string_match(result, "upload_id", expected_transfer_id)?;
    let size = u64_field(result, "size")?;
    if size != expected_size {
        return Err(TransferProtocolError::FieldMismatch {
            field: "size",
            expected: expected_size.to_string(),
            actual: size.to_string(),
        });
    }
    let sha256 = string_field(result, "sha256")?.to_ascii_lowercase();
    if sha256 != expected_sha256 {
        return Err(TransferProtocolError::FieldMismatch {
            field: "sha256",
            expected: expected_sha256.to_owned(),
            actual: sha256,
        });
    }
    Ok(UploadResult {
        size,
        sha256: expected_sha256.to_owned(),
        target_path: string_field(result, "target_path")?.to_owned(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use sentinel0_proto::Message;

    fn response(result: BTreeMap<String, Value>) -> Message {
        Message::Response {
            id: "r".to_owned(),
            ok: true,
            result: Some(result),
            error: None,
        }
    }

    #[test]
    fn export_plan_rejects_inconsistent_chunks() {
        let transfer_id = "00112233445566778899aabbccddeeff";
        let message = response(BTreeMap::from([
            ("transfer_id".into(), Value::String(transfer_id.into())),
            ("filename".into(), Value::String("x".into())),
            ("size".into(), Value::from(10_u64)),
            ("chunk_size".into(), Value::from(4_u64)),
            ("num_chunks".into(), Value::from(2_u64)),
        ]));
        assert!(matches!(
            parse_export_plan(&message, transfer_id),
            Err(TransferProtocolError::InvalidPlan(_))
        ));
    }

    #[test]
    fn transfer_ack_parses_success() -> Result<(), TransferProtocolError> {
        let message = serde_json::from_value::<Message>(serde_json::json!({
            "type": "event",
            "kind": TRANSFER_CHUNK_ACK_EVENT,
            "data": {
                "transfer_id": "00112233445566778899aabbccddeeff",
                "chunk_index": 7,
                "ok": true,
                "bytes": 123
            },
            "timestamp": "2026-10-07T00:00:00Z"
        }))
        .map_err(|error| TransferProtocolError::InvalidPlan(error.to_string()))?;
        let ack = parse_transfer_chunk_ack(&message)?.ok_or(TransferProtocolError::InvalidPlan(
            "missing parsed ack".to_owned(),
        ))?;
        assert_eq!(ack.chunk_index, 7);
        assert_eq!(ack.bytes, Some(123));
        Ok(())
    }
}
