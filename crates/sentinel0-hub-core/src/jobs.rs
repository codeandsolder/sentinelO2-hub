//! Shared background-job protocol semantics.

use sentinel0_proto::Message;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::BTreeMap;
use thiserror::Error;

pub const JOB_COMPLETED_EVENT: &str = "job_completed";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct JobCompletion {
    pub job_id: String,
    pub tool: String,
    pub host_id: String,
    pub status: String,
    pub data: BTreeMap<String, Value>,
}

impl JobCompletion {
    #[must_use]
    pub fn summary(&self) -> Value {
        let mut summary = Map::new();
        for key in [
            "job_id",
            "tool",
            "host",
            "status",
            "exit_code",
            "duration_s",
            "output_truncated",
            "error",
        ] {
            if let Some(value) = self.data.get(key) {
                summary.insert(key.to_owned(), value.clone());
            }
        }
        Value::Object(summary)
    }
}

#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum JobEventError {
    #[error("job_completed event is missing string field {0:?}")]
    MissingStringField(&'static str),
}

/// Parse a background completion while leaving unrelated events untouched.
///
/// # Errors
///
/// Returns [`JobEventError`] when a `job_completed` event is structurally
/// invalid. Non-job messages and unrelated events return `Ok(None)`.
pub fn parse_job_completion(message: &Message) -> Result<Option<JobCompletion>, JobEventError> {
    let Message::Event { kind, data, .. } = message else {
        return Ok(None);
    };
    if kind != JOB_COMPLETED_EVENT {
        return Ok(None);
    }

    let field = |name: &'static str| {
        data.get(name)
            .and_then(Value::as_str)
            .map(str::to_owned)
            .ok_or(JobEventError::MissingStringField(name))
    };
    Ok(Some(JobCompletion {
        job_id: field("job_id")?,
        tool: field("tool")?,
        host_id: field("host")?,
        status: field("status")?,
        data: data.clone(),
    }))
}
