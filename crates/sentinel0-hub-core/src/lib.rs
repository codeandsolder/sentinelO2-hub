#![forbid(unsafe_code)]

//! Portable Sentinel0² Hub state and compatibility logic.
//!
//! This crate deliberately contains no HTTP, WebSocket, filesystem, SQLite,
//! Cloudflare, or Tokio assumptions. Native and Worker frontends share it.

use sentinel0_proto::{HostInfo, Op};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fmt::Write as _;
use thiserror::Error;

/// One enrolled host as understood by the Hub control plane.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HostRecord {
    pub host_id: String,
    pub hostname: String,
    pub label: Option<String>,
    pub connected: bool,
    pub disabled: bool,
}

impl HostRecord {
    #[must_use]
    pub fn eligible(&self) -> bool {
        self.connected && !self.disabled
    }
}

/// Persistent host metadata plus the tenant's default-host pointer.
#[derive(Debug, Default)]
pub struct HostRegistry {
    hosts: BTreeMap<String, HostRecord>,
    default_host_id: Option<String>,
}

/// Observable host-selection failures.
#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum HostResolutionError {
    #[error("no eligible host matches selector {0:?}")]
    NotFound(String),
    #[error("host selector {0:?} is ambiguous")]
    Ambiguous(String),
    #[error("no eligible hosts are connected")]
    NoEligibleHost,
    #[error("multiple eligible hosts are connected and no default resolves")]
    AmbiguousDefault,
}

impl HostRegistry {
    /// Insert or refresh host metadata from a successful agent hello.
    pub fn register_hello(&mut self, host: &HostInfo) -> &HostRecord {
        let host_id = host.id.clone();
        let record = self
            .hosts
            .entry(host_id.clone())
            .or_insert_with(|| HostRecord {
                host_id: host_id.clone(),
                hostname: host.hostname.clone(),
                label: None,
                connected: true,
                disabled: false,
            });
        record.hostname.clone_from(&host.hostname);
        record.connected = true;
        record
    }

    pub fn disconnect(&mut self, host_id: &str) {
        if let Some(host) = self.hosts.get_mut(host_id) {
            host.connected = false;
        }
    }

    /// Set or clear a user-visible label.
    ///
    /// # Errors
    ///
    /// Returns [`HostResolutionError::NotFound`] when the host ID is unknown.
    pub fn set_label(
        &mut self,
        host_id: &str,
        label: Option<String>,
    ) -> Result<(), HostResolutionError> {
        let host = self
            .hosts
            .get_mut(host_id)
            .ok_or_else(|| HostResolutionError::NotFound(host_id.to_owned()))?;
        host.label = label;
        Ok(())
    }

    /// Set the stable host ID used as tenant default.
    ///
    /// # Errors
    ///
    /// Returns [`HostResolutionError::NotFound`] when the host ID is unknown.
    pub fn set_default(&mut self, host_id: &str) -> Result<(), HostResolutionError> {
        if !self.hosts.contains_key(host_id) {
            return Err(HostResolutionError::NotFound(host_id.to_owned()));
        }
        self.default_host_id = Some(host_id.to_owned());
        Ok(())
    }

    pub fn clear_default(&mut self) {
        self.default_host_id = None;
    }

    pub fn hosts(&self) -> impl Iterator<Item = &HostRecord> {
        self.hosts.values()
    }

    /// Resolve the production selector contract:
    /// host ID → label → hostname; with no selector use an eligible default,
    /// otherwise auto-route only when exactly one host is eligible.
    ///
    /// # Errors
    ///
    /// Returns an explicit not-found or ambiguity error when routing is not
    /// deterministic.
    pub fn resolve(&self, selector: Option<&str>) -> Result<&HostRecord, HostResolutionError> {
        if let Some(selector) = selector {
            if let Some(host) = self.hosts.get(selector)
                && host.eligible()
            {
                return Ok(host);
            }

            let by_label = self
                .hosts
                .values()
                .filter(|host| host.eligible() && host.label.as_deref() == Some(selector))
                .collect::<Vec<_>>();
            match by_label.as_slice() {
                [host] => return Ok(host),
                [] => {}
                _ => return Err(HostResolutionError::Ambiguous(selector.to_owned())),
            }

            let by_hostname = self
                .hosts
                .values()
                .filter(|host| host.eligible() && host.hostname == selector)
                .collect::<Vec<_>>();
            return match by_hostname.as_slice() {
                [host] => Ok(host),
                [] => Err(HostResolutionError::NotFound(selector.to_owned())),
                _ => Err(HostResolutionError::Ambiguous(selector.to_owned())),
            };
        }

        if let Some(default) = self
            .default_host_id
            .as_deref()
            .and_then(|host_id| self.hosts.get(host_id))
            .filter(|host| host.eligible())
        {
            return Ok(default);
        }

        let eligible = self
            .hosts
            .values()
            .filter(|host| host.eligible())
            .collect::<Vec<_>>();
        match eligible.as_slice() {
            [host] => Ok(host),
            [] => Err(HostResolutionError::NoEligibleHost),
            _ => Err(HostResolutionError::AmbiguousDefault),
        }
    }
}

/// Direct model-facing tools that route one-for-one to an agent operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DirectTool {
    pub public_name: &'static str,
    pub op: Op,
}

pub const DIRECT_TOOLS: [DirectTool; 27] = [
    DirectTool {
        public_name: "sentinel_ping",
        op: Op::Ping,
    },
    DirectTool {
        public_name: "sentinel_capabilities",
        op: Op::Capabilities,
    },
    DirectTool {
        public_name: "sentinel_help",
        op: Op::Help,
    },
    DirectTool {
        public_name: "sentinel_state",
        op: Op::State,
    },
    DirectTool {
        public_name: "sentinel_exec",
        op: Op::Exec,
    },
    DirectTool {
        public_name: "sentinel_script_run",
        op: Op::ScriptRun,
    },
    DirectTool {
        public_name: "sentinel_edit",
        op: Op::Edit,
    },
    DirectTool {
        public_name: "sentinel_edit_upload_init",
        op: Op::EditUploadInit,
    },
    DirectTool {
        public_name: "sentinel_edit_upload_file",
        op: Op::EditUploadFile,
    },
    DirectTool {
        public_name: "sentinel_edit_upload_complete",
        op: Op::EditUploadComplete,
    },
    DirectTool {
        public_name: "sentinel_restart",
        op: Op::Restart,
    },
    DirectTool {
        public_name: "sentinel_service",
        op: Op::Service,
    },
    DirectTool {
        public_name: "sentinel_upload_init",
        op: Op::UploadInit,
    },
    DirectTool {
        public_name: "sentinel_upload_chunk",
        op: Op::UploadChunk,
    },
    DirectTool {
        public_name: "sentinel_upload_complete",
        op: Op::UploadComplete,
    },
    DirectTool {
        public_name: "sentinel_upload_file",
        op: Op::UploadFile,
    },
    DirectTool {
        public_name: "sentinel_read",
        op: Op::Read,
    },
    DirectTool {
        public_name: "sentinel_list",
        op: Op::List,
    },
    DirectTool {
        public_name: "sentinel_search",
        op: Op::Search,
    },
    DirectTool {
        public_name: "sentinel_project_snapshot",
        op: Op::ProjectSnapshot,
    },
    DirectTool {
        public_name: "sentinel_move",
        op: Op::Move,
    },
    DirectTool {
        public_name: "sentinel_copy",
        op: Op::Copy,
    },
    DirectTool {
        public_name: "sentinel_delete",
        op: Op::Delete,
    },
    DirectTool {
        public_name: "sentinel_chmod",
        op: Op::Chmod,
    },
    DirectTool {
        public_name: "sentinel_chown",
        op: Op::Chown,
    },
    DirectTool {
        public_name: "sentinel_git",
        op: Op::Git,
    },
    DirectTool {
        public_name: "sentinel_local_api",
        op: Op::LocalApi,
    },
];

#[must_use]
pub fn parse_op(name: &str) -> Option<Op> {
    Op::ALL.into_iter().find(|op| op.as_str() == name)
}

/// Canonical request fingerprint used by REST/MCP idempotency.
#[must_use]
pub fn invocation_fingerprint(op: Op, host_id: &str, payload: &Value) -> String {
    let mut root = Map::new();
    root.insert("host_id".to_owned(), Value::String(host_id.to_owned()));
    root.insert("op".to_owned(), Value::String(op.as_str().to_owned()));
    root.insert("payload".to_owned(), canonical_json(payload));
    let bytes = serde_json::to_vec(&Value::Object(root)).unwrap_or_default();
    let digest = Sha256::digest(bytes);
    let mut out = String::with_capacity(digest.len() * 2);
    for byte in digest {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

fn canonical_json(value: &Value) -> Value {
    match value {
        Value::Array(values) => Value::Array(values.iter().map(canonical_json).collect()),
        Value::Object(values) => {
            let mut keys = values.keys().collect::<Vec<_>>();
            keys.sort_unstable();
            let mut out = Map::with_capacity(values.len());
            for key in keys {
                if let Some(value) = values.get(key) {
                    out.insert(key.clone(), canonical_json(value));
                }
            }
            Value::Object(out)
        }
        other => other.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sentinel0_proto::ConfigSummary;

    fn host(id: &str, hostname: &str) -> HostInfo {
        HostInfo {
            id: id.to_owned(),
            hostname: hostname.to_owned(),
            os: "linux".to_owned(),
            kernel: None,
            arch: None,
            cpu_model: None,
            cpu_cores: None,
            mem_total_bytes: None,
            disk_total_bytes: None,
            machine_type: None,
            distro: None,
            config_summary: Some(ConfigSummary::default()),
        }
    }

    #[test]
    fn host_resolution_matches_contract() -> Result<(), HostResolutionError> {
        let mut registry = HostRegistry::default();
        registry.register_hello(&host("host_a", "alpha"));
        registry.register_hello(&host("host_b", "beta"));
        registry.set_label("host_b", Some("build".to_owned()))?;
        registry.set_default("host_a")?;

        assert_eq!(registry.resolve(Some("host_b"))?.host_id, "host_b");
        assert_eq!(registry.resolve(Some("build"))?.host_id, "host_b");
        assert_eq!(registry.resolve(Some("beta"))?.host_id, "host_b");
        assert_eq!(registry.resolve(None)?.host_id, "host_a");
        Ok(())
    }

    #[test]
    fn canonical_fingerprint_ignores_object_key_order() {
        let left = serde_json::json!({"b": 2, "a": {"y": 1, "x": 0}});
        let right = serde_json::json!({"a": {"x": 0, "y": 1}, "b": 2});
        assert_eq!(
            invocation_fingerprint(Op::Exec, "host_a", &left),
            invocation_fingerprint(Op::Exec, "host_a", &right)
        );
    }

    #[test]
    fn direct_projection_is_exactly_27_unique_ops() {
        let mut names = DIRECT_TOOLS
            .iter()
            .map(|tool| tool.public_name)
            .collect::<Vec<_>>();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), 27);
    }
}
