use crate::{DIRECT_TOOLS, direct_tool_catalog};
use sentinel0_proto::Op;
use serde_json::{Map, Value, json};
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HubToolKind {
    ListHosts,
    SetHostLabel,
    RemoveHostLabel,
    SetDefaultHost,
    GetDefaultHost,
    ClearDefaultHost,
    TransferFile,
    GitDiff,
    GitApplyPatch,
    GitLsRemote,
    GitFetch,
    GitClone,
    GitPush,
    ServiceStatus,
    ServiceStart,
    ServiceStop,
    ServiceReload,
    NotificationsCheck,
    NotificationsGet,
    NotificationsAck,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HubTool {
    pub public_name: &'static str,
    pub kind: HubToolKind,
}

pub const HUB_TOOLS: [HubTool; 20] = [
    HubTool {
        public_name: "sentinel_list_hosts",
        kind: HubToolKind::ListHosts,
    },
    HubTool {
        public_name: "sentinel_set_host_label",
        kind: HubToolKind::SetHostLabel,
    },
    HubTool {
        public_name: "sentinel_remove_host_label",
        kind: HubToolKind::RemoveHostLabel,
    },
    HubTool {
        public_name: "sentinel_set_default_host",
        kind: HubToolKind::SetDefaultHost,
    },
    HubTool {
        public_name: "sentinel_get_default_host",
        kind: HubToolKind::GetDefaultHost,
    },
    HubTool {
        public_name: "sentinel_clear_default_host",
        kind: HubToolKind::ClearDefaultHost,
    },
    HubTool {
        public_name: "sentinel_transfer_file",
        kind: HubToolKind::TransferFile,
    },
    HubTool {
        public_name: "sentinel_git_diff",
        kind: HubToolKind::GitDiff,
    },
    HubTool {
        public_name: "sentinel_git_apply_patch",
        kind: HubToolKind::GitApplyPatch,
    },
    HubTool {
        public_name: "sentinel_git_ls_remote",
        kind: HubToolKind::GitLsRemote,
    },
    HubTool {
        public_name: "sentinel_git_fetch",
        kind: HubToolKind::GitFetch,
    },
    HubTool {
        public_name: "sentinel_git_clone",
        kind: HubToolKind::GitClone,
    },
    HubTool {
        public_name: "sentinel_git_push",
        kind: HubToolKind::GitPush,
    },
    HubTool {
        public_name: "sentinel_service_status",
        kind: HubToolKind::ServiceStatus,
    },
    HubTool {
        public_name: "sentinel_service_start",
        kind: HubToolKind::ServiceStart,
    },
    HubTool {
        public_name: "sentinel_service_stop",
        kind: HubToolKind::ServiceStop,
    },
    HubTool {
        public_name: "sentinel_service_reload",
        kind: HubToolKind::ServiceReload,
    },
    HubTool {
        public_name: "notifications_check",
        kind: HubToolKind::NotificationsCheck,
    },
    HubTool {
        public_name: "notifications_get",
        kind: HubToolKind::NotificationsGet,
    },
    HubTool {
        public_name: "notifications_ack",
        kind: HubToolKind::NotificationsAck,
    },
];

#[must_use]
pub fn hub_tool_by_public_name(name: &str) -> Option<&'static HubTool> {
    HUB_TOOLS.iter().find(|tool| tool.public_name == name)
}

#[must_use]
pub const fn hub_tool_description(kind: HubToolKind) -> &'static str {
    match kind {
        HubToolKind::ListHosts => {
            "List the authenticated user's connected, offline and disabled hosts."
        }
        HubToolKind::SetHostLabel => "Attach a human-friendly label to one enrolled host.",
        HubToolKind::RemoveHostLabel => "Remove the label attached to one enrolled host.",
        HubToolKind::SetDefaultHost => "Designate an exact stable host ID as the default target.",
        HubToolKind::GetDefaultHost => {
            "Return the currently configured default host and whether it is connected."
        }
        HubToolKind::ClearDefaultHost => "Remove the user's default-host preference.",
        HubToolKind::TransferFile => {
            "Move a file directly between two enrolled hosts through the Hub, verified by SHA-256."
        }
        HubToolKind::GitDiff => {
            "Show a repository's current changes in one bounded, structured view."
        }
        HubToolKind::GitApplyPatch => "Apply one unified diff to a repository, all or nothing.",
        HubToolKind::GitLsRemote => {
            "Read the refs a Git remote has right now, without a working tree."
        }
        HubToolKind::GitFetch => {
            "Fetch from a Git remote: updates remote-tracking refs, never the working tree."
        }
        HubToolKind::GitClone => "Clone a Git repository into a new directory on the host.",
        HubToolKind::GitPush => {
            "Push a branch to a Git remote, with force-with-lease protection for forced pushes."
        }
        HubToolKind::ServiceStatus => {
            "Read a service's status: its full status, whether it is running, and whether it starts at boot."
        }
        HubToolKind::ServiceStart => "Start a service on a host.",
        HubToolKind::ServiceStop => "Stop a service on a host.",
        HubToolKind::ServiceReload => {
            "Reload a service's configuration without a full restart where supported."
        }
        HubToolKind::NotificationsCheck => {
            "List unread completed background jobs and announcements plus currently running jobs."
        }
        HubToolKind::NotificationsGet => "Read one background-job notification in full.",
        HubToolKind::NotificationsAck => {
            "Clear one already-read notification, or all with job_id=\"all\"."
        }
    }
}

fn object_schema(
    required: &[&str],
    properties: impl IntoIterator<Item = (&'static str, Value)>,
) -> Value {
    let mut props = Map::new();
    for (name, schema) in properties {
        props.insert(name.to_owned(), schema);
    }
    json!({
        "type": "object",
        "properties": props,
        "required": required,
        "additionalProperties": false
    })
}

fn nullable_string() -> Value {
    json!({"type": ["string", "null"]})
}

fn with_host_and_ref(mut fields: Vec<(&'static str, Value)>) -> Vec<(&'static str, Value)> {
    fields.push(("host_id", nullable_string()));
    fields.push((
        "opaque_ref",
        json!({"type": ["string", "null"], "maxLength": 256}),
    ));
    fields
}

fn git_schema(kind: HubToolKind) -> Value {
    match kind {
        HubToolKind::GitDiff => object_schema(
            &["path"],
            with_host_and_ref(vec![
                ("path", json!({"type": "string"})),
                ("base_ref", json!({"type": "string", "default": "HEAD"})),
                ("staged", json!({"type": "boolean", "default": true})),
                ("unstaged", json!({"type": "boolean", "default": true})),
                (
                    "include_untracked",
                    json!({"type": "boolean", "default": true}),
                ),
                (
                    "max_files",
                    json!({"type": "integer", "minimum": 1, "default": 50}),
                ),
                (
                    "max_patch_bytes",
                    json!({"type": "integer", "minimum": 1, "default": 131_072}),
                ),
                (
                    "context_lines",
                    json!({"type": "integer", "minimum": 0, "default": 3}),
                ),
            ]),
        ),
        HubToolKind::GitApplyPatch => object_schema(
            &["path", "patch"],
            with_host_and_ref(vec![
                ("path", json!({"type": "string"})),
                ("patch", json!({"type": "string"})),
                ("dry_run", json!({"type": "boolean", "default": false})),
            ]),
        ),
        HubToolKind::GitLsRemote => object_schema(
            &[],
            with_host_and_ref(vec![
                ("remote", json!({"type": "string", "default": "origin"})),
                ("ref_pattern", nullable_string()),
                ("path", nullable_string()),
            ]),
        ),
        HubToolKind::GitFetch => object_schema(
            &["path"],
            with_host_and_ref(vec![
                ("path", json!({"type": "string"})),
                ("remote", json!({"type": "string", "default": "origin"})),
                ("ref", nullable_string()),
            ]),
        ),
        HubToolKind::GitClone => object_schema(
            &["url", "dest"],
            with_host_and_ref(vec![
                ("url", json!({"type": "string"})),
                ("dest", json!({"type": "string"})),
                ("branch", nullable_string()),
                ("depth", json!({"type": ["integer", "null"]})),
            ]),
        ),
        HubToolKind::GitPush => object_schema(
            &["path", "branch"],
            with_host_and_ref(vec![
                ("path", json!({"type": "string"})),
                ("branch", json!({"type": "string"})),
                ("remote", json!({"type": "string", "default": "origin"})),
                ("force", json!({"type": "boolean", "default": false})),
                ("expected_remote_sha", nullable_string()),
            ]),
        ),
        _ => object_schema(&[], with_host_and_ref(Vec::new())),
    }
}

fn service_schema() -> Value {
    object_schema(
        &["service"],
        with_host_and_ref(vec![("service", json!({"type": "string"}))]),
    )
}

#[must_use]
pub fn hub_tool_input_schema(kind: HubToolKind) -> Value {
    match kind {
        HubToolKind::ListHosts
        | HubToolKind::GetDefaultHost
        | HubToolKind::ClearDefaultHost
        | HubToolKind::NotificationsCheck => object_schema(&[], []),
        HubToolKind::SetHostLabel => object_schema(
            &["host_id", "label"],
            [
                ("host_id", json!({"type": "string"})),
                ("label", json!({"type": "string"})),
            ],
        ),
        HubToolKind::RemoveHostLabel | HubToolKind::SetDefaultHost => {
            object_schema(&["host_id"], [("host_id", json!({"type": "string"}))])
        }
        HubToolKind::NotificationsGet | HubToolKind::NotificationsAck => {
            object_schema(&["job_id"], [("job_id", json!({"type": "string"}))])
        }
        HubToolKind::GitDiff
        | HubToolKind::GitApplyPatch
        | HubToolKind::GitLsRemote
        | HubToolKind::GitFetch
        | HubToolKind::GitClone
        | HubToolKind::GitPush => git_schema(kind),
        HubToolKind::ServiceStatus
        | HubToolKind::ServiceStart
        | HubToolKind::ServiceStop
        | HubToolKind::ServiceReload => service_schema(),
        HubToolKind::TransferFile => object_schema(
            &[
                "source_host_id",
                "source_path",
                "destination_host_id",
                "destination_path",
            ],
            [
                ("source_host_id", json!({"type": "string"})),
                ("source_path", json!({"type": "string"})),
                ("destination_host_id", json!({"type": "string"})),
                ("destination_path", json!({"type": "string"})),
                ("overwrite", json!({"type": "boolean", "default": false})),
                (
                    "land_in_place",
                    json!({"type": "boolean", "default": false}),
                ),
                (
                    "opaque_ref",
                    json!({"type": ["string", "null"], "maxLength": 256}),
                ),
            ],
        ),
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct HubProtocolCall {
    pub op: Op,
    pub host_selector: Option<String>,
    pub payload: Value,
}

#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum HubToolArgumentError {
    #[error("Hub tool arguments must be an object")]
    NotObject,
    #[error("host_id must be a string or null")]
    InvalidHostSelector,
}

fn protocol_operation(kind: HubToolKind) -> Option<(Op, &'static str, &'static str)> {
    match kind {
        HubToolKind::GitDiff => Some((Op::Git, "operation", "diff")),
        HubToolKind::GitApplyPatch => Some((Op::Git, "operation", "apply_patch")),
        HubToolKind::GitLsRemote => Some((Op::Git, "operation", "ls_remote")),
        HubToolKind::GitFetch => Some((Op::Git, "operation", "fetch")),
        HubToolKind::GitClone => Some((Op::Git, "operation", "clone")),
        HubToolKind::GitPush => Some((Op::Git, "operation", "push")),
        HubToolKind::ServiceStart => Some((Op::Service, "action", "start")),
        HubToolKind::ServiceStop => Some((Op::Service, "action", "stop")),
        HubToolKind::ServiceReload => Some((Op::Service, "action", "reload")),
        _ => None,
    }
}

/// Translate a one-agent Hub wrapper into its internal wire operation.
///
/// `sentinel_service_status` is deliberately excluded because it composes
/// three read-only service actions and must retain each part's independent
/// outcome.
///
/// # Errors
/// Returns an argument error when the wrapper arguments or host selector have
/// the wrong JSON shape.
pub fn hub_protocol_call(
    kind: HubToolKind,
    arguments: &Value,
) -> Result<Option<HubProtocolCall>, HubToolArgumentError> {
    let Some((op, discriminator, value)) = protocol_operation(kind) else {
        return Ok(None);
    };
    let mut payload = arguments
        .as_object()
        .cloned()
        .ok_or(HubToolArgumentError::NotObject)?;
    let host_selector = match payload.remove("host_id") {
        None | Some(Value::Null) => None,
        Some(Value::String(value)) => Some(value),
        Some(_) => return Err(HubToolArgumentError::InvalidHostSelector),
    };
    payload.insert(discriminator.to_owned(), Value::String(value.to_owned()));
    Ok(Some(HubProtocolCall {
        op,
        host_selector,
        payload: Value::Object(payload),
    }))
}

#[must_use]
pub fn hub_tool_mcp_entry(tool: &HubTool) -> Value {
    json!({
        "name": tool.public_name,
        "description": hub_tool_description(tool.kind),
        "inputSchema": hub_tool_input_schema(tool.kind),
        "x-sentinel-hub-tool": true
    })
}

#[must_use]
pub fn hub_tool_catalog() -> Vec<Value> {
    HUB_TOOLS.iter().map(hub_tool_mcp_entry).collect()
}

#[must_use]
pub fn model_tool_catalog() -> Vec<Value> {
    let mut tools = direct_tool_catalog(&DIRECT_TOOLS);
    tools.extend(hub_tool_catalog());
    tools
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn hub_registry_has_unique_names_and_strict_schemas() {
        let names = HUB_TOOLS
            .iter()
            .map(|tool| tool.public_name)
            .collect::<BTreeSet<_>>();
        assert_eq!(names.len(), HUB_TOOLS.len());
        for tool in HUB_TOOLS {
            let schema = hub_tool_input_schema(tool.kind);
            assert_eq!(schema["type"], "object");
            assert_eq!(schema["additionalProperties"], false);
        }
    }

    #[test]
    fn git_and_service_wrappers_map_to_wire_only_ops() -> Result<(), HubToolArgumentError> {
        let cases = [
            (HubToolKind::GitDiff, Op::Git, "operation", "diff"),
            (
                HubToolKind::GitApplyPatch,
                Op::Git,
                "operation",
                "apply_patch",
            ),
            (HubToolKind::GitLsRemote, Op::Git, "operation", "ls_remote"),
            (HubToolKind::GitFetch, Op::Git, "operation", "fetch"),
            (HubToolKind::GitClone, Op::Git, "operation", "clone"),
            (HubToolKind::GitPush, Op::Git, "operation", "push"),
            (HubToolKind::ServiceStart, Op::Service, "action", "start"),
            (HubToolKind::ServiceStop, Op::Service, "action", "stop"),
            (HubToolKind::ServiceReload, Op::Service, "action", "reload"),
        ];
        for (kind, op, discriminator, expected) in cases {
            let Some(call) =
                hub_protocol_call(kind, &json!({"host_id": "build", "opaque_ref": "trace"}))?
            else {
                return Err(HubToolArgumentError::NotObject);
            };
            assert_eq!(call.op, op);
            assert_eq!(call.host_selector.as_deref(), Some("build"));
            assert_eq!(call.payload[discriminator], expected);
            assert_eq!(call.payload["opaque_ref"], "trace");
            assert!(call.payload.get("host_id").is_none());
        }
        assert!(hub_protocol_call(HubToolKind::ServiceStatus, &json!({}))?.is_none());
        Ok(())
    }

    #[test]
    fn model_catalog_is_direct_plus_hub() {
        assert_eq!(
            model_tool_catalog().len(),
            DIRECT_TOOLS.len() + HUB_TOOLS.len()
        );
    }
}
