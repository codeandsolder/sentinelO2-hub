use crate::{DIRECT_TOOLS, direct_tool_catalog};
use serde_json::{Map, Value, json};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HubToolKind {
    ListHosts,
    SetHostLabel,
    RemoveHostLabel,
    SetDefaultHost,
    GetDefaultHost,
    ClearDefaultHost,
    TransferFile,
    NotificationsCheck,
    NotificationsGet,
    NotificationsAck,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HubTool {
    pub public_name: &'static str,
    pub kind: HubToolKind,
}

pub const HUB_TOOLS: [HubTool; 10] = [
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
    fn model_catalog_is_direct_plus_hub() {
        assert_eq!(
            model_tool_catalog().len(),
            DIRECT_TOOLS.len() + HUB_TOOLS.len()
        );
    }
}
