use crate::DirectTool;
use sentinel0_proto::Op;
use serde_json::{Map, Value, json};

fn nullable_string() -> Value {
    json!({"type": ["string", "null"]})
}

fn opaque_ref_schema() -> Value {
    json!({"type": ["string", "null"], "maxLength": 256})
}

fn host_schema() -> Value {
    nullable_string()
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

fn with_host_and_ref(mut fields: Vec<(&'static str, Value)>) -> Vec<(&'static str, Value)> {
    fields.push(("host_id", host_schema()));
    fields.push(("opaque_ref", opaque_ref_schema()));
    fields
}

#[must_use]
pub fn direct_tool_description(op: Op) -> &'static str {
    match op {
        Op::Ping => "Simple SentinelX health check.",
        Op::Capabilities => "Get SentinelX capabilities and policy summary for a host.",
        Op::Help => "Get bounded SentinelX help and navigation sections exposed by the agent.",
        Op::State => "Get SentinelX internal state for a host.",
        Op::Exec => "Execute an allowed command through SentinelX, optionally as a background job.",
        Op::ScriptRun => "Run a one-off bash, Python or PowerShell script on the host.",
        Op::Edit => "Apply a policy-gated structured edit to a file on the host.",
        Op::EditUploadInit => "Initialize a staged edit-upload session.",
        Op::EditUploadFile => "Stage one old/new role file for an edit-upload session.",
        Op::EditUploadComplete => "Apply an edit using files staged in an edit-upload session.",
        Op::Restart => "Restart an allowed service through SentinelX.",
        Op::UploadInit => "Begin a chunked file upload to the host.",
        Op::UploadChunk => "Send one chunk of a chunked upload.",
        Op::UploadComplete => "Reassemble and optionally verify a chunked upload.",
        Op::UploadFile => "Upload a file from base64 content or a trusted URL.",
        Op::Read => "Read text from a policy-allowed file on the host.",
        Op::List => "List entries in a policy-allowed directory.",
        Op::Search => "Recursively search text content under a policy-allowed path.",
        Op::ProjectSnapshot => "Return a bounded repository or directory orientation snapshot.",
        Op::Move => "Move or rename a file or directory within writable policy paths.",
        Op::Copy => "Copy a file or directory within writable policy paths.",
        Op::Delete => "Delete a file or directory with SentinelX backup safeguards.",
        Op::Chmod => "Change mode bits of a path under a writable policy path.",
        Op::Chown => "Change owner and/or group of a path under a writable policy path.",
        Op::LocalApi => "Call a host-declared structured local API endpoint.",
        _ => "Internal SentinelX agent operation.",
    }
}

#[must_use]
pub fn direct_tool_input_schema(op: Op) -> Value {
    match op {
        Op::Ping | Op::Capabilities | Op::Help | Op::State | Op::EditUploadInit => {
            discovery_schema(op)
        }
        Op::Exec | Op::ScriptRun | Op::Edit | Op::EditUploadFile | Op::EditUploadComplete => {
            execution_schema(op)
        }
        Op::Restart | Op::UploadInit | Op::UploadChunk | Op::UploadComplete | Op::UploadFile => {
            upload_schema(op)
        }
        Op::Read | Op::List | Op::Search | Op::ProjectSnapshot => read_schema(op),
        Op::Move | Op::Copy | Op::Delete | Op::Chmod | Op::Chown | Op::LocalApi => {
            mutation_schema(op)
        }
        _ => object_schema(&[], with_host_and_ref(Vec::new())),
    }
}

fn discovery_schema(op: Op) -> Value {
    match op {
        Op::Ping | Op::State | Op::EditUploadInit => {
            object_schema(&[], with_host_and_ref(Vec::new()))
        }
        Op::Capabilities => {
            object_schema(&[], with_host_and_ref(vec![("detail", nullable_string())]))
        }
        Op::Help => object_schema(
            &[],
            with_host_and_ref(vec![
                ("topic", nullable_string()),
                ("path", nullable_string()),
                ("playbook", nullable_string()),
                ("offset", json!({"type": ["integer", "null"]})),
                ("limit", json!({"type": ["integer", "null"]})),
            ]),
        ),
        _ => object_schema(&[], with_host_and_ref(Vec::new())),
    }
}

fn execution_schema(op: Op) -> Value {
    match op {
        Op::Exec => object_schema(
            &["command"],
            with_host_and_ref(vec![
                ("command", json!({"type": "string"})),
                (
                    "timeout",
                    json!({"type": "integer", "minimum": 0, "default": 30}),
                ),
                ("background", json!({"type": "boolean", "default": false})),
                (
                    "notify_telegram",
                    json!({"anyOf": [{"type": "boolean"}, {"type": "string"}], "default": false}),
                ),
                (
                    "notify_resend",
                    json!({"anyOf": [{"type": "boolean"}, {"type": "string"}], "default": false}),
                ),
            ]),
        ),
        Op::ScriptRun => object_schema(
            &["content"],
            with_host_and_ref(vec![
                ("content", json!({"type": "string"})),
                (
                    "interpreter",
                    json!({"type": "string", "enum": ["bash", "python3", "powershell", "pwsh"], "default": "bash"}),
                ),
                (
                    "args",
                    json!({"type": ["array", "null"], "items": {"type": "string"}}),
                ),
                ("cwd", nullable_string()),
                (
                    "timeout",
                    json!({"type": "integer", "minimum": 0, "default": 60}),
                ),
                ("sudo", json!({"type": "boolean", "default": false})),
                ("cleanup", json!({"type": "boolean", "default": true})),
                ("filename", nullable_string()),
                (
                    "env",
                    json!({"type": ["object", "null"], "additionalProperties": {"type": "string"}}),
                ),
                ("background", json!({"type": "boolean", "default": false})),
                (
                    "notify_telegram",
                    json!({"anyOf": [{"type": "boolean"}, {"type": "string"}], "default": false}),
                ),
                (
                    "notify_resend",
                    json!({"anyOf": [{"type": "boolean"}, {"type": "string"}], "default": false}),
                ),
            ]),
        ),
        Op::Edit => object_schema(&["path", "mode"], with_host_and_ref(edit_fields(false))),
        Op::EditUploadFile => object_schema(
            &["upload_id", "role"],
            with_host_and_ref(vec![
                ("upload_id", json!({"type": "string"})),
                ("role", json!({"type": "string", "enum": ["old", "new"]})),
                ("content", nullable_string()),
                ("content_base64", nullable_string()),
                ("filename", nullable_string()),
            ]),
        ),
        Op::EditUploadComplete => object_schema(
            &["upload_id", "path", "mode"],
            with_host_and_ref(edit_fields(true)),
        ),
        _ => object_schema(&[], with_host_and_ref(Vec::new())),
    }
}

fn upload_schema(op: Op) -> Value {
    match op {
        Op::Restart => object_schema(
            &["service"],
            with_host_and_ref(vec![("service", json!({"type": "string"}))]),
        ),
        Op::UploadInit => object_schema(
            &["target_path"],
            with_host_and_ref(vec![
                ("target_path", json!({"type": "string"})),
                (
                    "total_size",
                    json!({"type": "integer", "minimum": 0, "default": 0}),
                ),
                ("overwrite", json!({"type": "boolean", "default": false})),
                ("filename", nullable_string()),
            ]),
        ),
        Op::UploadChunk => object_schema(
            &["upload_id", "index", "content_base64"],
            with_host_and_ref(vec![
                ("upload_id", json!({"type": "string"})),
                ("index", json!({"type": "integer", "minimum": 0})),
                ("content_base64", json!({"type": "string"})),
            ]),
        ),
        Op::UploadComplete => object_schema(
            &["upload_id"],
            with_host_and_ref(vec![
                ("upload_id", json!({"type": "string"})),
                ("sha256", nullable_string()),
            ]),
        ),
        Op::UploadFile => object_schema(
            &["target_path"],
            with_host_and_ref(vec![
                ("target_path", json!({"type": "string"})),
                ("content_base64", nullable_string()),
                ("file_url", nullable_string()),
                ("filename", nullable_string()),
                ("overwrite", json!({"type": "boolean", "default": false})),
            ]),
        ),
        _ => object_schema(&[], with_host_and_ref(Vec::new())),
    }
}

fn read_schema(op: Op) -> Value {
    match op {
        Op::Read => object_schema(
            &["path"],
            with_host_and_ref(vec![
                ("path", json!({"type": "string"})),
                (
                    "view_range",
                    json!({"type": ["array", "null"], "items": {"type": "integer"}, "minItems": 2, "maxItems": 2}),
                ),
                (
                    "max_bytes",
                    json!({"type": ["integer", "null"], "minimum": 1}),
                ),
            ]),
        ),
        Op::List => object_schema(
            &["path"],
            with_host_and_ref(vec![
                ("path", json!({"type": "string"})),
                (
                    "depth",
                    json!({"type": "integer", "minimum": 1, "maximum": 5, "default": 1}),
                ),
                ("glob", nullable_string()),
                ("show_hidden", json!({"type": "boolean", "default": false})),
            ]),
        ),
        Op::Search => object_schema(
            &["path", "pattern"],
            with_host_and_ref(vec![
                ("path", json!({"type": "string"})),
                ("pattern", json!({"type": "string"})),
                ("regex", json!({"type": "boolean", "default": false})),
                (
                    "case_sensitive",
                    json!({"type": "boolean", "default": false}),
                ),
                ("file_glob", nullable_string()),
                (
                    "max_results",
                    json!({"type": ["integer", "null"], "minimum": 1}),
                ),
            ]),
        ),
        Op::ProjectSnapshot => object_schema(
            &["path"],
            with_host_and_ref(vec![("path", json!({"type": "string"}))]),
        ),
        _ => object_schema(&[], with_host_and_ref(Vec::new())),
    }
}

fn mutation_schema(op: Op) -> Value {
    match op {
        Op::Move | Op::Copy => object_schema(
            &["src", "dst"],
            with_host_and_ref(vec![
                ("src", json!({"type": "string"})),
                ("dst", json!({"type": "string"})),
                ("overwrite", json!({"type": "boolean", "default": false})),
            ]),
        ),
        Op::Delete => object_schema(
            &["path"],
            with_host_and_ref(vec![
                ("path", json!({"type": "string"})),
                ("recursive", json!({"type": "boolean", "default": false})),
            ]),
        ),
        Op::Chmod => object_schema(
            &["path", "mode"],
            with_host_and_ref(vec![
                ("path", json!({"type": "string"})),
                (
                    "mode",
                    json!({"type": "string", "pattern": "^0?[0-7]{3,4}$"}),
                ),
            ]),
        ),
        Op::Chown => object_schema(
            &["path"],
            with_host_and_ref(vec![
                ("path", json!({"type": "string"})),
                ("owner", nullable_string()),
                ("group", nullable_string()),
            ]),
        ),
        Op::LocalApi => object_schema(
            &["operation"],
            with_host_and_ref(vec![
                (
                    "operation",
                    json!({"type": "string", "enum": ["list", "describe", "call"]}),
                ),
                ("endpoint", nullable_string()),
                ("action", nullable_string()),
                (
                    "params",
                    json!({"type": ["object", "null"], "additionalProperties": true}),
                ),
            ]),
        ),
        _ => object_schema(&[], with_host_and_ref(Vec::new())),
    }
}

fn edit_fields(upload_complete: bool) -> Vec<(&'static str, Value)> {
    let mut fields = Vec::new();
    if upload_complete {
        fields.push(("upload_id", json!({"type": "string"})));
    }
    fields.extend([
        ("path", json!({"type": "string"})),
        (
            "mode",
            json!({"type": "string", "enum": ["replace", "regex", "replace-block", "append", "prepend", "write"]}),
        ),
        ("sudo", json!({"type": "boolean", "default": false})),
        ("old", nullable_string()),
        ("new_text", nullable_string()),
        ("pattern", nullable_string()),
        ("start_marker", nullable_string()),
        ("end_marker", nullable_string()),
        ("count", json!({"type": "integer", "minimum": 0, "default": 0})),
        ("multiline", json!({"type": "boolean", "default": false})),
        ("dotall", json!({"type": "boolean", "default": false})),
        ("interpret_escapes", json!({"type": "boolean", "default": false})),
        ("backup_dir", nullable_string()),
        ("validator", nullable_string()),
        (
            "validator_preset",
            json!({"type": ["string", "null"], "enum": ["nginx", "json", "python", "sh", "yaml", "systemd", "toml", null]}),
        ),
        ("diff", json!({"type": "boolean", "default": false})),
        ("dry_run", json!({"type": "boolean", "default": false})),
        ("allow_no_change", json!({"type": "boolean", "default": false})),
        ("create", json!({"type": "boolean", "default": false})),
    ]);
    if upload_complete {
        fields.retain(|(name, _)| !matches!(*name, "old" | "new_text"));
    }
    fields
}

#[must_use]
pub fn direct_tool_mcp_entry(tool: &DirectTool) -> Value {
    json!({
        "name": tool.public_name,
        "description": direct_tool_description(tool.op),
        "inputSchema": direct_tool_input_schema(tool.op),
        "x-sentinel-op": tool.op.as_str()
    })
}

#[must_use]
pub fn direct_tool_catalog(tools: &[DirectTool]) -> Vec<Value> {
    tools.iter().map(direct_tool_mcp_entry).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DIRECT_TOOLS, direct_tool_by_public_name};
    use std::collections::BTreeSet;

    #[test]
    fn catalog_has_live_model_schema_shape() {
        let catalog = direct_tool_catalog(&DIRECT_TOOLS);
        assert_eq!(catalog.len(), 25);
        let names = catalog
            .iter()
            .filter_map(|entry| entry.get("name").and_then(Value::as_str))
            .collect::<BTreeSet<_>>();
        assert_eq!(names.len(), 25);
        for entry in catalog {
            assert!(entry.get("description").is_some_and(Value::is_string));
            let schema = entry.get("inputSchema").and_then(Value::as_object);
            assert!(schema.is_some());
            if let Some(schema) = schema {
                assert_eq!(schema.get("type"), Some(&Value::String("object".into())));
                assert_eq!(
                    schema.get("additionalProperties"),
                    Some(&Value::Bool(false))
                );
                let properties = schema.get("properties").and_then(Value::as_object);
                assert!(properties.is_some());
                if let Some(properties) = properties {
                    assert!(properties.contains_key("host_id"));
                    assert!(properties.contains_key("opaque_ref"));
                }
            }
        }
    }

    #[test]
    fn high_risk_schemas_keep_required_guards() {
        let exec = direct_tool_by_public_name("sentinel_exec");
        assert!(exec.is_some());
        if let Some(exec) = exec {
            let schema = direct_tool_input_schema(exec.op);
            assert_eq!(schema["required"], json!(["command"]));
            assert_eq!(schema["properties"]["background"]["default"], false);
        }

        let edit = direct_tool_by_public_name("sentinel_edit");
        assert!(edit.is_some());
        if let Some(edit) = edit {
            let schema = direct_tool_input_schema(edit.op);
            assert_eq!(schema["required"], json!(["path", "mode"]));
            assert_eq!(
                schema["properties"]["mode"]["enum"],
                json!([
                    "replace",
                    "regex",
                    "replace-block",
                    "append",
                    "prepend",
                    "write"
                ])
            );
        }

        let local_api = direct_tool_by_public_name("sentinel_local_api");
        assert!(local_api.is_some());
        if let Some(local_api) = local_api {
            let schema = direct_tool_input_schema(local_api.op);
            assert_eq!(schema["required"], json!(["operation"]));
            assert_eq!(
                schema["properties"]["operation"]["enum"],
                json!(["list", "describe", "call"])
            );
        }
    }
}
