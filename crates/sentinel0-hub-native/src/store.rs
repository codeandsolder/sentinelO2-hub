use chrono::Utc;
use rusqlite::{Connection, OptionalExtension as _, params};
use sentinel0_hub_core::{HostRecord, HostRegistry, JobCompletion};
use serde::Serialize;
use serde_json::Value;
use thiserror::Error;

const IDEMPOTENCY_TTL_MS: i64 = 24 * 60 * 60 * 1_000;

#[derive(Debug, PartialEq)]
pub enum BeginIdempotency {
    Start,
    Conflict,
    Pending { hub_request_id: String },
    Replay { status: u16, body: Value },
}

#[derive(Debug, Error)]
pub enum StoreError {
    #[error(transparent)]
    Sql(#[from] rusqlite::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("job completion host does not match existing job owner")]
    JobHostMismatch,
    #[error("corrupt idempotency row: {0}")]
    CorruptIdempotency(String),
    #[error("host label is already assigned to {0}")]
    LabelConflict(String),
}

#[derive(Debug, Serialize)]
pub struct JobSummary {
    pub job_id: String,
    #[serde(rename = "host")]
    pub host_id: String,
    pub tool: String,
    pub status: String,
}

#[derive(Debug, Serialize)]
pub struct NotificationsCheck {
    pub completed: Vec<Value>,
    pub running: Vec<JobSummary>,
    pub orphaned: Vec<JobSummary>,
}

#[derive(Debug, Serialize)]
pub struct JobView {
    pub job_id: String,
    #[serde(rename = "host")]
    pub host_id: String,
    pub tool: String,
    pub status: String,
    pub completion: Option<Value>,
    pub created_ms: i64,
    pub updated_ms: i64,
}

pub struct NativeStore {
    connection: Connection,
}

impl NativeStore {
    pub fn open(path: &str) -> Result<Self, StoreError> {
        let connection = Connection::open(path)?;
        connection.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA foreign_keys = ON;
             CREATE TABLE IF NOT EXISTS hosts (
                 host_id TEXT PRIMARY KEY,
                 hostname TEXT NOT NULL,
                 label TEXT,
                 disabled INTEGER NOT NULL DEFAULT 0,
                 agent_version TEXT NOT NULL DEFAULT '',
                 protocol_version TEXT NOT NULL DEFAULT '',
                 last_connected_ms INTEGER NOT NULL DEFAULT 0,
                 last_disconnected_ms INTEGER
             );
             CREATE UNIQUE INDEX IF NOT EXISTS hosts_label_unique
                 ON hosts(label) WHERE label IS NOT NULL;
             CREATE TABLE IF NOT EXISTS tenant_settings (
                 key TEXT PRIMARY KEY,
                 value TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS jobs (
                 job_id TEXT PRIMARY KEY,
                 host_id TEXT NOT NULL,
                 tool TEXT NOT NULL,
                 status TEXT NOT NULL,
                 completion_json TEXT,
                 created_ms INTEGER NOT NULL,
                 updated_ms INTEGER NOT NULL
             );
             CREATE INDEX IF NOT EXISTS jobs_status_idx ON jobs(status);
             CREATE TABLE IF NOT EXISTS idempotency (
                 client_request_id TEXT PRIMARY KEY,
                 fingerprint TEXT NOT NULL,
                 state TEXT NOT NULL CHECK (state IN ('pending', 'complete')),
                 hub_request_id TEXT NOT NULL UNIQUE,
                 response_json TEXT,
                 http_status INTEGER,
                 created_ms INTEGER NOT NULL,
                 expires_ms INTEGER NOT NULL
             );
             CREATE INDEX IF NOT EXISTS idempotency_expires_idx ON idempotency(expires_ms);
             CREATE TABLE IF NOT EXISTS notifications (
                 notification_id TEXT PRIMARY KEY,
                 kind TEXT NOT NULL,
                 ref_id TEXT NOT NULL UNIQUE,
                 summary_json TEXT NOT NULL,
                 created_ms INTEGER NOT NULL,
                 read_ms INTEGER,
                 acked_ms INTEGER
             );",
        )?;
        let now_ms = Utc::now().timestamp_millis();
        connection.execute(
            "UPDATE jobs SET status = 'orphaned', updated_ms = ?1 WHERE status = 'running'",
            params![now_ms],
        )?;
        Ok(Self { connection })
    }

    pub fn load_host_registry(&self) -> Result<HostRegistry, StoreError> {
        let mut statement = self
            .connection
            .prepare("SELECT host_id, hostname, label, disabled FROM hosts ORDER BY host_id")?;
        let hosts = statement
            .query_map([], |row| {
                Ok(HostRecord {
                    host_id: row.get(0)?,
                    hostname: row.get(1)?,
                    label: row.get(2)?,
                    connected: false,
                    disabled: row.get::<_, i64>(3)? != 0,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        let default_host_id = self
            .connection
            .query_row(
                "SELECT value FROM tenant_settings WHERE key = 'default_host_id' LIMIT 1",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        Ok(HostRegistry::from_records(hosts, default_host_id))
    }

    pub fn host_disabled(&self, host_id: &str) -> Result<bool, StoreError> {
        Ok(self
            .connection
            .query_row(
                "SELECT disabled FROM hosts WHERE host_id = ?1 LIMIT 1",
                params![host_id],
                |row| row.get::<_, i64>(0),
            )
            .optional()?
            .is_some_and(|disabled| disabled != 0))
    }

    pub fn persist_hello(
        &self,
        host_id: &str,
        hostname: &str,
        agent_version: &str,
        protocol_version: &str,
    ) -> Result<(), StoreError> {
        self.connection.execute(
            "INSERT INTO hosts
                (host_id, hostname, agent_version, protocol_version,
                 last_connected_ms, last_disconnected_ms)
             VALUES (?1, ?2, ?3, ?4, ?5, NULL)
             ON CONFLICT(host_id) DO UPDATE SET
                 hostname = excluded.hostname,
                 agent_version = excluded.agent_version,
                 protocol_version = excluded.protocol_version,
                 last_connected_ms = excluded.last_connected_ms,
                 last_disconnected_ms = NULL",
            params![
                host_id,
                hostname,
                agent_version,
                protocol_version,
                Utc::now().timestamp_millis()
            ],
        )?;
        Ok(())
    }

    pub fn mark_host_disconnected(&self, host_id: &str) -> Result<(), StoreError> {
        self.connection.execute(
            "UPDATE hosts SET last_disconnected_ms = ?1 WHERE host_id = ?2",
            params![Utc::now().timestamp_millis(), host_id],
        )?;
        Ok(())
    }

    pub fn set_default_host(&self, host_id: &str) -> Result<(), StoreError> {
        self.connection.execute(
            "INSERT INTO tenant_settings (key, value) VALUES ('default_host_id', ?1)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            params![host_id],
        )?;
        Ok(())
    }

    pub fn clear_default_host(&self) -> Result<bool, StoreError> {
        Ok(self.connection.execute(
            "DELETE FROM tenant_settings WHERE key = 'default_host_id'",
            [],
        )? > 0)
    }

    pub fn set_host_label(&self, host_id: &str, label: Option<&str>) -> Result<(), StoreError> {
        if let Some(label) = label {
            let owner = self
                .connection
                .query_row(
                    "SELECT host_id FROM hosts WHERE label = ?1 AND host_id <> ?2 LIMIT 1",
                    params![label, host_id],
                    |row| row.get::<_, String>(0),
                )
                .optional()?;
            if let Some(owner) = owner {
                return Err(StoreError::LabelConflict(owner));
            }
        }
        self.connection.execute(
            "UPDATE hosts SET label = ?1 WHERE host_id = ?2",
            params![label, host_id],
        )?;
        Ok(())
    }

    pub fn set_host_disabled(&self, host_id: &str, disabled: bool) -> Result<(), StoreError> {
        self.connection.execute(
            "UPDATE hosts SET disabled = ?1 WHERE host_id = ?2",
            params![disabled, host_id],
        )?;
        Ok(())
    }

    pub fn begin_idempotency(
        &self,
        client_request_id: &str,
        fingerprint: &str,
        hub_request_id: &str,
    ) -> Result<BeginIdempotency, StoreError> {
        let now_ms = Utc::now().timestamp_millis();
        self.connection.execute(
            "DELETE FROM idempotency WHERE expires_ms <= ?1",
            params![now_ms],
        )?;

        let row = self
            .connection
            .query_row(
                "SELECT fingerprint, state, hub_request_id, response_json, http_status
                 FROM idempotency WHERE client_request_id = ?1 LIMIT 1",
                params![client_request_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, Option<i64>>(4)?,
                    ))
                },
            )
            .optional()?;

        if let Some((stored_fingerprint, state, stored_request_id, response_json, http_status)) =
            row
        {
            if stored_fingerprint != fingerprint {
                return Ok(BeginIdempotency::Conflict);
            }
            return match state.as_str() {
                "pending" => Ok(BeginIdempotency::Pending {
                    hub_request_id: stored_request_id,
                }),
                "complete" => {
                    let response_json = response_json.ok_or_else(|| {
                        StoreError::CorruptIdempotency(
                            "complete row has no response_json".to_owned(),
                        )
                    })?;
                    let mut body = serde_json::from_str::<Value>(&response_json)?;
                    if let Value::Object(values) = &mut body {
                        values.insert("replayed".to_owned(), Value::Bool(true));
                    }
                    let status = http_status
                        .and_then(|value| u16::try_from(value).ok())
                        .filter(|value| (100..=599).contains(value))
                        .ok_or_else(|| {
                            StoreError::CorruptIdempotency(
                                "complete row has invalid http_status".to_owned(),
                            )
                        })?;
                    Ok(BeginIdempotency::Replay { status, body })
                }
                other => Err(StoreError::CorruptIdempotency(format!(
                    "unknown state {other:?}"
                ))),
            };
        }

        self.connection.execute(
            "INSERT INTO idempotency
                (client_request_id, fingerprint, state, hub_request_id,
                 response_json, http_status, created_ms, expires_ms)
             VALUES (?1, ?2, 'pending', ?3, NULL, NULL, ?4, ?5)",
            params![
                client_request_id,
                fingerprint,
                hub_request_id,
                now_ms,
                now_ms + IDEMPOTENCY_TTL_MS
            ],
        )?;
        Ok(BeginIdempotency::Start)
    }

    pub fn pending_client_request_id(
        &self,
        hub_request_id: &str,
    ) -> Result<Option<String>, StoreError> {
        Ok(self
            .connection
            .query_row(
                "SELECT client_request_id FROM idempotency
                 WHERE hub_request_id = ?1 AND state = 'pending' LIMIT 1",
                params![hub_request_id],
                |row| row.get(0),
            )
            .optional()?)
    }

    pub fn complete_idempotency<T: Serialize>(
        &self,
        client_request_id: &str,
        hub_request_id: &str,
        status: u16,
        body: &T,
    ) -> Result<(), StoreError> {
        let response_json = serde_json::to_string(body)?;
        self.connection.execute(
            "UPDATE idempotency SET
                 state = 'complete', response_json = ?1, http_status = ?2
             WHERE client_request_id = ?3 AND hub_request_id = ?4 AND state = 'pending'",
            params![
                response_json,
                i64::from(status),
                client_request_id,
                hub_request_id
            ],
        )?;
        Ok(())
    }

    pub fn mark_running_jobs_orphaned(&self, host_id: &str) -> Result<usize, StoreError> {
        let now_ms = Utc::now().timestamp_millis();
        Ok(self.connection.execute(
            "UPDATE jobs SET status = 'orphaned', updated_ms = ?1
             WHERE host_id = ?2 AND status = 'running'",
            params![now_ms, host_id],
        )?)
    }

    pub fn job_started(&self, job_id: &str, host_id: &str, tool: &str) -> Result<(), StoreError> {
        let now_ms = Utc::now().timestamp_millis();
        self.connection.execute(
            "INSERT INTO jobs
                (job_id, host_id, tool, status, completion_json, created_ms, updated_ms)
             VALUES (?1, ?2, ?3, 'running', NULL, ?4, ?4)
             ON CONFLICT(job_id) DO NOTHING",
            params![job_id, host_id, tool, now_ms],
        )?;
        Ok(())
    }

    pub fn job_completed(&self, completion: &JobCompletion) -> Result<(), StoreError> {
        let owner = self
            .connection
            .query_row(
                "SELECT host_id FROM jobs WHERE job_id = ?1 LIMIT 1",
                params![completion.job_id],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        if owner
            .as_deref()
            .is_some_and(|host_id| host_id != completion.host_id)
        {
            return Err(StoreError::JobHostMismatch);
        }

        let now_ms = Utc::now().timestamp_millis();
        let completion_json = serde_json::to_string(&completion.data)?;
        let summary_json = serde_json::to_string(&completion.summary())?;
        self.connection.execute(
            "INSERT INTO jobs
                (job_id, host_id, tool, status, completion_json, created_ms, updated_ms)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?6)
             ON CONFLICT(job_id) DO UPDATE SET
                 tool = excluded.tool,
                 status = excluded.status,
                 completion_json = excluded.completion_json,
                 updated_ms = excluded.updated_ms
             WHERE jobs.host_id = excluded.host_id",
            params![
                completion.job_id,
                completion.host_id,
                completion.tool,
                completion.status,
                completion_json,
                now_ms
            ],
        )?;
        self.connection.execute(
            "INSERT INTO notifications
                (notification_id, kind, ref_id, summary_json, created_ms, read_ms, acked_ms)
             VALUES (?1, 'job_completed', ?2, ?3, ?4, NULL, NULL)
             ON CONFLICT(notification_id) DO UPDATE SET summary_json = excluded.summary_json",
            params![
                format!("job:{}", completion.job_id),
                completion.job_id,
                summary_json,
                now_ms
            ],
        )?;
        Ok(())
    }

    pub fn notifications_check(&mut self) -> Result<NotificationsCheck, StoreError> {
        let transaction = self.connection.transaction()?;
        let mut unread_statement = transaction.prepare(
            "SELECT notification_id, summary_json FROM notifications
             WHERE read_ms IS NULL AND acked_ms IS NULL
             ORDER BY created_ms LIMIT 100",
        )?;
        let unread = unread_statement
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        drop(unread_statement);

        let now_ms = Utc::now().timestamp_millis();
        let mut completed = Vec::with_capacity(unread.len());
        for (notification_id, summary_json) in unread {
            completed.push(serde_json::from_str(&summary_json)?);
            transaction.execute(
                "UPDATE notifications SET read_ms = ?1
                 WHERE notification_id = ?2 AND read_ms IS NULL AND acked_ms IS NULL",
                params![now_ms, notification_id],
            )?;
        }

        let running = query_jobs_by_status(&transaction, "running", "created_ms")?;
        let orphaned = query_jobs_by_status(&transaction, "orphaned", "updated_ms")?;
        transaction.commit()?;
        Ok(NotificationsCheck {
            completed,
            running,
            orphaned,
        })
    }

    pub fn notifications_get(&self, job_id: &str) -> Result<Option<JobView>, StoreError> {
        let row = self
            .connection
            .query_row(
                "SELECT job_id, host_id, tool, status, completion_json, created_ms, updated_ms
                 FROM jobs WHERE job_id = ?1 LIMIT 1",
                params![job_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, Option<String>>(4)?,
                        row.get::<_, i64>(5)?,
                        row.get::<_, i64>(6)?,
                    ))
                },
            )
            .optional()?;
        let Some((job_id, host_id, tool, status, completion_json, created_ms, updated_ms)) = row
        else {
            return Ok(None);
        };
        let completion = completion_json
            .as_deref()
            .map(serde_json::from_str)
            .transpose()?;
        Ok(Some(JobView {
            job_id,
            host_id,
            tool,
            status,
            completion,
            created_ms,
            updated_ms,
        }))
    }

    pub fn notifications_ack(&self, job_id: Option<&str>) -> Result<usize, StoreError> {
        let now_ms = Utc::now().timestamp_millis();
        let changed = match job_id {
            None | Some("all") => self.connection.execute(
                "UPDATE notifications SET acked_ms = ?1 WHERE acked_ms IS NULL",
                params![now_ms],
            )?,
            Some(job_id) => self.connection.execute(
                "UPDATE notifications SET acked_ms = ?1
                 WHERE ref_id = ?2 AND acked_ms IS NULL",
                params![now_ms, job_id],
            )?,
        };
        Ok(changed)
    }
}

fn query_jobs_by_status(
    connection: &Connection,
    status: &str,
    order_column: &str,
) -> Result<Vec<JobSummary>, rusqlite::Error> {
    let sql = format!(
        "SELECT job_id, host_id, tool, status FROM jobs
         WHERE status = ?1 ORDER BY {order_column} LIMIT 100"
    );
    let mut statement = connection.prepare(&sql)?;
    statement
        .query_map(params![status], |row| {
            Ok(JobSummary {
                job_id: row.get(0)?,
                host_id: row.get(1)?,
                tool: row.get(2)?,
                status: row.get(3)?,
            })
        })?
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn host_registry_state_survives_store_reopen() -> Result<(), StoreError> {
        let path = std::env::temp_dir().join(format!(
            "sentinel0-hub-native-host-test-{}.sqlite3",
            uuid::Uuid::now_v7().simple()
        ));
        {
            let store = NativeStore::open(path.to_str().ok_or_else(|| {
                StoreError::CorruptIdempotency("temporary path is not UTF-8".to_owned())
            })?)?;
            store.persist_hello("host_a", "alpha", "1.0", "1.13")?;
            store.persist_hello("host_b", "beta", "1.0", "1.13")?;
            store.set_host_label("host_a", Some("build"))?;
            store.set_host_disabled("host_b", true)?;
            store.set_default_host("host_a")?;
        }
        {
            let store = NativeStore::open(path.to_str().ok_or_else(|| {
                StoreError::CorruptIdempotency("temporary path is not UTF-8".to_owned())
            })?)?;
            let registry = store.load_host_registry()?;
            assert_eq!(registry.default_host_id(), Some("host_a"));
            let hosts = registry.hosts().collect::<Vec<_>>();
            assert_eq!(hosts.len(), 2);
            assert!(hosts.iter().all(|host| !host.connected));
            assert_eq!(
                hosts
                    .iter()
                    .find(|host| host.host_id == "host_a")
                    .and_then(|host| host.label.as_deref()),
                Some("build")
            );
            assert!(
                hosts
                    .iter()
                    .find(|host| host.host_id == "host_b")
                    .is_some_and(|host| host.disabled)
            );
        }
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite3-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite3-shm"));
        Ok(())
    }

    #[test]
    fn idempotency_replays_conflicts_and_tracks_pending() -> Result<(), StoreError> {
        let store = NativeStore::open(":memory:")?;
        assert_eq!(
            store.begin_idempotency("client_1", "fingerprint_a", "hreq_1")?,
            BeginIdempotency::Start
        );
        assert_eq!(
            store.begin_idempotency("client_1", "fingerprint_a", "hreq_2")?,
            BeginIdempotency::Pending {
                hub_request_id: "hreq_1".to_owned()
            }
        );
        assert_eq!(
            store.begin_idempotency("client_1", "fingerprint_b", "hreq_3")?,
            BeginIdempotency::Conflict
        );
        assert_eq!(
            store.pending_client_request_id("hreq_1")?.as_deref(),
            Some("client_1")
        );

        let body = serde_json::json!({
            "ok": true,
            "hub_request_id": "hreq_1",
            "client_request_id": "client_1",
            "replayed": false,
            "result": {"value": 7}
        });
        store.complete_idempotency("client_1", "hreq_1", 200, &body)?;

        match store.begin_idempotency("client_1", "fingerprint_a", "hreq_4")? {
            BeginIdempotency::Replay { status, body } => {
                assert_eq!(status, 200);
                assert_eq!(body.get("replayed"), Some(&Value::Bool(true)));
                assert_eq!(
                    body.get("hub_request_id").and_then(Value::as_str),
                    Some("hreq_1")
                );
            }
            other => {
                return Err(StoreError::CorruptIdempotency(format!(
                    "expected replay, got {other:?}"
                )));
            }
        }
        Ok(())
    }

    #[test]
    fn job_lifecycle_tracks_running_orphaned_and_completed_once() -> Result<(), StoreError> {
        let mut store = NativeStore::open(":memory:")?;
        store.job_started("job_test", "host_test", "script_run")?;

        let running = store.notifications_check()?;
        assert_eq!(running.running.len(), 1);
        assert_eq!(running.orphaned.len(), 0);
        assert_eq!(running.completed.len(), 0);

        assert_eq!(store.mark_running_jobs_orphaned("host_test")?, 1);
        let orphaned = store.notifications_check()?;
        assert_eq!(orphaned.running.len(), 0);
        assert_eq!(orphaned.orphaned.len(), 1);

        let mut data = BTreeMap::new();
        data.insert("job_id".to_owned(), Value::String("job_test".to_owned()));
        data.insert("tool".to_owned(), Value::String("script_run".to_owned()));
        data.insert("host".to_owned(), Value::String("host_test".to_owned()));
        data.insert("status".to_owned(), Value::String("succeeded".to_owned()));
        data.insert("output".to_owned(), Value::String("done".to_owned()));
        let completion = JobCompletion {
            job_id: "job_test".to_owned(),
            tool: "script_run".to_owned(),
            host_id: "host_test".to_owned(),
            status: "succeeded".to_owned(),
            data,
        };

        store.job_completed(&completion)?;
        store.job_completed(&completion)?;
        let completed = store.notifications_check()?;
        assert_eq!(completed.completed.len(), 1);
        assert_eq!(completed.running.len(), 0);
        assert_eq!(completed.orphaned.len(), 0);

        let after_read = store.notifications_check()?;
        assert_eq!(after_read.completed.len(), 0);

        let job = store.notifications_get("job_test")?;
        assert!(job.is_some());
        if let Some(job) = job {
            assert_eq!(job.status, "succeeded");
            assert_eq!(
                job.completion
                    .as_ref()
                    .and_then(|value| value.get("output")),
                Some(&Value::String("done".to_owned()))
            );
        }
        Ok(())
    }
}
