//! The durable inference ledger on SQLite (WAL mode).
//!
//! State machine per work item: `pending` -> `submitted` -> `completed` or
//! `failed`. Completed results are immutable and reused by work key. The
//! `submitted` state carries the provider request ID so a restart can
//! reconcile in-flight work instead of re-billing it.

use anyhow::{Context, Result};
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::Value;
use std::path::Path;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkState {
    Pending,
    Submitted { request_id: String },
    Completed(RecordedResult),
    Failed { error: String },
}

/// The immutable, validated output of a completed work item with provenance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordedResult {
    pub output: Value,
    pub provider: String,
    pub model: String,
    pub request_id: String,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub validation: String,
}

pub struct Ledger {
    conn: Connection,
}

impl Ledger {
    pub fn open(path: &Path) -> Result<Self> {
        let conn = Connection::open(path).context("open ledger database")?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS work_items (
                work_key      TEXT PRIMARY KEY,
                state         TEXT NOT NULL CHECK (state IN ('pending','submitted','completed','failed')),
                spec_json     TEXT NOT NULL,
                request_id    TEXT,
                output_json   TEXT,
                provider      TEXT,
                model         TEXT,
                input_tokens  INTEGER,
                output_tokens INTEGER,
                validation    TEXT,
                error         TEXT,
                created_at    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
                updated_at    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
            );
            CREATE INDEX IF NOT EXISTS idx_work_items_state ON work_items(state);",
        )?;
        Ok(Self { conn })
    }

    /// Register work if unknown; a completed item is never reset.
    pub fn upsert_pending(&self, work_key: &str, spec_json: &str) -> Result<()> {
        self.conn.execute(
            "INSERT INTO work_items (work_key, state, spec_json)
             VALUES (?1, 'pending', ?2)
             ON CONFLICT(work_key) DO NOTHING",
            params![work_key, spec_json],
        )?;
        Ok(())
    }

    pub fn state(&self, work_key: &str) -> Result<Option<WorkState>> {
        self.conn
            .query_row(
                "SELECT state, request_id, output_json, provider, model,
                        input_tokens, output_tokens, validation, error
                 FROM work_items WHERE work_key = ?1",
                params![work_key],
                |row| {
                    let state: String = row.get(0)?;
                    Ok(match state.as_str() {
                        "pending" => WorkState::Pending,
                        "submitted" => WorkState::Submitted {
                            request_id: row.get::<_, Option<String>>(1)?.unwrap_or_default(),
                        },
                        "completed" => WorkState::Completed(RecordedResult {
                            output: serde_json::from_str(
                                &row.get::<_, String>(2).unwrap_or_default(),
                            )
                            .unwrap_or(Value::Null),
                            provider: row.get::<_, Option<String>>(3)?.unwrap_or_default(),
                            model: row.get::<_, Option<String>>(4)?.unwrap_or_default(),
                            request_id: row.get::<_, Option<String>>(1)?.unwrap_or_default(),
                            input_tokens: row.get::<_, Option<i64>>(5)?.unwrap_or_default() as u64,
                            output_tokens: row.get::<_, Option<i64>>(6)?.unwrap_or_default() as u64,
                            validation: row.get::<_, Option<String>>(7)?.unwrap_or_default(),
                        }),
                        _ => WorkState::Failed {
                            error: row.get::<_, Option<String>>(8)?.unwrap_or_default(),
                        },
                    })
                },
            )
            .optional()
            .context("read work item state")
    }

    /// Record the provider request ID *before* dispatch, so a crash between
    /// dispatch and completion leaves a reconcilable trace.
    pub fn mark_submitted(&self, work_key: &str, request_id: &str) -> Result<()> {
        self.conn.execute(
            "UPDATE work_items
             SET state = 'submitted', request_id = ?2,
                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
             WHERE work_key = ?1 AND state IN ('pending', 'submitted')",
            params![work_key, request_id],
        )?;
        Ok(())
    }

    /// Completion is idempotent and never overwrites an existing result.
    pub fn complete(&self, work_key: &str, result: &RecordedResult) -> Result<bool> {
        let changed = self.conn.execute(
            "UPDATE work_items
             SET state = 'completed', output_json = ?2, provider = ?3, model = ?4,
                 request_id = ?5, input_tokens = ?6, output_tokens = ?7,
                 validation = ?8, error = NULL,
                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
             WHERE work_key = ?1 AND state != 'completed'",
            params![
                work_key,
                serde_json::to_string(&result.output)?,
                result.provider,
                result.model,
                result.request_id,
                result.input_tokens as i64,
                result.output_tokens as i64,
                result.validation,
            ],
        )?;
        Ok(changed > 0)
    }

    pub fn mark_failed(&self, work_key: &str, error: &str) -> Result<()> {
        self.conn.execute(
            "UPDATE work_items
             SET state = 'failed', error = ?2,
                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
             WHERE work_key = ?1 AND state != 'completed'",
            params![work_key, error],
        )?;
        Ok(())
    }

    /// Work left in `submitted` after a restart: reconcile, don't re-bill.
    /// Exercised by the crash-recovery test; production use arrives with
    /// provider batch reconciliation (S2.1).
    #[allow(dead_code)]
    pub fn submitted_items(&self) -> Result<Vec<(String, String)>> {
        let mut stmt = self
            .conn
            .prepare("SELECT work_key, request_id FROM work_items WHERE state = 'submitted'")?;
        let rows = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<String>>(1)?.unwrap_or_default(),
                ))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn counts(&self) -> Result<(u64, u64, u64, u64)> {
        let count = |state: &str| -> Result<u64> {
            let n: i64 = self.conn.query_row(
                "SELECT COUNT(*) FROM work_items WHERE state = ?1",
                params![state],
                |r| r.get(0),
            )?;
            Ok(n as u64)
        };
        Ok((
            count("pending")?,
            count("submitted")?,
            count("completed")?,
            count("failed")?,
        ))
    }
}
