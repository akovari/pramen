//! SQLite (WAL) ledger — the local default backend.

use super::{LedgerStore, RecordedResult, WorkState};
use crate::error::AiError;
use crate::review::{ReviewItem, ReviewStatus};
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::Value;
use std::path::Path;

/// Local SQLite ledger in WAL mode.
///
/// Multiple handles may point at the same file; WAL mode serializes writers.
pub struct SqliteLedger {
    conn: Connection,
}

impl SqliteLedger {
    /// Open (creating if necessary) the ledger at `path`.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::Ledger`] when the database cannot be opened or
    /// migrated.
    pub fn open(path: &Path) -> Result<Self, AiError> {
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)
                .map_err(|e| AiError::Input(format!("create ledger directory: {e}")))?;
        }
        let conn = Connection::open(path)?;
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
            CREATE INDEX IF NOT EXISTS idx_work_items_state ON work_items(state);
            CREATE TABLE IF NOT EXISTS review_queue (
                work_key      TEXT PRIMARY KEY REFERENCES work_items(work_key),
                transform_id  TEXT NOT NULL,
                reason        TEXT NOT NULL,
                raw_output    TEXT,
                status        TEXT NOT NULL DEFAULT 'pending'
                              CHECK (status IN ('pending','accepted','rejected')),
                created_at    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
                decided_at    TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_review_queue_status ON review_queue(status);",
        )?;
        Ok(Self { conn })
    }
}

impl LedgerStore for SqliteLedger {
    fn upsert_pending(&self, work_key: &str, spec_json: &str) -> Result<(), AiError> {
        self.conn.execute(
            "INSERT INTO work_items (work_key, state, spec_json)
             VALUES (?1, 'pending', ?2)
             ON CONFLICT(work_key) DO NOTHING",
            params![work_key, spec_json],
        )?;
        Ok(())
    }

    fn state(&self, work_key: &str) -> Result<Option<WorkState>, AiError> {
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
                        "completed" => {
                            let output_json = row.get::<_, Option<String>>(2)?.unwrap_or_default();
                            WorkState::Completed(RecordedResult {
                                output: serde_json::from_str(&output_json).unwrap_or(Value::Null),
                                provider: row.get::<_, Option<String>>(3)?.unwrap_or_default(),
                                model: row.get::<_, Option<String>>(4)?.unwrap_or_default(),
                                request_id: row.get::<_, Option<String>>(1)?.unwrap_or_default(),
                                input_tokens: row.get::<_, Option<i64>>(5)?.unwrap_or_default()
                                    as u64,
                                output_tokens: row.get::<_, Option<i64>>(6)?.unwrap_or_default()
                                    as u64,
                                validation: row.get::<_, Option<String>>(7)?.unwrap_or_default(),
                            })
                        }
                        _ => WorkState::Failed {
                            error: row.get::<_, Option<String>>(8)?.unwrap_or_default(),
                        },
                    })
                },
            )
            .optional()
            .map_err(AiError::from)
    }

    fn mark_submitted(&self, work_key: &str, request_id: &str) -> Result<(), AiError> {
        self.conn.execute(
            "UPDATE work_items
             SET state = 'submitted', request_id = ?2,
                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
             WHERE work_key = ?1 AND state IN ('pending', 'submitted', 'failed')",
            params![work_key, request_id],
        )?;
        Ok(())
    }

    fn complete(&self, work_key: &str, result: &RecordedResult) -> Result<bool, AiError> {
        let output_json = result.output.to_string();
        let changed = self.conn.execute(
            "UPDATE work_items
             SET state = 'completed', output_json = ?2, provider = ?3, model = ?4,
                 request_id = ?5, input_tokens = ?6, output_tokens = ?7,
                 validation = ?8, error = NULL,
                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
             WHERE work_key = ?1 AND state != 'completed'",
            params![
                work_key,
                output_json,
                result.provider,
                result.model,
                result.request_id,
                i64::try_from(result.input_tokens).unwrap_or(i64::MAX),
                i64::try_from(result.output_tokens).unwrap_or(i64::MAX),
                result.validation,
            ],
        )?;
        Ok(changed > 0)
    }

    fn mark_failed(&self, work_key: &str, error: &str) -> Result<(), AiError> {
        self.conn.execute(
            "UPDATE work_items
             SET state = 'failed', error = ?2,
                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
             WHERE work_key = ?1 AND state != 'completed'",
            params![work_key, error],
        )?;
        Ok(())
    }

    fn submitted_items(&self) -> Result<Vec<(String, String)>, AiError> {
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
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    fn counts(&self) -> Result<(u64, u64, u64, u64), AiError> {
        let count = |state: &str| -> Result<u64, AiError> {
            let n: i64 = self.conn.query_row(
                "SELECT COUNT(*) FROM work_items WHERE state = ?1",
                params![state],
                |r| r.get(0),
            )?;
            Ok(u64::try_from(n).unwrap_or_default())
        };
        Ok((
            count("pending")?,
            count("submitted")?,
            count("completed")?,
            count("failed")?,
        ))
    }

    fn enqueue_review(
        &self,
        work_key: &str,
        transform_id: &str,
        reason: &str,
        raw_output: Option<&str>,
    ) -> Result<(), AiError> {
        self.conn.execute(
            "INSERT INTO review_queue (work_key, transform_id, reason, raw_output)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(work_key) DO NOTHING",
            params![work_key, transform_id, reason, raw_output],
        )?;
        Ok(())
    }

    fn review_status(&self, work_key: &str) -> Result<Option<ReviewStatus>, AiError> {
        self.conn
            .query_row(
                "SELECT status FROM review_queue WHERE work_key = ?1",
                params![work_key],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map(|status| status.map(|s| ReviewStatus::parse(&s)))
            .map_err(AiError::from)
    }

    fn pending_reviews(&self) -> Result<Vec<ReviewItem>, AiError> {
        let mut stmt = self.conn.prepare(
            "SELECT r.work_key, r.transform_id, r.reason, r.raw_output, r.status,
                    r.created_at, w.spec_json
             FROM review_queue r JOIN work_items w ON w.work_key = r.work_key
             WHERE r.status = 'pending'
             ORDER BY r.created_at, r.work_key",
        )?;
        let rows = stmt
            .query_map([], |row| {
                Ok(ReviewItem {
                    work_key: row.get(0)?,
                    transform_id: row.get(1)?,
                    reason: row.get(2)?,
                    raw_output: row.get(3)?,
                    status: ReviewStatus::parse(&row.get::<_, String>(4)?),
                    created_at: row.get(5)?,
                    spec: serde_json::from_str(&row.get::<_, String>(6)?).unwrap_or(Value::Null),
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    fn decide_review(&self, work_key: &str, status: ReviewStatus) -> Result<(), AiError> {
        self.conn.execute(
            "UPDATE review_queue
             SET status = ?2, decided_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
             WHERE work_key = ?1 AND status = 'pending'",
            params![work_key, status.as_str()],
        )?;
        Ok(())
    }

    fn review_counts(&self) -> Result<(u64, u64, u64), AiError> {
        let count = |status: &str| -> Result<u64, AiError> {
            let n: i64 = self.conn.query_row(
                "SELECT COUNT(*) FROM review_queue WHERE status = ?1",
                params![status],
                |r| r.get(0),
            )?;
            Ok(u64::try_from(n).unwrap_or_default())
        };
        Ok((count("pending")?, count("accepted")?, count("rejected")?))
    }
}
