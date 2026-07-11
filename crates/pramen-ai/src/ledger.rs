//! The durable inference ledger on SQLite (WAL mode).
//!
//! State machine per work item, keyed by [work key](crate::workkey):
//!
//! ```text
//! pending ──► submitted ──► completed   (immutable once reached)
//!    │             │
//!    └─────────────┴──────► failed      (retryable)
//! ```
//!
//! Completed results are recorded durably *before* they are used and are
//! never overwritten, so a crash or replay reuses them at zero cost. The
//! `submitted` state carries the provider request ID so a restart can
//! reconcile in-flight provider-batch work instead of re-billing it.
//!
//! Validated in spike S1.1: zero results lost across `kill -9`, 100% reuse
//! on replay, 27–312 µs per item.

use crate::error::AiError;
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::Value;
use std::path::Path;

/// The lifecycle state of one work item.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkState {
    /// Registered, not yet sent to a provider.
    Pending,
    /// Sent to a provider; the request ID enables reconciliation.
    Submitted {
        /// Provider-issued (or locally generated) request identifier.
        request_id: String,
    },
    /// Validated output recorded with provenance. Immutable.
    Completed(RecordedResult),
    /// The last attempt failed; retryable.
    Failed {
        /// Human-readable failure description.
        error: String,
    },
}

/// The immutable, validated output of a completed work item, with the
/// provenance needed for audit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordedResult {
    /// The validated model output.
    pub output: Value,
    /// Provider adapter that produced it.
    pub provider: String,
    /// Model identifier that produced it.
    pub model: String,
    /// Provider request identifier.
    pub request_id: String,
    /// Input tokens billed.
    pub input_tokens: u64,
    /// Output tokens billed.
    pub output_tokens: u64,
    /// Validation verdict recorded at completion time.
    pub validation: String,
}

/// A handle to the ledger database.
///
/// Multiple handles (from multiple operators or processes) may point at
/// the same file; WAL mode serializes writers safely.
pub struct Ledger {
    conn: Connection,
}

impl Ledger {
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
            CREATE INDEX IF NOT EXISTS idx_work_items_state ON work_items(state);",
        )?;
        Ok(Self { conn })
    }

    /// Register work if unknown. A known item — in any state — is left
    /// untouched; completed results are never reset.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::Ledger`] on database failure.
    pub fn upsert_pending(&self, work_key: &str, spec_json: &str) -> Result<(), AiError> {
        self.conn.execute(
            "INSERT INTO work_items (work_key, state, spec_json)
             VALUES (?1, 'pending', ?2)
             ON CONFLICT(work_key) DO NOTHING",
            params![work_key, spec_json],
        )?;
        Ok(())
    }

    /// The current state of a work item, or `None` if unknown.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::Ledger`] on database failure.
    pub fn state(&self, work_key: &str) -> Result<Option<WorkState>, AiError> {
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

    /// Record the request ID *before* dispatch, so a crash between dispatch
    /// and completion leaves a reconcilable trace instead of a silent
    /// double-bill.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::Ledger`] on database failure.
    pub fn mark_submitted(&self, work_key: &str, request_id: &str) -> Result<(), AiError> {
        self.conn.execute(
            "UPDATE work_items
             SET state = 'submitted', request_id = ?2,
                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
             WHERE work_key = ?1 AND state IN ('pending', 'submitted', 'failed')",
            params![work_key, request_id],
        )?;
        Ok(())
    }

    /// Record a validated result. Idempotent: an existing completed result
    /// is never overwritten. Returns whether this call recorded the result.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::Ledger`] on database failure.
    pub fn complete(&self, work_key: &str, result: &RecordedResult) -> Result<bool, AiError> {
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

    /// Record a failure. Never demotes a completed item.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::Ledger`] on database failure.
    pub fn mark_failed(&self, work_key: &str, error: &str) -> Result<(), AiError> {
        self.conn.execute(
            "UPDATE work_items
             SET state = 'failed', error = ?2,
                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
             WHERE work_key = ?1 AND state != 'completed'",
            params![work_key, error],
        )?;
        Ok(())
    }

    /// Work left in `submitted` after a restart, for reconciliation
    /// (provider-batch reconciliation lands with P1.8).
    ///
    /// # Errors
    ///
    /// Returns [`AiError::Ledger`] on database failure.
    pub fn submitted_items(&self) -> Result<Vec<(String, String)>, AiError> {
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

    /// Item counts by state: `(pending, submitted, completed, failed)`.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::Ledger`] on database failure.
    pub fn counts(&self) -> Result<(u64, u64, u64, u64), AiError> {
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn temp_ledger(name: &str) -> (std::path::PathBuf, Ledger) {
        let path = std::env::temp_dir().join(format!(
            "pramen-ledger-test-{}-{name}.sqlite",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        let ledger = Ledger::open(&path).unwrap();
        (path, ledger)
    }

    fn result() -> RecordedResult {
        RecordedResult {
            output: json!({"category": "billing"}),
            provider: "mock".into(),
            model: "mock-1".into(),
            request_id: "req-1".into(),
            input_tokens: 10,
            output_tokens: 5,
            validation: "passed".into(),
        }
    }

    #[test]
    fn lifecycle_and_reuse() {
        let (path, ledger) = temp_ledger("lifecycle");
        ledger.upsert_pending("k1", "{}").unwrap();
        assert_eq!(ledger.state("k1").unwrap(), Some(WorkState::Pending));

        ledger.mark_submitted("k1", "req-1").unwrap();
        assert!(matches!(
            ledger.state("k1").unwrap(),
            Some(WorkState::Submitted { .. })
        ));

        assert!(ledger.complete("k1", &result()).unwrap());
        let Some(WorkState::Completed(recorded)) = ledger.state("k1").unwrap() else {
            panic!("expected completed");
        };
        assert_eq!(recorded.output, json!({"category": "billing"}));
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn completed_results_are_immutable() {
        let (path, ledger) = temp_ledger("immutable");
        ledger.upsert_pending("k1", "{}").unwrap();
        assert!(ledger.complete("k1", &result()).unwrap());

        // A second completion attempt records nothing.
        let mut other = result();
        other.output = json!({"category": "technical"});
        assert!(!ledger.complete("k1", &other).unwrap());

        // Failure never demotes completion.
        ledger.mark_failed("k1", "should not stick").unwrap();
        let Some(WorkState::Completed(recorded)) = ledger.state("k1").unwrap() else {
            panic!("expected completed");
        };
        assert_eq!(recorded.output, json!({"category": "billing"}));

        // Re-registering is a no-op.
        ledger.upsert_pending("k1", "{}").unwrap();
        assert!(matches!(
            ledger.state("k1").unwrap(),
            Some(WorkState::Completed(_))
        ));
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn results_survive_reopen() {
        let (path, ledger) = temp_ledger("reopen");
        ledger.upsert_pending("k1", "{}").unwrap();
        ledger.complete("k1", &result()).unwrap();
        ledger.upsert_pending("k2", "{}").unwrap();
        ledger.mark_submitted("k2", "req-2").unwrap();
        drop(ledger);

        // Simulates a crash + restart: a fresh handle sees the same truth.
        let reopened = Ledger::open(&path).unwrap();
        assert!(matches!(
            reopened.state("k1").unwrap(),
            Some(WorkState::Completed(_))
        ));
        let submitted = reopened.submitted_items().unwrap();
        assert_eq!(submitted, vec![("k2".to_owned(), "req-2".to_owned())]);
        assert_eq!(reopened.counts().unwrap(), (0, 1, 1, 0));
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn failed_work_is_retryable() {
        let (path, ledger) = temp_ledger("retry");
        ledger.upsert_pending("k1", "{}").unwrap();
        ledger.mark_failed("k1", "throttled").unwrap();
        assert_eq!(
            ledger.state("k1").unwrap(),
            Some(WorkState::Failed {
                error: "throttled".into()
            })
        );
        // A retry moves it back through submitted to completed.
        ledger.mark_submitted("k1", "req-retry").unwrap();
        assert!(ledger.complete("k1", &result()).unwrap());
        let _ = std::fs::remove_file(path);
    }
}
