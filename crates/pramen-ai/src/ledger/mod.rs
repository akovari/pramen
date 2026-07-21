//! The durable inference ledger.
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
//! Backends:
//! - [`SqliteLedger`] — local default (WAL mode); selected by a filesystem
//!   path in `PRAMEN_LEDGER_PATH` (default `.pramen/ledger.sqlite`).
//! - [`PostgresLedger`] — shared fleet store (X1.8); selected when the
//!   location is a `postgres://` or `postgresql://` URL.
//!
//! The [`LedgerStore`] trait is the seam ADR 0003 fixed; [`Ledger`] is the
//! runtime facade used by the operator and CLI.

mod postgres;
mod sqlite;

pub use postgres::PostgresLedger;
pub use sqlite::SqliteLedger;

use crate::error::AiError;
use crate::review::{ReviewItem, ReviewStatus};
use pramen_core::checkpoint::is_postgres_url;
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

/// Backend-agnostic ledger operations used by the operator and review flow.
pub trait LedgerStore: Send {
    /// Register work if unknown. Known items (any state) are left untouched.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::Ledger`] on database failure.
    fn upsert_pending(&self, work_key: &str, spec_json: &str) -> Result<(), AiError>;

    /// The current state of a work item, or `None` if unknown.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::Ledger`] on database failure.
    fn state(&self, work_key: &str) -> Result<Option<WorkState>, AiError>;

    /// Record the request ID before dispatch for crash reconciliation.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::Ledger`] on database failure.
    fn mark_submitted(&self, work_key: &str, request_id: &str) -> Result<(), AiError>;

    /// Record a validated result. Idempotent: never overwrites completed.
    /// Returns whether this call recorded the result.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::Ledger`] on database failure.
    fn complete(&self, work_key: &str, result: &RecordedResult) -> Result<bool, AiError>;

    /// Record a failure. Never demotes a completed item.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::Ledger`] on database failure.
    fn mark_failed(&self, work_key: &str, error: &str) -> Result<(), AiError>;

    /// Work left in `submitted` after a restart, for reconciliation.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::Ledger`] on database failure.
    fn submitted_items(&self) -> Result<Vec<(String, String)>, AiError>;

    /// Item counts by state: `(pending, submitted, completed, failed)`.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::Ledger`] on database failure.
    fn counts(&self) -> Result<(u64, u64, u64, u64), AiError>;

    /// Durably route one record to review. Idempotent per work key.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::Ledger`] on database failure.
    fn enqueue_review(
        &self,
        work_key: &str,
        transform_id: &str,
        reason: &str,
        raw_output: Option<&str>,
    ) -> Result<(), AiError>;

    /// The decision state of a work key, or `None` when never routed.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::Ledger`] on database failure.
    fn review_status(&self, work_key: &str) -> Result<Option<ReviewStatus>, AiError>;

    /// Queued items awaiting a decision, oldest first.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::Ledger`] on database failure.
    fn pending_reviews(&self) -> Result<Vec<ReviewItem>, AiError>;

    /// Mark a pending review item decided.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::Ledger`] on database failure.
    fn decide_review(&self, work_key: &str, status: ReviewStatus) -> Result<(), AiError>;

    /// Queue counts: `(pending, accepted, rejected)`.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::Ledger`] on database failure.
    fn review_counts(&self) -> Result<(u64, u64, u64), AiError>;
}

/// Runtime facade over the SQLite or Postgres ledger backend.
pub enum Ledger {
    /// Local SQLite (WAL) ledger.
    Sqlite(SqliteLedger),
    /// Shared Postgres ledger.
    Postgres(PostgresLedger),
}

impl Ledger {
    /// Open (creating if necessary) a SQLite ledger at `path`.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::Ledger`] when the database cannot be opened or
    /// migrated.
    pub fn open(path: &Path) -> Result<Self, AiError> {
        Ok(Self::Sqlite(SqliteLedger::open(path)?))
    }

    /// Open a shared Postgres ledger at `dsn` (async; uses the current
    /// Tokio runtime).
    ///
    /// # Errors
    ///
    /// Returns [`AiError::Ledger`] on connection or migration failure.
    pub async fn open_postgres(dsn: &str) -> Result<Self, AiError> {
        Ok(Self::Postgres(PostgresLedger::connect(dsn).await?))
    }

    /// Open from a location string: `postgres://` / `postgresql://` selects
    /// the shared backend; anything else is a filesystem path for SQLite.
    ///
    /// When a Postgres URL is used outside a Tokio runtime, a private
    /// runtime is retained so the connection driver stays alive.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::Ledger`] or [`AiError::Input`] on open failure.
    pub fn open_location(location: &str) -> Result<Self, AiError> {
        if is_postgres_url(location) {
            Ok(Self::Postgres(PostgresLedger::open(location)?))
        } else {
            Self::open(Path::new(location))
        }
    }
}

macro_rules! ledger_dispatch {
    ($self:expr, $method:ident ( $($arg:expr),* $(,)? )) => {
        match $self {
            Ledger::Sqlite(inner) => inner.$method($($arg),*),
            Ledger::Postgres(inner) => inner.$method($($arg),*),
        }
    };
}

impl LedgerStore for Ledger {
    fn upsert_pending(&self, work_key: &str, spec_json: &str) -> Result<(), AiError> {
        ledger_dispatch!(self, upsert_pending(work_key, spec_json))
    }

    fn state(&self, work_key: &str) -> Result<Option<WorkState>, AiError> {
        ledger_dispatch!(self, state(work_key))
    }

    fn mark_submitted(&self, work_key: &str, request_id: &str) -> Result<(), AiError> {
        ledger_dispatch!(self, mark_submitted(work_key, request_id))
    }

    fn complete(&self, work_key: &str, result: &RecordedResult) -> Result<bool, AiError> {
        ledger_dispatch!(self, complete(work_key, result))
    }

    fn mark_failed(&self, work_key: &str, error: &str) -> Result<(), AiError> {
        ledger_dispatch!(self, mark_failed(work_key, error))
    }

    fn submitted_items(&self) -> Result<Vec<(String, String)>, AiError> {
        ledger_dispatch!(self, submitted_items())
    }

    fn counts(&self) -> Result<(u64, u64, u64, u64), AiError> {
        ledger_dispatch!(self, counts())
    }

    fn enqueue_review(
        &self,
        work_key: &str,
        transform_id: &str,
        reason: &str,
        raw_output: Option<&str>,
    ) -> Result<(), AiError> {
        ledger_dispatch!(
            self,
            enqueue_review(work_key, transform_id, reason, raw_output)
        )
    }

    fn review_status(&self, work_key: &str) -> Result<Option<ReviewStatus>, AiError> {
        ledger_dispatch!(self, review_status(work_key))
    }

    fn pending_reviews(&self) -> Result<Vec<ReviewItem>, AiError> {
        ledger_dispatch!(self, pending_reviews())
    }

    fn decide_review(&self, work_key: &str, status: ReviewStatus) -> Result<(), AiError> {
        ledger_dispatch!(self, decide_review(work_key, status))
    }

    fn review_counts(&self) -> Result<(u64, u64, u64), AiError> {
        ledger_dispatch!(self, review_counts())
    }
}

impl Ledger {
    /// Register work if unknown. A known item — in any state — is left
    /// untouched; completed results are never reset.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::Ledger`] on database failure.
    pub fn upsert_pending(&self, work_key: &str, spec_json: &str) -> Result<(), AiError> {
        LedgerStore::upsert_pending(self, work_key, spec_json)
    }

    /// The current state of a work item, or `None` if unknown.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::Ledger`] on database failure.
    pub fn state(&self, work_key: &str) -> Result<Option<WorkState>, AiError> {
        LedgerStore::state(self, work_key)
    }

    /// Record the request ID *before* dispatch, so a crash between dispatch
    /// and completion leaves a reconcilable trace instead of a silent
    /// double-bill.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::Ledger`] on database failure.
    pub fn mark_submitted(&self, work_key: &str, request_id: &str) -> Result<(), AiError> {
        LedgerStore::mark_submitted(self, work_key, request_id)
    }

    /// Record a validated result. Idempotent: an existing completed result
    /// is never overwritten. Returns whether this call recorded the result.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::Ledger`] on database failure.
    pub fn complete(&self, work_key: &str, result: &RecordedResult) -> Result<bool, AiError> {
        LedgerStore::complete(self, work_key, result)
    }

    /// Record a failure. Never demotes a completed item.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::Ledger`] on database failure.
    pub fn mark_failed(&self, work_key: &str, error: &str) -> Result<(), AiError> {
        LedgerStore::mark_failed(self, work_key, error)
    }

    /// Work left in `submitted` after a restart, for reconciliation
    /// (provider-batch reconciliation lands with P1.8).
    ///
    /// # Errors
    ///
    /// Returns [`AiError::Ledger`] on database failure.
    pub fn submitted_items(&self) -> Result<Vec<(String, String)>, AiError> {
        LedgerStore::submitted_items(self)
    }

    /// Item counts by state: `(pending, submitted, completed, failed)`.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::Ledger`] on database failure.
    pub fn counts(&self) -> Result<(u64, u64, u64, u64), AiError> {
        LedgerStore::counts(self)
    }

    /// Durably route one record to review. Idempotent per work key.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::Ledger`] on database failure.
    pub fn enqueue_review(
        &self,
        work_key: &str,
        transform_id: &str,
        reason: &str,
        raw_output: Option<&str>,
    ) -> Result<(), AiError> {
        LedgerStore::enqueue_review(self, work_key, transform_id, reason, raw_output)
    }

    /// The decision state of a work key, or `None` when never routed.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::Ledger`] on database failure.
    pub fn review_status(&self, work_key: &str) -> Result<Option<ReviewStatus>, AiError> {
        LedgerStore::review_status(self, work_key)
    }

    /// Queued items awaiting a decision, oldest first.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::Ledger`] on database failure.
    pub fn pending_reviews(&self) -> Result<Vec<ReviewItem>, AiError> {
        LedgerStore::pending_reviews(self)
    }

    /// Mark a pending review item decided.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::Ledger`] on database failure.
    pub(crate) fn decide_review(
        &self,
        work_key: &str,
        status: ReviewStatus,
    ) -> Result<(), AiError> {
        LedgerStore::decide_review(self, work_key, status)
    }

    /// Queue counts: `(pending, accepted, rejected)`.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::Ledger`] on database failure.
    pub fn review_counts(&self) -> Result<(u64, u64, u64), AiError> {
        LedgerStore::review_counts(self)
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

        let mut other = result();
        other.output = json!({"category": "technical"});
        assert!(!ledger.complete("k1", &other).unwrap());

        ledger.mark_failed("k1", "should not stick").unwrap();
        let Some(WorkState::Completed(recorded)) = ledger.state("k1").unwrap() else {
            panic!("expected completed");
        };
        assert_eq!(recorded.output, json!({"category": "billing"}));

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
        ledger.mark_submitted("k1", "req-retry").unwrap();
        assert!(ledger.complete("k1", &result()).unwrap());
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn open_location_selects_sqlite_path() {
        let path = std::env::temp_dir().join(format!(
            "pramen-ledger-loc-{}-.sqlite",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        let ledger = Ledger::open_location(path.to_str().unwrap()).unwrap();
        assert!(matches!(ledger, Ledger::Sqlite(_)));
        let _ = std::fs::remove_file(path);
    }

    /// L2: complete + reuse + immutability on real PostgreSQL when
    /// `PRAMEN_TEST_POSTGRES_DSN` is set (ADR 0005).
    #[tokio::test(flavor = "multi_thread")]
    async fn postgres_complete_reuse_and_immutability() {
        let Some(dsn) = pramen_testkit::env::postgres_dsn() else {
            return;
        };
        let (setup, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
            .await
            .unwrap();
        tokio::spawn(connection);
        setup
            .batch_execute(
                "DROP TABLE IF EXISTS pramen_review_queue CASCADE;
                 DROP TABLE IF EXISTS pramen_work_items CASCADE;",
            )
            .await
            .unwrap();

        let ledger = Ledger::open_postgres(&dsn).await.unwrap();
        ledger.upsert_pending("k1", "{}").unwrap();
        assert!(ledger.complete("k1", &result()).unwrap());

        let mut other = result();
        other.output = json!({"category": "technical"});
        assert!(!ledger.complete("k1", &other).unwrap());
        ledger.mark_failed("k1", "should not stick").unwrap();

        let Some(WorkState::Completed(recorded)) = ledger.state("k1").unwrap() else {
            panic!("expected completed");
        };
        assert_eq!(recorded.output, json!({"category": "billing"}));

        drop(ledger);
        let reopened = Ledger::open_postgres(&dsn).await.unwrap();
        assert!(matches!(
            reopened.state("k1").unwrap(),
            Some(WorkState::Completed(_))
        ));
        assert_eq!(reopened.counts().unwrap(), (0, 0, 1, 0));
    }
}
