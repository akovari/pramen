//! Shared Postgres ledger for fleet deployments (X1.8).
//!
//! Tables are prefixed `pramen_*` in the connection's default schema and
//! mirror the SQLite `work_items` / `review_queue` semantics: completed
//! results are immutable, `submitted` carries a request id for
//! reconciliation, and review rows join to work specs.

use super::{LedgerStore, RecordedResult, WorkState};
use crate::error::AiError;
use crate::review::{ReviewItem, ReviewStatus};
use serde_json::Value;
use tokio::sync::Mutex;

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS pramen_work_items (
    work_key      TEXT PRIMARY KEY,
    state         TEXT NOT NULL CHECK (state IN ('pending','submitted','completed','failed')),
    spec_json     TEXT NOT NULL,
    request_id    TEXT,
    output_json   TEXT,
    provider      TEXT,
    model         TEXT,
    input_tokens  BIGINT,
    output_tokens BIGINT,
    validation    TEXT,
    error         TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_pramen_work_items_state ON pramen_work_items(state);
CREATE TABLE IF NOT EXISTS pramen_review_queue (
    work_key      TEXT PRIMARY KEY REFERENCES pramen_work_items(work_key),
    transform_id  TEXT NOT NULL,
    reason        TEXT NOT NULL,
    raw_output    TEXT,
    status        TEXT NOT NULL DEFAULT 'pending'
                  CHECK (status IN ('pending','accepted','rejected')),
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    decided_at    TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_pramen_review_queue_status ON pramen_review_queue(status);
";

/// Shared Postgres-backed inference ledger.
pub struct PostgresLedger {
    client: Mutex<tokio_postgres::Client>,
    handle: tokio::runtime::Handle,
    _runtime: Option<std::sync::Arc<tokio::runtime::Runtime>>,
}

impl PostgresLedger {
    /// Connect and bootstrap schema on the current Tokio runtime.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::Ledger`] on connection or migration failure.
    pub async fn connect(dsn: &str) -> Result<Self, AiError> {
        let (client, connection) = tokio_postgres::connect(dsn, tokio_postgres::NoTls).await?;
        tokio::spawn(async move {
            if let Err(error) = connection.await {
                tracing::error!(%error, "postgres ledger connection closed");
            }
        });
        client.batch_execute(SCHEMA).await?;
        Ok(Self {
            client: Mutex::new(client),
            handle: tokio::runtime::Handle::current(),
            _runtime: None,
        })
    }

    /// Connect from a synchronous context, retaining a private runtime so the
    /// connection driver task stays alive for the life of this handle.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::Ledger`] or [`AiError::Input`] on failure.
    pub fn open(dsn: &str) -> Result<Self, AiError> {
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            return tokio::task::block_in_place(|| handle.block_on(Self::connect(dsn)));
        }
        let runtime = std::sync::Arc::new(
            tokio::runtime::Runtime::new()
                .map_err(|error| AiError::Input(format!("create ledger runtime: {error}")))?,
        );
        let handle = runtime.handle().clone();
        let client = handle.block_on(async {
            let (client, connection) = tokio_postgres::connect(dsn, tokio_postgres::NoTls).await?;
            handle.spawn(async move {
                if let Err(error) = connection.await {
                    tracing::error!(%error, "postgres ledger connection closed");
                }
            });
            client.batch_execute(SCHEMA).await?;
            Ok::<_, AiError>(client)
        })?;
        Ok(Self {
            client: Mutex::new(client),
            handle,
            _runtime: Some(runtime),
        })
    }

    fn block_on<T>(&self, future: impl std::future::Future<Output = T>) -> T {
        let handle = self.handle.clone();
        if tokio::runtime::Handle::try_current().is_ok() {
            tokio::task::block_in_place(|| handle.block_on(future))
        } else {
            handle.block_on(future)
        }
    }

    fn row_to_state(row: &tokio_postgres::Row) -> WorkState {
        let state: &str = row.get(0);
        let request_id: Option<String> = row.get(1);
        match state {
            "pending" => WorkState::Pending,
            "submitted" => WorkState::Submitted {
                request_id: request_id.unwrap_or_default(),
            },
            "completed" => {
                let output_json: Option<String> = row.get(2);
                WorkState::Completed(RecordedResult {
                    output: output_json
                        .as_deref()
                        .and_then(|s| serde_json::from_str(s).ok())
                        .unwrap_or(Value::Null),
                    provider: row.get::<_, Option<String>>(3).unwrap_or_default(),
                    model: row.get::<_, Option<String>>(4).unwrap_or_default(),
                    request_id: request_id.unwrap_or_default(),
                    input_tokens: u64::try_from(row.get::<_, Option<i64>>(5).unwrap_or_default())
                        .unwrap_or_default(),
                    output_tokens: u64::try_from(row.get::<_, Option<i64>>(6).unwrap_or_default())
                        .unwrap_or_default(),
                    validation: row.get::<_, Option<String>>(7).unwrap_or_default(),
                })
            }
            _ => WorkState::Failed {
                error: row.get::<_, Option<String>>(8).unwrap_or_default(),
            },
        }
    }
}

impl LedgerStore for PostgresLedger {
    fn upsert_pending(&self, work_key: &str, spec_json: &str) -> Result<(), AiError> {
        let work_key = work_key.to_owned();
        let spec_json = spec_json.to_owned();
        self.block_on(async {
            let client = self.client.lock().await;
            client
                .execute(
                    "INSERT INTO pramen_work_items (work_key, state, spec_json)
                     VALUES ($1, 'pending', $2)
                     ON CONFLICT (work_key) DO NOTHING",
                    &[&work_key, &spec_json],
                )
                .await?;
            Ok(())
        })
    }

    fn state(&self, work_key: &str) -> Result<Option<WorkState>, AiError> {
        let work_key = work_key.to_owned();
        self.block_on(async {
            let client = self.client.lock().await;
            let row = client
                .query_opt(
                    "SELECT state, request_id, output_json, provider, model,
                            input_tokens, output_tokens, validation, error
                     FROM pramen_work_items WHERE work_key = $1",
                    &[&work_key],
                )
                .await?;
            Ok(row.map(|row| Self::row_to_state(&row)))
        })
    }

    fn mark_submitted(&self, work_key: &str, request_id: &str) -> Result<(), AiError> {
        let work_key = work_key.to_owned();
        let request_id = request_id.to_owned();
        self.block_on(async {
            let client = self.client.lock().await;
            client
                .execute(
                    "UPDATE pramen_work_items
                     SET state = 'submitted', request_id = $2, updated_at = now()
                     WHERE work_key = $1 AND state IN ('pending', 'submitted', 'failed')",
                    &[&work_key, &request_id],
                )
                .await?;
            Ok(())
        })
    }

    fn complete(&self, work_key: &str, result: &RecordedResult) -> Result<bool, AiError> {
        let work_key = work_key.to_owned();
        let output_json = result.output.to_string();
        let provider = result.provider.clone();
        let model = result.model.clone();
        let request_id = result.request_id.clone();
        let input_tokens = i64::try_from(result.input_tokens).unwrap_or(i64::MAX);
        let output_tokens = i64::try_from(result.output_tokens).unwrap_or(i64::MAX);
        let validation = result.validation.clone();
        self.block_on(async {
            let client = self.client.lock().await;
            let changed = client
                .execute(
                    "UPDATE pramen_work_items
                     SET state = 'completed', output_json = $2, provider = $3, model = $4,
                         request_id = $5, input_tokens = $6, output_tokens = $7,
                         validation = $8, error = NULL, updated_at = now()
                     WHERE work_key = $1 AND state <> 'completed'",
                    &[
                        &work_key,
                        &output_json,
                        &provider,
                        &model,
                        &request_id,
                        &input_tokens,
                        &output_tokens,
                        &validation,
                    ],
                )
                .await?;
            Ok(changed > 0)
        })
    }

    fn mark_failed(&self, work_key: &str, error: &str) -> Result<(), AiError> {
        let work_key = work_key.to_owned();
        let error = error.to_owned();
        self.block_on(async {
            let client = self.client.lock().await;
            client
                .execute(
                    "UPDATE pramen_work_items
                     SET state = 'failed', error = $2, updated_at = now()
                     WHERE work_key = $1 AND state <> 'completed'",
                    &[&work_key, &error],
                )
                .await?;
            Ok(())
        })
    }

    fn submitted_items(&self) -> Result<Vec<(String, String)>, AiError> {
        self.block_on(async {
            let client = self.client.lock().await;
            let rows = client
                .query(
                    "SELECT work_key, request_id FROM pramen_work_items WHERE state = 'submitted'",
                    &[],
                )
                .await?;
            Ok(rows
                .into_iter()
                .map(|row| {
                    (
                        row.get::<_, String>(0),
                        row.get::<_, Option<String>>(1).unwrap_or_default(),
                    )
                })
                .collect())
        })
    }

    fn counts(&self) -> Result<(u64, u64, u64, u64), AiError> {
        self.block_on(async {
            let client = self.client.lock().await;
            let rows = client
                .query(
                    "SELECT state, COUNT(*)::bigint FROM pramen_work_items GROUP BY state",
                    &[],
                )
                .await?;
            let mut pending = 0u64;
            let mut submitted = 0u64;
            let mut completed = 0u64;
            let mut failed = 0u64;
            for row in rows {
                let state: &str = row.get(0);
                let n = u64::try_from(row.get::<_, i64>(1)).unwrap_or_default();
                match state {
                    "pending" => pending = n,
                    "submitted" => submitted = n,
                    "completed" => completed = n,
                    "failed" => failed = n,
                    _ => {}
                }
            }
            Ok((pending, submitted, completed, failed))
        })
    }

    fn enqueue_review(
        &self,
        work_key: &str,
        transform_id: &str,
        reason: &str,
        raw_output: Option<&str>,
    ) -> Result<(), AiError> {
        let work_key = work_key.to_owned();
        let transform_id = transform_id.to_owned();
        let reason = reason.to_owned();
        let raw_output = raw_output.map(str::to_owned);
        self.block_on(async {
            let client = self.client.lock().await;
            client
                .execute(
                    "INSERT INTO pramen_review_queue (work_key, transform_id, reason, raw_output)
                     VALUES ($1, $2, $3, $4)
                     ON CONFLICT (work_key) DO NOTHING",
                    &[&work_key, &transform_id, &reason, &raw_output],
                )
                .await?;
            Ok(())
        })
    }

    fn review_status(&self, work_key: &str) -> Result<Option<ReviewStatus>, AiError> {
        let work_key = work_key.to_owned();
        self.block_on(async {
            let client = self.client.lock().await;
            let row = client
                .query_opt(
                    "SELECT status FROM pramen_review_queue WHERE work_key = $1",
                    &[&work_key],
                )
                .await?;
            Ok(row.map(|row| ReviewStatus::parse(row.get::<_, &str>(0))))
        })
    }

    fn pending_reviews(&self) -> Result<Vec<ReviewItem>, AiError> {
        self.block_on(async {
            let client = self.client.lock().await;
            let rows = client
                .query(
                    "SELECT r.work_key, r.transform_id, r.reason, r.raw_output, r.status,
                            r.created_at::text, w.spec_json
                     FROM pramen_review_queue r
                     JOIN pramen_work_items w ON w.work_key = r.work_key
                     WHERE r.status = 'pending'
                     ORDER BY r.created_at, r.work_key",
                    &[],
                )
                .await?;
            Ok(rows
                .into_iter()
                .map(|row| ReviewItem {
                    work_key: row.get(0),
                    transform_id: row.get(1),
                    reason: row.get(2),
                    raw_output: row.get(3),
                    status: ReviewStatus::parse(row.get::<_, &str>(4)),
                    created_at: row.get(5),
                    spec: serde_json::from_str(row.get::<_, &str>(6)).unwrap_or(Value::Null),
                })
                .collect())
        })
    }

    fn decide_review(&self, work_key: &str, status: ReviewStatus) -> Result<(), AiError> {
        let work_key = work_key.to_owned();
        let status = status.as_str();
        self.block_on(async {
            let client = self.client.lock().await;
            client
                .execute(
                    "UPDATE pramen_review_queue
                     SET status = $2, decided_at = now()
                     WHERE work_key = $1 AND status = 'pending'",
                    &[&work_key, &status],
                )
                .await?;
            Ok(())
        })
    }

    fn review_counts(&self) -> Result<(u64, u64, u64), AiError> {
        self.block_on(async {
            let client = self.client.lock().await;
            let rows = client
                .query(
                    "SELECT status, COUNT(*)::bigint FROM pramen_review_queue GROUP BY status",
                    &[],
                )
                .await?;
            let mut pending = 0u64;
            let mut accepted = 0u64;
            let mut rejected = 0u64;
            for row in rows {
                let status: &str = row.get(0);
                let n = u64::try_from(row.get::<_, i64>(1)).unwrap_or_default();
                match status {
                    "pending" => pending = n,
                    "accepted" => accepted = n,
                    "rejected" => rejected = n,
                    _ => {}
                }
            }
            Ok((pending, accepted, rejected))
        })
    }
}
