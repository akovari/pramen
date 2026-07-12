//! The review queue (X1.6): durable routing for records whose model
//! output failed validation under `onInvalid: review`, and re-ingestion
//! of human decisions into the ledger.
//!
//! The queue lives in the same SQLite database as the ledger, so a queued
//! item is exactly one join away from its full work specification (inputs,
//! instruction, declared output schema) and its provenance. Lifecycle:
//!
//! ```text
//! pending ──► accepted   (corrected output validated + completed in the
//!    │                    ledger as `human-review`; replays reuse it free)
//!    └──────► rejected   (the record is permanently dropped — replays
//!                         neither re-dispatch nor re-bill it)
//! ```
//!
//! While an item is `pending`, replays leave it untouched: no re-dispatch,
//! no re-billing, no duplicate queue entries.

use crate::error::AiError;
use crate::ledger::{Ledger, RecordedResult};
use crate::schema::validate_output;
use pramen_core::spec::{FieldSpec, FieldType};
use rusqlite::{OptionalExtension, params};
use serde_json::Value;

/// Provider/model identity recorded for human-accepted results.
pub const HUMAN_REVIEW_ACTOR: &str = "human-review";

/// The decision state of one queued item.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReviewStatus {
    /// Awaiting a human decision.
    Pending,
    /// A corrected output was accepted into the ledger.
    Accepted,
    /// Permanently dropped by a human.
    Rejected,
}

impl ReviewStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Accepted => "accepted",
            Self::Rejected => "rejected",
        }
    }

    fn parse(text: &str) -> Self {
        match text {
            "accepted" => Self::Accepted,
            "rejected" => Self::Rejected,
            _ => Self::Pending,
        }
    }
}

/// One queued item, joined with its work specification.
#[derive(Debug, Clone)]
pub struct ReviewItem {
    /// Content-addressed work key (also the ledger key).
    pub work_key: String,
    /// The transform that routed the record here.
    pub transform_id: String,
    /// Why validation (or the budget gate / batch item) failed.
    pub reason: String,
    /// The raw model output text, when a model responded at all.
    pub raw_output: Option<String>,
    /// Decision state.
    pub status: ReviewStatus,
    /// Enqueue timestamp (UTC, ISO-8601).
    pub created_at: String,
    /// The canonical work specification (inputs, instruction, output
    /// schema, provider, model, params) from the ledger.
    pub spec: Value,
}

impl Ledger {
    /// Durably route one record to review. Idempotent per work key: a
    /// record already queued (from this run or an earlier one) keeps its
    /// first entry, so replays never duplicate the queue.
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
        self.connection().execute(
            "INSERT INTO review_queue (work_key, transform_id, reason, raw_output)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(work_key) DO NOTHING",
            params![work_key, transform_id, reason, raw_output],
        )?;
        Ok(())
    }

    /// The decision state of a work key, or `None` when it was never
    /// routed to review.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::Ledger`] on database failure.
    pub fn review_status(&self, work_key: &str) -> Result<Option<ReviewStatus>, AiError> {
        self.connection()
            .query_row(
                "SELECT status FROM review_queue WHERE work_key = ?1",
                params![work_key],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map(|status| status.map(|s| ReviewStatus::parse(&s)))
            .map_err(AiError::from)
    }

    /// Queued items awaiting a decision, oldest first, joined with their
    /// work specifications.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::Ledger`] on database failure.
    pub fn pending_reviews(&self) -> Result<Vec<ReviewItem>, AiError> {
        let mut stmt = self.connection().prepare(
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

    /// Accept a human-corrected output for a queued item.
    ///
    /// The correction is validated against the output schema stored in
    /// the item's work specification — human decisions obey exactly the
    /// same contract as model output — then recorded in the ledger as a
    /// completed result attributed to `human-review` with zero tokens.
    /// From that point, every run resolves the record from the ledger
    /// like any other completed work.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::Input`] when the key is not pending review or
    /// the correction violates the declared schema, [`AiError::Ledger`]
    /// on database failure.
    pub fn accept_review(&self, work_key: &str, corrected: &Value) -> Result<(), AiError> {
        let item = self.pending_review(work_key)?;
        let fields = fields_from_schema(&item.spec["output_schema"]).ok_or_else(|| {
            AiError::Input(format!(
                "review item {work_key}: stored work spec has no usable output schema"
            ))
        })?;
        let normalized = validate_output(&corrected.to_string(), &fields).map_err(|violation| {
            AiError::Input(format!(
                "corrected output for {work_key} violates the declared schema: {violation}"
            ))
        })?;
        let recorded = RecordedResult {
            output: normalized,
            provider: HUMAN_REVIEW_ACTOR.to_owned(),
            model: HUMAN_REVIEW_ACTOR.to_owned(),
            request_id: format!("review:{work_key}"),
            input_tokens: 0,
            output_tokens: 0,
            validation: "human-accepted".to_owned(),
        };
        if !self.complete(work_key, &recorded)? {
            return Err(AiError::Input(format!(
                "review item {work_key}: the ledger already holds a completed result"
            )));
        }
        self.decide_review(work_key, ReviewStatus::Accepted)
    }

    /// Reject a queued item: the record is permanently dropped. Replays
    /// neither re-dispatch nor re-bill it, and it never reaches the sink.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::Input`] when the key is not pending review,
    /// [`AiError::Ledger`] on database failure.
    pub fn reject_review(&self, work_key: &str) -> Result<(), AiError> {
        self.pending_review(work_key)?;
        self.decide_review(work_key, ReviewStatus::Rejected)
    }

    /// Queue counts: `(pending, accepted, rejected)`.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::Ledger`] on database failure.
    pub fn review_counts(&self) -> Result<(u64, u64, u64), AiError> {
        let count = |status: &str| -> Result<u64, AiError> {
            let n: i64 = self.connection().query_row(
                "SELECT COUNT(*) FROM review_queue WHERE status = ?1",
                params![status],
                |r| r.get(0),
            )?;
            Ok(u64::try_from(n).unwrap_or_default())
        };
        Ok((count("pending")?, count("accepted")?, count("rejected")?))
    }

    /// One pending item by key, or a clear error.
    fn pending_review(&self, work_key: &str) -> Result<ReviewItem, AiError> {
        self.pending_reviews()?
            .into_iter()
            .find(|item| item.work_key == work_key)
            .ok_or_else(|| {
                AiError::Input(format!(
                    "no pending review item with work key {work_key}; \
                     `pramen ai review list` shows the queue"
                ))
            })
    }

    fn decide_review(&self, work_key: &str, status: ReviewStatus) -> Result<(), AiError> {
        self.connection().execute(
            "UPDATE review_queue
             SET status = ?2, decided_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
             WHERE work_key = ?1 AND status = 'pending'",
            params![work_key, status.as_str()],
        )?;
        Ok(())
    }
}

/// Reconstruct declared fields from the JSON Schema stored in a work
/// specification (the exact shape [`crate::schema::output_json_schema`]
/// generates), so a correction can be validated without the pipeline
/// document.
fn fields_from_schema(schema: &Value) -> Option<Vec<FieldSpec>> {
    let properties = schema.get("properties")?.as_object()?;
    let mut fields = Vec::with_capacity(properties.len());
    for (name, spec) in properties {
        let (type_name, nullable) = match spec.get("type")? {
            Value::String(t) => (t.as_str(), false),
            Value::Array(types) => (types.first()?.as_str()?, true),
            _ => return None,
        };
        let field_type = match type_name {
            "string" => FieldType::Utf8,
            "integer" => FieldType::Int64,
            "number" => FieldType::Float64,
            "boolean" => FieldType::Bool,
            _ => return None,
        };
        fields.push(FieldSpec {
            name: name.clone(),
            field_type,
            nullable,
        });
    }
    Some(fields)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ledger::WorkState;
    use crate::workkey::WorkSpec;
    use serde_json::json;

    fn temp_ledger(name: &str) -> (std::path::PathBuf, Ledger) {
        let path = std::env::temp_dir().join(format!(
            "pramen-review-test-{}-{name}.sqlite",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        let ledger = Ledger::open(&path).unwrap();
        (path, ledger)
    }

    fn spec() -> WorkSpec {
        WorkSpec {
            operation: "ai.classify".into(),
            instruction: "classify".into(),
            inputs: json!({"description": "printer on fire"}),
            output_schema: crate::schema::output_json_schema(&[
                FieldSpec {
                    name: "category".into(),
                    field_type: FieldType::Utf8,
                    nullable: false,
                },
                FieldSpec {
                    name: "score".into(),
                    field_type: FieldType::Float64,
                    nullable: true,
                },
            ]),
            provider: "mock".into(),
            model: "mock-1".into(),
            params: json!({}),
        }
    }

    fn enqueue(ledger: &Ledger, spec: &WorkSpec) -> String {
        let key = spec.work_key();
        ledger.upsert_pending(&key, &spec.canonical()).unwrap();
        ledger
            .mark_failed(&key, "invalid output: wrong type")
            .unwrap();
        ledger
            .enqueue_review(
                &key,
                "classify",
                "invalid output: wrong type",
                Some("{\"category\": 3}"),
            )
            .unwrap();
        key
    }

    #[test]
    fn enqueue_is_idempotent_and_listable() {
        let (path, ledger) = temp_ledger("enqueue");
        let key = enqueue(&ledger, &spec());
        // A replay routing the same record again does not duplicate it.
        ledger
            .enqueue_review(&key, "classify", "second reason", None)
            .unwrap();

        let pending = ledger.pending_reviews().unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].work_key, key);
        assert_eq!(pending[0].reason, "invalid output: wrong type");
        assert_eq!(pending[0].raw_output.as_deref(), Some("{\"category\": 3}"));
        assert_eq!(pending[0].spec["inputs"]["description"], "printer on fire");
        assert_eq!(ledger.review_counts().unwrap(), (1, 0, 0));
        assert_eq!(
            ledger.review_status(&key).unwrap(),
            Some(ReviewStatus::Pending)
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn accepting_validates_and_completes_the_ledger() {
        let (path, ledger) = temp_ledger("accept");
        let key = enqueue(&ledger, &spec());

        // A correction violating the declared schema is refused.
        let error = ledger
            .accept_review(&key, &json!({"category": 3, "score": 0.5}))
            .unwrap_err();
        assert!(error.to_string().contains("violates"), "{error}");

        // A valid correction becomes a completed, human-attributed,
        // zero-token ledger result.
        ledger
            .accept_review(&key, &json!({"category": "hardware", "score": null}))
            .unwrap();
        let Some(WorkState::Completed(recorded)) = ledger.state(&key).unwrap() else {
            panic!("expected completed after acceptance");
        };
        assert_eq!(
            recorded.output,
            json!({"category": "hardware", "score": null})
        );
        assert_eq!(recorded.provider, HUMAN_REVIEW_ACTOR);
        assert_eq!(recorded.validation, "human-accepted");
        assert_eq!(recorded.input_tokens + recorded.output_tokens, 0);
        assert_eq!(ledger.review_counts().unwrap(), (0, 1, 0));

        // Deciding twice is refused.
        assert!(ledger.accept_review(&key, &json!({})).is_err());
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn rejecting_keeps_the_record_out_permanently() {
        let (path, ledger) = temp_ledger("reject");
        let key = enqueue(&ledger, &spec());
        ledger.reject_review(&key).unwrap();
        assert_eq!(
            ledger.review_status(&key).unwrap(),
            Some(ReviewStatus::Rejected)
        );
        assert_eq!(ledger.review_counts().unwrap(), (0, 0, 1));
        // The work item stays failed (never completed): the operator
        // consults the rejected status and drops without re-dispatch.
        assert!(matches!(
            ledger.state(&key).unwrap(),
            Some(WorkState::Failed { .. })
        ));
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn unknown_keys_are_a_clear_error() {
        let (path, ledger) = temp_ledger("unknown");
        let error = ledger.reject_review("no-such-key").unwrap_err();
        assert!(error.to_string().contains("no pending review"), "{error}");
        let _ = std::fs::remove_file(path);
    }
}
