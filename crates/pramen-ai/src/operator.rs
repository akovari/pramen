//! The `ai.extract` / `ai.classify` / `ai.generate` transform operator.
//!
//! Per record: build the content-addressed work key, consult the ledger,
//! enforce budgets, dispatch if needed, validate the output against the
//! declared schema, record the result durably, and append the typed
//! output columns to the batch. Records whose output fails validation
//! follow the transform's `onInvalid` policy.
//!
//! Two dispatch shapes share all of that governance:
//!
//! - **online** (`execution: online`, or `auto` without dispatch hints):
//!   one provider call per ledger miss, streamed row by row;
//! - **provider-batch** (`execution: batch`, or `auto` when the cost model
//!   recommends it): ledger misses are collected while input streams
//!   through, submitted as one asynchronous job whose id is durably
//!   recorded per item *before* results are awaited, then polled, fetched,
//!   validated, and joined back to the buffered rows. A run that crashes
//!   after submission reconciles on restart by job and item id instead of
//!   resubmitting — submitted work is never re-billed.
//!
//! `execution: auto` consults [`crate::dispatch`] when the transform sets
//! `dispatch.expectedRecords` and `dispatch.deadlineSeconds`; otherwise it
//! stays online (safe default for unbounded or unplanned work).
//!
//! `onInvalid: review` routes the record to the durable
//! [review queue](crate::review): the record is withheld from this run's
//! output, and replays leave it queued — no re-dispatch, no re-billing —
//! until a human accepts a correction (which re-enters the ledger) or
//! rejects it (permanent drop).
//!
//! v1 scope note: rows are processed sequentially (bounded concurrency is
//! a planned optimization).

use crate::budget;
use crate::dispatch::{self, DispatchPlan};
use crate::error::AiError;
use crate::ledger::{Ledger, RecordedResult, WorkState};
use crate::provider::{BatchStatus, InferenceRequest, Provider, ProviderResponse};
use crate::review::ReviewStatus;
use crate::schema::{output_json_schema, validate_output};
use crate::workkey::WorkSpec;
use arrow::array::{
    Array, ArrayRef, BooleanArray, BooleanBuilder, Float64Array, Float64Builder, Int32Array,
    Int64Array, Int64Builder, LargeStringArray, StringArray, StringBuilder, StringViewArray,
    TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use pramen_core::runtime::{StageError, Transform};
use pramen_core::spec::{AiTransform, FieldType, InvalidPolicy};
use serde_json::{Map, Value, json};
use std::sync::Arc;

/// How often an open provider-batch job is polled.
const BATCH_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(250);

/// Poll ceiling before an open job is declared stuck (~10 minutes).
const BATCH_MAX_POLLS: u32 = 2400;

/// A governed semantic transform stage.
pub struct SemanticTransform {
    operation: String,
    config: AiTransform,
    provider: Arc<dyn Provider>,
    model_id: String,
    ledger: Ledger,
    output_schema: Value,
    dropped_invalid: u64,
    /// Records routed to (or still withheld by) the review queue this run.
    routed_review: u64,
    /// Provider-reported tokens (input + output) consumed this run,
    /// checked against `budget.maxRunTokens`. Ledger reuse adds nothing.
    run_tokens: u64,
    /// Consecutive invalid-output records; trips the circuit breaker.
    consecutive_invalid: u32,
    /// Whether this stage dispatches via the provider's batch API.
    batch_mode: bool,
    /// Plan produced when `execution: auto` ran the cost model.
    auto_plan: Option<DispatchPlan>,
    /// Batch mode: input batches buffered until results arrive.
    buffered: Vec<RecordBatch>,
    /// Batch mode: ledger misses awaiting submission, keyed by work key.
    pending: std::collections::BTreeMap<String, InferenceRequest>,
    /// Batch mode: submitted job ids still awaiting results.
    open_jobs: std::collections::BTreeSet<String>,
    /// Batch mode: whether crashed-run reconciliation has run.
    reconciled: bool,
}

impl SemanticTransform {
    /// Build the operator for one `ai.extract`/`ai.classify`/`ai.generate` step.
    ///
    /// `execution: auto` runs the [`crate::dispatch`] cost model when
    /// `dispatch.expectedRecords` and `dispatch.deadlineSeconds` are set
    /// and the provider supports batch; otherwise it stays online.
    /// `execution: batch` requires the provider to declare batch capability.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::Unsupported`] for spec features this build does
    /// not execute (batch on a non-batch provider, timestamp output
    /// fields, unknown rate cards), so pipelines fail at plan time rather
    /// than mid-run.
    pub fn new(
        operation: &str,
        config: AiTransform,
        provider: Arc<dyn Provider>,
        model_id: &str,
        ledger: Ledger,
    ) -> Result<Self, AiError> {
        let (batch_mode, auto_plan) = dispatch::resolve_batch_mode(
            config.execution,
            provider.capabilities().batch,
            config.dispatch.as_ref(),
            provider.id(),
        )
        .map_err(AiError::Unsupported)?;
        if let Some(field) = config
            .output
            .fields
            .iter()
            .find(|f| f.field_type == FieldType::Timestamp)
        {
            return Err(AiError::Unsupported(format!(
                "output field `{}`: timestamp outputs are not supported yet",
                field.name
            )));
        }
        if let Some(plan) = &auto_plan {
            tracing::info!(
                transform = %config.id,
                recommended = %plan.recommended,
                reason = %plan.reason,
                "execution: auto resolved via dispatch cost model"
            );
        }
        let output_schema = output_json_schema(&config.output.fields);
        Ok(Self {
            operation: operation.to_owned(),
            config,
            provider,
            model_id: model_id.to_owned(),
            ledger,
            output_schema,
            dropped_invalid: 0,
            routed_review: 0,
            run_tokens: 0,
            consecutive_invalid: 0,
            batch_mode,
            auto_plan,
            buffered: Vec::new(),
            pending: std::collections::BTreeMap::new(),
            open_jobs: std::collections::BTreeSet::new(),
            reconciled: false,
        })
    }

    /// Provider-reported tokens (input + output) consumed so far this run.
    #[must_use]
    pub fn run_tokens(&self) -> u64 {
        self.run_tokens
    }

    /// Whether this stage is using provider-batch dispatch.
    #[must_use]
    pub fn batch_mode(&self) -> bool {
        self.batch_mode
    }

    /// The cost-model plan when `execution: auto` produced one.
    #[must_use]
    pub fn auto_plan(&self) -> Option<&DispatchPlan> {
        self.auto_plan.as_ref()
    }

    fn stage_error(&self, error: &AiError) -> StageError {
        match error {
            AiError::Provider { .. } | AiError::Ledger(_) => {
                StageError::InvalidData(format!("{}: {error}", self.config.id))
            }
            other => StageError::InvalidData(format!("{}: {other}", self.config.id)),
        }
    }

    /// The canonical work specification for one record's inputs.
    fn work_spec(&self, inputs: Value) -> WorkSpec {
        WorkSpec {
            operation: self.operation.clone(),
            instruction: self.config.instruction.clone(),
            inputs,
            output_schema: self.output_schema.clone(),
            provider: self.provider.id().to_owned(),
            model: self.model_id.clone(),
            params: json!({
                "temperature": 0,
                "max_output_tokens": budget::output_cap(self.config.budget.as_ref()),
            }),
        }
    }

    /// The provider request for one work specification.
    fn request_for(&self, spec: &WorkSpec) -> InferenceRequest {
        InferenceRequest {
            instruction: self.config.instruction.clone(),
            inputs: spec.inputs.clone(),
            output_schema: self.output_schema.clone(),
            max_output_tokens: budget::output_cap(self.config.budget.as_ref()),
        }
    }

    /// The text whose size is charged against input budgets and ceilings.
    fn request_text(request: &InferenceRequest) -> String {
        format!(
            "{}{}{}",
            request.instruction, request.inputs, request.output_schema
        )
    }

    /// Obtain the validated output for one record: ledger reuse or a fresh
    /// governed dispatch. `Ok(None)` means the record was dropped by policy.
    async fn resolve_record(&mut self, inputs: Value) -> Result<Option<Value>, StageError> {
        let spec = self.work_spec(inputs);
        let key = spec.work_key();

        self.ledger
            .upsert_pending(&key, &spec.canonical())
            .map_err(|e| self.stage_error(&e))?;
        match self.ledger.state(&key).map_err(|e| self.stage_error(&e))? {
            Some(WorkState::Completed(recorded)) => return Ok(Some(recorded.output)),
            // A failed record may be sitting in the review queue; a
            // queued or rejected record is never re-dispatched.
            Some(WorkState::Failed { .. }) if self.withheld_by_review(&key)? => return Ok(None),
            _ => {}
        }

        // Budget gate: nothing is dispatched for an over-budget record.
        let request = self.request_for(&spec);
        let request_text = Self::request_text(&request);
        if let Err(error) = budget::enforce_input_budget(self.config.budget.as_ref(), &request_text)
        {
            return self.apply_invalid_policy(&key, &error.to_string(), None);
        }

        // Run ceiling: before spending anything more, project this
        // request's worst case (estimated input + configured output cap)
        // onto the tokens already consumed. Crossing the ceiling is a hard
        // stop, not a per-record policy matter.
        if let Some(ceiling) = self.config.budget.as_ref().and_then(|b| b.max_run_tokens) {
            let projected = self.run_tokens
                + u64::from(budget::estimate_tokens(&request_text))
                + u64::from(budget::output_cap(self.config.budget.as_ref()).unwrap_or(0));
            if projected > ceiling {
                return Err(StageError::InvalidData(format!(
                    "{}: run token ceiling reached: {} tokens consumed, next record needs \
                     up to {} more, maxRunTokens is {ceiling}; raise the ceiling or narrow \
                     the input (already-completed records are in the ledger and stay free)",
                    self.config.id,
                    self.run_tokens,
                    projected - self.run_tokens,
                )));
            }
        }

        self.ledger
            .mark_submitted(&key, "online")
            .map_err(|e| self.stage_error(&e))?;
        let response = match self.provider.invoke(&request).await {
            Ok(response) => response,
            Err(error) => {
                self.ledger
                    .mark_failed(&key, &error.to_string())
                    .map_err(|e| self.stage_error(&e))?;
                return Err(self.stage_error(&error));
            }
        };
        self.run_tokens += response.input_tokens + response.output_tokens;
        self.accept_response(&key, &response)
    }

    /// Validate and durably record one provider response, applying the
    /// breaker and `onInvalid` policy. `Ok(None)` means the record fell to
    /// a drop/review policy.
    fn accept_response(
        &mut self,
        key: &str,
        response: &ProviderResponse,
    ) -> Result<Option<Value>, StageError> {
        if let Err(error) =
            budget::enforce_output_budget(self.config.budget.as_ref(), response.output_tokens)
        {
            return self.reject_invalid(key, &error.to_string(), Some(&response.text));
        }
        match validate_output(&response.text, &self.config.output.fields) {
            Ok(normalized) => {
                self.consecutive_invalid = 0;
                self.record_completion(key, response, &normalized)?;
                Ok(Some(normalized))
            }
            Err(violation) => self.reject_invalid(key, &violation, Some(&response.text)),
        }
    }

    /// Mark a record failed, arm the breaker, and apply `onInvalid`.
    fn reject_invalid(
        &mut self,
        key: &str,
        violation: &str,
        raw_output: Option<&str>,
    ) -> Result<Option<Value>, StageError> {
        self.ledger
            .mark_failed(key, &format!("invalid output: {violation}"))
            .map_err(|e| self.stage_error(&e))?;
        self.consecutive_invalid += 1;
        let trip_at = self.config.breaker.max_consecutive_invalid;
        if self.consecutive_invalid >= trip_at {
            return Err(StageError::InvalidData(format!(
                "{}: circuit breaker tripped: {trip_at} consecutive invalid outputs \
                 (last: {violation}); this looks systemic — check the instruction, \
                 model, and endpoint before re-running",
                self.config.id,
            )));
        }
        self.apply_invalid_policy(key, violation, raw_output)
    }

    fn record_completion(
        &self,
        key: &str,
        response: &ProviderResponse,
        normalized: &Value,
    ) -> Result<(), StageError> {
        let recorded = RecordedResult {
            output: normalized.clone(),
            provider: self.provider.id().to_owned(),
            model: self.model_id.clone(),
            request_id: response.request_id.clone(),
            input_tokens: response.input_tokens,
            output_tokens: response.output_tokens,
            validation: "passed".to_owned(),
        };
        self.ledger
            .complete(key, &recorded)
            .map_err(|e| self.stage_error(&e))?;
        Ok(())
    }

    fn apply_invalid_policy(
        &mut self,
        key: &str,
        reason: &str,
        raw_output: Option<&str>,
    ) -> Result<Option<Value>, StageError> {
        match self.config.validation.on_invalid {
            InvalidPolicy::Fail => Err(StageError::InvalidData(format!(
                "{}: record {key} rejected: {reason}",
                self.config.id
            ))),
            InvalidPolicy::Drop => {
                self.dropped_invalid += 1;
                tracing::warn!(transform = %self.config.id, %key, %reason, "record dropped (onInvalid: drop)");
                Ok(None)
            }
            InvalidPolicy::Review => {
                self.routed_review += 1;
                self.ledger
                    .enqueue_review(key, &self.config.id, reason, raw_output)
                    .map_err(|e| self.stage_error(&e))?;
                tracing::warn!(
                    transform = %self.config.id, %key, %reason,
                    "record routed to the review queue (decide with `pramen ai review`)"
                );
                Ok(None)
            }
        }
    }

    /// Whether a failed record is withheld from this run by the review
    /// queue: queued (awaiting a decision) or rejected (permanent drop).
    /// Retryable failures — never routed to review — return `false`.
    fn withheld_by_review(&mut self, key: &str) -> Result<bool, StageError> {
        match self
            .ledger
            .review_status(key)
            .map_err(|e| self.stage_error(&e))?
        {
            Some(ReviewStatus::Pending) => {
                self.routed_review += 1;
                tracing::info!(
                    transform = %self.config.id, %key,
                    "record awaiting review; withheld without re-dispatch"
                );
                Ok(true)
            }
            Some(ReviewStatus::Rejected) => {
                tracing::info!(
                    transform = %self.config.id, %key,
                    "record rejected in review; permanently dropped"
                );
                Ok(true)
            }
            // Accepted reviews are completed ledger results, resolved
            // before this check; anything else is a retryable failure.
            _ => Ok(false),
        }
    }
}

/// JSON value of one cell, in the canonical form used inside work keys.
fn cell_to_json(column: &dyn Array, row: usize, name: &str) -> Result<Value, StageError> {
    if column.is_null(row) {
        return Ok(Value::Null);
    }
    let value = match column.data_type() {
        DataType::Utf8 => column
            .as_any()
            .downcast_ref::<StringArray>()
            .map(|a| json!(a.value(row))),
        DataType::LargeUtf8 => column
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .map(|a| json!(a.value(row))),
        DataType::Utf8View => column
            .as_any()
            .downcast_ref::<StringViewArray>()
            .map(|a| json!(a.value(row))),
        DataType::Int32 => column
            .as_any()
            .downcast_ref::<Int32Array>()
            .map(|a| json!(a.value(row))),
        DataType::Int64 => column
            .as_any()
            .downcast_ref::<Int64Array>()
            .map(|a| json!(a.value(row))),
        DataType::Float64 => column
            .as_any()
            .downcast_ref::<Float64Array>()
            .map(|a| json!(a.value(row))),
        DataType::Boolean => column
            .as_any()
            .downcast_ref::<BooleanArray>()
            .map(|a| json!(a.value(row))),
        // Canonical input form for timestamps: microseconds since epoch.
        DataType::Timestamp(TimeUnit::Microsecond, _) => column
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .map(|a| json!(a.value(row))),
        other => {
            return Err(StageError::InvalidData(format!(
                "input column `{name}` has unsupported type {other} for semantic transforms"
            )));
        }
    };
    value.ok_or_else(|| StageError::InvalidData(format!("column `{name}`: array/type mismatch")))
}

/// Build one typed Arrow column from validated output values.
fn build_column(
    field_type: FieldType,
    name: &str,
    values: &[Value],
) -> Result<ArrayRef, StageError> {
    let bad = |value: &Value| {
        StageError::InvalidData(format!(
            "validated value for `{name}` no longer matches its type: {value}"
        ))
    };
    Ok(match field_type {
        FieldType::Utf8 => {
            let mut builder = StringBuilder::new();
            for value in values {
                match value {
                    Value::Null => builder.append_null(),
                    Value::String(s) => builder.append_value(s),
                    other => return Err(bad(other)),
                }
            }
            Arc::new(builder.finish())
        }
        FieldType::Int64 => {
            let mut builder = Int64Builder::new();
            for value in values {
                match value {
                    Value::Null => builder.append_null(),
                    other => builder.append_value(other.as_i64().ok_or_else(|| bad(other))?),
                }
            }
            Arc::new(builder.finish())
        }
        FieldType::Float64 => {
            let mut builder = Float64Builder::new();
            for value in values {
                match value {
                    Value::Null => builder.append_null(),
                    other => builder.append_value(other.as_f64().ok_or_else(|| bad(other))?),
                }
            }
            Arc::new(builder.finish())
        }
        FieldType::Bool => {
            let mut builder = BooleanBuilder::new();
            for value in values {
                match value {
                    Value::Null => builder.append_null(),
                    Value::Bool(b) => builder.append_value(*b),
                    other => return Err(bad(other)),
                }
            }
            Arc::new(builder.finish())
        }
        FieldType::Timestamp => {
            return Err(StageError::InvalidData(
                "timestamp outputs are rejected at construction".to_owned(),
            ));
        }
    })
}

impl SemanticTransform {
    /// Resolve declared input columns against an incoming schema.
    fn input_column_indices(&self, schema: &Schema) -> Result<Vec<(String, usize)>, StageError> {
        let mut input_columns = Vec::with_capacity(self.config.inputs.len());
        for name in &self.config.inputs {
            let (index, _) = schema.column_with_name(name).ok_or_else(|| {
                StageError::InvalidData(format!(
                    "{}: input column `{name}` not found in incoming schema",
                    self.config.id
                ))
            })?;
            input_columns.push((name.clone(), index));
        }
        Ok(input_columns)
    }

    /// Attach output columns to a batch, keeping only resolved rows.
    fn assemble(
        &self,
        batch: &RecordBatch,
        resolved: &[Option<Value>],
    ) -> Result<RecordBatch, StageError> {
        let schema = batch.schema();
        let mut keep = BooleanBuilder::new();
        let mut outputs: Vec<Vec<Value>> = vec![Vec::new(); self.config.output.fields.len()];
        for outcome in resolved {
            match outcome {
                Some(normalized) => {
                    keep.append_value(true);
                    for (slot, field) in outputs.iter_mut().zip(&self.config.output.fields) {
                        slot.push(normalized.get(&field.name).cloned().unwrap_or(Value::Null));
                    }
                }
                None => keep.append_value(false),
            }
        }

        let filtered = arrow::compute::filter_record_batch(batch, &keep.finish())
            .map_err(StageError::external)?;

        let mut fields: Vec<Field> = schema.fields().iter().map(|f| f.as_ref().clone()).collect();
        let mut columns: Vec<ArrayRef> = filtered.columns().to_vec();
        for (values, field_spec) in outputs.iter().zip(&self.config.output.fields) {
            fields.push(Field::new(
                &field_spec.name,
                field_spec.field_type.arrow_type(),
                field_spec.nullable,
            ));
            columns.push(build_column(
                field_spec.field_type,
                &field_spec.name,
                values,
            )?);
        }
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).map_err(StageError::external)
    }

    /// Reconcile jobs left in `submitted` by an earlier crashed run: ingest
    /// finished ones, keep still-running ones open, re-queue failed ones.
    /// Nothing here re-bills — no submission happens during reconciliation.
    async fn reconcile_submitted(&mut self) -> Result<(), StageError> {
        if self.reconciled {
            return Ok(());
        }
        self.reconciled = true;
        let submitted = self
            .ledger
            .submitted_items()
            .map_err(|e| self.stage_error(&e))?;
        let mut jobs: std::collections::BTreeMap<String, Vec<String>> = Default::default();
        for (key, request_id) in submitted {
            // `online` markers come from crashed online dispatches; the
            // online path retries those itself.
            if request_id.is_empty() || request_id == "online" {
                continue;
            }
            jobs.entry(request_id).or_default().push(key);
        }
        for (job_id, keys) in jobs {
            tracing::info!(
                transform = %self.config.id, %job_id, items = keys.len(),
                "reconciling batch job from a previous run"
            );
            self.open_jobs.insert(job_id);
        }
        Ok(())
    }

    /// Register one record's work in batch mode: ledger reuse needs
    /// nothing, submitted work stays with its open job, and fresh work is
    /// queued for submission (budget-gated, deduplicated by work key).
    fn collect_record(&mut self, inputs: Value) -> Result<(), StageError> {
        let spec = self.work_spec(inputs);
        let key = spec.work_key();
        self.ledger
            .upsert_pending(&key, &spec.canonical())
            .map_err(|e| self.stage_error(&e))?;
        match self.ledger.state(&key).map_err(|e| self.stage_error(&e))? {
            Some(WorkState::Completed(_)) => return Ok(()),
            Some(WorkState::Submitted { request_id })
                if !request_id.is_empty() && request_id != "online" =>
            {
                // Belongs to a job reconciliation already reopened.
                return Ok(());
            }
            // Queued for review or rejected: withheld, not resubmitted.
            Some(WorkState::Failed { .. }) if self.withheld_by_review(&key)? => return Ok(()),
            _ => {}
        }
        if self.pending.contains_key(&key) {
            return Ok(());
        }

        let request = self.request_for(&spec);
        let request_text = Self::request_text(&request);
        if let Err(violation) =
            budget::enforce_input_budget(self.config.budget.as_ref(), &request_text)
        {
            // Policy applies now; the buffered row is filtered at emit
            // because its work never completes.
            self.apply_invalid_policy(&key, &violation.to_string(), None)?;
            return Ok(());
        }
        self.pending.insert(key, request);
        Ok(())
    }

    /// Submit queued work as one provider-batch job, recording the job id
    /// per item in the ledger *before* awaiting anything — the crash
    /// window between submission and completion is exactly what
    /// reconciliation covers.
    async fn submit_pending(&mut self) -> Result<(), StageError> {
        if self.pending.is_empty() {
            return Ok(());
        }
        if let Some(ceiling) = self.config.budget.as_ref().and_then(|b| b.max_run_tokens) {
            let needed: u64 = self
                .pending
                .values()
                .map(|request| {
                    u64::from(budget::estimate_tokens(&Self::request_text(request)))
                        + u64::from(request.max_output_tokens.unwrap_or(0))
                })
                .sum();
            if self.run_tokens + needed > ceiling {
                return Err(StageError::InvalidData(format!(
                    "{}: run token ceiling reached: batch of {} items needs up to {needed} \
                     tokens, {} already consumed, maxRunTokens is {ceiling}; raise the \
                     ceiling or narrow the input (already-completed records are in the \
                     ledger and stay free)",
                    self.config.id,
                    self.pending.len(),
                    self.run_tokens,
                )));
            }
        }
        let items: Vec<(String, InferenceRequest)> = self
            .pending
            .iter()
            .map(|(key, request)| (key.clone(), request.clone()))
            .collect();
        let job_id = self
            .provider
            .submit_batch(&items)
            .await
            .map_err(|e| self.stage_error(&e))?;
        for (key, _) in &items {
            self.ledger
                .mark_submitted(key, &job_id)
                .map_err(|e| self.stage_error(&e))?;
        }
        tracing::info!(
            transform = %self.config.id, %job_id, items = items.len(),
            "batch job submitted"
        );
        self.pending.clear();
        self.open_jobs.insert(job_id);
        Ok(())
    }

    /// Poll open jobs to completion and ingest their results.
    async fn drain_open_jobs(&mut self) -> Result<(), StageError> {
        let mut polls = 0u32;
        while let Some(job_id) = self.open_jobs.iter().next().cloned() {
            match self
                .provider
                .poll_batch(&job_id)
                .await
                .map_err(|e| self.stage_error(&e))?
            {
                BatchStatus::Completed => {
                    let results = self
                        .provider
                        .fetch_batch(&job_id)
                        .await
                        .map_err(|e| self.stage_error(&e))?;
                    tracing::info!(
                        transform = %self.config.id, %job_id, items = results.len(),
                        "batch job completed"
                    );
                    for (key, outcome) in results {
                        self.ingest_item(&key, outcome)?;
                    }
                    self.open_jobs.remove(&job_id);
                }
                BatchStatus::Failed(reason) => {
                    // Items stay `submitted` in the ledger; mark them
                    // failed so the next run re-dispatches them.
                    let submitted = self
                        .ledger
                        .submitted_items()
                        .map_err(|e| self.stage_error(&e))?;
                    for (key, request_id) in submitted {
                        if request_id == job_id {
                            self.ledger
                                .mark_failed(&key, &format!("batch job failed: {reason}"))
                                .map_err(|e| self.stage_error(&e))?;
                        }
                    }
                    return Err(StageError::InvalidData(format!(
                        "{}: batch job {job_id} failed provider-side: {reason}",
                        self.config.id
                    )));
                }
                BatchStatus::InProgress => {
                    polls += 1;
                    if polls > BATCH_MAX_POLLS {
                        return Err(StageError::InvalidData(format!(
                            "{}: batch job {job_id} still running after {BATCH_MAX_POLLS} \
                             polls; it stays submitted in the ledger and the next run \
                             will reconcile it",
                            self.config.id
                        )));
                    }
                    tokio::time::sleep(BATCH_POLL_INTERVAL).await;
                }
            }
        }
        Ok(())
    }

    /// Validate and record one fetched batch item.
    fn ingest_item(
        &mut self,
        key: &str,
        outcome: Result<ProviderResponse, String>,
    ) -> Result<(), StageError> {
        match outcome {
            Ok(response) => {
                self.run_tokens += response.input_tokens + response.output_tokens;
                self.accept_response(key, &response)?;
                Ok(())
            }
            Err(reason) => {
                self.ledger
                    .mark_failed(key, &format!("batch item failed: {reason}"))
                    .map_err(|e| self.stage_error(&e))?;
                self.apply_invalid_policy(key, &reason, None)?;
                Ok(())
            }
        }
    }

    /// Join buffered rows to their (now ledger-resident) results.
    fn emit_buffered(&mut self) -> Result<Vec<RecordBatch>, StageError> {
        let buffered = std::mem::take(&mut self.buffered);
        let mut out = Vec::with_capacity(buffered.len());
        for batch in buffered {
            let input_columns = self.input_column_indices(&batch.schema())?;
            let mut resolved = Vec::with_capacity(batch.num_rows());
            for row in 0..batch.num_rows() {
                let inputs = row_inputs(&input_columns, &batch, row)?;
                let key = self.work_spec(inputs).work_key();
                let state = self.ledger.state(&key).map_err(|e| self.stage_error(&e))?;
                resolved.push(match state {
                    Some(WorkState::Completed(recorded)) => Some(recorded.output),
                    // Dropped by policy (budget, invalid output, or a
                    // failed item) — filtered out of the emitted batch.
                    _ => None,
                });
            }
            out.push(self.assemble(&batch, &resolved)?);
        }
        Ok(out)
    }
}

/// The canonical JSON inputs object for one row.
fn row_inputs(
    input_columns: &[(String, usize)],
    batch: &RecordBatch,
    row: usize,
) -> Result<Value, StageError> {
    let mut inputs = Map::new();
    for (name, index) in input_columns {
        inputs.insert(
            name.clone(),
            cell_to_json(batch.column(*index).as_ref(), row, name)?,
        );
    }
    Ok(Value::Object(inputs))
}

#[async_trait::async_trait]
impl Transform for SemanticTransform {
    async fn apply(&mut self, batch: RecordBatch) -> Result<Vec<RecordBatch>, StageError> {
        let input_columns = self.input_column_indices(&batch.schema())?;

        if self.batch_mode {
            self.reconcile_submitted().await?;
            for row in 0..batch.num_rows() {
                let inputs = row_inputs(&input_columns, &batch, row)?;
                self.collect_record(inputs)?;
            }
            self.buffered.push(batch);
            return Ok(Vec::new());
        }

        let mut resolved = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let inputs = row_inputs(&input_columns, &batch, row)?;
            resolved.push(self.resolve_record(inputs).await?);
        }
        Ok(vec![self.assemble(&batch, &resolved)?])
    }

    async fn finish(&mut self) -> Result<Vec<RecordBatch>, StageError> {
        let emitted = if self.batch_mode {
            self.reconcile_submitted().await?;
            self.submit_pending().await?;
            self.drain_open_jobs().await?;
            self.emit_buffered()?
        } else {
            Vec::new()
        };
        if self.dropped_invalid > 0 {
            tracing::warn!(
                transform = %self.config.id,
                dropped = self.dropped_invalid,
                "records dropped by onInvalid policy during this run"
            );
        }
        if self.routed_review > 0 {
            tracing::warn!(
                transform = %self.config.id,
                queued = self.routed_review,
                "records withheld for review this run; decide with `pramen ai review`"
            );
        }
        Ok(emitted)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::{Capabilities, MockProvider};
    use pramen_core::spec::{AiBreaker, AiOutput, AiValidation, ExecutionMode, FieldSpec};

    /// A provider whose output never validates — for breaker tests.
    struct GarbageProvider {
        calls: std::sync::atomic::AtomicU64,
    }

    #[async_trait::async_trait]
    impl Provider for GarbageProvider {
        fn id(&self) -> &str {
            "garbage"
        }
        fn capabilities(&self) -> Capabilities {
            Capabilities {
                online: true,
                batch: false,
                structured_output: false,
                token_accounting: true,
            }
        }
        async fn invoke(&self, _request: &InferenceRequest) -> Result<ProviderResponse, AiError> {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(ProviderResponse {
                text: "{\"wrong\": true}".to_owned(),
                input_tokens: 10,
                output_tokens: 5,
                request_id: "garbage".to_owned(),
            })
        }
    }

    /// A batch-capable provider whose output never validates.
    #[derive(Default)]
    struct GarbageBatchProvider {
        keys: std::sync::Mutex<Vec<String>>,
    }

    #[async_trait::async_trait]
    impl Provider for GarbageBatchProvider {
        fn id(&self) -> &str {
            "garbage-batch"
        }
        fn capabilities(&self) -> Capabilities {
            Capabilities {
                online: false,
                batch: true,
                structured_output: false,
                token_accounting: true,
            }
        }
        async fn invoke(&self, _request: &InferenceRequest) -> Result<ProviderResponse, AiError> {
            Err(AiError::Unsupported("batch only".to_owned()))
        }
        async fn submit_batch(
            &self,
            items: &[(String, InferenceRequest)],
        ) -> Result<String, AiError> {
            *self.keys.lock().unwrap() = items.iter().map(|(k, _)| k.clone()).collect();
            Ok("garbage-job".to_owned())
        }
        async fn poll_batch(&self, _job_id: &str) -> Result<crate::provider::BatchStatus, AiError> {
            Ok(crate::provider::BatchStatus::Completed)
        }
        async fn fetch_batch(
            &self,
            _job_id: &str,
        ) -> Result<Vec<crate::provider::BatchItemResult>, AiError> {
            Ok(self
                .keys
                .lock()
                .unwrap()
                .iter()
                .map(|key| {
                    (
                        key.clone(),
                        Ok(ProviderResponse {
                            text: "{\"wrong\": true}".to_owned(),
                            input_tokens: 10,
                            output_tokens: 5,
                            request_id: "garbage".to_owned(),
                        }),
                    )
                })
                .collect())
        }
    }

    fn config(on_invalid: InvalidPolicy) -> AiTransform {
        AiTransform {
            id: "classify".into(),
            model: "m".into(),
            execution: ExecutionMode::Auto,
            dispatch: None,
            inputs: vec!["description".into()],
            instruction: "classify the ticket".into(),
            output: AiOutput {
                fields: vec![
                    FieldSpec {
                        name: "category".into(),
                        field_type: FieldType::Utf8,
                        nullable: false,
                        max_chars: None,
                    },
                    FieldSpec {
                        name: "score".into(),
                        field_type: FieldType::Float64,
                        nullable: false,
                        max_chars: None,
                    },
                ],
            },
            validation: AiValidation { on_invalid },
            budget: None,
            breaker: AiBreaker::default(),
        }
    }

    fn batch(descriptions: &[&str]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("description", DataType::Utf8, false),
        ]));
        let ids: Vec<i64> = (0..descriptions.len() as i64).collect();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(descriptions.to_vec())),
            ],
        )
        .unwrap()
    }

    fn temp_ledger(name: &str) -> (std::path::PathBuf, Ledger) {
        let path = std::env::temp_dir().join(format!(
            "pramen-operator-test-{}-{name}.sqlite",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        let ledger = Ledger::open(&path).unwrap();
        (path, ledger)
    }

    #[tokio::test]
    async fn appends_typed_columns_and_reuses_the_ledger() {
        let (path, ledger) = temp_ledger("reuse");
        let provider = Arc::new(MockProvider::new());
        let mut transform = SemanticTransform::new(
            "ai.extract",
            config(InvalidPolicy::Fail),
            Arc::clone(&provider) as Arc<dyn Provider>,
            "mock-1",
            ledger,
        )
        .unwrap();

        let out = transform
            .apply(batch(&["printer on fire", "invoice is wrong"]))
            .await
            .unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].num_rows(), 2);
        assert_eq!(out[0].num_columns(), 4);
        assert_eq!(out[0].schema().field(2).name(), "category");
        assert_eq!(out[0].schema().field(3).name(), "score");
        assert_eq!(provider.calls(), 2);

        // Same content again: everything served from the ledger.
        let again = transform
            .apply(batch(&["printer on fire", "invoice is wrong"]))
            .await
            .unwrap();
        assert_eq!(again[0].num_rows(), 2);
        assert_eq!(provider.calls(), 2, "no new provider calls on replay");

        // One new record: exactly one new call.
        transform
            .apply(batch(&["printer on fire", "vpn is down"]))
            .await
            .unwrap();
        assert_eq!(provider.calls(), 3);
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn over_budget_records_follow_policy_without_dispatch() {
        let (path, ledger) = temp_ledger("budget");
        let provider = Arc::new(MockProvider::new());
        let mut cfg = config(InvalidPolicy::Drop);
        cfg.budget = Some(pramen_core::spec::AiBudget {
            max_input_tokens_per_record: Some(1),
            max_output_tokens_per_record: None,
            max_run_tokens: None,
        });
        let mut transform = SemanticTransform::new(
            "ai.extract",
            cfg,
            Arc::clone(&provider) as Arc<dyn Provider>,
            "mock-1",
            ledger,
        )
        .unwrap();

        let out = transform
            .apply(batch(&["this text is far beyond a one-token budget"]))
            .await
            .unwrap();
        assert_eq!(out[0].num_rows(), 0, "over-budget record dropped");
        assert_eq!(provider.calls(), 0, "nothing dispatched");
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn run_token_ceiling_stops_dispatch_but_reuse_stays_free() {
        let ledger_path = std::env::temp_dir().join(format!(
            "pramen-operator-test-{}-ceiling.sqlite",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&ledger_path);
        let provider = Arc::new(MockProvider::new());

        // First: no ceiling; record 1 dispatches and costs a measurable
        // number of tokens.
        let mut unlimited = SemanticTransform::new(
            "ai.classify",
            config(InvalidPolicy::Fail),
            Arc::clone(&provider) as Arc<dyn Provider>,
            "mock-1",
            Ledger::open(&ledger_path).unwrap(),
        )
        .unwrap();
        unlimited.apply(batch(&["printer on fire"])).await.unwrap();
        let first_cost = unlimited.run_tokens();
        assert!(first_cost > 0);
        assert_eq!(provider.calls(), 1);

        // Second: a ceiling barely above record 1's cost, same ledger.
        // Record 1 replays for free (reuse precedes the ceiling check);
        // record 2 would blow the ceiling and is stopped before dispatch.
        let mut cfg = config(InvalidPolicy::Fail);
        cfg.budget = Some(pramen_core::spec::AiBudget {
            max_input_tokens_per_record: None,
            max_output_tokens_per_record: None,
            max_run_tokens: Some(first_cost + 1),
        });
        let mut capped = SemanticTransform::new(
            "ai.classify",
            cfg,
            Arc::clone(&provider) as Arc<dyn Provider>,
            "mock-1",
            Ledger::open(&ledger_path).unwrap(),
        )
        .unwrap();
        let out = capped.apply(batch(&["printer on fire"])).await.unwrap();
        assert_eq!(
            out[0].num_rows(),
            1,
            "reused record passes under the ceiling"
        );
        assert_eq!(provider.calls(), 1, "no new dispatch for the reused record");

        let error = capped
            .apply(batch(&["a brand new never-seen ticket"]))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("run token ceiling"), "{error}");
        assert_eq!(provider.calls(), 1, "new record blocked before dispatch");
        let _ = std::fs::remove_file(ledger_path);
    }

    #[tokio::test]
    async fn breaker_trips_on_consecutive_invalid_outputs() {
        let (path, ledger) = temp_ledger("breaker");
        let provider = Arc::new(GarbageProvider {
            calls: std::sync::atomic::AtomicU64::new(0),
        });
        let mut cfg = config(InvalidPolicy::Drop);
        cfg.breaker = AiBreaker {
            max_consecutive_invalid: 3,
        };
        let mut transform = SemanticTransform::new(
            "ai.classify",
            cfg,
            Arc::clone(&provider) as Arc<dyn Provider>,
            "mock-1",
            ledger,
        )
        .unwrap();

        let error = transform
            .apply(batch(&["a", "b", "c", "d", "e"]))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("circuit breaker"), "{error}");
        assert_eq!(
            provider.calls.load(std::sync::atomic::Ordering::SeqCst),
            3,
            "the run stops at the trip threshold instead of paying for the rest"
        );
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn batch_mode_requires_a_batch_capable_provider() {
        let (path, ledger) = temp_ledger("plan");
        let mut cfg = config(InvalidPolicy::Fail);
        cfg.execution = ExecutionMode::Batch;
        let error = SemanticTransform::new(
            "ai.extract",
            cfg.clone(),
            Arc::new(GarbageProvider {
                calls: std::sync::atomic::AtomicU64::new(0),
            }),
            "mock-1",
            Ledger::open(&path).unwrap(),
        )
        .err()
        .map(|e| e.to_string())
        .unwrap_or_default();
        assert!(error.contains("does not support batch"), "{error}");

        // A batch-capable provider plans fine.
        SemanticTransform::new(
            "ai.extract",
            cfg,
            Arc::new(MockProvider::new()),
            "mock-1",
            ledger,
        )
        .unwrap();
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn auto_dispatch_cost_model_selects_batch_when_deadline_allows() {
        let (path, ledger) = temp_ledger("autodispatch");
        let mut cfg = config(InvalidPolicy::Fail);
        cfg.execution = ExecutionMode::Auto;
        cfg.dispatch = Some(pramen_core::spec::AutoDispatchHints {
            expected_records: Some(10_000),
            deadline_seconds: Some(86_400),
            input_tokens_per_record: None,
            output_tokens_per_record: None,
            rate_card: Some("mock".to_owned()),
        });
        let transform = SemanticTransform::new(
            "ai.classify",
            cfg,
            Arc::new(MockProvider::new()),
            "mock-1",
            ledger,
        )
        .unwrap();
        assert!(transform.batch_mode());
        assert_eq!(
            transform.auto_plan().expect("plan").recommended,
            crate::dispatch::RecommendedMode::Batch
        );
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn auto_without_dispatch_hints_stays_online() {
        let (path, ledger) = temp_ledger("autoonline");
        let transform = SemanticTransform::new(
            "ai.classify",
            config(InvalidPolicy::Fail),
            Arc::new(MockProvider::new()),
            "mock-1",
            ledger,
        )
        .unwrap();
        assert!(!transform.batch_mode());
        assert!(transform.auto_plan().is_none());
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn batch_execution_submits_once_and_joins_results() {
        let (path, _unused) = temp_ledger("batchexec");
        let provider = Arc::new(MockProvider::with_batch_latency(2));
        let mut cfg = config(InvalidPolicy::Fail);
        cfg.execution = ExecutionMode::Batch;

        let mut transform = SemanticTransform::new(
            "ai.classify",
            cfg.clone(),
            Arc::clone(&provider) as Arc<dyn Provider>,
            "mock-1",
            Ledger::open(&path).unwrap(),
        )
        .unwrap();

        // Two input batches, one duplicate work item across them: nothing
        // is emitted while input streams through, and the duplicate is
        // submitted only once.
        let empty = transform
            .apply(batch(&["printer on fire", "invoice is wrong"]))
            .await
            .unwrap();
        assert!(empty.is_empty(), "batch mode buffers until finish");
        transform
            .apply(batch(&["printer on fire", "vpn is down"]))
            .await
            .unwrap();

        let out = transform.finish().await.unwrap();
        assert_eq!(provider.calls(), 3, "three unique items, one submission");
        assert_eq!(out.len(), 2, "one emitted batch per buffered batch");
        assert_eq!(out[0].num_rows(), 2);
        assert_eq!(out[1].num_rows(), 2);
        assert_eq!(out[0].num_columns(), 4);
        assert_eq!(out[0].schema().field(2).name(), "category");

        // A second run over the same ledger is pure reuse: no submission.
        let mut replay = SemanticTransform::new(
            "ai.classify",
            cfg,
            Arc::clone(&provider) as Arc<dyn Provider>,
            "mock-1",
            Ledger::open(&path).unwrap(),
        )
        .unwrap();
        replay
            .apply(batch(&["printer on fire", "invoice is wrong"]))
            .await
            .unwrap();
        let again = replay.finish().await.unwrap();
        assert_eq!(again[0].num_rows(), 2);
        assert_eq!(provider.calls(), 3, "replay costs nothing");
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn crash_after_submit_reconciles_without_resubmitting() {
        let (path, _unused) = temp_ledger("reconcile");
        let provider = Arc::new(MockProvider::with_batch_latency(1));
        let mut cfg = config(InvalidPolicy::Fail);
        cfg.execution = ExecutionMode::Batch;

        // Run 1 submits the job and then "crashes" before results arrive:
        // the ledger holds both items as submitted under the job id.
        let mut crashed = SemanticTransform::new(
            "ai.classify",
            cfg.clone(),
            Arc::clone(&provider) as Arc<dyn Provider>,
            "mock-1",
            Ledger::open(&path).unwrap(),
        )
        .unwrap();
        crashed
            .apply(batch(&["printer on fire", "invoice is wrong"]))
            .await
            .unwrap();
        crashed.submit_pending().await.unwrap();
        assert_eq!(provider.calls(), 2, "billed at submission");
        drop(crashed);

        // Run 2 over the same ledger sees the open job, waits for it, and
        // ingests its results — the items are never resubmitted.
        let mut recovered = SemanticTransform::new(
            "ai.classify",
            cfg,
            Arc::clone(&provider) as Arc<dyn Provider>,
            "mock-1",
            Ledger::open(&path).unwrap(),
        )
        .unwrap();
        recovered
            .apply(batch(&["printer on fire", "invoice is wrong"]))
            .await
            .unwrap();
        let out = recovered.finish().await.unwrap();
        assert_eq!(out[0].num_rows(), 2, "both rows recovered from the job");
        assert_eq!(
            provider.calls(),
            2,
            "reconciliation never re-bills submitted work"
        );
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn batch_items_with_invalid_output_follow_policy() {
        let (path, ledger) = temp_ledger("batchinvalid");
        let provider = Arc::new(GarbageBatchProvider::default());
        let mut cfg = config(InvalidPolicy::Drop);
        cfg.execution = ExecutionMode::Batch;
        let mut transform = SemanticTransform::new(
            "ai.classify",
            cfg,
            Arc::clone(&provider) as Arc<dyn Provider>,
            "mock-1",
            ledger,
        )
        .unwrap();
        transform.apply(batch(&["a", "b"])).await.unwrap();
        let out = transform.finish().await.unwrap();
        assert_eq!(out[0].num_rows(), 0, "invalid batch items dropped");
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn review_routing_withholds_then_reuses_the_human_decision() {
        let ledger_path = std::env::temp_dir().join(format!(
            "pramen-operator-test-{}-review.sqlite",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&ledger_path);
        let provider = Arc::new(GarbageProvider {
            calls: std::sync::atomic::AtomicU64::new(0),
        });
        let cfg = config(InvalidPolicy::Review);

        // Run 1: invalid output routes the record to the queue; the row
        // is withheld from the output.
        let mut first = SemanticTransform::new(
            "ai.classify",
            cfg.clone(),
            Arc::clone(&provider) as Arc<dyn Provider>,
            "mock-1",
            Ledger::open(&ledger_path).unwrap(),
        )
        .unwrap();
        let out = first.apply(batch(&["printer on fire"])).await.unwrap();
        assert_eq!(out[0].num_rows(), 0, "queued record withheld");
        assert_eq!(provider.calls.load(std::sync::atomic::Ordering::SeqCst), 1);

        let ledger = Ledger::open(&ledger_path).unwrap();
        let pending = ledger.pending_reviews().unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(
            pending[0].raw_output.as_deref(),
            Some("{\"wrong\": true}"),
            "the raw model output is preserved for the reviewer"
        );

        // Run 2 (still undecided): withheld again, and — the crucial
        // governance property — not re-dispatched, not re-billed.
        let mut replay = SemanticTransform::new(
            "ai.classify",
            cfg.clone(),
            Arc::clone(&provider) as Arc<dyn Provider>,
            "mock-1",
            Ledger::open(&ledger_path).unwrap(),
        )
        .unwrap();
        let out = replay.apply(batch(&["printer on fire"])).await.unwrap();
        assert_eq!(out[0].num_rows(), 0);
        assert_eq!(
            provider.calls.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "a queued record is never re-dispatched"
        );

        // A human accepts a correction; the next run resolves the record
        // from the ledger like any other completed work.
        ledger
            .accept_review(
                &pending[0].work_key,
                &serde_json::json!({"category": "hardware", "score": 0.99}),
            )
            .unwrap();
        let mut resolved = SemanticTransform::new(
            "ai.classify",
            cfg,
            Arc::clone(&provider) as Arc<dyn Provider>,
            "mock-1",
            Ledger::open(&ledger_path).unwrap(),
        )
        .unwrap();
        let out = resolved.apply(batch(&["printer on fire"])).await.unwrap();
        assert_eq!(out[0].num_rows(), 1, "accepted record flows through");
        assert_eq!(
            provider.calls.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "the human decision costs zero model calls"
        );
        let _ = std::fs::remove_file(ledger_path);
    }

    #[tokio::test]
    async fn rejected_reviews_stay_dropped_on_replay() {
        let ledger_path = std::env::temp_dir().join(format!(
            "pramen-operator-test-{}-reject.sqlite",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&ledger_path);
        let provider = Arc::new(GarbageProvider {
            calls: std::sync::atomic::AtomicU64::new(0),
        });
        let cfg = config(InvalidPolicy::Review);
        let mut first = SemanticTransform::new(
            "ai.classify",
            cfg.clone(),
            Arc::clone(&provider) as Arc<dyn Provider>,
            "mock-1",
            Ledger::open(&ledger_path).unwrap(),
        )
        .unwrap();
        first.apply(batch(&["printer on fire"])).await.unwrap();

        let ledger = Ledger::open(&ledger_path).unwrap();
        let key = ledger.pending_reviews().unwrap()[0].work_key.clone();
        ledger.reject_review(&key).unwrap();

        let mut replay = SemanticTransform::new(
            "ai.classify",
            cfg,
            Arc::clone(&provider) as Arc<dyn Provider>,
            "mock-1",
            Ledger::open(&ledger_path).unwrap(),
        )
        .unwrap();
        let out = replay.apply(batch(&["printer on fire"])).await.unwrap();
        assert_eq!(out[0].num_rows(), 0, "rejected record stays out");
        assert_eq!(
            provider.calls.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "rejection is permanent: no re-dispatch"
        );
        let _ = std::fs::remove_file(ledger_path);
    }

    #[tokio::test]
    async fn missing_input_column_is_a_clear_error() {
        let (path, ledger) = temp_ledger("missing");
        let mut cfg = config(InvalidPolicy::Fail);
        cfg.inputs = vec!["nonexistent".into()];
        let mut transform = SemanticTransform::new(
            "ai.extract",
            cfg,
            Arc::new(MockProvider::new()),
            "mock-1",
            ledger,
        )
        .unwrap();
        let error = transform.apply(batch(&["x"])).await.unwrap_err();
        assert!(error.to_string().contains("`nonexistent`"), "{error}");
        let _ = std::fs::remove_file(path);
    }

    fn generate_config(on_invalid: InvalidPolicy) -> AiTransform {
        AiTransform {
            id: "summarize".into(),
            model: "m".into(),
            execution: ExecutionMode::Auto,
            dispatch: None,
            inputs: vec!["description".into()],
            instruction: "write a short summary".into(),
            output: AiOutput {
                fields: vec![FieldSpec {
                    name: "summary".into(),
                    field_type: FieldType::Utf8,
                    nullable: false,
                    max_chars: Some(32),
                }],
            },
            validation: AiValidation { on_invalid },
            budget: Some(pramen_core::spec::AiBudget {
                max_input_tokens_per_record: Some(2048),
                max_output_tokens_per_record: Some(64),
                max_run_tokens: None,
            }),
            breaker: AiBreaker::default(),
        }
    }

    /// Provider that returns a schema-shaped but over-long UTF-8 field.
    struct VerboseProvider {
        calls: std::sync::atomic::AtomicU64,
        output_tokens: u64,
        text: String,
    }

    #[async_trait::async_trait]
    impl Provider for VerboseProvider {
        fn id(&self) -> &str {
            "verbose"
        }
        fn capabilities(&self) -> Capabilities {
            Capabilities {
                online: true,
                batch: false,
                structured_output: true,
                token_accounting: true,
            }
        }
        async fn invoke(&self, _request: &InferenceRequest) -> Result<ProviderResponse, AiError> {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(ProviderResponse {
                text: self.text.clone(),
                input_tokens: 10,
                output_tokens: self.output_tokens,
                request_id: "verbose".to_owned(),
            })
        }
    }

    #[tokio::test]
    async fn generate_reuses_ledger_and_differs_from_extract_work_key() {
        let (path, ledger) = temp_ledger("generate-reuse");
        let provider = Arc::new(MockProvider::new());
        let mut generate = SemanticTransform::new(
            "ai.generate",
            generate_config(InvalidPolicy::Fail),
            Arc::clone(&provider) as Arc<dyn Provider>,
            "mock-1",
            ledger,
        )
        .unwrap();

        let out = generate.apply(batch(&["printer on fire"])).await.unwrap();
        assert_eq!(out[0].num_rows(), 1);
        assert_eq!(out[0].schema().field(2).name(), "summary");
        assert_eq!(provider.calls(), 1);

        generate.apply(batch(&["printer on fire"])).await.unwrap();
        assert_eq!(provider.calls(), 1, "generate ledger reuse");

        // Same inputs under ai.extract are a different operation → new work.
        let mut extract = SemanticTransform::new(
            "ai.extract",
            {
                let mut cfg = generate_config(InvalidPolicy::Fail);
                cfg.id = "extract-same".into();
                cfg
            },
            Arc::clone(&provider) as Arc<dyn Provider>,
            "mock-1",
            Ledger::open(&path).unwrap(),
        )
        .unwrap();
        extract.apply(batch(&["printer on fire"])).await.unwrap();
        assert_eq!(
            provider.calls(),
            2,
            "ai.generate and ai.extract must not share work keys"
        );
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn generate_budget_ceiling_blocks_dispatch() {
        let (path, ledger) = temp_ledger("generate-budget");
        let provider = Arc::new(MockProvider::new());
        let mut cfg = generate_config(InvalidPolicy::Drop);
        cfg.budget = Some(pramen_core::spec::AiBudget {
            max_input_tokens_per_record: Some(1),
            max_output_tokens_per_record: Some(64),
            max_run_tokens: None,
        });
        let mut transform = SemanticTransform::new(
            "ai.generate",
            cfg,
            Arc::clone(&provider) as Arc<dyn Provider>,
            "mock-1",
            ledger,
        )
        .unwrap();

        let out = transform
            .apply(batch(&["this text is far beyond a one-token budget"]))
            .await
            .unwrap();
        assert_eq!(out[0].num_rows(), 0);
        assert_eq!(provider.calls(), 0);
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn generate_rejects_over_long_field_without_truncating() {
        let (path, ledger) = temp_ledger("generate-maxlen");
        let long = "x".repeat(80);
        let provider = Arc::new(VerboseProvider {
            calls: std::sync::atomic::AtomicU64::new(0),
            output_tokens: 8,
            text: format!(r#"{{"summary":"{long}"}}"#),
        });
        let mut transform = SemanticTransform::new(
            "ai.generate",
            generate_config(InvalidPolicy::Fail),
            Arc::clone(&provider) as Arc<dyn Provider>,
            "mock-1",
            ledger,
        )
        .unwrap();

        let error = transform
            .apply(batch(&["printer on fire"]))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("maxChars"), "{error}");
        assert_eq!(provider.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn generate_rejects_reported_output_token_overrun() {
        let (path, ledger) = temp_ledger("generate-out-tokens");
        let provider = Arc::new(VerboseProvider {
            calls: std::sync::atomic::AtomicU64::new(0),
            output_tokens: 1000,
            text: r#"{"summary":"ok"}"#.to_owned(),
        });
        let mut transform = SemanticTransform::new(
            "ai.generate",
            generate_config(InvalidPolicy::Fail),
            Arc::clone(&provider) as Arc<dyn Provider>,
            "mock-1",
            ledger,
        )
        .unwrap();

        let error = transform
            .apply(batch(&["printer on fire"]))
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("maxOutputTokensPerRecord")
                || error.to_string().contains("output tokens"),
            "{error}"
        );
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn generate_breaker_still_trips_on_consecutive_invalid() {
        let (path, ledger) = temp_ledger("generate-breaker");
        let long = "x".repeat(80);
        let provider = Arc::new(VerboseProvider {
            calls: std::sync::atomic::AtomicU64::new(0),
            output_tokens: 8,
            text: format!(r#"{{"summary":"{long}"}}"#),
        });
        let mut cfg = generate_config(InvalidPolicy::Drop);
        cfg.breaker = AiBreaker {
            max_consecutive_invalid: 2,
        };
        let mut transform = SemanticTransform::new(
            "ai.generate",
            cfg,
            Arc::clone(&provider) as Arc<dyn Provider>,
            "mock-1",
            ledger,
        )
        .unwrap();

        let error = transform.apply(batch(&["a", "b", "c"])).await.unwrap_err();
        assert!(error.to_string().contains("circuit breaker"), "{error}");
        assert_eq!(provider.calls.load(std::sync::atomic::Ordering::SeqCst), 2);
        let _ = std::fs::remove_file(path);
    }
}
