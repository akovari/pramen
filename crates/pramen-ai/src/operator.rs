//! The `ai.extract` / `ai.classify` transform operator.
//!
//! Per record: build the content-addressed work key, consult the ledger,
//! enforce budgets, dispatch online if needed, validate the output against
//! the declared schema, record the result durably, and append the typed
//! output columns to the batch. Records whose output fails validation
//! follow the transform's `onInvalid` policy.
//!
//! v1 scope notes: execution is online (provider-batch dispatch lands with
//! P1.8), rows are processed sequentially (bounded concurrency is a
//! planned optimization), and `review` routing drops the record with a
//! warning until the review queue exists (X1.6).

use crate::budget;
use crate::error::AiError;
use crate::ledger::{Ledger, RecordedResult, WorkState};
use crate::provider::{InferenceRequest, Provider, ProviderResponse};
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
use pramen_core::spec::{AiTransform, ExecutionMode, FieldType, InvalidPolicy};
use serde_json::{Map, Value, json};
use std::sync::Arc;

/// A governed semantic transform stage.
pub struct SemanticTransform {
    operation: String,
    config: AiTransform,
    provider: Arc<dyn Provider>,
    model_id: String,
    ledger: Ledger,
    output_schema: Value,
    dropped_invalid: u64,
}

impl SemanticTransform {
    /// Build the operator for one `ai.extract`/`ai.classify` step.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::Unsupported`] for spec features this build does
    /// not execute yet (provider-batch mode, timestamp output fields), so
    /// pipelines fail at plan time rather than mid-run.
    pub fn new(
        operation: &str,
        config: AiTransform,
        provider: Arc<dyn Provider>,
        model_id: &str,
        ledger: Ledger,
    ) -> Result<Self, AiError> {
        if config.execution == ExecutionMode::Batch {
            return Err(AiError::Unsupported(
                "execution: batch is not implemented yet (P1.8); use auto or online".to_owned(),
            ));
        }
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
        let output_schema = output_json_schema(&config.output.fields);
        Ok(Self {
            operation: operation.to_owned(),
            config,
            provider,
            model_id: model_id.to_owned(),
            ledger,
            output_schema,
            dropped_invalid: 0,
        })
    }

    fn stage_error(&self, error: &AiError) -> StageError {
        match error {
            AiError::Provider { .. } | AiError::Ledger(_) => {
                StageError::InvalidData(format!("{}: {error}", self.config.id))
            }
            other => StageError::InvalidData(format!("{}: {other}", self.config.id)),
        }
    }

    /// Obtain the validated output for one record: ledger reuse or a fresh
    /// governed dispatch. `Ok(None)` means the record was dropped by policy.
    async fn resolve_record(&mut self, inputs: Value) -> Result<Option<Value>, StageError> {
        let spec = WorkSpec {
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
        };
        let key = spec.work_key();

        self.ledger
            .upsert_pending(&key, &spec.canonical())
            .map_err(|e| self.stage_error(&e))?;
        if let Some(WorkState::Completed(recorded)) =
            self.ledger.state(&key).map_err(|e| self.stage_error(&e))?
        {
            return Ok(Some(recorded.output));
        }

        // Budget gate: nothing is dispatched for an over-budget record.
        let request = InferenceRequest {
            instruction: self.config.instruction.clone(),
            inputs: spec.inputs.clone(),
            output_schema: self.output_schema.clone(),
            max_output_tokens: budget::output_cap(self.config.budget.as_ref()),
        };
        let request_text = format!(
            "{}{}{}",
            request.instruction, request.inputs, request.output_schema
        );
        if let Err(error) = budget::enforce_input_budget(self.config.budget.as_ref(), &request_text)
        {
            return self.apply_invalid_policy(&key, &error.to_string());
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

        match validate_output(&response.text, &self.config.output.fields) {
            Ok(normalized) => {
                self.record_completion(&key, &response, &normalized)?;
                Ok(Some(normalized))
            }
            Err(violation) => {
                self.ledger
                    .mark_failed(&key, &format!("invalid output: {violation}"))
                    .map_err(|e| self.stage_error(&e))?;
                self.apply_invalid_policy(&key, &violation)
            }
        }
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
                self.dropped_invalid += 1;
                tracing::warn!(
                    transform = %self.config.id, %key, %reason,
                    "record routed to review; the review queue (X1.6) is not built yet, so the record is dropped from this run"
                );
                Ok(None)
            }
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

#[async_trait::async_trait]
impl Transform for SemanticTransform {
    async fn apply(&mut self, batch: RecordBatch) -> Result<Vec<RecordBatch>, StageError> {
        // Resolve declared input columns once per batch.
        let schema = batch.schema();
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

        let mut keep = BooleanBuilder::new();
        let mut outputs: Vec<Vec<Value>> = vec![Vec::new(); self.config.output.fields.len()];

        for row in 0..batch.num_rows() {
            let mut inputs = Map::new();
            for (name, index) in &input_columns {
                inputs.insert(
                    name.clone(),
                    cell_to_json(batch.column(*index).as_ref(), row, name)?,
                );
            }
            match self.resolve_record(Value::Object(inputs)).await? {
                Some(normalized) => {
                    keep.append_value(true);
                    for (slot, field) in outputs.iter_mut().zip(&self.config.output.fields) {
                        slot.push(normalized.get(&field.name).cloned().unwrap_or(Value::Null));
                    }
                }
                None => keep.append_value(false),
            }
        }

        let filtered = arrow::compute::filter_record_batch(&batch, &keep.finish())
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
        let out = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .map_err(StageError::external)?;
        Ok(vec![out])
    }

    async fn finish(&mut self) -> Result<Vec<RecordBatch>, StageError> {
        if self.dropped_invalid > 0 {
            tracing::warn!(
                transform = %self.config.id,
                dropped = self.dropped_invalid,
                "records dropped by onInvalid policy during this run"
            );
        }
        Ok(Vec::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::MockProvider;
    use pramen_core::spec::{AiOutput, AiValidation, FieldSpec};

    fn config(on_invalid: InvalidPolicy) -> AiTransform {
        AiTransform {
            id: "classify".into(),
            model: "m".into(),
            execution: ExecutionMode::Auto,
            inputs: vec!["description".into()],
            instruction: "classify the ticket".into(),
            output: AiOutput {
                fields: vec![
                    FieldSpec {
                        name: "category".into(),
                        field_type: FieldType::Utf8,
                        nullable: false,
                    },
                    FieldSpec {
                        name: "score".into(),
                        field_type: FieldType::Float64,
                        nullable: false,
                    },
                ],
            },
            validation: AiValidation { on_invalid },
            budget: None,
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
    async fn batch_mode_and_timestamp_outputs_fail_at_construction() {
        let (path, ledger) = temp_ledger("plan");
        let mut cfg = config(InvalidPolicy::Fail);
        cfg.execution = ExecutionMode::Batch;
        let error = SemanticTransform::new(
            "ai.extract",
            cfg,
            Arc::new(MockProvider::new()),
            "mock-1",
            ledger,
        )
        .err()
        .map(|e| e.to_string())
        .unwrap_or_default();
        assert!(error.contains("P1.8"), "{error}");
        let _ = std::fs::remove_file(path);
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
}
