//! Serde/schemars data model for the v1alpha1 pipeline document.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// A complete, parsed pipeline document.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PipelineSpec {
    /// Versioned API surface identifier; only `pramen.dev/v1alpha1` today.
    pub api_version: ApiVersion,
    /// Document kind; only `Pipeline` today.
    pub kind: Kind,
    /// Names and labels for the pipeline.
    pub metadata: Metadata,
    /// The pipeline itself: models, source, transforms, sink, runtime.
    pub spec: PipelineSpecBody,
}

/// The accepted `apiVersion` values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum ApiVersion {
    /// The initial, still-unstable schema version.
    #[serde(rename = "pramen.dev/v1alpha1")]
    V1Alpha1,
}

/// The accepted `kind` values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum Kind {
    /// A data pipeline definition.
    Pipeline,
}

/// Pipeline identity.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Metadata {
    /// Pipeline name: lowercase alphanumerics and hyphens, DNS-label style.
    pub name: String,
}

/// The `spec` body of a pipeline document.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PipelineSpecBody {
    /// Named model configurations referenced by semantic transforms.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub models: BTreeMap<String, ModelSpec>,
    /// Where records come from.
    pub source: SourceSpec,
    /// Ordered transform steps; may be empty for pure movement pipelines.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub transforms: Vec<TransformSpec>,
    /// Where records go.
    pub sink: SinkSpec,
    /// Engine tuning and checkpointing; every field has a default.
    #[serde(default)]
    pub runtime: RuntimeSpec,
}

/// A named model configuration.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ModelSpec {
    /// Provider adapter identifier, e.g. `bedrock` or `openai-compat`.
    pub provider: String,
    /// Provider-specific model identifier.
    pub model: String,
    /// Provider region pin, where the provider has regions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
    /// Endpoint override, primarily for self-hosted or stubbed providers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
}

/// Record sources.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum SourceSpec {
    /// Files in an object store or on the local filesystem.
    #[serde(rename_all = "camelCase")]
    ObjectStore {
        /// Location URL: `s3://…`, `file://…`, or a bare local path prefix.
        url: String,
        /// File format of the objects.
        format: FormatSpec,
    },
}

/// File formats understood by the v1 source.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum FormatSpec {
    /// Apache Parquet.
    Parquet,
    /// Newline-delimited JSON.
    Ndjson,
}

/// A transform step.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", deny_unknown_fields)]
pub enum TransformSpec {
    /// A deterministic SQL transform over the incoming stream.
    #[serde(rename = "sql")]
    Sql(SqlTransform),
    /// Governed semantic extraction into typed columns.
    #[serde(rename = "ai.extract")]
    AiExtract(AiTransform),
    /// Governed semantic classification into typed columns.
    #[serde(rename = "ai.classify")]
    AiClassify(AiTransform),
}

impl TransformSpec {
    /// The unique identifier of this transform step.
    #[must_use]
    pub fn id(&self) -> &str {
        match self {
            Self::Sql(transform) => &transform.id,
            Self::AiExtract(transform) | Self::AiClassify(transform) => &transform.id,
        }
    }
}

/// A deterministic SQL transform.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SqlTransform {
    /// Unique step identifier.
    pub id: String,
    /// SQL text; the incoming stream is visible as the table `input`.
    pub query: String,
}

/// A governed semantic transform (`ai.extract` / `ai.classify`).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AiTransform {
    /// Unique step identifier.
    pub id: String,
    /// Name of a model declared under `spec.models`.
    pub model: String,
    /// How invocations are dispatched to the provider.
    #[serde(default)]
    pub execution: ExecutionMode,
    /// Input column names passed to the model.
    pub inputs: Vec<String>,
    /// The fixed instruction; part of the work key.
    pub instruction: String,
    /// The typed output contract.
    pub output: AiOutput,
    /// What happens when the model output fails validation.
    #[serde(default)]
    pub validation: AiValidation,
    /// Hard per-record token budgets.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub budget: Option<AiBudget>,
}

/// Dispatch policy for semantic transforms.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionMode {
    /// Let the runtime choose between online and provider-batch execution.
    #[default]
    Auto,
    /// Always call the provider synchronously.
    Online,
    /// Always use the provider's asynchronous batch API.
    Batch,
}

/// The typed output contract of a semantic transform.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AiOutput {
    /// Columns the model must produce.
    pub fields: Vec<FieldSpec>,
}

/// One output column of a semantic transform.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FieldSpec {
    /// Column name.
    pub name: String,
    /// Column type.
    #[serde(rename = "type")]
    pub field_type: FieldType,
    /// Whether the model may return null for this column.
    #[serde(default)]
    pub nullable: bool,
}

/// Scalar types available to semantic transform outputs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum FieldType {
    /// UTF-8 string.
    Utf8,
    /// 64-bit signed integer.
    Int64,
    /// 64-bit float.
    Float64,
    /// Boolean.
    Bool,
    /// Microsecond-precision UTC timestamp.
    Timestamp,
}

impl FieldType {
    /// The Arrow data type this field materializes as.
    #[must_use]
    pub fn arrow_type(self) -> arrow::datatypes::DataType {
        use arrow::datatypes::{DataType, TimeUnit};
        match self {
            Self::Utf8 => DataType::Utf8,
            Self::Int64 => DataType::Int64,
            Self::Float64 => DataType::Float64,
            Self::Bool => DataType::Boolean,
            Self::Timestamp => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        }
    }
}

/// Validation policy for semantic transform outputs.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AiValidation {
    /// What to do with records whose model output fails schema validation.
    #[serde(default)]
    pub on_invalid: InvalidPolicy,
}

/// Disposition of records that fail output validation.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum InvalidPolicy {
    /// Fail the run.
    #[default]
    Fail,
    /// Drop the record and count it.
    Drop,
    /// Route the record to the review destination.
    Review,
}

/// Hard per-record token budgets for a semantic transform.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AiBudget {
    /// Maximum input tokens per record.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_input_tokens_per_record: Option<u32>,
    /// Maximum output tokens per record.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_output_tokens_per_record: Option<u32>,
}

/// Record sinks.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum SinkSpec {
    /// A PostgreSQL table loaded via native binary `COPY`.
    #[serde(rename_all = "camelCase")]
    Postgres {
        /// Qualified table name, `schema.table`.
        target: String,
        /// Load semantics.
        #[serde(default)]
        mode: SinkMode,
        /// Environment variable holding the connection string.
        ///
        /// Connection strings are secrets and never appear in the document.
        #[serde(default = "default_dsn_env")]
        dsn_env: String,
    },
}

fn default_dsn_env() -> String {
    "PRAMEN_POSTGRES_DSN".to_owned()
}

/// Load semantics for database sinks.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SinkMode {
    /// Append rows; replays may duplicate unless the run is idempotent.
    #[default]
    Append,
    /// Stage and merge on the target's primary key for idempotent replays.
    Upsert,
}

/// Engine tuning and checkpointing.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RuntimeSpec {
    /// Target size of one Arrow batch, in bytes.
    #[serde(default = "default_target_batch_bytes")]
    pub target_batch_bytes: u64,
    /// Ceiling on bytes in flight across all channels.
    #[serde(default = "default_max_inflight_bytes")]
    pub max_inflight_bytes: u64,
    /// Checkpoint location; omit to run without resumability.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint: Option<CheckpointSpec>,
}

fn default_target_batch_bytes() -> u64 {
    8 * 1024 * 1024
}

fn default_max_inflight_bytes() -> u64 {
    256 * 1024 * 1024
}

impl Default for RuntimeSpec {
    fn default() -> Self {
        Self {
            target_batch_bytes: default_target_batch_bytes(),
            max_inflight_bytes: default_max_inflight_bytes(),
            checkpoint: None,
        }
    }
}

/// Checkpoint storage location.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CheckpointSpec {
    /// Checkpoint directory URL, e.g. `file:///var/lib/pramen/checkpoints/`.
    pub url: String,
}
