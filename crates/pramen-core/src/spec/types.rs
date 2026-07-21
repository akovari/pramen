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

/// Stage id of the implicit source node in a pipeline graph (ADR 0007).
pub const SOURCE_STAGE_ID: &str = "source";

/// The `spec` body of a pipeline document.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PipelineSpecBody {
    /// Named model configurations referenced by semantic transforms.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub models: BTreeMap<String, ModelSpec>,
    /// Where records come from.
    pub source: SourceSpec,
    /// Transform steps; may be empty for pure movement pipelines.
    ///
    /// Order is the default wiring when `from` is omitted (linear). Explicit
    /// `from` edges enable fan-out (ADR 0007); fan-in is rejected.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub transforms: Vec<TransformSpec>,
    /// Single sink (linear form). Mutually exclusive with [`Self::sinks`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sink: Option<SinkSpec>,
    /// One or more sinks for fan-out (ADR 0007). Mutually exclusive with
    /// [`Self::sink`].
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sinks: Vec<BoundSinkSpec>,
    /// Engine tuning and checkpointing; every field has a default.
    #[serde(default)]
    pub runtime: RuntimeSpec,
}

/// A sink binding with an optional upstream stage id.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BoundSinkSpec {
    /// Unique sink identifier within the pipeline.
    pub id: String,
    /// Upstream stage id (`source` or a transform id). When omitted, defaults
    /// to the last transform, or `source` when there are no transforms.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from: Option<String>,
    /// Sink implementation fields (`type`, `target`, …).
    ///
    /// Flattened so YAML matches a normal sink plus `id`/`from`. This struct
    /// omits `deny_unknown_fields` because serde forbids combining that
    /// attribute with `flatten`.
    #[serde(flatten)]
    pub sink: SinkSpec,
}

/// A resolved sink edge after applying default `from` wiring.
#[derive(Debug, Clone, Copy)]
pub struct ResolvedSink<'a> {
    /// Sink id (`"sink"` for the singular form).
    pub id: &'a str,
    /// Resolved upstream stage id.
    pub from: &'a str,
    /// Sink configuration.
    pub sink: &'a SinkSpec,
}

impl PipelineSpecBody {
    /// Resolve transform `(id, from)` edges with linear defaults.
    #[must_use]
    pub fn resolved_transform_edges(&self) -> Vec<(String, String)> {
        self.transforms
            .iter()
            .enumerate()
            .map(|(index, transform)| {
                let from = transform.from_stage().map_or_else(
                    || {
                        if index == 0 {
                            SOURCE_STAGE_ID.to_owned()
                        } else {
                            self.transforms[index - 1].id().to_owned()
                        }
                    },
                    str::to_owned,
                );
                (transform.id().to_owned(), from)
            })
            .collect()
    }

    /// Resolve sinks with linear defaults. Empty when neither `sink` nor
    /// `sinks` is set (validation catches that).
    #[must_use]
    pub fn resolved_sinks(&self) -> Vec<ResolvedSink<'_>> {
        let default_from: &str = self
            .transforms
            .last()
            .map_or(SOURCE_STAGE_ID, TransformSpec::id);
        if !self.sinks.is_empty() {
            return self
                .sinks
                .iter()
                .map(|bound| ResolvedSink {
                    id: bound.id.as_str(),
                    from: bound.from.as_deref().unwrap_or(default_from),
                    sink: &bound.sink,
                })
                .collect();
        }
        match &self.sink {
            Some(sink) => vec![ResolvedSink {
                id: "sink",
                from: default_from,
                sink,
            }],
            None => Vec::new(),
        }
    }
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
    /// Provider-batch configuration. Required by `provider: bedrock` for
    /// `execution: batch` (model invocation jobs stage through S3 under an
    /// IAM role); `openai-compat` batches through the provider's Files API
    /// and needs no configuration here.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub batch: Option<ModelBatchSpec>,
}

/// S3-staged provider-batch configuration (Bedrock model invocation jobs).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ModelBatchSpec {
    /// ARN of the IAM service role the batch job assumes to read the
    /// staged input and write results.
    pub role_arn: String,
    /// S3 staging prefix, e.g. `s3://my-bucket/pramen-batch/`. Inputs are
    /// written under `input/`, the provider writes results under `output/`.
    pub s3: String,
}

/// Record sources.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum SourceSpec {
    /// Files in an object store or on the local filesystem.
    #[serde(rename_all = "camelCase")]
    ObjectStore {
        /// Location URL: `s3://…`, `gs://…`, `az://…` / `abfs(s)://…`,
        /// `file://…`, or a bare local path prefix. Credentials come from
        /// the standard provider environment, never from this document.
        url: String,
        /// File format of the objects.
        format: FormatSpec,
        /// Declared storage location / region for offline residency checks
        /// (e.g. `eu-central-1`, `europe-west1`, `westeurope`). Required
        /// when [`RuntimeSpec::residency`] is set and `url` is a cloud
        /// scheme; never probed from the provider at plan time (ADR 0005).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        location: Option<String>,
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
    /// Governed generation of bounded UTF-8 text fields.
    #[serde(rename = "ai.generate")]
    AiGenerate(AiTransform),
    /// A sandboxed WebAssembly component transform (Arrow IPC in/out).
    #[serde(rename = "wasm")]
    Wasm(WasmTransform),
}

impl TransformSpec {
    /// The unique identifier of this transform step.
    #[must_use]
    pub fn id(&self) -> &str {
        match self {
            Self::Sql(transform) => &transform.id,
            Self::AiExtract(transform)
            | Self::AiClassify(transform)
            | Self::AiGenerate(transform) => &transform.id,
            Self::Wasm(transform) => &transform.id,
        }
    }

    /// Optional upstream stage id (`source` or another transform).
    #[must_use]
    pub fn from_stage(&self) -> Option<&str> {
        match self {
            Self::Sql(transform) => transform.from.as_deref(),
            Self::AiExtract(transform)
            | Self::AiClassify(transform)
            | Self::AiGenerate(transform) => transform.from.as_deref(),
            Self::Wasm(transform) => transform.from.as_deref(),
        }
    }
}

/// A deterministic SQL transform.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SqlTransform {
    /// Unique step identifier.
    pub id: String,
    /// Upstream stage id. When omitted, defaults to the previous transform
    /// (or `source` for the first step).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from: Option<String>,
    /// SQL text; the incoming stream is visible as the table `input`.
    pub query: String,
}

/// A sandboxed WebAssembly component transform.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WasmTransform {
    /// Unique step identifier.
    pub id: String,
    /// Upstream stage id. When omitted, defaults to the previous transform
    /// (or `source` for the first step).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from: Option<String>,
    /// Component artifact: a filesystem path (absolute or relative to the
    /// pipeline document), or an OCI reference pinned by digest
    /// (`oci://registry/repo@sha256:…`). Tag-only OCI refs are rejected.
    pub component: String,
    /// Resource limits enforced on every batch invocation.
    #[serde(default)]
    pub limits: WasmLimitsSpec,
}

/// Host-enforced limits for a `type: wasm` transform.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WasmLimitsSpec {
    /// Maximum guest linear memory in mebibytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_mb: Option<u32>,
    /// Fuel budget per invocation (`None` = use the runtime default).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fuel: Option<u64>,
    /// Maximum Arrow IPC input size in mebibytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_input_mb: Option<u32>,
    /// Maximum Arrow IPC output size in mebibytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_output_mb: Option<u32>,
}

/// A governed semantic transform (`ai.extract` / `ai.classify` / `ai.generate`).
///
/// `ai.generate` reuses this shape but requires UTF-8 output fields with
/// explicit `maxChars` bounds and a declared `maxOutputTokensPerRecord`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AiTransform {
    /// Unique step identifier.
    pub id: String,
    /// Upstream stage id. When omitted, defaults to the previous transform
    /// (or `source` for the first step).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from: Option<String>,
    /// Name of a model declared under `spec.models`.
    pub model: String,
    /// How invocations are dispatched to the provider.
    #[serde(default)]
    pub execution: ExecutionMode,
    /// Optional planning inputs for [`ExecutionMode::Auto`] (cost model).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dispatch: Option<AutoDispatchHints>,
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
    /// Error-spike circuit breaker; always armed.
    #[serde(default)]
    pub breaker: AiBreaker,
}

/// Dispatch policy for semantic transforms.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionMode {
    /// Let the runtime choose between online and provider-batch execution
    /// using the cost model when [`AiTransform::dispatch`] hints are set;
    /// otherwise online.
    #[default]
    Auto,
    /// Always call the provider synchronously.
    Online,
    /// Always use the provider's asynchronous batch API.
    Batch,
}

/// Planning inputs that let `execution: auto` run the online-vs-batch cost
/// model (architecture §18 RQ1). When either `expectedRecords` or
/// `deadlineSeconds` is omitted, auto falls back to online.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AutoDispatchHints {
    /// Expected ledger-miss record count for this step.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_records: Option<u64>,
    /// Wall-clock deadline for completing the semantic step, in seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deadline_seconds: Option<u64>,
    /// Assumed input tokens per record (default 800 when planning).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_tokens_per_record: Option<u64>,
    /// Assumed output tokens per record (default 200 when planning).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_tokens_per_record: Option<u64>,
    /// Named rate card: `mock`, `openai-compat-stub`, or `bedrock-illustrative`.
    /// Defaults from the provider adapter id when omitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rate_card: Option<String>,
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
    /// Maximum Unicode scalar values for a `utf8` field.
    ///
    /// Required on every `ai.generate` output field. Optional on
    /// `ai.extract` / `ai.classify`. Over-long model output fails
    /// validation and follows `onInvalid` — never silently truncated.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_chars: Option<u32>,
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

/// Hard token budgets for a semantic transform.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AiBudget {
    /// Maximum input tokens per record.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_input_tokens_per_record: Option<u32>,
    /// Maximum output tokens per record.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_output_tokens_per_record: Option<u32>,
    /// Hard ceiling on total tokens (input + output, provider-reported)
    /// this transform may consume in one run. Ledger reuse costs nothing
    /// against it; crossing the ceiling fails the run before dispatching
    /// further work.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_run_tokens: Option<u64>,
}

/// Error-spike circuit breaker for a semantic transform.
///
/// A burst of consecutive invalid outputs almost always means something
/// systemic — a broken prompt revision, a misconfigured model, a degraded
/// endpoint — and under `onInvalid: drop`/`review` each one still costs
/// real tokens. The breaker fails the run instead of paying to discard
/// the rest of the dataset.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AiBreaker {
    /// Consecutive invalid-output records that abort the run.
    #[serde(default = "default_max_consecutive_invalid")]
    pub max_consecutive_invalid: u32,
}

impl Default for AiBreaker {
    fn default() -> Self {
        Self {
            max_consecutive_invalid: default_max_consecutive_invalid(),
        }
    }
}

fn default_max_consecutive_invalid() -> u32 {
    25
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
        /// Merge-key columns for `upsert` mode; the target needs a unique
        /// index over exactly these columns. Must be empty for `append`.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        keys: Vec<String>,
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
    /// Digests (`sha256:…`) and/or `registry/repository` prefixes permitted
    /// for OCI-distributed WASM components. Merged with
    /// `PRAMEN_WASM_OCI_ALLOWLIST`. Empty (and empty env) denies all OCI pulls.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub wasm_oci_allowlist: Vec<String>,
    /// Optional data-residency constraints enforced at plan validation.
    ///
    /// When set, cloud source URLs must declare [`SourceSpec`] `location`,
    /// and that location plus every model `region` must appear in
    /// [`ResidencySpec`] `allowedLocations`. Scheme allow-lists are
    /// optional. Validation is declaration-only — no live cloud lookups
    /// (ADR 0005).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub residency: Option<ResidencySpec>,
}

/// Offline data-residency constraints for sources and models.
///
/// Locations are opaque provider identifiers compared case-sensitively to
/// declared `source.location` and `models.*.region` values. Schemes are
/// the URL scheme tokens (`s3`, `gs`, `az`, `abfs`, `abfss`, …) compared
/// case-insensitively. Local/`file` sources are always permitted.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ResidencySpec {
    /// Allowed region / location identifiers (non-empty).
    pub allowed_locations: Vec<String>,
    /// Optional allow-list of cloud URL schemes. When omitted, every
    /// scheme Pramen supports is accepted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allowed_schemes: Option<Vec<String>>,
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
            wasm_oci_allowlist: Vec::new(),
            residency: None,
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
