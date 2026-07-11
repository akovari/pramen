//! The `pramen run` command: plan a spec into concrete stages and execute.

use pramen_ai::ledger::Ledger;
use pramen_ai::operator::SemanticTransform;
use pramen_ai::provider::{MockProvider, OpenAiCompatProvider, Provider};
use pramen_core::observe::RunMetrics;
use pramen_core::runtime::{self, RunOptions, Sink, Source, Transform};
use pramen_core::spec::{
    AiTransform, FormatSpec, ModelSpec, PipelineSpec, SinkSpec, SourceSpec, TransformSpec,
};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use tokio_util::sync::CancellationToken;

/// Rows per Arrow batch requested from sources.
const BATCH_SIZE_ROWS: usize = 8192;

/// Environment variable overriding the inference ledger location.
const LEDGER_PATH_ENV: &str = "PRAMEN_LEDGER_PATH";

/// Default inference ledger location, relative to the working directory.
const DEFAULT_LEDGER_PATH: &str = ".pramen/ledger.sqlite";

/// Execute `spec` to completion, honoring Ctrl-C.
///
/// # Errors
///
/// Returns a human-readable message when planning or execution fails.
pub fn execute(spec: &PipelineSpec) -> Result<(), String> {
    let tokio_runtime = tokio::runtime::Runtime::new()
        .map_err(|error| format!("failed to start async runtime: {error}"))?;
    tokio_runtime.block_on(execute_async(spec))
}

async fn execute_async(spec: &PipelineSpec) -> Result<(), String> {
    let started = Instant::now();
    let runtime_spec = &spec.spec.runtime;

    if runtime_spec.checkpoint.is_some() {
        tracing::warn!("checkpointing is not implemented yet (P1.3); running without resumability");
    }

    let source = plan_source(spec).await?;
    let transforms = plan_transforms(spec)?;
    let sink = plan_sink(spec).await?;

    let capacity = usize::try_from(
        (runtime_spec.max_inflight_bytes / runtime_spec.target_batch_bytes.max(1)).clamp(1, 32),
    )
    .unwrap_or(4);
    let metrics = Arc::new(RunMetrics::default());
    let cancel = CancellationToken::new();

    {
        let cancel = cancel.clone();
        tokio::spawn(async move {
            if tokio::signal::ctrl_c().await.is_ok() {
                tracing::warn!("interrupt received; cancelling run");
                cancel.cancel();
            }
        });
    }

    tracing::info!(pipeline = %spec.metadata.name, "run starting");
    let summary = runtime::run_pipeline(
        source,
        transforms,
        sink,
        RunOptions {
            channel_capacity: capacity,
        },
        Arc::clone(&metrics),
        cancel,
    )
    .await
    .map_err(|error| error.to_string())?;

    let elapsed = started.elapsed();
    let m = summary.metrics;
    println!(
        "run complete: {} rows in / {} rows out in {elapsed:.2?} ({:.0} rows/s out, {} batches, {:.1} MiB written)",
        m.rows_in,
        m.rows_out,
        m.rows_out as f64 / elapsed.as_secs_f64().max(f64::EPSILON),
        m.batches_out,
        m.bytes_out as f64 / 1_048_576.0,
    );
    Ok(())
}

async fn plan_source(spec: &PipelineSpec) -> Result<Box<dyn Source>, String> {
    let SourceSpec::ObjectStore { url, format } = &spec.spec.source;
    match format {
        FormatSpec::Parquet => {
            let memory_limit =
                usize::try_from(spec.spec.runtime.max_inflight_bytes).unwrap_or(256 * 1024 * 1024);
            let source = pramen_io::ParquetSource::open(url, memory_limit, BATCH_SIZE_ROWS)
                .await
                .map_err(|error| format!("source: {error}"))?;
            Ok(Box::new(source))
        }
        FormatSpec::Ndjson => {
            let memory_limit =
                usize::try_from(spec.spec.runtime.max_inflight_bytes).unwrap_or(256 * 1024 * 1024);
            let source = pramen_io::NdjsonSource::open(url, memory_limit, BATCH_SIZE_ROWS)
                .await
                .map_err(|error| format!("source: {error}"))?;
            Ok(Box::new(source))
        }
    }
}

/// A planned, named transform stage.
type PlannedTransform = (String, Box<dyn Transform>);

fn plan_transforms(spec: &PipelineSpec) -> Result<Vec<PlannedTransform>, String> {
    spec.spec
        .transforms
        .iter()
        .map(|transform| match transform {
            TransformSpec::Sql(sql) => Ok((
                sql.id.clone(),
                Box::new(pramen_io::SqlTransform::new(&sql.query)) as Box<dyn Transform>,
            )),
            TransformSpec::AiExtract(ai) => plan_semantic("ai.extract", ai, spec),
            TransformSpec::AiClassify(ai) => plan_semantic("ai.classify", ai, spec),
        })
        .collect()
}

/// Build one governed semantic transform: resolve the model reference to a
/// provider adapter and open a handle to the shared inference ledger.
fn plan_semantic(
    operation: &str,
    ai: &AiTransform,
    spec: &PipelineSpec,
) -> Result<PlannedTransform, String> {
    let model = spec.spec.models.get(&ai.model).ok_or_else(|| {
        format!(
            "transform `{}`: model `{}` is not declared under spec.models",
            ai.id, ai.model
        )
    })?;
    let provider = plan_provider(&ai.id, model)?;
    let ledger =
        Ledger::open(&ledger_path()).map_err(|error| format!("transform `{}`: {error}", ai.id))?;
    let transform = SemanticTransform::new(operation, ai.clone(), provider, &model.model, ledger)
        .map_err(|error| format!("transform `{}`: {error}", ai.id))?;
    Ok((ai.id.clone(), Box::new(transform) as Box<dyn Transform>))
}

fn plan_provider(transform_id: &str, model: &ModelSpec) -> Result<Arc<dyn Provider>, String> {
    match model.provider.as_str() {
        "mock" => Ok(Arc::new(MockProvider::new())),
        "openai-compat" => {
            let endpoint = model.endpoint.as_deref().ok_or_else(|| {
                format!(
                    "transform `{transform_id}`: provider `openai-compat` requires an `endpoint` \
                     (e.g. http://localhost:11434/v1 for Ollama)"
                )
            })?;
            let api_key = std::env::var("PRAMEN_OPENAI_API_KEY").ok();
            Ok(Arc::new(OpenAiCompatProvider::new(
                endpoint,
                &model.model,
                api_key,
            )))
        }
        "bedrock" => Err(format!(
            "transform `{transform_id}`: the Bedrock adapter is not implemented yet (P1.7); \
             use `openai-compat` (vLLM/Ollama) or `mock`"
        )),
        other => Err(format!(
            "transform `{transform_id}`: unknown provider `{other}` \
             (available: mock, openai-compat)"
        )),
    }
}

/// The inference ledger location: `PRAMEN_LEDGER_PATH` or
/// `.pramen/ledger.sqlite` under the working directory.
pub fn ledger_path() -> PathBuf {
    std::env::var(LEDGER_PATH_ENV)
        .map_or_else(|_| PathBuf::from(DEFAULT_LEDGER_PATH), PathBuf::from)
}

async fn plan_sink(spec: &PipelineSpec) -> Result<Box<dyn Sink>, String> {
    let SinkSpec::Postgres {
        target,
        mode,
        dsn_env,
    } = &spec.spec.sink;
    let dsn = std::env::var(dsn_env).map_err(|_| {
        format!("sink: environment variable `{dsn_env}` with the PostgreSQL DSN is not set")
    })?;
    let sink = pramen_io::PostgresCopySink::connect(&dsn, target, *mode)
        .await
        .map_err(|error| format!("sink: {error}"))?;
    Ok(Box::new(sink))
}
