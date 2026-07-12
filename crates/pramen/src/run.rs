//! The `pramen run` command: plan a spec into concrete stages and execute.

use pramen_ai::ledger::Ledger;
use pramen_ai::operator::SemanticTransform;
use pramen_ai::provider::{BedrockProvider, MockProvider, OpenAiCompatProvider, Provider};
use pramen_core::checkpoint::{CheckpointStore, FileCheckpointStore, WorkUnit};
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
    let run_id = format!(
        "{}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or_default(),
        std::process::id()
    );

    // With checkpointing configured, enumerate the source into work units,
    // skip completed ones, and durably claim the rest before reading
    // (architecture §11 steps 1–2).
    let mut checkpoint: Option<(FileCheckpointStore, Vec<String>)> = None;
    let planned_paths = match &runtime_spec.checkpoint {
        None => None,
        Some(config) => {
            let mut store = open_checkpoint_store(&config.url)?;
            let units = enumerate_units(spec)?;
            let total = units.len();
            let pipeline = &spec.metadata.name;
            let mut pending: Vec<(WorkUnit, String)> = Vec::new();
            for unit in units {
                let key = unit.key(pipeline);
                if !store.is_complete(&key) {
                    pending.push((unit, key));
                }
            }
            if pending.is_empty() {
                println!(
                    "nothing to do: all {total} work unit(s) under the source are already \
                     completed in the checkpoint store"
                );
                return Ok(());
            }
            tracing::info!(
                total_units = total,
                pending_units = pending.len(),
                "checkpoint store consulted"
            );
            let mut keys = Vec::with_capacity(pending.len());
            let mut paths = Vec::with_capacity(pending.len());
            for (unit, key) in &pending {
                store
                    .claim(unit, key, &run_id)
                    .map_err(|error| error.to_string())?;
                keys.push(key.clone());
                paths.push(unit.url.clone());
            }
            checkpoint = Some((store, keys));
            Some(paths)
        }
    };

    let source = plan_source(spec, planned_paths).await?;
    let transforms = plan_transforms(spec).await?;
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

    // The sink transaction is committed; durably mark the consumed units
    // complete (architecture §11 step 5). A crash inside this window
    // duplicates those units on the next run — the documented at-least-once
    // window (ADR 0006).
    if let Some((mut store, keys)) = checkpoint {
        for key in &keys {
            store
                .complete(key, &run_id)
                .map_err(|error| format!("checkpoint completion: {error}"))?;
        }
        tracing::info!(completed_units = keys.len(), "checkpoint updated");
    }

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

/// Open the file checkpoint store configured at `url` (a local directory,
/// optionally as a `file://` URL).
fn open_checkpoint_store(url: &str) -> Result<FileCheckpointStore, String> {
    let path = url.strip_prefix("file://").unwrap_or(url);
    if path.contains("://") {
        return Err(format!(
            "checkpoint: unsupported URL scheme in `{url}`; the v1 checkpoint store is a local \
             directory (shared backends are tracked in X1.8)"
        ));
    }
    FileCheckpointStore::open(std::path::Path::new(path)).map_err(|error| error.to_string())
}

/// Enumerate the source into checkpointable work units.
fn enumerate_units(spec: &PipelineSpec) -> Result<Vec<WorkUnit>, String> {
    let SourceSpec::ObjectStore { url, format } = &spec.spec.source;
    let extensions: &[&str] = match format {
        FormatSpec::Parquet => &["parquet"],
        FormatSpec::Ndjson => &["ndjson", "jsonl", "json"],
    };
    pramen_io::list_work_units(url, extensions)
        .map_err(|error| format!("checkpoint enumeration: {error}"))
}

async fn plan_source(
    spec: &PipelineSpec,
    paths: Option<Vec<String>>,
) -> Result<Box<dyn Source>, String> {
    let SourceSpec::ObjectStore { url, format } = &spec.spec.source;
    let memory_limit =
        usize::try_from(spec.spec.runtime.max_inflight_bytes).unwrap_or(256 * 1024 * 1024);
    let paths = paths.unwrap_or_else(|| vec![url.clone()]);
    match format {
        FormatSpec::Parquet => {
            let source = if paths == [url.clone()] {
                pramen_io::ParquetSource::open(url, memory_limit, BATCH_SIZE_ROWS).await
            } else {
                pramen_io::ParquetSource::open_files(paths, memory_limit, BATCH_SIZE_ROWS).await
            }
            .map_err(|error| format!("source: {error}"))?;
            Ok(Box::new(source))
        }
        FormatSpec::Ndjson => {
            let source = if paths == [url.clone()] {
                pramen_io::NdjsonSource::open(url, memory_limit, BATCH_SIZE_ROWS).await
            } else {
                pramen_io::NdjsonSource::open_files(paths, memory_limit, BATCH_SIZE_ROWS).await
            }
            .map_err(|error| format!("source: {error}"))?;
            Ok(Box::new(source))
        }
    }
}

/// A planned, named transform stage.
type PlannedTransform = (String, Box<dyn Transform>);

async fn plan_transforms(spec: &PipelineSpec) -> Result<Vec<PlannedTransform>, String> {
    let mut planned = Vec::with_capacity(spec.spec.transforms.len());
    for transform in &spec.spec.transforms {
        planned.push(match transform {
            TransformSpec::Sql(sql) => (
                sql.id.clone(),
                Box::new(pramen_io::SqlTransform::new(&sql.query)) as Box<dyn Transform>,
            ),
            TransformSpec::AiExtract(ai) => plan_semantic("ai.extract", ai, spec).await?,
            TransformSpec::AiClassify(ai) => plan_semantic("ai.classify", ai, spec).await?,
        });
    }
    Ok(planned)
}

/// Build one governed semantic transform: resolve the model reference to a
/// provider adapter and open a handle to the shared inference ledger.
async fn plan_semantic(
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
    let provider = plan_provider(&ai.id, model).await?;
    let ledger =
        Ledger::open(&ledger_path()).map_err(|error| format!("transform `{}`: {error}", ai.id))?;
    let transform = SemanticTransform::new(operation, ai.clone(), provider, &model.model, ledger)
        .map_err(|error| format!("transform `{}`: {error}", ai.id))?;
    Ok((ai.id.clone(), Box::new(transform) as Box<dyn Transform>))
}

/// Resolve a model declaration to a provider adapter (shared with
/// `ai evaluate`, which measures models through the same adapters the
/// pipeline uses).
pub(crate) async fn plan_provider(
    transform_id: &str,
    model: &ModelSpec,
) -> Result<Arc<dyn Provider>, String> {
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
        "bedrock" => Ok(Arc::new(
            BedrockProvider::new(
                &model.model,
                model.region.as_deref(),
                model.endpoint.as_deref(),
            )
            .await,
        )),
        other => Err(format!(
            "transform `{transform_id}`: unknown provider `{other}` \
             (available: mock, openai-compat, bedrock)"
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
        keys,
        dsn_env,
    } = &spec.spec.sink;
    let dsn = std::env::var(dsn_env).map_err(|_| {
        format!("sink: environment variable `{dsn_env}` with the PostgreSQL DSN is not set")
    })?;
    let sink = pramen_io::PostgresCopySink::connect(&dsn, target, *mode, keys)
        .await
        .map_err(|error| format!("sink: {error}"))?;
    Ok(Box::new(sink))
}
