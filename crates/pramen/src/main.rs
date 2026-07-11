//! The `pramen` CLI.
//!
//! v1 surface: `validate`, `explain`, `run`, `ai`. Pipelines may combine
//! deterministic SQL steps with governed semantic transforms backed by the
//! durable inference ledger (`pramen ai status` inspects it).

mod run;

use clap::{Parser, Subcommand};
use pramen_core::observe::LogFormat;
use pramen_core::spec::{
    self, FormatSpec, PipelineSpec, SinkSpec, SourceSpec, SpecError, TransformSpec,
};
use std::path::PathBuf;
use std::process::ExitCode;

#[derive(Parser)]
#[command(
    name = "pramen",
    version,
    about = "Lean, columnar data movement with governed LLM enrichment"
)]
struct Cli {
    /// Log output format.
    #[arg(long, global = true, default_value = "pretty")]
    log_format: LogFormatArg,
    #[command(subcommand)]
    command: Command,
}

/// Clap-friendly wrapper for [`LogFormat`].
#[derive(Clone, Copy, clap::ValueEnum)]
enum LogFormatArg {
    /// Human-oriented output.
    Pretty,
    /// One JSON object per line.
    Json,
    /// No log output.
    Silent,
}

impl From<LogFormatArg> for LogFormat {
    fn from(arg: LogFormatArg) -> Self {
        match arg {
            LogFormatArg::Pretty => Self::Pretty,
            LogFormatArg::Json => Self::Json,
            LogFormatArg::Silent => Self::Silent,
        }
    }
}

#[derive(Subcommand)]
enum Command {
    /// Check a pipeline document and report every problem found.
    Validate {
        /// Path to the pipeline YAML document.
        file: PathBuf,
    },
    /// Show the resolved plan for a pipeline document.
    Explain {
        /// Path to the pipeline YAML document.
        file: PathBuf,
        /// Emit the plan as JSON instead of text.
        #[arg(long)]
        json: bool,
    },
    /// Execute a pipeline.
    Run {
        /// Path to the pipeline YAML document.
        file: PathBuf,
    },
    /// AI governance utilities.
    Ai {
        #[command(subcommand)]
        command: AiCommand,
    },
}

#[derive(Subcommand)]
enum AiCommand {
    /// Show the inference ledger's work-item counts by state.
    Status {
        /// Ledger path (defaults to $PRAMEN_LEDGER_PATH or .pramen/ledger.sqlite).
        #[arg(long)]
        ledger: Option<PathBuf>,
    },
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    if let Err(error) = pramen_core::observe::init_logging(cli.log_format.into()) {
        eprintln!("pramen: failed to initialize logging: {error}");
        return ExitCode::FAILURE;
    }
    match cli.command {
        Command::Validate { file } => match load(&file) {
            Ok(spec) => {
                println!(
                    "OK: `{}` is a valid pramen.dev/v1alpha1 pipeline",
                    spec.metadata.name
                );
                ExitCode::SUCCESS
            }
            Err(exit) => exit,
        },
        Command::Explain { file, json } => match load(&file) {
            Ok(spec) => {
                if json {
                    match serde_json::to_string_pretty(&spec) {
                        Ok(text) => println!("{text}"),
                        Err(error) => {
                            eprintln!("pramen: failed to render plan: {error}");
                            return ExitCode::FAILURE;
                        }
                    }
                } else {
                    explain(&spec);
                }
                ExitCode::SUCCESS
            }
            Err(exit) => exit,
        },
        Command::Run { file } => match load(&file) {
            Ok(spec) => match run::execute(&spec) {
                Ok(()) => ExitCode::SUCCESS,
                Err(message) => {
                    eprintln!("pramen: run failed: {message}");
                    ExitCode::FAILURE
                }
            },
            Err(exit) => exit,
        },
        Command::Ai {
            command: AiCommand::Status { ledger },
        } => ai_status(ledger),
    }
}

fn ai_status(ledger: Option<PathBuf>) -> ExitCode {
    let path = ledger.unwrap_or_else(run::ledger_path);
    if !path.exists() {
        println!("no ledger at {} (nothing recorded yet)", path.display());
        return ExitCode::SUCCESS;
    }
    match pramen_ai::ledger::Ledger::open(&path).and_then(|l| l.counts()) {
        Ok((pending, submitted, completed, failed)) => {
            println!("ledger: {}", path.display());
            println!("  pending:   {pending}");
            println!("  submitted: {submitted}");
            println!("  completed: {completed}");
            println!("  failed:    {failed}");
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("pramen: cannot read ledger {}: {error}", path.display());
            ExitCode::FAILURE
        }
    }
}

fn load(file: &PathBuf) -> Result<PipelineSpec, ExitCode> {
    let text = std::fs::read_to_string(file).map_err(|error| {
        eprintln!("pramen: cannot read {}: {error}", file.display());
        ExitCode::FAILURE
    })?;
    spec::parse(&text).map_err(|error| {
        match &error {
            SpecError::Parse(message) => {
                eprintln!("pramen: {}: {message}", file.display());
            }
            SpecError::Invalid(issues) => {
                eprintln!(
                    "pramen: {} has {} validation issue(s):",
                    file.display(),
                    issues.len()
                );
                for issue in issues {
                    eprintln!("  - {issue}");
                }
            }
        }
        ExitCode::from(2)
    })
}

fn explain(spec: &PipelineSpec) {
    println!("pipeline: {}", spec.metadata.name);

    let SourceSpec::ObjectStore { url, format } = &spec.spec.source;
    let format = match format {
        FormatSpec::Parquet => "parquet",
        FormatSpec::Ndjson => "ndjson",
    };
    println!("  source: object_store {url} ({format})");

    for transform in &spec.spec.transforms {
        match transform {
            TransformSpec::Sql(sql) => {
                println!("  transform {}: sql", sql.id);
            }
            TransformSpec::AiExtract(ai) | TransformSpec::AiClassify(ai) => {
                let kind = match transform {
                    TransformSpec::AiExtract(_) => "ai.extract",
                    _ => "ai.classify",
                };
                let model = spec.spec.models.get(&ai.model);
                let provider = model.map_or("?", |m| m.provider.as_str());
                let model_id = model.map_or("?", |m| m.model.as_str());
                println!(
                    "  transform {}: {kind} via {provider}/{model_id}, execution {:?}, {} output field(s), on invalid {:?}",
                    ai.id,
                    ai.execution,
                    ai.output.fields.len(),
                    ai.validation.on_invalid,
                );
            }
        }
    }

    let SinkSpec::Postgres {
        target,
        mode,
        dsn_env,
    } = &spec.spec.sink;
    println!("  sink: postgres {target} (mode {mode:?}, dsn from ${dsn_env})");

    let runtime = &spec.spec.runtime;
    println!(
        "  runtime: target batch {} B, max inflight {} B, checkpoint {}",
        runtime.target_batch_bytes,
        runtime.max_inflight_bytes,
        runtime
            .checkpoint
            .as_ref()
            .map_or("disabled", |c| c.url.as_str()),
    );
}
