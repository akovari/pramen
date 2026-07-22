//! The `pramen` CLI.
//!
//! v1 surface: `validate`, `explain`, `run`, `ai`. Pipelines may combine
//! deterministic SQL steps with governed semantic transforms backed by the
//! durable inference ledger (`pramen ai status` inspects it;
//! `pramen ai evaluate` measures model quality and cost on a golden corpus).

mod dispatch_plan;
mod evaluate;
mod review;
mod run;
mod transform_test;

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
        /// Smoke mode: a bounded rehearsal — cap source rows, clamp every
        /// semantic transform's run-token ceiling, skip checkpointing.
        #[arg(long)]
        smoke: bool,
        /// Row cap for --smoke.
        #[arg(long, default_value_t = 100, requires = "smoke")]
        smoke_rows: usize,
        /// OTLP collector base URL (e.g. http://localhost:4318); the final
        /// run metrics are pushed there over HTTP/protobuf.
        #[arg(long, env = "PRAMEN_OTLP_ENDPOINT")]
        otlp_endpoint: Option<String>,
    },
    /// AI governance utilities.
    Ai {
        #[command(subcommand)]
        command: AiCommand,
    },
    /// WebAssembly transform utilities.
    Transform {
        #[command(subcommand)]
        command: TransformCommand,
    },
}

#[derive(Subcommand)]
enum AiCommand {
    /// Show the inference ledger's work-item counts by state.
    Status {
        /// Ledger location: SQLite path or `postgres://` DSN
        /// (defaults to $PRAMEN_LEDGER_PATH or .pramen/ledger.sqlite).
        #[arg(long)]
        ledger: Option<String>,
    },
    /// Evaluate a model on a golden corpus; write a timestamped report.
    Evaluate {
        /// Corpus YAML path.
        #[arg(long, default_value = "corpora/support-tickets.v1.yaml")]
        corpus: PathBuf,
        /// Provider adapter: mock, openai-compat, or bedrock.
        #[arg(long, default_value = "mock")]
        provider: String,
        /// Model identifier.
        #[arg(long, default_value = "mock-1")]
        model: String,
        /// Endpoint (required for openai-compat, e.g. http://localhost:11434/v1).
        #[arg(long)]
        endpoint: Option<String>,
        /// Provider region pin (bedrock).
        #[arg(long)]
        region: Option<String>,
        /// Evaluate only the first N items.
        #[arg(long)]
        limit: Option<usize>,
        /// Results root directory; each run writes a fresh subdirectory.
        #[arg(long, default_value = ".pramen/eval")]
        out: PathBuf,
        /// USD per million input tokens (adds a cost estimate).
        #[arg(long)]
        input_price: Option<f64>,
        /// USD per million output tokens.
        #[arg(long)]
        output_price: Option<f64>,
    },
    /// The review queue: records withheld by `onInvalid: review`.
    Review {
        #[command(subcommand)]
        command: ReviewCommand,
        /// Ledger location: SQLite path or `postgres://` DSN
        /// (defaults to $PRAMEN_LEDGER_PATH or .pramen/ledger.sqlite).
        #[arg(long, global = true)]
        ledger: Option<String>,
    },
    /// Plan online vs provider-batch dispatch under a deadline (E2.1).
    DispatchPlan {
        /// Rate card: mock, openai-compat-stub, or bedrock-illustrative.
        #[arg(long, default_value = "mock")]
        rate_card: String,
        /// Expected ledger-miss record count.
        #[arg(long, default_value_t = 10_000)]
        records: u64,
        /// Wall-clock deadline in seconds.
        #[arg(long, default_value_t = 3_600)]
        deadline_seconds: u64,
        /// Assumed input tokens per record.
        #[arg(long, default_value_t = 800)]
        input_tokens: u64,
        /// Assumed output tokens per record.
        #[arg(long, default_value_t = 200)]
        output_tokens: u64,
        /// Sweep volumes × deadlines × mock/stub rate cards.
        #[arg(long)]
        sweep: bool,
        /// Write the sweep Markdown report to this path.
        #[arg(long)]
        out: Option<PathBuf>,
        /// Emit a single plan as JSON.
        #[arg(long)]
        json: bool,
    },
}

#[derive(Subcommand)]
enum TransformCommand {
    /// Run a fixture batch through a component with production limits.
    Test {
        /// Path to the `.wasm` component (defaults to the S1.4 conformance fixture).
        #[arg(long)]
        component: Option<PathBuf>,
        /// Rows in the synthetic fixture batch.
        #[arg(long, default_value_t = 8_192)]
        rows: usize,
    },
}

#[derive(Subcommand)]
enum ReviewCommand {
    /// Show pending items awaiting a decision.
    List,
    /// Emit pending items as JSONL (one self-contained object per item).
    Export,
    /// Accept a corrected output; it is schema-validated and recorded in
    /// the ledger as a completed human-review result.
    Accept {
        /// Work key (a unique prefix is enough).
        #[arg(long)]
        key: String,
        /// The corrected output as JSON, matching the declared fields.
        #[arg(long)]
        output: String,
    },
    /// Permanently drop a queued record.
    Reject {
        /// Work key (a unique prefix is enough).
        #[arg(long)]
        key: String,
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
        Command::Run {
            file,
            smoke,
            smoke_rows,
            otlp_endpoint,
        } => match load(&file) {
            Ok(spec) => {
                let smoke = smoke.then_some(run::SmokeOptions { rows: smoke_rows });
                match run::execute(&spec, Some(&file), smoke, otlp_endpoint.as_deref()) {
                    Ok(()) => ExitCode::SUCCESS,
                    Err(message) => {
                        eprintln!("pramen: run failed: {message}");
                        ExitCode::FAILURE
                    }
                }
            }
            Err(exit) => exit,
        },
        Command::Ai {
            command: AiCommand::Status { ledger },
        } => ai_status(ledger),
        Command::Ai {
            command:
                AiCommand::Evaluate {
                    corpus,
                    provider,
                    model,
                    endpoint,
                    region,
                    limit,
                    out,
                    input_price,
                    output_price,
                },
        } => {
            let args = evaluate::EvaluateArgs {
                corpus,
                provider,
                model,
                endpoint,
                region,
                limit,
                out,
                input_price,
                output_price,
            };
            match evaluate::execute(&args) {
                Ok(()) => ExitCode::SUCCESS,
                Err(message) => {
                    eprintln!("pramen: ai evaluate failed: {message}");
                    ExitCode::FAILURE
                }
            }
        }
        Command::Ai {
            command: AiCommand::Review { command, ledger },
        } => {
            let location = ledger.unwrap_or_else(run::ledger_location);
            let result = match command {
                ReviewCommand::List => review::list(&location),
                ReviewCommand::Export => review::export(&location),
                ReviewCommand::Accept { key, output } => review::accept(&location, &key, &output),
                ReviewCommand::Reject { key } => review::reject(&location, &key),
            };
            match result {
                Ok(()) => ExitCode::SUCCESS,
                Err(message) => {
                    eprintln!("pramen: ai review failed: {message}");
                    ExitCode::FAILURE
                }
            }
        }
        Command::Ai {
            command:
                AiCommand::DispatchPlan {
                    rate_card,
                    records,
                    deadline_seconds,
                    input_tokens,
                    output_tokens,
                    sweep,
                    out,
                    json,
                },
        } => {
            let args = dispatch_plan::DispatchPlanArgs {
                rate_card,
                records,
                deadline_seconds,
                input_tokens,
                output_tokens,
                sweep,
                out,
                json,
            };
            match dispatch_plan::execute(&args) {
                Ok(()) => ExitCode::SUCCESS,
                Err(message) => {
                    eprintln!("pramen: ai dispatch-plan failed: {message}");
                    ExitCode::FAILURE
                }
            }
        }
        Command::Transform {
            command: TransformCommand::Test { component, rows },
        } => {
            let args = transform_test::TransformTestArgs {
                component: component.unwrap_or_else(transform_test::default_component),
                rows,
            };
            match transform_test::execute(&args) {
                Ok(()) => ExitCode::SUCCESS,
                Err(message) => {
                    eprintln!("pramen: transform test failed: {message}");
                    ExitCode::FAILURE
                }
            }
        }
    }
}

fn ai_status(ledger: Option<String>) -> ExitCode {
    let location = ledger.unwrap_or_else(run::ledger_location);
    if !pramen_core::checkpoint::is_postgres_url(&location)
        && !std::path::Path::new(&location).exists()
    {
        println!("no ledger at {location} (nothing recorded yet)");
        return ExitCode::SUCCESS;
    }
    match pramen_ai::ledger::Ledger::open_location(&location)
        .and_then(|l| Ok((l.counts()?, l.review_counts()?)))
    {
        Ok(((pending, submitted, completed, failed), (in_review, accepted, rejected))) => {
            println!("ledger: {location}");
            println!("  pending:   {pending}");
            println!("  submitted: {submitted}");
            println!("  completed: {completed}");
            println!("  failed:    {failed}");
            println!("  review:    {in_review} pending, {accepted} accepted, {rejected} rejected");
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("pramen: cannot read ledger {location}: {error}");
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

    let SourceSpec::ObjectStore { url, format, .. } = &spec.spec.source;
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
            TransformSpec::AiExtract(ai)
            | TransformSpec::AiClassify(ai)
            | TransformSpec::AiGenerate(ai) => {
                let kind = match transform {
                    TransformSpec::AiExtract(_) => "ai.extract",
                    TransformSpec::AiClassify(_) => "ai.classify",
                    TransformSpec::AiGenerate(_) => "ai.generate",
                    _ => unreachable!("matched only AI transforms"),
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
            TransformSpec::Wasm(wasm) => {
                println!("  transform {}: wasm component {}", wasm.id, wasm.component);
            }
        }
    }

    for resolved in spec.spec.resolved_sinks() {
        match resolved.sink {
            SinkSpec::Postgres {
                target,
                mode,
                keys,
                dsn_env,
            } => {
                let keys_note = if keys.is_empty() {
                    String::new()
                } else {
                    format!(" on [{}]", keys.join(", "))
                };
                println!(
                    "  sink {}: postgres {target} (mode {mode:?}{keys_note}, from {}, dsn from ${dsn_env})",
                    resolved.id, resolved.from
                );
            }
            SinkSpec::FlightSql {
                endpoint,
                target,
                mode,
                token_env,
            } => {
                println!(
                    "  sink {}: flightSql {endpoint} → {target} (mode {mode:?}, from {}, token from ${token_env})",
                    resolved.id, resolved.from
                );
            }
        }
    }

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
