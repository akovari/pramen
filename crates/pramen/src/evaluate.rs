//! The `pramen ai evaluate` command: run a golden corpus against a model
//! and write a diffable, timestamped report (S2.2 / P1.17).

use pramen_ai::eval::{self, Corpus, EvalReport, ItemResult, Prices};
use pramen_core::spec::ModelSpec;
use std::io::Write;
use std::path::{Path, PathBuf};

/// Everything `ai evaluate` needs, resolved from CLI flags.
pub struct EvaluateArgs {
    /// Corpus YAML path.
    pub corpus: PathBuf,
    /// Provider adapter id.
    pub provider: String,
    /// Model identifier.
    pub model: String,
    /// Endpoint override (openai-compat, stubbed bedrock).
    pub endpoint: Option<String>,
    /// Region pin (bedrock).
    pub region: Option<String>,
    /// Evaluate only the first N items.
    pub limit: Option<usize>,
    /// Results root; each run writes a fresh subdirectory.
    pub out: PathBuf,
    /// USD per million input tokens.
    pub input_price: Option<f64>,
    /// USD per million output tokens.
    pub output_price: Option<f64>,
}

/// Load, evaluate, report, persist.
///
/// # Errors
///
/// Returns a human-readable message when the corpus cannot be read, the
/// provider fails, or results cannot be written.
pub fn execute(args: &EvaluateArgs) -> Result<(), String> {
    let text = std::fs::read_to_string(&args.corpus)
        .map_err(|error| format!("cannot read corpus {}: {error}", args.corpus.display()))?;
    let corpus = Corpus::from_yaml(&text).map_err(|error| error.to_string())?;

    let runtime = tokio::runtime::Runtime::new()
        .map_err(|error| format!("failed to start async runtime: {error}"))?;
    let (report, items) = runtime.block_on(async {
        let model = ModelSpec {
            provider: args.provider.clone(),
            model: args.model.clone(),
            region: args.region.clone(),
            endpoint: args.endpoint.clone(),
            // Evaluation always measures online round trips.
            batch: None,
        };
        let provider = crate::run::plan_provider("ai evaluate", &model).await?;
        eval::run_eval(
            &corpus,
            provider.as_ref(),
            &args.model,
            args.limit,
            Prices {
                input_per_mtok: args.input_price,
                output_per_mtok: args.output_price,
            },
        )
        .await
        .map_err(|error| error.to_string())
    })?;

    let dir = results_dir(&args.out, &report);
    persist(&dir, &report, &items)
        .map_err(|error| format!("cannot write results to {}: {error}", dir.display()))?;

    print!("{}", eval::render_text(&report));
    println!("results: {}", dir.display());
    Ok(())
}

/// `<out>/<UTC timestamp>-<provider>-<model>` with path-hostile
/// characters in the model id flattened.
fn results_dir(out: &Path, report: &EvalReport) -> PathBuf {
    let model: String = report
        .model
        .chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect();
    out.join(format!("{}-{}-{model}", utc_timestamp(), report.provider))
}

fn persist(
    dir: &Path,
    report: &EvalReport,
    items: &[ItemResult],
) -> Result<(), Box<dyn std::error::Error>> {
    std::fs::create_dir_all(dir)?;
    std::fs::write(
        dir.join("report.json"),
        serde_json::to_string_pretty(report)?,
    )?;
    let mut jsonl = std::io::BufWriter::new(std::fs::File::create(dir.join("items.jsonl"))?);
    for item in items {
        serde_json::to_writer(&mut jsonl, item)?;
        jsonl.write_all(b"\n")?;
    }
    jsonl.flush()?;
    Ok(())
}

/// Compact UTC timestamp (`20260712T094500Z`) without a date-time
/// dependency; civil-from-days per Howard Hinnant's algorithm.
fn utc_timestamp() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or_default();
    let days = i64::try_from(secs / 86_400).unwrap_or_default();
    let (hh, mm, ss) = (secs % 86_400 / 3_600, secs % 3_600 / 60, secs % 60);
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!("{year:04}{month:02}{day:02}T{hh:02}{mm:02}{ss:02}Z")
}

#[cfg(test)]
mod tests {
    use super::utc_timestamp;

    #[test]
    fn timestamp_is_well_formed() {
        let ts = utc_timestamp();
        assert_eq!(ts.len(), 16, "{ts}");
        assert_eq!(&ts[8..9], "T");
        assert!(ts.ends_with('Z'));
        assert!(ts.starts_with("20"), "{ts}");
    }
}
