//! Regenerate the RQ2 memoization metrics artifacts (task E2.2).
//!
//! Offline-only: mock provider + temporary SQLite ledgers. Writes
//! `rq2-memoization-metrics.json` and prints the markdown table that
//! `docs/research/rq2-memoization.md` embeds.
//!
//! ```console
//! ./scripts/rq2-memoization.sh
//! # or:
//! cargo run -p pramen-ai --example rq2_memoization -- \
//!   --json docs/research/rq2-memoization-metrics.json
//! ```

use pramen_ai::reuse::{self, SuiteReport};
use std::env;
use std::path::PathBuf;
use std::process::ExitCode;

fn main() -> ExitCode {
    let args: Vec<String> = env::args().skip(1).collect();
    let mut json_path = PathBuf::from("docs/research/rq2-memoization-metrics.json");
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--json" => {
                i += 1;
                let Some(path) = args.get(i) else {
                    eprintln!("rq2_memoization: --json requires a path");
                    return ExitCode::FAILURE;
                };
                json_path = PathBuf::from(path);
            }
            "-h" | "--help" => {
                println!(
                    "Usage: rq2_memoization [--json <path>]\n\
                     \n\
                     Runs the offline RQ2 memoization suite and writes JSON metrics."
                );
                return ExitCode::SUCCESS;
            }
            other => {
                eprintln!("rq2_memoization: unknown argument `{other}`");
                return ExitCode::FAILURE;
            }
        }
        i += 1;
    }

    let runtime = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(error) => {
            eprintln!("rq2_memoization: failed to start runtime: {error}");
            return ExitCode::FAILURE;
        }
    };

    let report: SuiteReport = match runtime.block_on(reuse::run_suite()) {
        Ok(report) => report,
        Err(error) => {
            eprintln!("rq2_memoization: suite failed: {error}");
            return ExitCode::FAILURE;
        }
    };

    if let Err(error) = reuse::publish_metrics(&report, &json_path) {
        eprintln!("rq2_memoization: {error}");
        return ExitCode::FAILURE;
    }

    println!("# RQ2 memoization metrics ({})", report.task);
    println!();
    print!("{}", report.to_markdown_table());
    println!();
    println!("wrote {}", json_path.display());
    ExitCode::SUCCESS
}
