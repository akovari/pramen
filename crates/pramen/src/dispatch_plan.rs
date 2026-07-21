//! The `pramen ai dispatch-plan` command: run the online-vs-batch cost
//! model for one workload or sweep the offline frontier (E2.1 / RQ1).

use pramen_ai::dispatch::{
    self, FRONTIER_CARDS, FRONTIER_DEADLINES_SECS, FRONTIER_VOLUMES, RateCard, RateCardId, Workload,
};
use std::io::Write;
use std::path::PathBuf;

/// Arguments for a single-point plan or a frontier sweep.
pub struct DispatchPlanArgs {
    /// Named rate card (`mock`, `openai-compat-stub`, `bedrock-illustrative`).
    pub rate_card: String,
    /// Expected ledger-miss records.
    pub records: u64,
    /// Deadline in seconds.
    pub deadline_seconds: u64,
    /// Input tokens per record.
    pub input_tokens: u64,
    /// Output tokens per record.
    pub output_tokens: u64,
    /// When set, sweep the default (or custom) grid and write Markdown.
    pub sweep: bool,
    /// Optional path for the sweep Markdown report.
    pub out: Option<PathBuf>,
    /// Emit JSON for a single plan (ignored with `--sweep`).
    pub json: bool,
}

/// Run a single plan or the frontier sweep.
///
/// # Errors
///
/// Returns a human-readable message for unknown rate cards or I/O failures.
pub fn execute(args: &DispatchPlanArgs) -> Result<(), String> {
    if args.sweep {
        return execute_sweep(args);
    }
    execute_one(args)
}

fn execute_one(args: &DispatchPlanArgs) -> Result<(), String> {
    let card_id = RateCardId::parse(&args.rate_card).ok_or_else(|| {
        format!(
            "unknown rate card `{}` (available: mock, openai-compat-stub, bedrock-illustrative)",
            args.rate_card
        )
    })?;
    let card = RateCard::builtin(card_id);
    let workload = Workload {
        records: args.records,
        input_tokens_per_record: args.input_tokens,
        output_tokens_per_record: args.output_tokens,
        deadline_seconds: args.deadline_seconds as f64,
    };
    let planned = dispatch::plan(&workload, &card, card.batch.is_some());

    if args.json {
        let batch = planned.batch.as_ref();
        let value = serde_json::json!({
            "rateCard": card_id.as_str(),
            "records": args.records,
            "deadlineSeconds": args.deadline_seconds,
            "recommended": planned.recommended.as_str(),
            "reason": planned.reason,
            "online": {
                "costUsd": planned.online.cost_usd,
                "latencySeconds": planned.online.latency_seconds,
                "meetsDeadline": planned.online.meets_deadline,
            },
            "batch": batch.map(|b| serde_json::json!({
                "costUsd": b.cost_usd,
                "latencySeconds": b.latency_seconds,
                "meetsDeadline": b.meets_deadline,
            })),
        });
        println!(
            "{}",
            serde_json::to_string_pretty(&value).map_err(|e| e.to_string())?
        );
        return Ok(());
    }

    println!("rate card: {}", card_id.as_str());
    println!(
        "workload: {} records, {}s deadline, {}/{} tokens in/out per record",
        args.records, args.deadline_seconds, args.input_tokens, args.output_tokens
    );
    println!(
        "online:  ${:.4}  ~{:.0}s  (deadline {})",
        planned.online.cost_usd,
        planned.online.latency_seconds,
        if planned.online.meets_deadline {
            "ok"
        } else {
            "miss"
        }
    );
    if let Some(batch) = &planned.batch {
        println!(
            "batch:   ${:.4}  ~{:.0}s  (deadline {})",
            batch.cost_usd,
            batch.latency_seconds,
            if batch.meets_deadline { "ok" } else { "miss" }
        );
    } else {
        println!("batch:   (unavailable)");
    }
    println!("recommended: {}", planned.recommended);
    println!("reason: {}", planned.reason);
    Ok(())
}

fn execute_sweep(args: &DispatchPlanArgs) -> Result<(), String> {
    let rows = dispatch::sweep_frontier(FRONTIER_CARDS, FRONTIER_VOLUMES, FRONTIER_DEADLINES_SECS);
    let table = dispatch::render_frontier_markdown(&rows);
    let report = format!(
        "# E2.1 dispatch frontier (offline / mock-calibrated)\n\
         \n\
         **Label: mock/stub-measured analytical frontier — not live Bedrock.**\n\
         Reopen when S2.2 live provider numbers exist.\n\
         \n\
         ## Method\n\
         \n\
         The [`pramen_ai::dispatch`] cost model estimates online vs\n\
         provider-batch USD cost and wall-clock latency for each\n\
         (rate card × record volume × deadline) cell, then recommends the\n\
         cheaper mode that still meets the deadline. Token assumptions:\n\
         {} input / {} output per record. Rate cards:\n\
         \n\
         - `mock` — 50% batch discount, ~60s synthetic completion window\n\
         - `openai-compat-stub` — 50% batch discount, 1h completion window\n\
         \n\
         Regenerate:\n\
         \n\
         ```bash\n\
         pramen ai dispatch-plan --sweep --out docs/research/e2-1-dispatch-frontier.md\n\
         ```\n\
         \n\
         ## Frontier\n\
         \n\
         {table}",
        Workload::DEFAULT_INPUT_TOKENS,
        Workload::DEFAULT_OUTPUT_TOKENS,
    );

    if let Some(path) = &args.out {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("cannot create {}: {e}", parent.display()))?;
        }
        let mut file = std::fs::File::create(path)
            .map_err(|e| format!("cannot write {}: {e}", path.display()))?;
        file.write_all(report.as_bytes())
            .map_err(|e| format!("cannot write {}: {e}", path.display()))?;
        println!("wrote {} ({} rows)", path.display(), rows.len());
    } else {
        print!("{report}");
    }
    Ok(())
}
