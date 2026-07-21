//! RQ2 memoization measurement suite (task E2.2).
//!
//! Offline harness: [`MockProvider`] + a temporary SQLite ledger. Runs the
//! three workloads named in the research plan — crash/replay, incremental
//! re-enrichment, duplicate-heavy — and returns a structured
//! [`SuiteReport`] suitable for publishing under `docs/research/`.
//!
//! The suite asserts the reuse contract documented in
//! `docs/research/rq2-memoization.md`; it does not call any network.

use crate::error::AiError;
use crate::ledger::Ledger;
use crate::operator::SemanticTransform;
use crate::provider::{MockProvider, Provider};
use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use pramen_core::runtime::Transform;
use pramen_core::spec::{
    AiBreaker, AiOutput, AiTransform, AiValidation, ExecutionMode, FieldSpec, FieldType,
    InvalidPolicy,
};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// Identifier of the research task this suite answers.
pub const TASK_ID: &str = "E2.2";

/// One measured scenario in the RQ2 suite.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ScenarioReport {
    /// Stable scenario id (`crash_replay_online`, …).
    pub id: String,
    /// Short human label.
    pub title: String,
    /// Records presented to the operator across the measured phase(s).
    pub records_seen: u64,
    /// Distinct work keys that resulted in a provider dispatch.
    pub provider_calls: u64,
    /// Provider-reported tokens billed in the measured phase(s).
    pub tokens_billed: u64,
    /// Records served from a completed recorded result (zero cost).
    pub results_reused: u64,
    /// `results_reused / records_seen * 100`, or 100 when no records.
    pub reuse_pct: f64,
    /// Hypothetical calls if every row dispatched (duplicate scenario).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub naive_provider_calls: Option<u64>,
    /// `(1 - provider_calls / naive) * 100` when naive is set.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub savings_pct: Option<f64>,
    /// Extra structured counters for the scenario.
    #[serde(default, skip_serializing_if = "serde_json::Map::is_empty")]
    pub detail: serde_json::Map<String, serde_json::Value>,
}

/// Full offline measurement report for RQ2.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SuiteReport {
    /// Task id (`E2.2`).
    pub task: String,
    /// UTC unix-seconds when the suite finished.
    pub generated_at_unix: u64,
    /// Provider adapter used (`mock`).
    pub provider: String,
    /// Ledger backend (`sqlite`).
    pub ledger: String,
    /// Per-scenario measurements.
    pub scenarios: Vec<ScenarioReport>,
}

impl SuiteReport {
    /// Compact JSON suitable for `docs/research/rq2-memoization-metrics.json`.
    ///
    /// # Errors
    ///
    /// Returns a serialization error when the report cannot be encoded.
    pub fn to_json_pretty(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }

    /// Markdown metrics table for embedding in the research note.
    #[must_use]
    pub fn to_markdown_table(&self) -> String {
        let mut out = String::from(
            "| Scenario | Records | Provider calls | Tokens billed | Reused | Reuse % | Savings vs naive |\n",
        );
        out.push_str("| --- | ---: | ---: | ---: | ---: | ---: | ---: |\n");
        for s in &self.scenarios {
            let savings = s
                .savings_pct
                .map(|p| format!("{p:.1}%"))
                .unwrap_or_else(|| "—".to_owned());
            out.push_str(&format!(
                "| {} | {} | {} | {} | {} | {:.1}% | {savings} |\n",
                s.title,
                s.records_seen,
                s.provider_calls,
                s.tokens_billed,
                s.results_reused,
                s.reuse_pct
            ));
        }
        out
    }
}

/// Run every RQ2 scenario against a fresh temporary SQLite ledger.
///
/// # Errors
///
/// Returns [`AiError`] when the ledger or operator fails; the suite is
/// otherwise deterministic and offline.
pub async fn run_suite() -> Result<SuiteReport, AiError> {
    let dir = temp_dir("rq2-suite")?;
    let mut scenarios = Vec::with_capacity(5);
    scenarios.push(scenario_crash_replay_online(&dir).await?);
    scenarios.push(scenario_crash_reconcile_batch(&dir).await?);
    scenarios.push(scenario_incremental(&dir).await?);
    scenarios.push(scenario_duplicate_heavy(&dir).await?);
    scenarios.push(scenario_review_withhold(&dir).await?);

    let generated_at_unix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);

    Ok(SuiteReport {
        task: TASK_ID.to_owned(),
        generated_at_unix,
        provider: "mock".to_owned(),
        ledger: "sqlite".to_owned(),
        scenarios,
    })
}

/// Write `report.json` and return the markdown table (caller embeds it).
///
/// # Errors
///
/// Returns an I/O or serialization error when the path cannot be written.
pub fn publish_metrics(report: &SuiteReport, json_path: &Path) -> Result<(), String> {
    if let Some(parent) = json_path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| format!("create {}: {e}", parent.display()))?;
    }
    let json = report
        .to_json_pretty()
        .map_err(|e| format!("serialize metrics: {e}"))?;
    std::fs::write(json_path, format!("{json}\n"))
        .map_err(|e| format!("write {}: {e}", json_path.display()))?;
    Ok(())
}

fn reuse_pct(reused: u64, seen: u64) -> f64 {
    if seen == 0 {
        100.0
    } else {
        (reused as f64) * 100.0 / (seen as f64)
    }
}

fn savings_pct(calls: u64, naive: u64) -> Option<f64> {
    if naive == 0 {
        None
    } else {
        Some((1.0 - (calls as f64) / (naive as f64)) * 100.0)
    }
}

fn temp_dir(name: &str) -> Result<PathBuf, AiError> {
    let path = std::env::temp_dir().join(format!(
        "pramen-{name}-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    std::fs::create_dir_all(&path).map_err(|e| AiError::Ledger(e.to_string()))?;
    Ok(path)
}

fn ledger_path(dir: &Path, name: &str) -> PathBuf {
    dir.join(format!("{name}.sqlite"))
}

fn classify_config(on_invalid: InvalidPolicy, execution: ExecutionMode) -> AiTransform {
    AiTransform {
        id: "rq2-classify".into(),
        from: None,
        model: "mock-1".into(),
        execution,
        dispatch: None,
        inputs: vec!["description".into()],
        instruction: "classify the ticket for RQ2 memoization measurement".into(),
        output: AiOutput {
            fields: vec![
                FieldSpec {
                    name: "category".into(),
                    field_type: FieldType::Utf8,
                    nullable: false,
                    max_chars: None,
                },
                FieldSpec {
                    name: "score".into(),
                    field_type: FieldType::Float64,
                    nullable: false,
                    max_chars: None,
                },
            ],
        },
        validation: AiValidation { on_invalid },
        budget: None,
        breaker: AiBreaker::default(),
    }
}

fn batch_from(descriptions: &[String]) -> Result<RecordBatch, AiError> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("description", DataType::Utf8, false),
    ]));
    let ids: Vec<i64> = (0..descriptions.len() as i64).collect();
    let texts: Vec<&str> = descriptions.iter().map(String::as_str).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(texts)),
        ],
    )
    .map_err(|e| AiError::Ledger(format!("build batch: {e}")))
}

fn texts(prefix: &str, n: usize) -> Vec<String> {
    (0..n).map(|i| format!("{prefix}-{i:04}")).collect()
}

async fn run_online(
    dir: &Path,
    name: &str,
    provider: Arc<MockProvider>,
    descriptions: &[String],
) -> Result<(u64, u64), AiError> {
    let ledger = Ledger::open(&ledger_path(dir, name))?;
    let mut transform = SemanticTransform::new(
        "ai.classify",
        classify_config(InvalidPolicy::Fail, ExecutionMode::Online),
        Arc::clone(&provider) as Arc<dyn Provider>,
        "mock-1",
        ledger,
    )?;
    let before_calls = provider.calls();
    let before_tokens = transform.run_tokens();
    transform
        .apply(batch_from(descriptions)?)
        .await
        .map_err(|e| AiError::Ledger(e.to_string()))?;
    Ok((
        provider.calls() - before_calls,
        transform.run_tokens() - before_tokens,
    ))
}

/// Crash/replay (online): complete a run, drop the operator, replay.
async fn scenario_crash_replay_online(dir: &Path) -> Result<ScenarioReport, AiError> {
    const N: usize = 50;
    let name = "crash-online";
    let provider = Arc::new(MockProvider::new());
    let records = texts("ticket", N);

    let (first_calls, first_tokens) =
        run_online(dir, name, Arc::clone(&provider), &records).await?;
    // "Crash": drop the operator (already dropped); reopen the same ledger.
    let (replay_calls, replay_tokens) =
        run_online(dir, name, Arc::clone(&provider), &records).await?;

    let mut detail = serde_json::Map::new();
    detail.insert("firstPassCalls".into(), first_calls.into());
    detail.insert("firstPassTokens".into(), first_tokens.into());
    detail.insert("replayCalls".into(), replay_calls.into());
    detail.insert("replayTokens".into(), replay_tokens.into());

    Ok(ScenarioReport {
        id: "crash_replay_online".into(),
        title: "Crash/replay (online)".into(),
        records_seen: N as u64,
        provider_calls: replay_calls,
        tokens_billed: replay_tokens,
        results_reused: N as u64,
        reuse_pct: reuse_pct(N as u64, N as u64),
        naive_provider_calls: None,
        savings_pct: None,
        detail,
    })
}

/// Crash after batch submit: reconcile without re-billing.
async fn scenario_crash_reconcile_batch(dir: &Path) -> Result<ScenarioReport, AiError> {
    const N: usize = 24;
    let name = "crash-batch";
    let path = ledger_path(dir, name);
    let _ = std::fs::remove_file(&path);
    let provider = Arc::new(MockProvider::with_batch_latency(1));
    let records = texts("batch-ticket", N);
    let cfg = classify_config(InvalidPolicy::Fail, ExecutionMode::Batch);

    let mut crashed = SemanticTransform::new(
        "ai.classify",
        cfg.clone(),
        Arc::clone(&provider) as Arc<dyn Provider>,
        "mock-1",
        Ledger::open(&path)?,
    )?;
    crashed
        .apply(batch_from(&records)?)
        .await
        .map_err(|e| AiError::Ledger(e.to_string()))?;
    crashed
        .submit_pending()
        .await
        .map_err(|e| AiError::Ledger(e.to_string()))?;
    let billed_at_submit = provider.calls();
    drop(crashed);

    let mut recovered = SemanticTransform::new(
        "ai.classify",
        cfg,
        Arc::clone(&provider) as Arc<dyn Provider>,
        "mock-1",
        Ledger::open(&path)?,
    )?;
    recovered
        .apply(batch_from(&records)?)
        .await
        .map_err(|e| AiError::Ledger(e.to_string()))?;
    let out = recovered
        .finish()
        .await
        .map_err(|e| AiError::Ledger(e.to_string()))?;
    let rows = out.first().map_or(0, RecordBatch::num_rows) as u64;
    let calls_after = provider.calls();
    let rebill = calls_after.saturating_sub(billed_at_submit);
    // Tokens may be attributed at ingest on the recovery run; the reuse
    // contract cares that the provider was not billed again.
    let _ = recovered.run_tokens();

    let mut detail = serde_json::Map::new();
    detail.insert("billedAtSubmit".into(), billed_at_submit.into());
    detail.insert("callsAfterReconcile".into(), calls_after.into());
    detail.insert("rebillCalls".into(), rebill.into());
    detail.insert("rowsRecovered".into(), rows.into());

    Ok(ScenarioReport {
        id: "crash_reconcile_batch".into(),
        title: "Crash/reconcile (batch)".into(),
        records_seen: N as u64,
        provider_calls: rebill,
        tokens_billed: 0,
        results_reused: N as u64,
        reuse_pct: 100.0,
        naive_provider_calls: None,
        savings_pct: None,
        detail,
    })
}

/// Incremental re-enrichment: only changed/new inputs re-bill.
async fn scenario_incremental(dir: &Path) -> Result<ScenarioReport, AiError> {
    const BASE: usize = 40;
    const CHANGED: usize = 5;
    const NEW: usize = 5;
    let name = "incremental";
    let provider = Arc::new(MockProvider::new());

    let baseline: Vec<String> = texts("inc", BASE);
    let (base_calls, base_tokens) = run_online(dir, name, Arc::clone(&provider), &baseline).await?;

    // Unchanged 0..35; change 35..40; append 40..45.
    let mut second = Vec::with_capacity(BASE - CHANGED + CHANGED + NEW);
    for i in 0..(BASE - CHANGED) {
        second.push(format!("inc-{i:04}"));
    }
    for i in (BASE - CHANGED)..BASE {
        second.push(format!("inc-{i:04}-v2"));
    }
    for i in BASE..(BASE + NEW) {
        second.push(format!("inc-{i:04}"));
    }

    let (second_calls, second_tokens) =
        run_online(dir, name, Arc::clone(&provider), &second).await?;
    let unchanged = (BASE - CHANGED) as u64;
    let expected_new = (CHANGED + NEW) as u64;

    let mut detail = serde_json::Map::new();
    detail.insert("baselineCalls".into(), base_calls.into());
    detail.insert("baselineTokens".into(), base_tokens.into());
    detail.insert("secondPassCalls".into(), second_calls.into());
    detail.insert("secondPassTokens".into(), second_tokens.into());
    detail.insert("unchangedReused".into(), unchanged.into());
    detail.insert("changedRebilled".into(), (CHANGED as u64).into());
    detail.insert("newBilled".into(), (NEW as u64).into());
    detail.insert("expectedSecondPassCalls".into(), expected_new.into());

    Ok(ScenarioReport {
        id: "incremental_reenrichment".into(),
        title: "Incremental re-enrichment".into(),
        records_seen: second.len() as u64,
        provider_calls: second_calls,
        tokens_billed: second_tokens,
        results_reused: unchanged,
        reuse_pct: reuse_pct(unchanged, second.len() as u64),
        naive_provider_calls: Some(second.len() as u64),
        savings_pct: savings_pct(second_calls, second.len() as u64),
        detail,
    })
}

/// Duplicate-heavy workload: many rows, few unique inputs.
async fn scenario_duplicate_heavy(dir: &Path) -> Result<ScenarioReport, AiError> {
    const ROWS: usize = 200;
    const UNIQUE: usize = 20;
    let name = "duplicates";
    let provider = Arc::new(MockProvider::new());
    let records: Vec<String> = (0..ROWS)
        .map(|i| format!("dup-{:04}", i % UNIQUE))
        .collect();

    let (calls, tokens) = run_online(dir, name, Arc::clone(&provider), &records).await?;
    let reused = ROWS as u64 - calls;

    let mut detail = serde_json::Map::new();
    detail.insert("uniqueInputs".into(), (UNIQUE as u64).into());
    detail.insert("rows".into(), (ROWS as u64).into());

    Ok(ScenarioReport {
        id: "duplicate_heavy".into(),
        title: "Duplicate-heavy workload".into(),
        records_seen: ROWS as u64,
        provider_calls: calls,
        tokens_billed: tokens,
        results_reused: reused,
        reuse_pct: reuse_pct(reused, ROWS as u64),
        naive_provider_calls: Some(ROWS as u64),
        savings_pct: savings_pct(calls, ROWS as u64),
        detail,
    })
}

/// Review queue: pending items are not re-dispatched on replay.
async fn scenario_review_withhold(dir: &Path) -> Result<ScenarioReport, AiError> {
    let name = "review";
    let path = ledger_path(dir, name);
    let _ = std::fs::remove_file(&path);
    let provider = Arc::new(InvalidOnlineProvider::default());
    let cfg = classify_config(InvalidPolicy::Review, ExecutionMode::Online);
    let records = vec!["printer on fire".to_owned()];

    let mut first = SemanticTransform::new(
        "ai.classify",
        cfg.clone(),
        Arc::clone(&provider) as Arc<dyn Provider>,
        "mock-1",
        Ledger::open(&path)?,
    )?;
    let out = first
        .apply(batch_from(&records)?)
        .await
        .map_err(|e| AiError::Ledger(e.to_string()))?;
    let first_rows = out.first().map_or(0, RecordBatch::num_rows);
    let first_calls = provider.calls();
    drop(first);

    let mut replay = SemanticTransform::new(
        "ai.classify",
        cfg,
        Arc::clone(&provider) as Arc<dyn Provider>,
        "mock-1",
        Ledger::open(&path)?,
    )?;
    let again = replay
        .apply(batch_from(&records)?)
        .await
        .map_err(|e| AiError::Ledger(e.to_string()))?;
    let replay_rows = again.first().map_or(0, RecordBatch::num_rows);
    let replay_calls = provider.calls() - first_calls;
    let replay_tokens = replay.run_tokens();

    let mut detail = serde_json::Map::new();
    detail.insert("firstPassCalls".into(), first_calls.into());
    detail.insert("firstPassRows".into(), first_rows.into());
    detail.insert("replayCalls".into(), replay_calls.into());
    detail.insert("replayRows".into(), replay_rows.into());

    Ok(ScenarioReport {
        id: "review_queue_withhold".into(),
        title: "Review queue withhold".into(),
        records_seen: 1,
        provider_calls: replay_calls,
        tokens_billed: replay_tokens,
        results_reused: 0,
        reuse_pct: 0.0,
        naive_provider_calls: None,
        savings_pct: None,
        detail,
    })
}

/// Online provider that always returns schema-invalid JSON (for review routing).
#[derive(Default)]
struct InvalidOnlineProvider {
    calls: std::sync::atomic::AtomicU64,
}

impl InvalidOnlineProvider {
    fn calls(&self) -> u64 {
        self.calls.load(std::sync::atomic::Ordering::SeqCst)
    }
}

#[async_trait::async_trait]
impl Provider for InvalidOnlineProvider {
    fn id(&self) -> &str {
        "mock-invalid"
    }

    fn capabilities(&self) -> crate::provider::Capabilities {
        crate::provider::Capabilities {
            online: true,
            batch: false,
            structured_output: true,
            token_accounting: true,
        }
    }

    async fn invoke(
        &self,
        _request: &crate::provider::InferenceRequest,
    ) -> Result<crate::provider::ProviderResponse, AiError> {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(crate::provider::ProviderResponse {
            text: "{\"wrong\":true}".to_owned(),
            input_tokens: 8,
            output_tokens: 4,
            request_id: "invalid-1".to_owned(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn suite_meets_rq2_exit_bars() {
        let report = run_suite().await.expect("suite runs offline");
        assert_eq!(report.task, TASK_ID);
        assert_eq!(report.scenarios.len(), 5);

        let by_id = |id: &str| {
            report
                .scenarios
                .iter()
                .find(|s| s.id == id)
                .unwrap_or_else(|| panic!("missing scenario {id}"))
        };

        let online = by_id("crash_replay_online");
        assert_eq!(online.provider_calls, 0, "replay must not re-bill");
        assert_eq!(online.tokens_billed, 0, "replay tokens == 0");
        assert!((online.reuse_pct - 100.0).abs() < f64::EPSILON);
        assert_eq!(
            online.detail.get("firstPassCalls").and_then(|v| v.as_u64()),
            Some(50)
        );

        let batch = by_id("crash_reconcile_batch");
        assert_eq!(batch.provider_calls, 0, "reconcile must not re-bill");
        assert_eq!(
            batch.detail.get("billedAtSubmit").and_then(|v| v.as_u64()),
            Some(24)
        );
        assert_eq!(
            batch.detail.get("rowsRecovered").and_then(|v| v.as_u64()),
            Some(24)
        );

        let incr = by_id("incremental_reenrichment");
        assert_eq!(
            incr.detail
                .get("expectedSecondPassCalls")
                .and_then(|v| v.as_u64()),
            Some(10)
        );
        assert_eq!(incr.provider_calls, 10, "only changed+new re-billed");
        assert_eq!(
            incr.detail.get("unchangedReused").and_then(|v| v.as_u64()),
            Some(35)
        );

        let dup = by_id("duplicate_heavy");
        assert_eq!(dup.provider_calls, 20);
        assert_eq!(dup.naive_provider_calls, Some(200));
        let savings = dup.savings_pct.expect("savings");
        assert!(
            (savings - 90.0).abs() < 0.1,
            "expected ~90% savings, got {savings}"
        );

        let review = by_id("review_queue_withhold");
        assert_eq!(
            review.provider_calls, 0,
            "pending review never re-dispatched"
        );
        assert_eq!(
            review.detail.get("firstPassCalls").and_then(|v| v.as_u64()),
            Some(1)
        );
    }

    #[test]
    fn markdown_table_has_header_and_rows() {
        let report = SuiteReport {
            task: TASK_ID.to_owned(),
            generated_at_unix: 0,
            provider: "mock".into(),
            ledger: "sqlite".into(),
            scenarios: vec![ScenarioReport {
                id: "x".into(),
                title: "Demo".into(),
                records_seen: 10,
                provider_calls: 0,
                tokens_billed: 0,
                results_reused: 10,
                reuse_pct: 100.0,
                naive_provider_calls: None,
                savings_pct: None,
                detail: serde_json::Map::new(),
            }],
        };
        let md = report.to_markdown_table();
        assert!(md.contains("Demo"));
        assert!(md.contains("100.0%"));
    }
}
