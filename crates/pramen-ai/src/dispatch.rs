//! Online vs provider-batch dispatch cost model (architecture §18 RQ1 / E2.1).
//!
//! Given a workload estimate, a provider rate card, and a deadline, choose
//! the cheaper mode that still meets the deadline. Used by `execution: auto`
//! when the transform declares [`pramen_core::spec::AutoDispatchHints`], and
//! by `pramen ai dispatch-plan` for offline frontier sweeps.
//!
//! Numbers from the built-in rate cards are **illustrative / mock-calibrated**,
//! not live Bedrock quotes. Reopen the published frontier when S2.2 live
//! provider measurements exist.

use pramen_core::spec::{AutoDispatchHints, ExecutionMode};
use std::fmt;

/// USD prices per million tokens for one execution path.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TokenPrices {
    /// USD per 1e6 input tokens.
    pub input_per_mtok: f64,
    /// USD per 1e6 output tokens.
    pub output_per_mtok: f64,
}

/// Latency assumptions used for planning (not measured wall clock).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LatencyModel {
    /// Mean online latency per record, seconds.
    pub online_seconds_per_record: f64,
    /// Max concurrent online requests when estimating wall time.
    pub online_concurrency: u64,
    /// Fixed batch overhead (submit + poll + fetch), seconds.
    pub batch_fixed_seconds: f64,
    /// Expected provider batch completion wait, seconds.
    pub batch_completion_seconds: f64,
    /// Marginal batch staging cost per record, seconds.
    pub batch_seconds_per_record: f64,
}

/// Named pricing + latency profile for offline planning.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RateCardId {
    /// Deterministic mock provider; short synthetic batch window.
    Mock,
    /// OpenAI-compatible stub with a 1h-class batch window and 50% batch discount.
    OpenaiCompatStub,
    /// Illustrative Bedrock-class card (not live quotes); 24h batch window.
    BedrockIllustrative,
}

impl RateCardId {
    /// Parse a rate-card identifier from CLI / YAML text.
    #[must_use]
    pub fn parse(name: &str) -> Option<Self> {
        match name {
            "mock" => Some(Self::Mock),
            "openai-compat" | "openai-compat-stub" => Some(Self::OpenaiCompatStub),
            "bedrock" | "bedrock-illustrative" => Some(Self::BedrockIllustrative),
            _ => None,
        }
    }

    /// Stable id used in tables and JSON.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Mock => "mock",
            Self::OpenaiCompatStub => "openai-compat-stub",
            Self::BedrockIllustrative => "bedrock-illustrative",
        }
    }

    /// Map a provider adapter id to a default rate card.
    #[must_use]
    pub fn for_provider(provider_id: &str) -> Option<Self> {
        Self::parse(provider_id)
    }
}

impl fmt::Display for RateCardId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Online and (optional) batch prices plus latency for one rate card.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RateCard {
    /// Card identity.
    pub id: RateCardId,
    /// Online token prices.
    pub online: TokenPrices,
    /// Batch token prices when the provider supports batch.
    pub batch: Option<TokenPrices>,
    /// Latency model for both modes.
    pub latency: LatencyModel,
}

impl RateCard {
    /// Built-in card for [`RateCardId`].
    #[must_use]
    pub fn builtin(id: RateCardId) -> Self {
        match id {
            RateCardId::Mock => Self {
                id,
                online: TokenPrices {
                    input_per_mtok: 3.0,
                    output_per_mtok: 15.0,
                },
                batch: Some(TokenPrices {
                    input_per_mtok: 1.5,
                    output_per_mtok: 7.5,
                }),
                latency: LatencyModel {
                    online_seconds_per_record: 0.05,
                    online_concurrency: 32,
                    batch_fixed_seconds: 2.0,
                    // Mock completes quickly so deadlines can discriminate.
                    batch_completion_seconds: 60.0,
                    batch_seconds_per_record: 0.001,
                },
            },
            RateCardId::OpenaiCompatStub => Self {
                id,
                online: TokenPrices {
                    input_per_mtok: 0.15,
                    output_per_mtok: 0.60,
                },
                batch: Some(TokenPrices {
                    input_per_mtok: 0.075,
                    output_per_mtok: 0.30,
                }),
                latency: LatencyModel {
                    online_seconds_per_record: 0.20,
                    online_concurrency: 8,
                    batch_fixed_seconds: 5.0,
                    batch_completion_seconds: 3_600.0,
                    batch_seconds_per_record: 0.002,
                },
            },
            RateCardId::BedrockIllustrative => Self {
                id,
                // Illustrative Haiku-class list prices; not a live quote.
                online: TokenPrices {
                    input_per_mtok: 0.25,
                    output_per_mtok: 1.25,
                },
                batch: Some(TokenPrices {
                    input_per_mtok: 0.125,
                    output_per_mtok: 0.625,
                }),
                latency: LatencyModel {
                    online_seconds_per_record: 0.35,
                    online_concurrency: 16,
                    batch_fixed_seconds: 30.0,
                    batch_completion_seconds: 86_400.0,
                    batch_seconds_per_record: 0.005,
                },
            },
        }
    }
}

/// Workload estimate for one semantic step.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Workload {
    /// Expected ledger-miss record count.
    pub records: u64,
    /// Assumed input tokens per record.
    pub input_tokens_per_record: u64,
    /// Assumed output tokens per record.
    pub output_tokens_per_record: u64,
    /// Wall-clock deadline for the step, seconds.
    pub deadline_seconds: f64,
}

impl Workload {
    /// Defaults used when YAML omits per-record token assumptions.
    pub const DEFAULT_INPUT_TOKENS: u64 = 800;
    /// Defaults used when YAML omits per-record token assumptions.
    pub const DEFAULT_OUTPUT_TOKENS: u64 = 200;

    /// Build a workload from optional auto-dispatch hints.
    ///
    /// Returns `None` when `expected_records` or `deadline_seconds` is
    /// missing — `execution: auto` then falls back to online.
    #[must_use]
    pub fn from_hints(hints: &AutoDispatchHints) -> Option<Self> {
        let records = hints.expected_records?;
        let deadline_seconds = hints.deadline_seconds? as f64;
        Some(Self {
            records,
            input_tokens_per_record: hints
                .input_tokens_per_record
                .unwrap_or(Self::DEFAULT_INPUT_TOKENS),
            output_tokens_per_record: hints
                .output_tokens_per_record
                .unwrap_or(Self::DEFAULT_OUTPUT_TOKENS),
            deadline_seconds,
        })
    }
}

/// Recommended concrete execution mode (never `auto`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecommendedMode {
    /// Synchronous online calls.
    Online,
    /// Asynchronous provider-batch job.
    Batch,
}

impl RecommendedMode {
    /// Stable lowercase name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Online => "online",
            Self::Batch => "batch",
        }
    }

    /// Convert to the pipeline [`ExecutionMode`] enum.
    #[must_use]
    pub const fn to_execution_mode(self) -> ExecutionMode {
        match self {
            Self::Online => ExecutionMode::Online,
            Self::Batch => ExecutionMode::Batch,
        }
    }
}

impl fmt::Display for RecommendedMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Cost and latency estimate for one mode.
#[derive(Debug, Clone, PartialEq)]
pub struct ModeEstimate {
    /// Mode this estimate describes.
    pub mode: RecommendedMode,
    /// Estimated USD cost for the workload.
    pub cost_usd: f64,
    /// Estimated wall-clock seconds.
    pub latency_seconds: f64,
    /// Whether `latency_seconds` is within the workload deadline.
    pub meets_deadline: bool,
}

/// Full plan: both mode estimates and the recommendation.
#[derive(Debug, Clone, PartialEq)]
pub struct DispatchPlan {
    /// Chosen mode.
    pub recommended: RecommendedMode,
    /// Online estimate (always present).
    pub online: ModeEstimate,
    /// Batch estimate when the rate card / provider supports batch.
    pub batch: Option<ModeEstimate>,
    /// Human-readable rationale.
    pub reason: String,
}

/// Estimate token cost for `records` under `prices`.
#[must_use]
pub fn estimate_cost_usd(
    records: u64,
    input_tokens_per_record: u64,
    output_tokens_per_record: u64,
    prices: &TokenPrices,
) -> f64 {
    let total_in = records as f64 * input_tokens_per_record as f64;
    let total_out = records as f64 * output_tokens_per_record as f64;
    (total_in * prices.input_per_mtok + total_out * prices.output_per_mtok) / 1_000_000.0
}

/// Online wall time: concurrent waves of per-record latency.
#[must_use]
pub fn estimate_online_latency_seconds(records: u64, latency: &LatencyModel) -> f64 {
    if records == 0 {
        return 0.0;
    }
    let concurrency = latency.online_concurrency.max(1) as f64;
    let waves = (records as f64 / concurrency).ceil();
    waves * latency.online_seconds_per_record
}

/// Batch wall time: fixed overhead + completion window + marginal staging.
#[must_use]
pub fn estimate_batch_latency_seconds(records: u64, latency: &LatencyModel) -> f64 {
    if records == 0 {
        return 0.0;
    }
    latency.batch_fixed_seconds
        + latency.batch_completion_seconds
        + records as f64 * latency.batch_seconds_per_record
}

fn mode_estimate(
    mode: RecommendedMode,
    cost_usd: f64,
    latency_seconds: f64,
    deadline_seconds: f64,
) -> ModeEstimate {
    ModeEstimate {
        mode,
        cost_usd,
        latency_seconds,
        meets_deadline: latency_seconds <= deadline_seconds,
    }
}

/// Choose online vs batch under deadline and cost constraints.
///
/// When `supports_batch` is false or the rate card has no batch prices,
/// the plan always recommends online.
#[must_use]
pub fn plan(workload: &Workload, card: &RateCard, supports_batch: bool) -> DispatchPlan {
    let online = mode_estimate(
        RecommendedMode::Online,
        estimate_cost_usd(
            workload.records,
            workload.input_tokens_per_record,
            workload.output_tokens_per_record,
            &card.online,
        ),
        estimate_online_latency_seconds(workload.records, &card.latency),
        workload.deadline_seconds,
    );

    let batch = if supports_batch {
        card.batch.map(|prices| {
            mode_estimate(
                RecommendedMode::Batch,
                estimate_cost_usd(
                    workload.records,
                    workload.input_tokens_per_record,
                    workload.output_tokens_per_record,
                    &prices,
                ),
                estimate_batch_latency_seconds(workload.records, &card.latency),
                workload.deadline_seconds,
            )
        })
    } else {
        None
    };

    let (recommended, reason) = select_mode(&online, batch.as_ref(), workload.deadline_seconds);
    DispatchPlan {
        recommended,
        online,
        batch,
        reason,
    }
}

fn select_mode(
    online: &ModeEstimate,
    batch: Option<&ModeEstimate>,
    deadline_seconds: f64,
) -> (RecommendedMode, String) {
    let Some(batch) = batch else {
        return (
            RecommendedMode::Online,
            "provider has no batch path; using online".to_owned(),
        );
    };

    match (online.meets_deadline, batch.meets_deadline) {
        (true, true) => {
            if batch.cost_usd < online.cost_usd {
                (
                    RecommendedMode::Batch,
                    format!(
                        "both meet deadline; batch cheaper (${:.4} vs ${:.4})",
                        batch.cost_usd, online.cost_usd
                    ),
                )
            } else if online.cost_usd < batch.cost_usd {
                (
                    RecommendedMode::Online,
                    format!(
                        "both meet deadline; online cheaper (${:.4} vs ${:.4})",
                        online.cost_usd, batch.cost_usd
                    ),
                )
            } else if batch.latency_seconds <= online.latency_seconds {
                (
                    RecommendedMode::Batch,
                    "both meet deadline at equal cost; batch no slower".to_owned(),
                )
            } else {
                (
                    RecommendedMode::Online,
                    "both meet deadline at equal cost; online faster".to_owned(),
                )
            }
        }
        (true, false) => (
            RecommendedMode::Online,
            format!(
                "batch misses deadline ({:.0}s > {:.0}s); using online",
                batch.latency_seconds, deadline_seconds
            ),
        ),
        (false, true) => (
            RecommendedMode::Batch,
            format!(
                "online misses deadline ({:.0}s > {:.0}s); batch fits ({:.0}s)",
                online.latency_seconds, deadline_seconds, batch.latency_seconds
            ),
        ),
        (false, false) => {
            if online.latency_seconds <= batch.latency_seconds {
                (
                    RecommendedMode::Online,
                    format!(
                        "neither meets deadline; online finishes sooner ({:.0}s vs {:.0}s)",
                        online.latency_seconds, batch.latency_seconds
                    ),
                )
            } else {
                (
                    RecommendedMode::Batch,
                    format!(
                        "neither meets deadline; batch finishes sooner ({:.0}s vs {:.0}s)",
                        batch.latency_seconds, online.latency_seconds
                    ),
                )
            }
        }
    }
}

/// Resolve whether the operator should use provider-batch dispatch.
///
/// - `online` / `batch` are honored literally (`batch` requires capability).
/// - `auto` runs the cost model when hints are complete and the provider
///   supports batch; otherwise it falls back to online.
///
/// # Errors
///
/// Returns a message when `execution: batch` is requested without capability,
/// or when `dispatch.rateCard` names an unknown card.
pub fn resolve_batch_mode(
    execution: ExecutionMode,
    supports_batch: bool,
    hints: Option<&AutoDispatchHints>,
    provider_id: &str,
) -> Result<(bool, Option<DispatchPlan>), String> {
    match execution {
        ExecutionMode::Online => Ok((false, None)),
        ExecutionMode::Batch => {
            if !supports_batch {
                return Err(format!(
                    "execution: batch, but provider `{provider_id}` does not support batch \
                     execution; use auto or online"
                ));
            }
            Ok((true, None))
        }
        ExecutionMode::Auto => {
            if !supports_batch {
                return Ok((false, None));
            }
            let Some(hints) = hints else {
                return Ok((false, None));
            };
            let Some(workload) = Workload::from_hints(hints) else {
                return Ok((false, None));
            };
            let card_id = match hints.rate_card.as_deref() {
                Some(name) => RateCardId::parse(name).ok_or_else(|| {
                    format!(
                        "unknown dispatch.rateCard `{name}` \
                         (available: mock, openai-compat-stub, bedrock-illustrative)"
                    )
                })?,
                None => RateCardId::for_provider(provider_id).unwrap_or(RateCardId::Mock),
            };
            let card = RateCard::builtin(card_id);
            let planned = plan(&workload, &card, true);
            Ok((planned.recommended == RecommendedMode::Batch, Some(planned)))
        }
    }
}

/// One cell of a frontier sweep.
#[derive(Debug, Clone, PartialEq)]
pub struct FrontierRow {
    /// Rate card / provider profile id.
    pub rate_card: String,
    /// Record volume.
    pub records: u64,
    /// Deadline seconds.
    pub deadline_seconds: u64,
    /// Recommended mode.
    pub recommended: RecommendedMode,
    /// Online cost USD.
    pub online_cost_usd: f64,
    /// Online latency seconds.
    pub online_latency_seconds: f64,
    /// Batch cost USD, when available.
    pub batch_cost_usd: Option<f64>,
    /// Batch latency seconds, when available.
    pub batch_latency_seconds: Option<f64>,
    /// Short reason.
    pub reason: String,
}

/// Default volumes for the published offline frontier.
pub const FRONTIER_VOLUMES: &[u64] = &[100, 1_000, 10_000, 100_000];

/// Default deadlines (seconds) for the published offline frontier.
pub const FRONTIER_DEADLINES_SECS: &[u64] = &[300, 3_600, 86_400];

/// Rate cards included in the default offline frontier sweep.
pub const FRONTIER_CARDS: &[RateCardId] = &[RateCardId::Mock, RateCardId::OpenaiCompatStub];

/// Sweep volumes × deadlines × rate cards with the analytical cost model.
#[must_use]
pub fn sweep_frontier(
    cards: &[RateCardId],
    volumes: &[u64],
    deadlines_secs: &[u64],
) -> Vec<FrontierRow> {
    let mut rows = Vec::with_capacity(cards.len() * volumes.len() * deadlines_secs.len());
    for &card_id in cards {
        let card = RateCard::builtin(card_id);
        for &records in volumes {
            for &deadline_seconds in deadlines_secs {
                let workload = Workload {
                    records,
                    input_tokens_per_record: Workload::DEFAULT_INPUT_TOKENS,
                    output_tokens_per_record: Workload::DEFAULT_OUTPUT_TOKENS,
                    deadline_seconds: deadline_seconds as f64,
                };
                let planned = plan(&workload, &card, card.batch.is_some());
                rows.push(FrontierRow {
                    rate_card: card_id.as_str().to_owned(),
                    records,
                    deadline_seconds,
                    recommended: planned.recommended,
                    online_cost_usd: planned.online.cost_usd,
                    online_latency_seconds: planned.online.latency_seconds,
                    batch_cost_usd: planned.batch.as_ref().map(|b| b.cost_usd),
                    batch_latency_seconds: planned.batch.as_ref().map(|b| b.latency_seconds),
                    reason: planned.reason,
                });
            }
        }
    }
    rows
}

/// Render a frontier table as Markdown (mock/stub-calibrated).
#[must_use]
pub fn render_frontier_markdown(rows: &[FrontierRow]) -> String {
    let mut out = String::new();
    out.push_str("| rate card | records | deadline | recommended | online $ | online s | batch $ | batch s | reason |\n");
    out.push_str("| --- | ---: | ---: | --- | ---: | ---: | ---: | ---: | --- |\n");
    for row in rows {
        let batch_cost = row
            .batch_cost_usd
            .map(|c| format!("{c:.4}"))
            .unwrap_or_else(|| "—".to_owned());
        let batch_lat = row
            .batch_latency_seconds
            .map(|s| format!("{s:.1}"))
            .unwrap_or_else(|| "—".to_owned());
        out.push_str(&format!(
            "| {} | {} | {}s | {} | {:.4} | {:.1} | {} | {} | {} |\n",
            row.rate_card,
            row.records,
            row.deadline_seconds,
            row.recommended,
            row.online_cost_usd,
            row.online_latency_seconds,
            batch_cost,
            batch_lat,
            row.reason.replace('|', "/"),
        ));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mock_card() -> RateCard {
        RateCard::builtin(RateCardId::Mock)
    }

    #[test]
    fn batch_is_half_online_cost_on_mock_card() {
        let cost_online = estimate_cost_usd(1_000, 800, 200, &mock_card().online);
        let cost_batch =
            estimate_cost_usd(1_000, 800, 200, &mock_card().batch.expect("mock has batch"));
        assert!((cost_online - 2.0 * cost_batch).abs() < 1e-9);
    }

    #[test]
    fn tight_deadline_forces_online_when_batch_window_is_long() {
        let card = RateCard::builtin(RateCardId::OpenaiCompatStub);
        let workload = Workload {
            records: 1_000,
            input_tokens_per_record: 800,
            output_tokens_per_record: 200,
            deadline_seconds: 300.0,
        };
        let planned = plan(&workload, &card, true);
        assert_eq!(planned.recommended, RecommendedMode::Online);
        assert!(planned.online.meets_deadline);
        assert!(!planned.batch.as_ref().expect("batch").meets_deadline);
    }

    #[test]
    fn loose_deadline_prefers_cheaper_batch() {
        let card = mock_card();
        let workload = Workload {
            records: 10_000,
            input_tokens_per_record: 800,
            output_tokens_per_record: 200,
            deadline_seconds: 86_400.0,
        };
        let planned = plan(&workload, &card, true);
        assert_eq!(planned.recommended, RecommendedMode::Batch);
        assert!(planned.batch.as_ref().expect("batch").cost_usd < planned.online.cost_usd);
    }

    #[test]
    fn no_batch_capability_always_online() {
        let card = mock_card();
        let workload = Workload {
            records: 10_000,
            input_tokens_per_record: 800,
            output_tokens_per_record: 200,
            deadline_seconds: 86_400.0,
        };
        let planned = plan(&workload, &card, false);
        assert_eq!(planned.recommended, RecommendedMode::Online);
        assert!(planned.batch.is_none());
    }

    #[test]
    fn auto_without_hints_stays_online() {
        let (batch, plan) =
            resolve_batch_mode(ExecutionMode::Auto, true, None, "mock").expect("ok");
        assert!(!batch);
        assert!(plan.is_none());
    }

    #[test]
    fn auto_with_hints_selects_batch_on_mock() {
        let hints = AutoDispatchHints {
            expected_records: Some(10_000),
            deadline_seconds: Some(86_400),
            input_tokens_per_record: None,
            output_tokens_per_record: None,
            rate_card: Some("mock".to_owned()),
        };
        let (batch, planned) =
            resolve_batch_mode(ExecutionMode::Auto, true, Some(&hints), "mock").expect("ok");
        assert!(batch);
        assert_eq!(planned.expect("plan").recommended, RecommendedMode::Batch);
    }

    #[test]
    fn auto_with_tight_deadline_selects_online() {
        let hints = AutoDispatchHints {
            expected_records: Some(1_000),
            deadline_seconds: Some(300),
            input_tokens_per_record: None,
            output_tokens_per_record: None,
            rate_card: Some("openai-compat-stub".to_owned()),
        };
        let (batch, _) =
            resolve_batch_mode(ExecutionMode::Auto, true, Some(&hints), "openai-compat")
                .expect("ok");
        assert!(!batch);
    }

    #[test]
    fn frontier_sweep_is_deterministic() {
        let a = sweep_frontier(FRONTIER_CARDS, FRONTIER_VOLUMES, FRONTIER_DEADLINES_SECS);
        let b = sweep_frontier(FRONTIER_CARDS, FRONTIER_VOLUMES, FRONTIER_DEADLINES_SECS);
        assert_eq!(a, b);
        assert_eq!(
            a.len(),
            FRONTIER_CARDS.len() * FRONTIER_VOLUMES.len() * FRONTIER_DEADLINES_SECS.len()
        );
        let md = render_frontier_markdown(&a);
        assert!(md.contains("openai-compat-stub"));
        assert!(md.contains("recommended"));
    }

    #[test]
    fn online_latency_scales_with_concurrency() {
        let latency = LatencyModel {
            online_seconds_per_record: 1.0,
            online_concurrency: 10,
            batch_fixed_seconds: 0.0,
            batch_completion_seconds: 0.0,
            batch_seconds_per_record: 0.0,
        };
        assert!((estimate_online_latency_seconds(25, &latency) - 3.0).abs() < 1e-9);
    }
}
