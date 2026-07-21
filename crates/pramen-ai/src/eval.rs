//! Golden-corpus evaluation: measure a model's quality, cost, and latency
//! on a versioned, labelled corpus (S2.2 / `pramen ai evaluate`).
//!
//! A corpus is a YAML document carrying the task (instruction + declared
//! output fields + per-field rubric weights) and labelled items. The
//! runner sends every item through a provider adapter — the same
//! [`InferenceRequest`] shape the pipeline operator uses, so measured
//! quality transfers — validates output against the declared schema, and
//! scores it against the expected labels. Results are written to a
//! timestamped directory so quality regressions across prompt or model
//! revisions are diffable artifacts, not anecdotes.
//!
//! Scoring: per-field exact-match accuracy (strings compared
//! case-insensitively after trimming, floats within 1e-6), macro-F1 for
//! string fields (each distinct expected value is a class), and one
//! weighted overall score. A model-provided confidence number is never
//! treated as calibrated confidence.

use crate::error::AiError;
use crate::provider::{InferenceRequest, Provider};
use crate::schema::{output_json_schema, validate_output};
use pramen_core::spec::{FieldSpec, FieldType};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::BTreeMap;

/// A versioned, labelled evaluation corpus.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Corpus {
    /// Corpus name, e.g. `support-tickets`.
    pub name: String,
    /// Corpus version; bump when items or labels change.
    pub version: u32,
    /// The task every item is evaluated on.
    pub task: EvalTask,
    /// Labelled items.
    pub items: Vec<EvalItem>,
}

/// The semantic task under evaluation.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EvalTask {
    /// The fixed instruction, exactly as a pipeline would declare it.
    pub instruction: String,
    /// Declared output fields (name, type, nullability).
    pub fields: Vec<FieldSpec>,
    /// Rubric weight per field; absent fields weigh 1.0.
    #[serde(default)]
    pub weights: BTreeMap<String, f64>,
}

/// One labelled record.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EvalItem {
    /// Stable item identifier.
    pub id: String,
    /// Input values, keyed by column name.
    pub input: Map<String, Value>,
    /// Expected output values, keyed by declared field name.
    pub expected: Map<String, Value>,
}

impl Corpus {
    /// Parse a corpus from its YAML form.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::Input`] for malformed documents or a corpus
    /// whose expected labels do not cover the declared fields.
    pub fn from_yaml(text: &str) -> Result<Self, AiError> {
        let corpus: Self =
            serde_yaml_ng::from_str(text).map_err(|e| AiError::Input(format!("corpus: {e}")))?;
        for item in &corpus.items {
            for field in &corpus.task.fields {
                if !item.expected.contains_key(&field.name) {
                    return Err(AiError::Input(format!(
                        "corpus item `{}` lacks an expected value for field `{}`",
                        item.id, field.name
                    )));
                }
            }
        }
        Ok(corpus)
    }

    /// Serialize to the canonical YAML form.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::Input`] if serialization fails (it cannot for
    /// well-formed corpora).
    pub fn to_yaml(&self) -> Result<String, AiError> {
        serde_yaml_ng::to_string(self).map_err(|e| AiError::Input(format!("corpus: {e}")))
    }
}

/// Per-item outcome, written to `items.jsonl` for drill-down.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ItemResult {
    /// The corpus item id.
    pub id: String,
    /// Whether output validated against the declared schema.
    pub schema_valid: bool,
    /// The normalized output (present when valid).
    pub output: Option<Value>,
    /// The expected labels.
    pub expected: Map<String, Value>,
    /// Which fields matched their expected value.
    pub matches: BTreeMap<String, bool>,
    /// Wall-clock latency for this item, milliseconds.
    pub latency_ms: f64,
    /// Provider-reported input tokens.
    pub input_tokens: u64,
    /// Provider-reported output tokens.
    pub output_tokens: u64,
}

/// One field's quality summary.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FieldScore {
    /// Field name.
    pub field: String,
    /// Rubric weight.
    pub weight: f64,
    /// Exact-match accuracy over schema-valid items.
    pub accuracy: f64,
    /// Macro-F1 over expected classes (string fields only).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub macro_f1: Option<f64>,
}

/// The evaluation report, written to `report.json`.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EvalReport {
    /// Corpus name.
    pub corpus: String,
    /// Corpus version.
    pub version: u32,
    /// Provider adapter id.
    pub provider: String,
    /// Model identifier.
    pub model: String,
    /// Items evaluated.
    pub items: usize,
    /// Items whose output validated against the declared schema.
    pub schema_valid: usize,
    /// Per-field quality.
    pub fields: Vec<FieldScore>,
    /// Weighted overall score in [0, 1]: accuracy per field, weighted by
    /// rubric weight; schema-invalid items score zero on every field.
    pub weighted_score: f64,
    /// Total provider-reported input tokens.
    pub input_tokens: u64,
    /// Total provider-reported output tokens.
    pub output_tokens: u64,
    /// Median per-item latency, milliseconds.
    pub latency_p50_ms: f64,
    /// 95th-percentile per-item latency, milliseconds.
    pub latency_p95_ms: f64,
    /// Estimated cost in USD, when prices were supplied.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cost_usd: Option<f64>,
}

/// Optional USD prices per million tokens, for the cost column.
#[derive(Debug, Clone, Copy, Default)]
pub struct Prices {
    /// USD per million input tokens.
    pub input_per_mtok: Option<f64>,
    /// USD per million output tokens.
    pub output_per_mtok: Option<f64>,
}

/// Evaluate `corpus` (optionally capped at `limit` items) against a
/// provider, sequentially and without any ledger — an evaluation measures
/// the model, so nothing is reused and nothing is recorded.
///
/// # Errors
///
/// Returns [`AiError::Provider`] when the provider fails; invalid output
/// *content* is not an error — it lowers the schema-valid rate.
pub async fn run_eval(
    corpus: &Corpus,
    provider: &dyn Provider,
    model: &str,
    limit: Option<usize>,
    prices: Prices,
) -> Result<(EvalReport, Vec<ItemResult>), AiError> {
    let schema = output_json_schema(&corpus.task.fields);
    let taken = limit.unwrap_or(corpus.items.len()).min(corpus.items.len());
    let mut results = Vec::with_capacity(taken);

    for item in &corpus.items[..taken] {
        let request = InferenceRequest {
            instruction: corpus.task.instruction.clone(),
            inputs: Value::Object(item.input.clone()),
            output_schema: schema.clone(),
            max_output_tokens: None,
        };
        let started = std::time::Instant::now();
        let response = provider.invoke(&request).await?;
        let latency_ms = started.elapsed().as_secs_f64() * 1000.0;

        let (schema_valid, output) = match validate_output(&response.text, &corpus.task.fields) {
            Ok(normalized) => (true, Some(normalized)),
            Err(_) => (false, None),
        };
        let mut matches = BTreeMap::new();
        for field in &corpus.task.fields {
            let matched = output.as_ref().is_some_and(|out| {
                values_match(
                    item.expected.get(&field.name).unwrap_or(&Value::Null),
                    out.get(&field.name).unwrap_or(&Value::Null),
                    field.field_type,
                )
            });
            matches.insert(field.name.clone(), matched);
        }
        results.push(ItemResult {
            id: item.id.clone(),
            schema_valid,
            output,
            expected: item.expected.clone(),
            matches,
            latency_ms,
            input_tokens: response.input_tokens,
            output_tokens: response.output_tokens,
        });
    }

    let report = score(corpus, provider.id(), model, &results, prices);
    Ok((report, results))
}

/// Aggregate per-item results into the report.
fn score(
    corpus: &Corpus,
    provider: &str,
    model: &str,
    results: &[ItemResult],
    prices: Prices,
) -> EvalReport {
    let items = results.len();
    let schema_valid = results.iter().filter(|r| r.schema_valid).count();

    let mut fields = Vec::with_capacity(corpus.task.fields.len());
    let mut weighted_sum = 0.0;
    let mut weight_total = 0.0;
    for field in &corpus.task.fields {
        let weight = corpus.task.weights.get(&field.name).copied().unwrap_or(1.0);
        let matched = results
            .iter()
            .filter(|r| r.matches.get(&field.name).copied().unwrap_or(false))
            .count();
        let accuracy = if items == 0 {
            0.0
        } else {
            matched as f64 / items as f64
        };
        let macro_f1 = (field.field_type == FieldType::Utf8)
            .then(|| macro_f1(&field.name, results))
            .flatten();
        weighted_sum += weight * accuracy;
        weight_total += weight;
        fields.push(FieldScore {
            field: field.name.clone(),
            weight,
            accuracy,
            macro_f1,
        });
    }

    let mut latencies: Vec<f64> = results.iter().map(|r| r.latency_ms).collect();
    latencies.sort_by(f64::total_cmp);
    let input_tokens: u64 = results.iter().map(|r| r.input_tokens).sum();
    let output_tokens: u64 = results.iter().map(|r| r.output_tokens).sum();
    let cost_usd = match (prices.input_per_mtok, prices.output_per_mtok) {
        (None, None) => None,
        (i, o) => Some(
            input_tokens as f64 * i.unwrap_or(0.0) / 1e6
                + output_tokens as f64 * o.unwrap_or(0.0) / 1e6,
        ),
    };

    EvalReport {
        corpus: corpus.name.clone(),
        version: corpus.version,
        provider: provider.to_owned(),
        model: model.to_owned(),
        items,
        schema_valid,
        fields,
        weighted_score: if weight_total > 0.0 {
            weighted_sum / weight_total
        } else {
            0.0
        },
        input_tokens,
        output_tokens,
        latency_p50_ms: percentile(&latencies, 0.50),
        latency_p95_ms: percentile(&latencies, 0.95),
        cost_usd,
    }
}

/// Macro-F1 over the classes present in the expected labels. Items whose
/// expected value is null (a nullable field with no label) do not define
/// a class and are excluded from the computation.
fn macro_f1(field: &str, results: &[ItemResult]) -> Option<f64> {
    let norm = |v: &Value| v.as_str().map(|s| s.trim().to_lowercase());
    let mut classes: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    let mut pairs = Vec::with_capacity(results.len());
    for result in results {
        let Some(expected) = norm(result.expected.get(field).unwrap_or(&Value::Null)) else {
            continue;
        };
        let predicted = result
            .output
            .as_ref()
            .and_then(|out| norm(out.get(field).unwrap_or(&Value::Null)));
        classes.insert(expected.clone());
        pairs.push((expected, predicted));
    }
    if classes.is_empty() {
        return None;
    }
    let mut f1_sum = 0.0;
    for class in &classes {
        let tp = pairs
            .iter()
            .filter(|(e, p)| e == class && p.as_deref() == Some(class))
            .count() as f64;
        let fp = pairs
            .iter()
            .filter(|(e, p)| e != class && p.as_deref() == Some(class))
            .count() as f64;
        let fn_ = pairs
            .iter()
            .filter(|(e, p)| e == class && p.as_deref() != Some(class))
            .count() as f64;
        let precision = if tp + fp > 0.0 { tp / (tp + fp) } else { 0.0 };
        let recall = if tp + fn_ > 0.0 { tp / (tp + fn_) } else { 0.0 };
        f1_sum += if precision + recall > 0.0 {
            2.0 * precision * recall / (precision + recall)
        } else {
            0.0
        };
    }
    Some(f1_sum / classes.len() as f64)
}

/// Nearest-rank percentile of an ascending-sorted slice.
fn percentile(sorted: &[f64], q: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let rank = ((sorted.len() as f64 * q).ceil() as usize).clamp(1, sorted.len());
    sorted[rank - 1]
}

/// Whether a predicted value matches its expected label.
fn values_match(expected: &Value, actual: &Value, field_type: FieldType) -> bool {
    match (expected, actual) {
        (Value::Null, Value::Null) => true,
        (Value::Null, _) | (_, Value::Null) => false,
        _ => match field_type {
            FieldType::Utf8 | FieldType::Timestamp => match (expected.as_str(), actual.as_str()) {
                (Some(e), Some(a)) => e.trim().eq_ignore_ascii_case(a.trim()),
                _ => false,
            },
            FieldType::Int64 => expected.as_i64() == actual.as_i64(),
            FieldType::Float64 => match (expected.as_f64(), actual.as_f64()) {
                (Some(e), Some(a)) => (e - a).abs() < 1e-6,
                _ => false,
            },
            FieldType::Bool => expected.as_bool() == actual.as_bool(),
        },
    }
}

/// Render the report as the human-readable table `ai evaluate` prints.
#[must_use]
pub fn render_text(report: &EvalReport) -> String {
    use std::fmt::Write;
    let mut out = String::new();
    let _ = writeln!(
        out,
        "corpus: {} v{} ({} items)",
        report.corpus, report.version, report.items
    );
    let _ = writeln!(out, "provider/model: {}/{}", report.provider, report.model);
    let _ = writeln!(
        out,
        "schema-valid: {}/{} ({:.1}%)",
        report.schema_valid,
        report.items,
        if report.items > 0 {
            report.schema_valid as f64 / report.items as f64 * 100.0
        } else {
            0.0
        }
    );
    let _ = writeln!(
        out,
        "{:<18} {:>6} {:>9} {:>9}",
        "field", "weight", "accuracy", "macro-F1"
    );
    for field in &report.fields {
        let f1 = field
            .macro_f1
            .map_or_else(|| "-".to_owned(), |v| format!("{v:.3}"));
        let _ = writeln!(
            out,
            "{:<18} {:>6.1} {:>9.3} {:>9}",
            field.field, field.weight, field.accuracy, f1
        );
    }
    let _ = writeln!(out, "weighted score: {:.3}", report.weighted_score);
    let _ = writeln!(
        out,
        "tokens: {} in / {} out{}",
        report.input_tokens,
        report.output_tokens,
        report
            .cost_usd
            .map_or_else(String::new, |c| format!(" (~${c:.4})"))
    );
    let _ = writeln!(
        out,
        "latency: p50 {:.1} ms, p95 {:.1} ms",
        report.latency_p50_ms, report.latency_p95_ms
    );
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::MockProvider;
    use serde_json::json;

    fn map(pairs: &[(&str, Value)]) -> Map<String, Value> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), v.clone()))
            .collect()
    }

    fn tiny_corpus() -> Corpus {
        Corpus {
            name: "tiny".into(),
            version: 1,
            task: EvalTask {
                instruction: "classify".into(),
                fields: vec![
                    FieldSpec {
                        name: "category".into(),
                        field_type: FieldType::Utf8,
                        nullable: false,
                        max_chars: None,
                    },
                    FieldSpec {
                        name: "urgent".into(),
                        field_type: FieldType::Bool,
                        nullable: false,
                        max_chars: None,
                    },
                ],
                weights: BTreeMap::from([("category".to_owned(), 2.0)]),
            },
            items: vec![
                EvalItem {
                    id: "t1".into(),
                    input: map(&[("description", json!("printer on fire"))]),
                    expected: map(&[("category", json!("hardware")), ("urgent", json!(true))]),
                },
                EvalItem {
                    id: "t2".into(),
                    input: map(&[("description", json!("invoice is wrong"))]),
                    expected: map(&[("category", json!("billing")), ("urgent", json!(false))]),
                },
            ],
        }
    }

    #[test]
    fn checked_in_corpus_parses_and_meets_the_size_bar() {
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../corpora/support-tickets.v1.yaml"
        );
        let text = std::fs::read_to_string(path).expect("corpus file present");
        let corpus = Corpus::from_yaml(&text).expect("corpus parses");
        assert_eq!(corpus.name, "support-tickets");
        assert_eq!(corpus.version, 1);
        assert!(
            corpus.items.len() >= 500,
            "S2.2 requires >=500 labelled records, found {}",
            corpus.items.len()
        );
        assert_eq!(corpus.task.fields.len(), 4);
        // Weighted rubric present.
        assert!(corpus.task.weights.contains_key("category"));
    }

    #[test]
    fn corpus_yaml_round_trips_and_missing_labels_are_rejected() {
        let corpus = tiny_corpus();
        let yaml = corpus.to_yaml().unwrap();
        let parsed = Corpus::from_yaml(&yaml).unwrap();
        assert_eq!(parsed.items.len(), 2);
        assert_eq!(parsed.task.weights.get("category"), Some(&2.0));

        let broken = yaml.replace("category: hardware", "wrong_key: hardware");
        let error = Corpus::from_yaml(&broken).unwrap_err().to_string();
        assert!(error.contains("lacks an expected value"), "{error}");
    }

    #[test]
    fn matching_is_normalized_and_typed() {
        assert!(values_match(
            &json!("Hardware "),
            &json!("hardware"),
            FieldType::Utf8
        ));
        assert!(!values_match(
            &json!("hardware"),
            &json!("billing"),
            FieldType::Utf8
        ));
        assert!(values_match(
            &json!(0.5),
            &json!(0.5000000001),
            FieldType::Float64
        ));
        assert!(values_match(&Value::Null, &Value::Null, FieldType::Utf8));
        assert!(!values_match(&json!("x"), &Value::Null, FieldType::Utf8));
    }

    #[test]
    fn macro_f1_matches_a_hand_computed_case() {
        // Two classes; class `a`: 1 tp, 1 fn; class `b`: 1 tp, 1 fp.
        // P/R(a) = 1/1, 1/2 → F1 2/3; P/R(b) = 1/2, 1/1 → F1 2/3.
        let results: Vec<ItemResult> = [("a", "a"), ("a", "b"), ("b", "b")]
            .iter()
            .map(|(expected, predicted)| ItemResult {
                id: "x".into(),
                schema_valid: true,
                output: Some(json!({"f": predicted})),
                expected: map(&[("f", json!(expected))]),
                matches: BTreeMap::new(),
                latency_ms: 0.0,
                input_tokens: 0,
                output_tokens: 0,
            })
            .collect();
        let f1 = macro_f1("f", &results).unwrap();
        assert!((f1 - 2.0 / 3.0).abs() < 1e-9, "{f1}");
    }

    #[tokio::test]
    async fn end_to_end_eval_produces_a_complete_report() {
        let corpus = tiny_corpus();
        let provider = MockProvider::new();
        let (report, items) = run_eval(
            &corpus,
            &provider,
            "mock-1",
            None,
            Prices {
                input_per_mtok: Some(1.0),
                output_per_mtok: Some(2.0),
            },
        )
        .await
        .unwrap();

        assert_eq!(report.items, 2);
        assert_eq!(
            report.schema_valid, 2,
            "mock output always satisfies the schema"
        );
        assert_eq!(report.fields.len(), 2);
        assert_eq!(report.fields[0].weight, 2.0);
        assert!(report.fields[0].macro_f1.is_some(), "utf8 field carries F1");
        assert!(report.fields[1].macro_f1.is_none(), "bool field does not");
        assert!(report.input_tokens > 0);
        assert!(report.cost_usd.unwrap() > 0.0);
        assert_eq!(items.len(), 2);

        let text = render_text(&report);
        assert!(text.contains("weighted score"), "{text}");

        // The cap applies.
        let (capped, _) = run_eval(&corpus, &provider, "mock-1", Some(1), Prices::default())
            .await
            .unwrap();
        assert_eq!(capped.items, 1);
        assert!(capped.cost_usd.is_none());
    }
}
