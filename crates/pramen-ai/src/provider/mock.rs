//! The deterministic offline provider.

use super::{
    BatchItemResult, BatchStatus, Capabilities, InferenceRequest, Provider, ProviderResponse,
};
use crate::error::{AiError, ProviderFault};
use crate::workkey::canonical_json;
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

/// One in-flight fake batch job.
#[derive(Debug)]
struct MockJob {
    items: Vec<(String, InferenceRequest)>,
    /// Polls remaining before the job reports completed.
    polls_remaining: u32,
}

/// A provider that fabricates schema-conforming output deterministically
/// from the request content — no network, no cost, stable across runs.
///
/// Useful for pipeline dry-runs (`provider: mock` in the pipeline document)
/// and for every offline test of the governance machinery: because output
/// is a pure function of the request, ledger reuse and work-key semantics
/// behave exactly as with a real provider.
///
/// The batch surface is implemented too: `submit_batch` opens an
/// in-memory job that completes after a configurable number of polls,
/// which is the local stand-in for provider-batch services (ADR 0005).
/// Jobs live on the provider instance, so a "crashed" operator that is
/// re-created over the same provider and ledger exercises real
/// reconciliation.
#[derive(Debug, Default)]
pub struct MockProvider {
    calls: AtomicU64,
    batch_latency_polls: u32,
    jobs: Mutex<HashMap<String, MockJob>>,
    next_job: AtomicU64,
}

impl MockProvider {
    /// A fresh mock with a zero call counter; batch jobs complete on the
    /// first poll.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// A mock whose batch jobs report `InProgress` for the first
    /// `polls` status checks before completing.
    #[must_use]
    pub fn with_batch_latency(polls: u32) -> Self {
        Self {
            batch_latency_polls: polls,
            ..Self::default()
        }
    }

    /// How many invocations were billed by this provider (i.e. were not
    /// served from the ledger): online calls plus batch items at
    /// submission, matching how real batch APIs bill.
    #[must_use]
    pub fn calls(&self) -> u64 {
        self.calls.load(Ordering::SeqCst)
    }

    /// Deterministic response for one request (shared by both paths).
    fn respond(&self, request: &InferenceRequest) -> ProviderResponse {
        let mut hasher = Sha256::new();
        hasher.update(canonical_json(&request.inputs).as_bytes());
        hasher.update(request.instruction.as_bytes());
        let digest = hasher.finalize();
        let seed = u64::from_be_bytes([
            digest[0], digest[1], digest[2], digest[3], digest[4], digest[5], digest[6], digest[7],
        ]);

        // Fabricate a value per declared property, honoring its type and
        // any maxLength bound so bounded `ai.generate` fields stay valid.
        let mut output = Map::new();
        if let Some(properties) = request
            .output_schema
            .get("properties")
            .and_then(Value::as_object)
        {
            for (index, (name, prop)) in properties.iter().enumerate() {
                let salt = seed.wrapping_add(index as u64);
                let type_name = match prop.get("type") {
                    Some(Value::String(t)) => t.as_str(),
                    Some(Value::Array(types)) => types
                        .iter()
                        .find_map(Value::as_str)
                        .filter(|t| *t != "null")
                        .unwrap_or("string"),
                    _ => "string",
                };
                let value = match type_name {
                    "integer" => json!((salt % 1000) as i64),
                    "number" => json!((salt % 1000) as f64 / 10.0),
                    "boolean" => json!(salt % 2 == 0),
                    _ => {
                        let mut text = format!("{name}-{:04x}", salt % 0xFFFF);
                        if let Some(max) = prop.get("maxLength").and_then(Value::as_u64) {
                            let max = usize::try_from(max).unwrap_or(usize::MAX);
                            if text.chars().count() > max {
                                text = text.chars().take(max).collect();
                            }
                        }
                        json!(text)
                    }
                };
                output.insert(name.clone(), value);
            }
        }

        let text = Value::Object(output).to_string();
        let input_tokens =
            (request.instruction.len() + canonical_json(&request.inputs).len()) as u64 / 4;
        ProviderResponse {
            output_tokens: text.len() as u64 / 4,
            input_tokens,
            request_id: format!("mock-{seed:016x}"),
            text,
        }
    }
}

#[async_trait::async_trait]
impl Provider for MockProvider {
    fn id(&self) -> &str {
        "mock"
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities {
            online: true,
            batch: true,
            structured_output: true,
            token_accounting: true,
        }
    }

    async fn invoke(&self, request: &InferenceRequest) -> Result<ProviderResponse, AiError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(self.respond(request))
    }

    async fn submit_batch(&self, items: &[(String, InferenceRequest)]) -> Result<String, AiError> {
        // Real batch APIs bill at submission; the counter mirrors that so
        // reconciliation tests can assert "no double billing".
        self.calls.fetch_add(items.len() as u64, Ordering::SeqCst);
        let job_id = format!("mock-job-{}", self.next_job.fetch_add(1, Ordering::SeqCst));
        let mut jobs = self.jobs.lock().map_err(|_| AiError::Provider {
            provider: "mock".to_owned(),
            fault: ProviderFault::Server,
            message: "job store poisoned".to_owned(),
        })?;
        jobs.insert(
            job_id.clone(),
            MockJob {
                items: items.to_vec(),
                polls_remaining: self.batch_latency_polls,
            },
        );
        Ok(job_id)
    }

    async fn poll_batch(&self, job_id: &str) -> Result<BatchStatus, AiError> {
        let mut jobs = self.jobs.lock().map_err(|_| AiError::Provider {
            provider: "mock".to_owned(),
            fault: ProviderFault::Server,
            message: "job store poisoned".to_owned(),
        })?;
        let job = jobs.get_mut(job_id).ok_or_else(|| AiError::Provider {
            provider: "mock".to_owned(),
            fault: ProviderFault::Protocol,
            message: format!("unknown batch job `{job_id}`"),
        })?;
        if job.polls_remaining > 0 {
            job.polls_remaining -= 1;
            return Ok(BatchStatus::InProgress);
        }
        Ok(BatchStatus::Completed)
    }

    async fn fetch_batch(&self, job_id: &str) -> Result<Vec<BatchItemResult>, AiError> {
        let jobs = self.jobs.lock().map_err(|_| AiError::Provider {
            provider: "mock".to_owned(),
            fault: ProviderFault::Server,
            message: "job store poisoned".to_owned(),
        })?;
        let job = jobs.get(job_id).ok_or_else(|| AiError::Provider {
            provider: "mock".to_owned(),
            fault: ProviderFault::Protocol,
            message: format!("unknown batch job `{job_id}`"),
        })?;
        Ok(job
            .items
            .iter()
            .map(|(key, request)| (key.clone(), Ok(self.respond(request))))
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(description: &str) -> InferenceRequest {
        InferenceRequest {
            instruction: "classify".into(),
            inputs: json!({"description": description}),
            output_schema: json!({
                "type": "object",
                "properties": {
                    "category": {"type": "string"},
                    "score": {"type": "number"},
                    "urgent": {"type": "boolean"}
                }
            }),
            max_output_tokens: None,
        }
    }

    #[tokio::test]
    async fn batch_jobs_complete_after_latency_and_match_online_output() {
        let provider = MockProvider::with_batch_latency(2);
        let items = vec![
            ("key-a".to_owned(), request("printer on fire")),
            ("key-b".to_owned(), request("invoice is wrong")),
        ];
        let job = provider.submit_batch(&items).await.unwrap();
        assert_eq!(provider.calls(), 2, "batch bills per item at submission");

        assert_eq!(
            provider.poll_batch(&job).await.unwrap(),
            BatchStatus::InProgress
        );
        assert_eq!(
            provider.poll_batch(&job).await.unwrap(),
            BatchStatus::InProgress
        );
        assert_eq!(
            provider.poll_batch(&job).await.unwrap(),
            BatchStatus::Completed
        );

        let results = provider.fetch_batch(&job).await.unwrap();
        assert_eq!(results.len(), 2);
        let (key, outcome) = &results[0];
        assert_eq!(key, "key-a");
        let online = provider.invoke(&request("printer on fire")).await.unwrap();
        assert_eq!(
            outcome.as_ref().unwrap().text,
            online.text,
            "batch and online agree for the same request"
        );

        let missing = provider.poll_batch("mock-job-999").await;
        assert!(missing.is_err(), "unknown job ids are provider errors");
    }

    #[tokio::test]
    async fn output_is_deterministic_and_schema_shaped() {
        let provider = MockProvider::new();
        let a = provider.invoke(&request("printer on fire")).await.unwrap();
        let b = provider.invoke(&request("printer on fire")).await.unwrap();
        let c = provider.invoke(&request("printer fine")).await.unwrap();
        assert_eq!(a.text, b.text, "same input must give same output");
        assert_ne!(a.text, c.text, "different input must give different output");
        assert_eq!(provider.calls(), 3);

        let parsed: Value = serde_json::from_str(&a.text).unwrap();
        assert!(parsed.get("category").unwrap().is_string());
        assert!(parsed.get("score").unwrap().is_number());
        assert!(parsed.get("urgent").unwrap().is_boolean());
    }
}
