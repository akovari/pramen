//! The provider abstraction: one trait, uniform governance for every model
//! backend.
//!
//! Budgets, validation, the ledger, and provenance live *outside* the
//! adapters — an adapter only turns an [`InferenceRequest`] into a
//! [`ProviderResponse`]. Available adapters: [`MockProvider`] (deterministic,
//! offline, for tests and pipeline dry-runs; also implements the batch
//! surface), [`OpenAiCompatProvider`] (vLLM, Ollama, llama.cpp, or any
//! OpenAI-compatible endpoint; implements provider-batch via the OpenAI
//! Files + Batches APIs where the server offers them), and
//! [`BedrockProvider`] (Amazon Bedrock Converse; its provider-batch leg is
//! the remainder of P1.8).

mod bedrock;
mod mock;
mod openai_compat;

pub use bedrock::BedrockProvider;
pub use mock::MockProvider;
pub use openai_compat::OpenAiCompatProvider;

use crate::error::AiError;
use serde_json::Value;

/// One schema-bound inference request.
#[derive(Debug, Clone)]
pub struct InferenceRequest {
    /// The fixed instruction for the transform.
    pub instruction: String,
    /// The record's selected input values, keyed by column name.
    pub inputs: Value,
    /// JSON Schema the output must satisfy (also sent to the model).
    pub output_schema: Value,
    /// Hard cap on output tokens, enforced provider-side where supported.
    pub max_output_tokens: Option<u32>,
}

/// A provider's answer plus its billing accounting.
#[derive(Debug, Clone)]
pub struct ProviderResponse {
    /// Raw output text (expected to be a single JSON object).
    pub text: String,
    /// Input tokens billed.
    pub input_tokens: u64,
    /// Output tokens billed.
    pub output_tokens: u64,
    /// Provider-issued request identifier for provenance.
    pub request_id: String,
}

/// What an adapter can and cannot do, declared rather than discovered.
#[derive(Debug, Clone, Copy)]
pub struct Capabilities {
    /// Supports synchronous invocation.
    pub online: bool,
    /// Supports asynchronous provider-batch execution.
    pub batch: bool,
    /// Enforces a JSON output format provider-side.
    pub structured_output: bool,
    /// Reports token usage in responses.
    pub token_accounting: bool,
}

/// The observed state of a provider-batch job.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BatchStatus {
    /// Submitted and still running provider-side.
    InProgress,
    /// Finished; results are ready to fetch.
    Completed,
    /// The job as a whole failed (individual item failures are reported
    /// per item in the fetched results instead).
    Failed(String),
}

/// One item's outcome within a fetched batch: the response, or a
/// provider-reported per-item failure.
pub type BatchItemResult = (String, Result<ProviderResponse, String>);

/// Strip Markdown code fences some models wrap around JSON output.
pub(crate) fn strip_fences(text: &str) -> &str {
    let trimmed = text.trim();
    let Some(inner) = trimmed.strip_prefix("```") else {
        return trimmed;
    };
    let inner = inner.strip_prefix("json").unwrap_or(inner);
    inner.strip_suffix("```").unwrap_or(inner).trim()
}

/// A model backend adapter.
#[async_trait::async_trait]
pub trait Provider: Send + Sync {
    /// Stable adapter identifier (matches `spec.models.*.provider`).
    fn id(&self) -> &str;

    /// The declared capability report.
    fn capabilities(&self) -> Capabilities;

    /// Perform one online inference.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::Provider`] for transport, authentication, or
    /// provider-side failures. Output *content* problems are not errors
    /// here — validation happens in the operator.
    async fn invoke(&self, request: &InferenceRequest) -> Result<ProviderResponse, AiError>;

    /// Submit work items (keyed by their ledger work key) as one
    /// asynchronous provider-batch job, returning the provider's job id.
    ///
    /// The job id is durably recorded in the ledger before results are
    /// awaited, so a crashed run reconciles by job and item id instead of
    /// resubmitting (and re-paying for) the same work.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::Unsupported`] unless `capabilities().batch`.
    async fn submit_batch(&self, items: &[(String, InferenceRequest)]) -> Result<String, AiError> {
        let _ = items;
        Err(AiError::Unsupported(format!(
            "provider `{}` does not implement batch execution",
            self.id()
        )))
    }

    /// The current status of a previously submitted batch job.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::Provider`] when the job cannot be looked up and
    /// [`AiError::Unsupported`] unless `capabilities().batch`.
    async fn poll_batch(&self, job_id: &str) -> Result<BatchStatus, AiError> {
        let _ = job_id;
        Err(AiError::Unsupported(format!(
            "provider `{}` does not implement batch execution",
            self.id()
        )))
    }

    /// Fetch the per-item results of a [`BatchStatus::Completed`] job.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::Provider`] when results cannot be retrieved and
    /// [`AiError::Unsupported`] unless `capabilities().batch`.
    async fn fetch_batch(&self, job_id: &str) -> Result<Vec<BatchItemResult>, AiError> {
        let _ = job_id;
        Err(AiError::Unsupported(format!(
            "provider `{}` does not implement batch execution",
            self.id()
        )))
    }
}
