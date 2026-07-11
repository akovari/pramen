//! The provider abstraction: one trait, uniform governance for every model
//! backend.
//!
//! Budgets, validation, the ledger, and provenance live *outside* the
//! adapters — an adapter only turns an [`InferenceRequest`] into a
//! [`ProviderResponse`]. Available adapters: [`MockProvider`] (deterministic,
//! offline, for tests and pipeline dry-runs) and [`OpenAiCompatProvider`]
//! (vLLM, Ollama, llama.cpp, or any OpenAI-compatible endpoint). Amazon
//! Bedrock arrives with P1.7.

mod mock;
mod openai_compat;

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
}
