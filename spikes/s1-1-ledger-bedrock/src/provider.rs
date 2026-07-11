//! Provider abstraction for the spike: a deterministic mock (bills per call,
//! so tests can assert reuse) and Amazon Bedrock Converse online.

use crate::workkey::WorkSpec;
use anyhow::{Context, Result, anyhow};
use serde_json::{Value, json};
use std::sync::atomic::{AtomicU64, Ordering};

pub struct ProviderResponse {
    pub output: Value,
    pub request_id: String,
    pub input_tokens: u64,
    pub output_tokens: u64,
}

pub trait Provider {
    fn name(&self) -> &str;
    fn invoke(&self, spec: &WorkSpec) -> Result<ProviderResponse>;
}

/// Deterministic mock: every invocation increments a billing counter. Tests
/// assert the counter to prove that recorded results are reused.
#[derive(Default)]
pub struct MockProvider {
    pub calls: AtomicU64,
}

impl Provider for MockProvider {
    fn name(&self) -> &str {
        "mock"
    }

    fn invoke(&self, spec: &WorkSpec) -> Result<ProviderResponse> {
        let n = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        let description = spec.inputs["description"].as_str().unwrap_or_default();
        let category = if description.contains("fire") || description.contains("crash") {
            "incident"
        } else if description.contains("invoice") || description.contains("billing") {
            "billing"
        } else {
            "question"
        };
        Ok(ProviderResponse {
            output: json!({
                "category": category,
                "priority": if category == "incident" { "high" } else { "normal" },
                "rationale": format!("matched keyword rules over: {description}"),
            }),
            request_id: format!("mock-req-{n}"),
            input_tokens: description.len() as u64 / 4,
            output_tokens: 32,
        })
    }
}

/// Any OpenAI-compatible chat-completions endpoint: vLLM, Ollama, or
/// llama.cpp locally (ADR 0005, layer L2 real local inference), and hosted
/// compatibles later. JSON output is requested via `response_format` where
/// the server honors it and parsed from the message content.
pub struct OpenAiCompatProvider {
    endpoint: String,
    api_key: Option<String>,
    client: reqwest::blocking::Client,
}

impl OpenAiCompatProvider {
    pub fn new(endpoint: &str, api_key: Option<String>) -> Result<Self> {
        Ok(Self {
            endpoint: endpoint.trim_end_matches('/').to_owned(),
            api_key,
            client: reqwest::blocking::Client::builder()
                .timeout(std::time::Duration::from_secs(120))
                .build()?,
        })
    }
}

impl Provider for OpenAiCompatProvider {
    fn name(&self) -> &str {
        "openai-compat"
    }

    fn invoke(&self, spec: &WorkSpec) -> Result<ProviderResponse> {
        let prompt = format!(
            "You are a strict data extraction function. Given the input record, \
             respond with ONLY a JSON object matching this JSON Schema, no prose:\n\
             {}\n\nInput record:\n{}",
            spec.output_schema, spec.inputs
        );
        let body = json!({
            "model": spec.model,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": spec.params.get("temperature").cloned().unwrap_or(json!(0)),
            "response_format": {"type": "json_object"},
        });

        let mut request = self
            .client
            .post(format!("{}/chat/completions", self.endpoint))
            .json(&body);
        if let Some(key) = &self.api_key {
            request = request.bearer_auth(key);
        }
        let response: Value = request
            .send()
            .context("openai-compatible request failed")?
            .error_for_status()
            .context("openai-compatible endpoint returned an error status")?
            .json()
            .context("response body is not JSON")?;

        let text = response["choices"][0]["message"]["content"]
            .as_str()
            .context("no message content in response")?;
        let start = text.find('{').context("no JSON object in response")?;
        let end = text.rfind('}').context("no JSON object in response")?;
        let output: Value =
            serde_json::from_str(&text[start..=end]).context("model output is not valid JSON")?;

        Ok(ProviderResponse {
            output,
            request_id: response["id"]
                .as_str()
                .map(str::to_owned)
                .unwrap_or_else(|| format!("local-{}", uuid::Uuid::new_v4())),
            input_tokens: response["usage"]["prompt_tokens"].as_u64().unwrap_or(0),
            output_tokens: response["usage"]["completion_tokens"].as_u64().unwrap_or(0),
        })
    }
}

/// Amazon Bedrock Converse online, region-pinned, with JSON output requested
/// through the prompt and parsed from the first text block.
pub struct BedrockProvider {
    client: aws_sdk_bedrockruntime::Client,
    runtime: tokio::runtime::Runtime,
}

impl BedrockProvider {
    pub fn new(region: &str) -> Result<Self> {
        let runtime = tokio::runtime::Runtime::new()?;
        let region = aws_config::Region::new(region.to_owned());
        let config = runtime.block_on(
            aws_config::defaults(aws_config::BehaviorVersion::latest())
                .region(region)
                .load(),
        );
        Ok(Self {
            client: aws_sdk_bedrockruntime::Client::new(&config),
            runtime,
        })
    }

    /// Local-first testing seam (ADR 0005, layer L1): point the real adapter
    /// at a localhost protocol stub with static credentials. The request,
    /// parsing, validation, and recording code paths are identical to
    /// production; only the endpoint differs.
    pub fn with_endpoint(region: &str, endpoint_url: &str) -> Result<Self> {
        let runtime = tokio::runtime::Runtime::new()?;
        let config = aws_sdk_bedrockruntime::config::Builder::new()
            .behavior_version(aws_config::BehaviorVersion::latest())
            .region(aws_config::Region::new(region.to_owned()))
            .endpoint_url(endpoint_url)
            .credentials_provider(aws_sdk_bedrockruntime::config::Credentials::new(
                "test-access-key",
                "test-secret-key",
                None,
                None,
                "static-test",
            ))
            .build();
        Ok(Self {
            client: aws_sdk_bedrockruntime::Client::from_conf(config),
            runtime,
        })
    }
}

impl Provider for BedrockProvider {
    fn name(&self) -> &str {
        "bedrock"
    }

    fn invoke(&self, spec: &WorkSpec) -> Result<ProviderResponse> {
        use aws_sdk_bedrockruntime::operation::RequestId;
        use aws_sdk_bedrockruntime::types::{ContentBlock, ConversationRole, Message};

        let prompt = format!(
            "You are a strict data extraction function. Given the input record, \
             respond with ONLY a JSON object matching this JSON Schema, no prose:\n\
             {}\n\nInput record:\n{}",
            spec.output_schema, spec.inputs
        );
        let message = Message::builder()
            .role(ConversationRole::User)
            .content(ContentBlock::Text(prompt))
            .build()
            .map_err(|e| anyhow!("build message: {e}"))?;

        let response = self
            .runtime
            .block_on(
                self.client
                    .converse()
                    .model_id(&spec.model)
                    .messages(message)
                    .send(),
            )
            .map_err(|e| anyhow!("bedrock converse: {}", aws_sdk_bedrockruntime::error::DisplayErrorContext(e)))?;

        let request_id = response
            .request_id()
            .map(str::to_owned)
            .unwrap_or_else(|| format!("local-{}", uuid::Uuid::new_v4()));
        let usage = response.usage().context("missing usage")?;
        let text = response
            .output()
            .and_then(|o| o.as_message().ok())
            .and_then(|m| m.content().first())
            .and_then(|c| c.as_text().ok())
            .context("no text content in response")?
            .clone();
        let start = text.find('{').context("no JSON object in response")?;
        let end = text.rfind('}').context("no JSON object in response")?;
        let output: Value =
            serde_json::from_str(&text[start..=end]).context("model output is not valid JSON")?;

        Ok(ProviderResponse {
            output,
            request_id,
            input_tokens: usage.input_tokens() as u64,
            output_tokens: usage.output_tokens() as u64,
        })
    }
}
