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
