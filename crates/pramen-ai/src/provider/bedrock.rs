//! Amazon Bedrock Converse online adapter.
//!
//! Credentials come from the AWS default chain (environment, profile, SSO,
//! IMDS) — never from the pipeline document. The region is pinned per
//! model declaration; an endpoint override exists for the local protocol
//! stubs (ADR 0005, layer L1), where the request, parsing, and error paths
//! are byte-for-byte the production ones.

use super::{Capabilities, InferenceRequest, Provider, ProviderResponse, strip_fences};
use crate::error::AiError;
use aws_sdk_bedrockruntime::operation::RequestId;
use aws_sdk_bedrockruntime::types::{
    ContentBlock, ConversationRole, InferenceConfiguration, Message, SystemContentBlock,
};

/// Calls the Bedrock Converse API for one model.
pub struct BedrockProvider {
    client: aws_sdk_bedrockruntime::Client,
    model: String,
}

impl BedrockProvider {
    /// Create an adapter for `model` using the AWS default credential
    /// chain. `region` pins the provider region (falls back to the
    /// environment's default when `None`); `endpoint` overrides the API
    /// endpoint — used by local protocol stubs, with static test
    /// credentials so the chain never consults the network.
    pub async fn new(model: &str, region: Option<&str>, endpoint: Option<&str>) -> Self {
        let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest());
        if let Some(region) = region {
            loader = loader.region(aws_config::Region::new(region.to_owned()));
        }
        if let Some(endpoint) = endpoint {
            loader = loader.endpoint_url(endpoint).credentials_provider(
                aws_sdk_bedrockruntime::config::Credentials::new(
                    "test-access-key",
                    "test-secret-key",
                    None,
                    None,
                    "static-endpoint-override",
                ),
            );
        }
        let config = loader.load().await;
        Self {
            client: aws_sdk_bedrockruntime::Client::new(&config),
            model: model.to_owned(),
        }
    }

    fn provider_error(
        &self,
        fault: crate::error::ProviderFault,
        message: impl std::fmt::Display,
    ) -> AiError {
        AiError::Provider {
            provider: self.id().to_owned(),
            fault,
            message: message.to_string(),
        }
    }

    /// Classify a Converse SDK failure onto the typed fault taxonomy.
    fn classify<R>(
        error: &aws_sdk_bedrockruntime::error::SdkError<
            aws_sdk_bedrockruntime::operation::converse::ConverseError,
            R,
        >,
    ) -> crate::error::ProviderFault {
        use crate::error::ProviderFault;
        use aws_sdk_bedrockruntime::error::SdkError;
        use aws_sdk_bedrockruntime::operation::converse::ConverseError;
        match error {
            SdkError::TimeoutError(_) => ProviderFault::Timeout,
            SdkError::DispatchFailure(failure) if failure.is_timeout() => ProviderFault::Timeout,
            SdkError::DispatchFailure(_) => ProviderFault::Transport,
            SdkError::ResponseError(_) => ProviderFault::Protocol,
            SdkError::ServiceError(service) => match service.err() {
                ConverseError::ThrottlingException(_) => ProviderFault::Throttled,
                ConverseError::ModelTimeoutException(_) => ProviderFault::Timeout,
                _ => ProviderFault::Server,
            },
            _ => ProviderFault::Transport,
        }
    }
}

#[async_trait::async_trait]
impl Provider for BedrockProvider {
    fn id(&self) -> &str {
        "bedrock"
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities {
            online: true,
            // Provider-batch (CreateModelInvocationJob) lands with P1.8.
            batch: false,
            // JSON output is prompt-enforced; Converse has no response_format.
            structured_output: false,
            token_accounting: true,
        }
    }

    async fn invoke(&self, request: &InferenceRequest) -> Result<ProviderResponse, AiError> {
        let system = format!(
            "{}\n\nRespond with exactly one JSON object that conforms to this JSON Schema, \
             with no additional text:\n{}",
            request.instruction, request.output_schema
        );
        let message = Message::builder()
            .role(ConversationRole::User)
            .content(ContentBlock::Text(request.inputs.to_string()))
            .build()
            .map_err(|e| {
                self.provider_error(
                    crate::error::ProviderFault::Protocol,
                    format!("build message: {e}"),
                )
            })?;
        let mut inference = InferenceConfiguration::builder().temperature(0.0);
        if let Some(cap) = request.max_output_tokens {
            inference = inference.max_tokens(i32::try_from(cap).unwrap_or(i32::MAX));
        }

        let response = self
            .client
            .converse()
            .model_id(&self.model)
            .system(SystemContentBlock::Text(system))
            .messages(message)
            .inference_config(inference.build())
            .send()
            .await
            .map_err(|e| {
                let fault = Self::classify(&e);
                self.provider_error(fault, aws_sdk_bedrockruntime::error::DisplayErrorContext(e))
            })?;

        let request_id = response.request_id().unwrap_or_default().to_owned();
        let usage = response.usage().ok_or_else(|| {
            self.provider_error(
                crate::error::ProviderFault::Protocol,
                "response is missing usage accounting",
            )
        })?;
        let text = response
            .output()
            .and_then(|o| o.as_message().ok())
            .and_then(|m| m.content().first())
            .and_then(|c| c.as_text().ok())
            .ok_or_else(|| {
                self.provider_error(
                    crate::error::ProviderFault::Protocol,
                    "response contained no text content",
                )
            })?;

        Ok(ProviderResponse {
            text: strip_fences(text).to_owned(),
            input_tokens: u64::try_from(usage.input_tokens()).unwrap_or_default(),
            output_tokens: u64::try_from(usage.output_tokens()).unwrap_or_default(),
            request_id,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pramen_testkit::http::one_shot_json;
    use serde_json::json;

    /// L1 protocol-stub test (ADR 0005): the real adapter and AWS SDK
    /// against a local HTTP server returning a canned Converse response.
    #[tokio::test(flavor = "multi_thread")]
    async fn adapter_parses_converse_responses() {
        let (base, server) = one_shot_json(
            json!({
                "output": {"message": {"role": "assistant",
                    "content": [{"text": "{\"category\":\"incident\"}"}]}},
                "stopReason": "end_turn",
                "usage": {"inputTokens": 55, "outputTokens": 9, "totalTokens": 64}
            }),
            &[("x-amzn-requestid", "req-stub-42")],
        );

        let provider = BedrockProvider::new(
            "anthropic.claude-3-haiku-20240307-v1:0",
            Some("eu-central-1"),
            Some(&base),
        )
        .await;
        let response = provider
            .invoke(&InferenceRequest {
                instruction: "classify the ticket".into(),
                inputs: json!({"description": "printer on fire"}),
                output_schema: json!({"type": "object"}),
                max_output_tokens: Some(128),
            })
            .await
            .unwrap();

        assert_eq!(response.text, "{\"category\":\"incident\"}");
        assert_eq!(response.input_tokens, 55);
        assert_eq!(response.output_tokens, 9);
        assert_eq!(response.request_id, "req-stub-42");

        let captured = server.join().unwrap();
        let request = captured.json();
        assert!(
            captured
                .path
                .contains("/model/anthropic.claude-3-haiku-20240307-v1%3A0/converse"),
            "{}",
            captured.path
        );
        assert_eq!(
            request["messages"][0]["content"][0]["text"],
            "{\"description\":\"printer on fire\"}"
        );
        assert!(
            request["system"][0]["text"]
                .as_str()
                .unwrap()
                .starts_with("classify the ticket"),
            "instruction travels as the system block"
        );
        assert_eq!(request["inferenceConfig"]["maxTokens"], 128);
    }
}
