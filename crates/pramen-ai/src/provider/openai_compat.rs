//! Adapter for OpenAI-compatible chat-completions endpoints (vLLM, Ollama,
//! llama.cpp server, and hosted OpenAI-protocol services).

use super::{Capabilities, InferenceRequest, Provider, ProviderResponse, strip_fences};
use crate::error::AiError;
use serde::Deserialize;
use serde_json::json;

/// Calls `POST {endpoint}/chat/completions` and expects a single JSON
/// object back in the assistant message.
pub struct OpenAiCompatProvider {
    client: reqwest::Client,
    endpoint: String,
    model: String,
    api_key: Option<String>,
}

#[derive(Deserialize)]
struct ChatResponse {
    id: Option<String>,
    choices: Vec<Choice>,
    usage: Option<Usage>,
}

#[derive(Deserialize)]
struct Choice {
    message: Message,
}

#[derive(Deserialize)]
struct Message {
    content: String,
}

#[derive(Deserialize)]
struct Usage {
    prompt_tokens: Option<u64>,
    completion_tokens: Option<u64>,
}

impl OpenAiCompatProvider {
    /// Create an adapter for `endpoint` (e.g. `http://localhost:11434/v1`)
    /// and `model`. `api_key` is optional for local servers.
    #[must_use]
    pub fn new(endpoint: &str, model: &str, api_key: Option<String>) -> Self {
        Self {
            client: reqwest::Client::new(),
            endpoint: endpoint.trim_end_matches('/').to_owned(),
            model: model.to_owned(),
            api_key,
        }
    }

    fn provider_error(&self, message: impl std::fmt::Display) -> AiError {
        AiError::Provider {
            provider: self.id().to_owned(),
            message: message.to_string(),
        }
    }
}

#[async_trait::async_trait]
impl Provider for OpenAiCompatProvider {
    fn id(&self) -> &str {
        "openai-compat"
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities {
            online: true,
            batch: false,
            structured_output: true,
            token_accounting: true,
        }
    }

    async fn invoke(&self, request: &InferenceRequest) -> Result<ProviderResponse, AiError> {
        let system = format!(
            "{}\n\nRespond with exactly one JSON object that conforms to this JSON Schema, \
             with no additional text:\n{}",
            request.instruction, request.output_schema
        );
        let mut body = json!({
            "model": self.model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": request.inputs.to_string()},
            ],
            "temperature": 0,
            "response_format": {"type": "json_object"},
        });
        if let Some(cap) = request.max_output_tokens
            && let Some(map) = body.as_object_mut()
        {
            map.insert("max_tokens".to_owned(), json!(cap));
        }

        let mut http = self
            .client
            .post(format!("{}/chat/completions", self.endpoint))
            .json(&body);
        if let Some(key) = &self.api_key {
            http = http.bearer_auth(key);
        }
        let response = http
            .send()
            .await
            .map_err(|e| self.provider_error(format!("request failed: {e}")))?;

        let status = response.status();
        if !status.is_success() {
            let detail = response.text().await.unwrap_or_default();
            return Err(self.provider_error(format!("HTTP {status}: {detail}")));
        }
        let parsed: ChatResponse = response
            .json()
            .await
            .map_err(|e| self.provider_error(format!("malformed response: {e}")))?;
        let choice = parsed
            .choices
            .first()
            .ok_or_else(|| self.provider_error("response contained no choices"))?;

        Ok(ProviderResponse {
            text: strip_fences(&choice.message.content).to_owned(),
            input_tokens: parsed
                .usage
                .as_ref()
                .and_then(|u| u.prompt_tokens)
                .unwrap_or(0),
            output_tokens: parsed
                .usage
                .as_ref()
                .and_then(|u| u.completion_tokens)
                .unwrap_or(0),
            request_id: parsed.id.unwrap_or_default(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufRead, BufReader, Read, Write};
    use std::net::TcpListener;

    #[test]
    fn fences_are_stripped() {
        assert_eq!(strip_fences("{\"a\":1}"), "{\"a\":1}");
        assert_eq!(strip_fences("```json\n{\"a\":1}\n```"), "{\"a\":1}");
        assert_eq!(strip_fences("```\n{\"a\":1}\n```"), "{\"a\":1}");
    }

    /// L1 protocol-stub test (ADR 0005): the real adapter against a local
    /// HTTP server returning a canned OpenAI-shaped response.
    #[tokio::test(flavor = "multi_thread")]
    async fn adapter_parses_openai_shaped_responses() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut reader = BufReader::new(&mut stream);
            let mut request_line = String::new();
            reader.read_line(&mut request_line).unwrap();
            let mut content_length = 0;
            loop {
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                    content_length = value.trim().parse().unwrap();
                }
                if line == "\r\n" {
                    break;
                }
            }
            let mut body = vec![0u8; content_length];
            reader.read_exact(&mut body).unwrap();
            let request: serde_json::Value = serde_json::from_slice(&body).unwrap();

            let payload = json!({
                "id": "chatcmpl-stub-1",
                "choices": [{"message": {"role": "assistant",
                    "content": "```json\n{\"category\":\"billing\"}\n```"}}],
                "usage": {"prompt_tokens": 42, "completion_tokens": 7}
            })
            .to_string();
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{payload}",
                payload.len()
            );
            stream.write_all(response.as_bytes()).unwrap();
            (request_line, request)
        });

        let provider = OpenAiCompatProvider::new(&format!("http://{addr}/v1"), "test-model", None);
        let response = provider
            .invoke(&InferenceRequest {
                instruction: "classify".into(),
                inputs: json!({"description": "invoice is wrong"}),
                output_schema: json!({"type": "object"}),
                max_output_tokens: Some(64),
            })
            .await
            .unwrap();

        assert_eq!(response.text, "{\"category\":\"billing\"}");
        assert_eq!(response.input_tokens, 42);
        assert_eq!(response.output_tokens, 7);
        assert_eq!(response.request_id, "chatcmpl-stub-1");

        let (request_line, request) = server.join().unwrap();
        assert!(request_line.starts_with("POST /v1/chat/completions"));
        assert_eq!(request["model"], "test-model");
        assert_eq!(request["max_tokens"], 64);
        assert_eq!(
            request["messages"][1]["content"],
            "{\"description\":\"invoice is wrong\"}"
        );
    }
}
