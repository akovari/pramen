//! Adapter for OpenAI-compatible endpoints (vLLM, Ollama, llama.cpp
//! server, and hosted OpenAI-protocol services).
//!
//! Online inference uses `POST {endpoint}/chat/completions`. Provider-batch
//! execution uses the OpenAI Files + Batches APIs (`/files`, `/batches`,
//! `/files/{id}/content`): items are uploaded as one JSONL file keyed by
//! `custom_id` (the ledger work key), submitted as a single job, polled,
//! and fetched. Hosted OpenAI implements these; most self-hosted servers
//! (Ollama, plain vLLM) do not — using `execution: batch` against one of
//! those fails with a typed `Server` fault at submission, not silently.

use super::{
    BatchItemResult, BatchStatus, Capabilities, InferenceRequest, Provider, ProviderResponse,
    strip_fences,
};
use crate::error::{AiError, ProviderFault};
use serde::Deserialize;
use serde_json::{Value, json};
use std::time::Duration;

/// Default per-request deadline. Local models on modest hardware can be
/// slow, so this is generous; tighten it per deployment via
/// [`OpenAiCompatProvider::with_timeout`].
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(120);

/// Calls `POST {endpoint}/chat/completions` and expects a single JSON
/// object back in the assistant message.
///
/// Failures are classified onto [`ProviderFault`]: deadline overruns are
/// `Timeout`, refused/reset connections `Transport`, HTTP 429 `Throttled`,
/// other non-success statuses `Server`, and well-formed HTTP with a body
/// that is not protocol-shaped `Protocol`.
pub struct OpenAiCompatProvider {
    client: reqwest::Client,
    endpoint: String,
    model: String,
    api_key: Option<String>,
    timeout: Duration,
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

#[derive(Deserialize)]
struct FileResponse {
    id: String,
}

#[derive(Deserialize)]
struct BatchResponse {
    id: String,
    status: String,
    output_file_id: Option<String>,
    error_file_id: Option<String>,
    errors: Option<Value>,
}

/// One line of a batch output/error file.
#[derive(Deserialize)]
struct BatchLine {
    custom_id: String,
    response: Option<BatchLineResponse>,
    error: Option<Value>,
}

#[derive(Deserialize)]
struct BatchLineResponse {
    status_code: Option<u16>,
    body: Option<Value>,
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
            timeout: DEFAULT_TIMEOUT,
        }
    }

    /// Override the per-request deadline (default two minutes).
    #[must_use]
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    fn provider_error(&self, fault: ProviderFault, message: impl std::fmt::Display) -> AiError {
        AiError::Provider {
            provider: self.id().to_owned(),
            fault,
            message: message.to_string(),
        }
    }

    /// Classify a reqwest transport-level failure.
    fn classify(error: &reqwest::Error) -> ProviderFault {
        if error.is_timeout() {
            ProviderFault::Timeout
        } else if error.is_connect() {
            ProviderFault::Transport
        } else if error.is_decode() {
            ProviderFault::Protocol
        } else {
            ProviderFault::Transport
        }
    }

    /// The chat-completions request body for one inference request —
    /// identical for online calls and batch file lines, so both execution
    /// shapes send byte-for-byte the same prompt.
    fn chat_body(&self, request: &InferenceRequest) -> Value {
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
        body
    }

    /// Send a request, classifying transport failures and non-success
    /// statuses onto typed faults.
    async fn send(&self, builder: reqwest::RequestBuilder) -> Result<reqwest::Response, AiError> {
        let builder = match &self.api_key {
            Some(key) => builder.bearer_auth(key),
            None => builder,
        };
        let response =
            builder.timeout(self.timeout).send().await.map_err(|e| {
                self.provider_error(Self::classify(&e), format!("request failed: {e}"))
            })?;
        let status = response.status();
        if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            let detail = response.text().await.unwrap_or_default();
            return Err(self.provider_error(
                ProviderFault::Throttled,
                format!("HTTP 429 Too Many Requests: {detail}"),
            ));
        }
        if !status.is_success() {
            let detail = response.text().await.unwrap_or_default();
            return Err(
                self.provider_error(ProviderFault::Server, format!("HTTP {status}: {detail}"))
            );
        }
        Ok(response)
    }

    /// Turn one chat-completions response body into a [`ProviderResponse`].
    fn parse_chat(&self, parsed: ChatResponse) -> Result<ProviderResponse, AiError> {
        let choice = parsed.choices.first().ok_or_else(|| {
            self.provider_error(ProviderFault::Protocol, "response contained no choices")
        })?;
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

    /// Download a batch result file and parse its JSONL lines into
    /// per-item outcomes.
    async fn fetch_result_file(
        &self,
        file_id: &str,
        results: &mut Vec<BatchItemResult>,
    ) -> Result<(), AiError> {
        let content = self
            .send(
                self.client
                    .get(format!("{}/files/{file_id}/content", self.endpoint)),
            )
            .await?
            .text()
            .await
            .map_err(|e| {
                self.provider_error(ProviderFault::Protocol, format!("result file read: {e}"))
            })?;
        for line in content.lines().filter(|l| !l.trim().is_empty()) {
            let parsed: BatchLine = serde_json::from_str(line).map_err(|e| {
                self.provider_error(
                    ProviderFault::Protocol,
                    format!("malformed batch result line: {e}"),
                )
            })?;
            let outcome = match (parsed.response, parsed.error) {
                (Some(response), None) => {
                    let ok = response.status_code.is_none_or(|c| (200..300).contains(&c));
                    match (ok, response.body) {
                        (true, Some(body)) => serde_json::from_value::<ChatResponse>(body)
                            .map_err(|e| format!("malformed item body: {e}"))
                            .and_then(|chat| self.parse_chat(chat).map_err(|e| e.to_string())),
                        (true, None) => Err("item response had no body".to_owned()),
                        (false, body) => Err(format!(
                            "item failed with status {}: {}",
                            response.status_code.unwrap_or_default(),
                            body.unwrap_or(Value::Null)
                        )),
                    }
                }
                (_, Some(error)) => Err(format!("item failed provider-side: {error}")),
                (None, None) => Err("item had neither response nor error".to_owned()),
            };
            results.push((parsed.custom_id, outcome));
        }
        Ok(())
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
            batch: true,
            structured_output: true,
            token_accounting: true,
        }
    }

    async fn invoke(&self, request: &InferenceRequest) -> Result<ProviderResponse, AiError> {
        let body = self.chat_body(request);
        let response = self
            .send(
                self.client
                    .post(format!("{}/chat/completions", self.endpoint))
                    .json(&body),
            )
            .await?;
        let parsed: ChatResponse = response.json().await.map_err(|e| {
            self.provider_error(ProviderFault::Protocol, format!("malformed response: {e}"))
        })?;
        self.parse_chat(parsed)
    }

    async fn submit_batch(&self, items: &[(String, InferenceRequest)]) -> Result<String, AiError> {
        let mut jsonl = String::new();
        for (key, request) in items {
            let line = json!({
                "custom_id": key,
                "method": "POST",
                "url": "/v1/chat/completions",
                "body": self.chat_body(request),
            });
            jsonl.push_str(&line.to_string());
            jsonl.push('\n');
        }

        let form = reqwest::multipart::Form::new()
            .text("purpose", "batch")
            .part(
                "file",
                reqwest::multipart::Part::text(jsonl)
                    .file_name("pramen-batch.jsonl")
                    .mime_str("application/jsonl")
                    .map_err(|e| self.provider_error(ProviderFault::Protocol, e))?,
            );
        let file: FileResponse = self
            .send(
                self.client
                    .post(format!("{}/files", self.endpoint))
                    .multipart(form),
            )
            .await?
            .json()
            .await
            .map_err(|e| {
                self.provider_error(
                    ProviderFault::Protocol,
                    format!("file upload response: {e}"),
                )
            })?;

        let batch: BatchResponse = self
            .send(
                self.client
                    .post(format!("{}/batches", self.endpoint))
                    .json(&json!({
                        "input_file_id": file.id,
                        "endpoint": "/v1/chat/completions",
                        "completion_window": "24h",
                    })),
            )
            .await?
            .json()
            .await
            .map_err(|e| {
                self.provider_error(
                    ProviderFault::Protocol,
                    format!("batch create response: {e}"),
                )
            })?;
        Ok(batch.id)
    }

    async fn poll_batch(&self, job_id: &str) -> Result<BatchStatus, AiError> {
        let batch: BatchResponse = self
            .send(
                self.client
                    .get(format!("{}/batches/{job_id}", self.endpoint)),
            )
            .await?
            .json()
            .await
            .map_err(|e| {
                self.provider_error(ProviderFault::Protocol, format!("batch poll response: {e}"))
            })?;
        Ok(match batch.status.as_str() {
            "validating" | "in_progress" | "finalizing" | "cancelling" => BatchStatus::InProgress,
            "completed" => BatchStatus::Completed,
            other => BatchStatus::Failed(format!(
                "batch ended in state `{other}`: {}",
                batch.errors.unwrap_or(Value::Null)
            )),
        })
    }

    async fn fetch_batch(&self, job_id: &str) -> Result<Vec<BatchItemResult>, AiError> {
        let batch: BatchResponse = self
            .send(
                self.client
                    .get(format!("{}/batches/{job_id}", self.endpoint)),
            )
            .await?
            .json()
            .await
            .map_err(|e| {
                self.provider_error(
                    ProviderFault::Protocol,
                    format!("batch fetch response: {e}"),
                )
            })?;

        let mut results = Vec::new();
        if let Some(file_id) = &batch.output_file_id {
            self.fetch_result_file(file_id, &mut results).await?;
        }
        if let Some(file_id) = &batch.error_file_id {
            self.fetch_result_file(file_id, &mut results).await?;
        }
        if results.is_empty() {
            return Err(self.provider_error(
                ProviderFault::Protocol,
                format!("batch `{job_id}` exposed no output or error file"),
            ));
        }
        Ok(results)
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
