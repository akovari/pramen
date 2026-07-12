//! Amazon Bedrock adapter: Converse online, model invocation jobs batch.
//!
//! Credentials come from the AWS default chain (environment, profile, SSO,
//! IMDS) — never from the pipeline document. The region is pinned per
//! model declaration; an endpoint override exists for the local protocol
//! stubs (ADR 0005, layer L1), where the request, parsing, and error paths
//! are byte-for-byte the production ones.
//!
//! Provider-batch execution uses Bedrock **model invocation jobs**
//! ([`BedrockBatchConfig`]): items are staged as JSONL under the
//! configured S3 prefix, submitted as one `CreateModelInvocationJob`,
//! polled via `GetModelInvocationJob`, and read back from the job's S3
//! output. Jobs use the default `InvokeModel` invocation type with the
//! Anthropic Messages body format — the v1 hosted profile is Claude on
//! Bedrock (Converse-format jobs need a control-plane SDK newer than our
//! MSRV allows; switch when the toolchain moves). Because Bedrock has
//! historically mangled user `recordId`s, a `keys.jsonl` companion
//! (recordId **and** a canonical modelInput hash, each mapped to the
//! ledger work key) is staged next to the input, so results always join
//! back to work keys — even across a crash, since everything needed
//! lives in S3 and the job ARN is in the ledger.
//!
//! Live quota note: Bedrock enforces a *minimum* records-per-job quota
//! (1,000 at the time of writing, non-adjustable) — smaller `execution:
//! batch` runs fail provider-side at validation and surface as a typed
//! job failure.

use super::{
    BatchItemResult, BatchStatus, Capabilities, InferenceRequest, Provider, ProviderResponse,
    strip_fences,
};
use crate::error::{AiError, ProviderFault};
use crate::workkey::canonical_json;
use aws_sdk_bedrockruntime::operation::RequestId;
use aws_sdk_bedrockruntime::types::{
    ContentBlock, ConversationRole, InferenceConfiguration, Message, SystemContentBlock,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::HashMap;

/// S3-staged batch execution settings for one model declaration
/// (`spec.models.*.batch` in the pipeline document).
#[derive(Debug, Clone)]
pub struct BedrockBatchConfig {
    /// IAM service role the job assumes to read inputs and write results.
    pub role_arn: String,
    /// S3 staging prefix, e.g. `s3://bucket/pramen-batch/`.
    pub s3: String,
}

/// Calls the Bedrock Converse API for one model; with a
/// [`BedrockBatchConfig`], also runs provider-batch via model invocation
/// jobs.
pub struct BedrockProvider {
    client: aws_sdk_bedrockruntime::Client,
    /// Control plane (job management) — same credentials and endpoint
    /// resolution as the runtime client.
    control: aws_sdk_bedrock::Client,
    model: String,
    batch: Option<BedrockBatchConfig>,
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
            control: aws_sdk_bedrock::Client::new(&config),
            model: model.to_owned(),
            batch: None,
        }
    }

    /// Output-token ceiling for batch lines with no declared budget cap
    /// (the Anthropic Messages format requires an explicit `max_tokens`).
    const DEFAULT_BATCH_MAX_TOKENS: u32 = 4096;

    /// Enable provider-batch execution through the given S3 staging
    /// prefix and service role.
    #[must_use]
    pub fn with_batch(mut self, batch: BedrockBatchConfig) -> Self {
        self.batch = Some(batch);
        self
    }

    fn provider_error(&self, fault: ProviderFault, message: impl std::fmt::Display) -> AiError {
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
    ) -> ProviderFault {
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

    /// Classify a control-plane (bedrock service) SDK failure.
    fn classify_control<E, R>(error: &aws_sdk_bedrock::error::SdkError<E, R>) -> ProviderFault
    where
        E: aws_sdk_bedrock::error::ProvideErrorMetadata,
    {
        use aws_sdk_bedrock::error::SdkError;
        match error {
            SdkError::TimeoutError(_) => ProviderFault::Timeout,
            SdkError::DispatchFailure(failure) if failure.is_timeout() => ProviderFault::Timeout,
            SdkError::DispatchFailure(_) => ProviderFault::Transport,
            SdkError::ResponseError(_) => ProviderFault::Protocol,
            SdkError::ServiceError(service) => match service.err().meta().code() {
                Some("ThrottlingException") => ProviderFault::Throttled,
                _ => ProviderFault::Server,
            },
            _ => ProviderFault::Transport,
        }
    }

    /// The full system prompt (instruction plus the schema contract) for
    /// one request — identical for online calls and batch job lines.
    fn system_prompt(request: &InferenceRequest) -> String {
        format!(
            "{}\n\nRespond with exactly one JSON object that conforms to this JSON Schema, \
             with no additional text:\n{}",
            request.instruction, request.output_schema
        )
    }

    /// The `modelInput` JSON for one batch job line: the Anthropic
    /// Messages body format `InvokeModel`-type jobs expect for Claude
    /// models. `max_tokens` is mandatory in this format, so an undeclared
    /// output cap falls back to [`Self::DEFAULT_BATCH_MAX_TOKENS`].
    fn model_input(request: &InferenceRequest) -> Value {
        json!({
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": request
                .max_output_tokens
                .unwrap_or(Self::DEFAULT_BATCH_MAX_TOKENS),
            "temperature": 0.0,
            "system": Self::system_prompt(request),
            "messages": [{
                "role": "user",
                "content": [{"type": "text", "text": request.inputs.to_string()}],
            }],
        })
    }

    /// SHA-256 hex of a canonicalized `modelInput` — the join fallback
    /// when a result line comes back with a mangled `recordId`.
    fn input_hash(model_input: &Value) -> String {
        let mut hasher = Sha256::new();
        hasher.update(canonical_json(model_input).as_bytes());
        hasher
            .finalize()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect()
    }

    /// Split an `s3://bucket/prefix` URL into bucket and key prefix.
    fn split_s3(&self, url: &str) -> Result<(String, String), AiError> {
        let rest = url.strip_prefix("s3://").ok_or_else(|| {
            self.provider_error(
                ProviderFault::Protocol,
                format!("batch staging URL `{url}` is not an s3:// prefix"),
            )
        })?;
        let (bucket, prefix) = rest.split_once('/').unwrap_or((rest, ""));
        if bucket.is_empty() {
            return Err(self.provider_error(
                ProviderFault::Protocol,
                format!("batch staging URL `{url}` has no bucket"),
            ));
        }
        Ok((bucket.to_owned(), prefix.trim_matches('/').to_owned()))
    }

    /// An S3 store for `bucket`, configured from the standard `AWS_*`
    /// environment (`AWS_ENDPOINT`/`AWS_ALLOW_HTTP` for MinIO and other
    /// S3-compatible services).
    fn s3_store(&self, bucket: &str) -> Result<object_store::aws::AmazonS3, AiError> {
        object_store::aws::AmazonS3Builder::from_env()
            .with_bucket_name(bucket)
            .build()
            .map_err(|e| {
                self.provider_error(
                    ProviderFault::Transport,
                    format!(
                        "S3 staging configuration for bucket `{bucket}`: {e} (set \
                         AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, and \
                         optionally AWS_ENDPOINT / AWS_ALLOW_HTTP)"
                    ),
                )
            })
    }

    /// Read one S3 object as text.
    async fn s3_read(
        &self,
        store: &object_store::aws::AmazonS3,
        key: &str,
    ) -> Result<String, AiError> {
        use object_store::ObjectStore as _;
        let bytes = store
            .get(&object_store::path::Path::from(key))
            .await
            .map_err(|e| {
                self.provider_error(ProviderFault::Transport, format!("S3 read `{key}`: {e}"))
            })?
            .bytes()
            .await
            .map_err(|e| {
                self.provider_error(ProviderFault::Transport, format!("S3 read `{key}`: {e}"))
            })?;
        String::from_utf8(bytes.to_vec()).map_err(|e| {
            self.provider_error(
                ProviderFault::Protocol,
                format!("S3 object `{key}` is not UTF-8: {e}"),
            )
        })
    }

    /// Join fetched result lines back to ledger work keys: by `recordId`
    /// first, then by the canonical `modelInput` hash (Bedrock has been
    /// observed rewriting user record ids).
    fn join_results(
        &self,
        lines: &str,
        by_record_id: &HashMap<String, String>,
        by_input_hash: &HashMap<String, String>,
        results: &mut Vec<BatchItemResult>,
    ) -> Result<(), AiError> {
        for line in lines.lines().filter(|l| !l.trim().is_empty()) {
            let parsed: Value = serde_json::from_str(line).map_err(|e| {
                self.provider_error(
                    ProviderFault::Protocol,
                    format!("malformed batch result line: {e}"),
                )
            })?;
            let record_id = parsed.get("recordId").and_then(Value::as_str);
            let key = record_id
                .and_then(|id| by_record_id.get(id))
                .or_else(|| {
                    parsed
                        .get("modelInput")
                        .map(Self::input_hash)
                        .and_then(|hash| by_input_hash.get(&hash))
                })
                .cloned();
            let Some(key) = key else {
                return Err(self.provider_error(
                    ProviderFault::Protocol,
                    format!(
                        "batch result line (recordId {record_id:?}) matches no staged \
                         work item by record id or input hash"
                    ),
                ));
            };
            let outcome = match (parsed.get("modelOutput"), parsed.get("error")) {
                (Some(output), _) => {
                    Self::parse_model_output(output, record_id.unwrap_or_default())
                }
                (None, Some(error)) => Err(format!("item failed provider-side: {error}")),
                (None, None) => Err("item had neither modelOutput nor error".to_owned()),
            };
            results.push((key, outcome));
        }
        Ok(())
    }

    /// One Anthropic-Messages-shaped `modelOutput` into a
    /// [`ProviderResponse`].
    fn parse_model_output(output: &Value, record_id: &str) -> Result<ProviderResponse, String> {
        let text = output
            .pointer("/content/0/text")
            .and_then(Value::as_str)
            .ok_or_else(|| format!("modelOutput contained no text content: {output}"))?;
        let usage = |field: &str| {
            output
                .pointer(&format!("/usage/{field}"))
                .and_then(Value::as_u64)
                .unwrap_or(0)
        };
        Ok(ProviderResponse {
            text: strip_fences(text).to_owned(),
            input_tokens: usage("input_tokens"),
            output_tokens: usage("output_tokens"),
            request_id: record_id.to_owned(),
        })
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
            // Model invocation jobs need a staging prefix and role.
            batch: self.batch.is_some(),
            // JSON output is prompt-enforced; Converse has no response_format.
            structured_output: false,
            token_accounting: true,
        }
    }

    async fn invoke(&self, request: &InferenceRequest) -> Result<ProviderResponse, AiError> {
        let system = Self::system_prompt(request);
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

    async fn submit_batch(&self, items: &[(String, InferenceRequest)]) -> Result<String, AiError> {
        use object_store::ObjectStore as _;
        let Some(batch) = &self.batch else {
            return Err(AiError::Unsupported(
                "provider `bedrock` needs `batch: {roleArn, s3}` on the model declaration \
                 for batch execution"
                    .to_owned(),
            ));
        };
        let (bucket, prefix) = self.split_s3(&batch.s3)?;
        let store = self.s3_store(&bucket)?;

        // A deterministic stamp over the work keys names the staging
        // paths and the idempotency token: retrying the same submission
        // overwrites the same objects and dedupes provider-side.
        let mut stamp = Sha256::new();
        for (key, _) in items {
            stamp.update(key.as_bytes());
        }
        let stamp: String = stamp
            .finalize()
            .iter()
            .take(8)
            .map(|b| format!("{b:02x}"))
            .collect();

        let mut records = String::new();
        let mut keymap = String::new();
        for (index, (key, request)) in items.iter().enumerate() {
            let record_id = format!("R{index:010}");
            let model_input = Self::model_input(request);
            let line = json!({"recordId": record_id, "modelInput": model_input});
            records.push_str(&line.to_string());
            records.push('\n');
            let mapping = json!({
                "recordId": record_id,
                "inputHash": Self::input_hash(&model_input),
                "workKey": key,
            });
            keymap.push_str(&mapping.to_string());
            keymap.push('\n');
        }

        let dir = if prefix.is_empty() {
            String::new()
        } else {
            format!("{prefix}/")
        };
        let input_key = format!("{dir}input/{stamp}/records.jsonl");
        let keys_key = format!("{dir}input/{stamp}/keys.jsonl");
        let output_uri = format!("s3://{bucket}/{dir}output/{stamp}/");
        for (key, body) in [(&input_key, records), (&keys_key, keymap)] {
            store
                .put(
                    &object_store::path::Path::from(key.as_str()),
                    body.into_bytes().into(),
                )
                .await
                .map_err(|e| {
                    self.provider_error(
                        ProviderFault::Transport,
                        format!("S3 staging write `{key}`: {e}"),
                    )
                })?;
        }

        let created = self
            .control
            .create_model_invocation_job()
            .job_name(format!("pramen-{stamp}"))
            .client_request_token(format!("pramen-{stamp}"))
            .role_arn(&batch.role_arn)
            .model_id(&self.model)
            .input_data_config(
                aws_sdk_bedrock::types::ModelInvocationJobInputDataConfig::S3InputDataConfig(
                    aws_sdk_bedrock::types::ModelInvocationJobS3InputDataConfig::builder()
                        .s3_uri(format!("s3://{bucket}/{input_key}"))
                        .build()
                        .map_err(|e| self.provider_error(ProviderFault::Protocol, e))?,
                ),
            )
            .output_data_config(
                aws_sdk_bedrock::types::ModelInvocationJobOutputDataConfig::S3OutputDataConfig(
                    aws_sdk_bedrock::types::ModelInvocationJobS3OutputDataConfig::builder()
                        .s3_uri(&output_uri)
                        .build()
                        .map_err(|e| self.provider_error(ProviderFault::Protocol, e))?,
                ),
            )
            .send()
            .await
            .map_err(|e| {
                let fault = Self::classify_control(&e);
                self.provider_error(fault, aws_sdk_bedrock::error::DisplayErrorContext(e))
            })?;
        Ok(created.job_arn().to_owned())
    }

    async fn poll_batch(&self, job_id: &str) -> Result<BatchStatus, AiError> {
        use aws_sdk_bedrock::types::ModelInvocationJobStatus as JobStatus;
        let job = self
            .control
            .get_model_invocation_job()
            .job_identifier(job_id)
            .send()
            .await
            .map_err(|e| {
                let fault = Self::classify_control(&e);
                self.provider_error(fault, aws_sdk_bedrock::error::DisplayErrorContext(e))
            })?;
        Ok(match job.status() {
            Some(JobStatus::Completed | JobStatus::PartiallyCompleted) => BatchStatus::Completed,
            Some(
                JobStatus::Submitted
                | JobStatus::Validating
                | JobStatus::Scheduled
                | JobStatus::InProgress
                | JobStatus::Stopping,
            ) => BatchStatus::InProgress,
            other => BatchStatus::Failed(format!(
                "job ended in state {other:?}: {}",
                job.message().unwrap_or("no failure message")
            )),
        })
    }

    async fn fetch_batch(&self, job_id: &str) -> Result<Vec<BatchItemResult>, AiError> {
        use futures::TryStreamExt as _;
        use object_store::ObjectStore as _;
        let job = self
            .control
            .get_model_invocation_job()
            .job_identifier(job_id)
            .send()
            .await
            .map_err(|e| {
                let fault = Self::classify_control(&e);
                self.provider_error(fault, aws_sdk_bedrock::error::DisplayErrorContext(e))
            })?;
        let input_uri = job
            .input_data_config()
            .and_then(|c| c.as_s3_input_data_config().ok())
            .map(|c| c.s3_uri().to_owned())
            .ok_or_else(|| {
                self.provider_error(
                    ProviderFault::Protocol,
                    format!("job `{job_id}` reports no S3 input configuration"),
                )
            })?;
        let output_uri = job
            .output_data_config()
            .and_then(|c| c.as_s3_output_data_config().ok())
            .map(|c| c.s3_uri().to_owned())
            .ok_or_else(|| {
                self.provider_error(
                    ProviderFault::Protocol,
                    format!("job `{job_id}` reports no S3 output configuration"),
                )
            })?;

        // The key map was staged next to the input file at submission.
        let (bucket, input_key) = self.split_s3(&input_uri)?;
        let store = self.s3_store(&bucket)?;
        let keys_key = input_key
            .strip_suffix("records.jsonl")
            .map(|dir| format!("{dir}keys.jsonl"))
            .ok_or_else(|| {
                self.provider_error(
                    ProviderFault::Protocol,
                    format!("job input `{input_uri}` was not staged by this adapter"),
                )
            })?;
        let mut by_record_id = HashMap::new();
        let mut by_input_hash = HashMap::new();
        for line in self
            .s3_read(&store, &keys_key)
            .await?
            .lines()
            .filter(|l| !l.trim().is_empty())
        {
            let mapping: Value = serde_json::from_str(line).map_err(|e| {
                self.provider_error(
                    ProviderFault::Protocol,
                    format!("malformed key-map line: {e}"),
                )
            })?;
            let field = |name: &str| {
                mapping
                    .get(name)
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned()
            };
            by_record_id.insert(field("recordId"), field("workKey"));
            by_input_hash.insert(field("inputHash"), field("workKey"));
        }

        // Bedrock writes results under a job-id subfolder of the output
        // URI; list everything below it and read every JSONL-out object.
        let (output_bucket, output_prefix) = self.split_s3(&output_uri)?;
        let output_store = self.s3_store(&output_bucket)?;
        let listed: Vec<object_store::ObjectMeta> = output_store
            .list(Some(&object_store::path::Path::from(
                output_prefix.as_str(),
            )))
            .try_collect()
            .await
            .map_err(|e| {
                self.provider_error(
                    ProviderFault::Transport,
                    format!("S3 list `{output_uri}`: {e}"),
                )
            })?;

        let mut results = Vec::new();
        for object in listed {
            let name = object.location.as_ref();
            if !name.ends_with(".jsonl.out") {
                continue;
            }
            let content = self.s3_read(&output_store, name).await?;
            self.join_results(&content, &by_record_id, &by_input_hash, &mut results)?;
        }
        if results.is_empty() {
            return Err(self.provider_error(
                ProviderFault::Protocol,
                format!("job `{job_id}` produced no result records under `{output_uri}`"),
            ));
        }
        Ok(results)
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

    fn request() -> InferenceRequest {
        InferenceRequest {
            instruction: "classify the ticket".into(),
            inputs: json!({"description": "printer on fire"}),
            output_schema: json!({"type": "object"}),
            max_output_tokens: Some(128),
        }
    }

    #[test]
    fn model_input_is_an_anthropic_messages_body() {
        let input = BedrockProvider::model_input(&request());
        assert_eq!(input["anthropic_version"], "bedrock-2023-05-31");
        assert_eq!(input["max_tokens"], 128);
        assert_eq!(input["temperature"], 0.0);
        assert!(
            input["system"]
                .as_str()
                .unwrap()
                .starts_with("classify the ticket")
        );
        assert_eq!(
            input["messages"][0]["content"][0]["text"],
            "{\"description\":\"printer on fire\"}"
        );

        // No declared cap: max_tokens is still present (the format
        // requires it), at the adapter default.
        let mut uncapped = request();
        uncapped.max_output_tokens = None;
        let input = BedrockProvider::model_input(&uncapped);
        assert_eq!(
            input["max_tokens"],
            BedrockProvider::DEFAULT_BATCH_MAX_TOKENS
        );
    }

    #[test]
    fn model_output_parsing_reads_text_and_usage() {
        let output = json!({
            "id": "msg_1",
            "content": [{"type": "text", "text": "```json\n{\"category\":\"incident\"}\n```"}],
            "usage": {"input_tokens": 55, "output_tokens": 9},
        });
        let response = BedrockProvider::parse_model_output(&output, "R0000000001").unwrap();
        assert_eq!(response.text, "{\"category\":\"incident\"}");
        assert_eq!(response.input_tokens, 55);
        assert_eq!(response.output_tokens, 9);
        assert_eq!(response.request_id, "R0000000001");

        let empty = json!({"content": []});
        assert!(BedrockProvider::parse_model_output(&empty, "R").is_err());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn batch_without_configuration_is_unsupported_at_submission() {
        let provider = BedrockProvider::new(
            "anthropic.claude-3-haiku-20240307-v1:0",
            Some("eu-central-1"),
            Some("http://127.0.0.1:1"),
        )
        .await;
        assert!(!provider.capabilities().batch);
        let error = provider
            .submit_batch(&[("wk-1".to_owned(), request())])
            .await
            .unwrap_err();
        assert!(
            matches!(error, AiError::Unsupported(_)),
            "expected Unsupported, got: {error}"
        );
    }
}
