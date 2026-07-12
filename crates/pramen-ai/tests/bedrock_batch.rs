//! Bedrock provider-batch tests (ADR 0005, layer L2): the real adapter
//! staging through a real local S3 (MinIO) while the Bedrock control
//! plane — `CreateModelInvocationJob` / `GetModelInvocationJob` — is a
//! local protocol stub behind the SDK endpoint override.
//!
//! Guarded by `PRAMEN_TEST_S3_URL` (with `AWS_*` variables pointing at
//! the local endpoint); self-skips offline. The only thing these tests
//! cannot prove is the live IAM/quota behavior of the real service —
//! that is the S2.1 cloud acceptance leg.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use pramen_ai::provider::{
    BatchStatus, BedrockBatchConfig, BedrockProvider, InferenceRequest, Provider,
};
use pramen_testkit::http::{StubRequest, serve_router};
use serde_json::{Value, json};
use std::sync::atomic::{AtomicUsize, Ordering};

const JOB_ARN: &str = "arn:aws:bedrock:eu-central-1:000000000000:model-invocation-job/testjob01";
const MODEL: &str = "anthropic.claude-3-haiku-20240307-v1:0";
const ROLE: &str = "arn:aws:iam::000000000000:role/pramen-batch";

fn request_for(text: &str) -> InferenceRequest {
    InferenceRequest {
        instruction: "classify the ticket".into(),
        inputs: json!({"description": text}),
        output_schema: json!({"type": "object"}),
        max_output_tokens: Some(128),
    }
}

/// A control-plane stub speaking the recorded REST shapes: create returns
/// the job ARN; the first status read is `InProgress`, later ones
/// `Completed` echoing the input/output configuration from the create
/// request (exactly what the real service does).
fn serve_control_plane() -> (String, std::sync::Arc<std::sync::Mutex<Vec<StubRequest>>>) {
    let polls = AtomicUsize::new(0);
    let created: std::sync::Mutex<Option<Value>> = std::sync::Mutex::new(None);
    serve_router(
        move |request| match (request.method.as_str(), request.path.as_str()) {
            ("POST", "/model-invocation-job") => {
                *created.lock().unwrap() = Some(request.json());
                json!({"jobArn": JOB_ARN})
            }
            ("GET", path) if path.starts_with("/model-invocation-job/") => {
                let create = created.lock().unwrap().clone().expect("job created first");
                let status = if polls.fetch_add(1, Ordering::SeqCst) == 0 {
                    "InProgress"
                } else {
                    "Completed"
                };
                json!({
                    "jobArn": JOB_ARN,
                    "jobName": create["jobName"],
                    "modelId": create["modelId"],
                    "roleArn": create["roleArn"],
                    "status": status,
                    "submitTime": "2026-07-12T12:00:00Z",
                    "inputDataConfig": create["inputDataConfig"],
                    "outputDataConfig": create["outputDataConfig"],
                })
            }
            other => panic!("stub received an unexpected request: {other:?}"),
        },
    )
}

/// The MinIO bucket named by `PRAMEN_TEST_S3_URL`, or `None` offline.
fn test_bucket() -> Option<String> {
    let url = pramen_testkit::env::s3_url()?;
    let rest = url
        .strip_prefix("s3://")
        .expect("PRAMEN_TEST_S3_URL is s3://");
    Some(rest.split('/').next().unwrap_or_default().to_owned())
}

fn store(bucket: &str) -> object_store::aws::AmazonS3 {
    object_store::aws::AmazonS3Builder::from_env()
        .with_bucket_name(bucket)
        .build()
        .expect("AWS_* variables point at the local S3 endpoint")
}

async fn read_text(store: &object_store::aws::AmazonS3, key: &str) -> String {
    use object_store::ObjectStore as _;
    let bytes = store
        .get(&object_store::path::Path::from(key))
        .await
        .unwrap_or_else(|e| panic!("read {key}: {e}"))
        .bytes()
        .await
        .unwrap();
    String::from_utf8(bytes.to_vec()).unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn batch_lifecycle_stages_polls_and_joins_results_by_work_key() {
    let Some(bucket) = test_bucket() else { return };
    let (endpoint, captured) = serve_control_plane();
    let staging = format!("s3://{bucket}/bedrock-batch-{}", std::process::id());

    let provider = BedrockProvider::new(MODEL, Some("eu-central-1"), Some(&endpoint))
        .await
        .with_batch(BedrockBatchConfig {
            role_arn: ROLE.to_owned(),
            s3: staging.clone(),
        });
    assert!(provider.capabilities().batch);

    // --- Submit: inputs and the key map land in S3, the job is created.
    let job_id = provider
        .submit_batch(&[
            ("wk-a".to_owned(), request_for("printer on fire")),
            ("wk-b".to_owned(), request_for("invoice is wrong")),
        ])
        .await
        .unwrap();
    assert_eq!(job_id, JOB_ARN);

    let create = captured.lock().unwrap()[0].json();
    assert_eq!(create["modelId"], MODEL);
    assert_eq!(create["roleArn"], ROLE);
    let input_uri = create["inputDataConfig"]["s3InputDataConfig"]["s3Uri"]
        .as_str()
        .unwrap()
        .to_owned();
    let output_uri = create["outputDataConfig"]["s3OutputDataConfig"]["s3Uri"]
        .as_str()
        .unwrap()
        .to_owned();
    assert!(input_uri.starts_with(&staging), "{input_uri}");
    assert!(input_uri.ends_with("/records.jsonl"), "{input_uri}");
    assert!(output_uri.starts_with(&staging), "{output_uri}");

    let s3 = store(&bucket);
    let input_key = input_uri
        .strip_prefix(&format!("s3://{bucket}/"))
        .unwrap()
        .to_owned();
    let records = read_text(&s3, &input_key).await;
    let lines: Vec<Value> = records
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert_eq!(lines.len(), 2);
    assert_eq!(lines[0]["recordId"], "R0000000000");
    assert_eq!(lines[1]["recordId"], "R0000000001");
    assert_eq!(
        lines[0]["modelInput"]["anthropic_version"],
        "bedrock-2023-05-31"
    );

    let keymap = read_text(&s3, &input_key.replace("records.jsonl", "keys.jsonl")).await;
    assert!(keymap.contains("\"workKey\":\"wk-a\""), "{keymap}");
    assert!(keymap.contains("\"workKey\":\"wk-b\""), "{keymap}");

    // --- Poll: the stub reports InProgress once, then Completed.
    assert_eq!(
        provider.poll_batch(&job_id).await.unwrap(),
        BatchStatus::InProgress
    );
    assert_eq!(
        provider.poll_batch(&job_id).await.unwrap(),
        BatchStatus::Completed
    );

    // --- Results: written by "Bedrock" under a job-id subfolder. The
    // successful line carries a *mangled* recordId (an observed service
    // behavior), so the join must fall back to the modelInput hash; the
    // failed line keeps its record id.
    use object_store::ObjectStore as _;
    let output_key = format!(
        "{}testjob01/records.jsonl.out",
        output_uri.strip_prefix(&format!("s3://{bucket}/")).unwrap()
    );
    let out_lines = format!(
        "{}\n{}\n",
        json!({
            "recordId": "1",
            "modelInput": lines[0]["modelInput"],
            "modelOutput": {
                "id": "msg_1",
                "content": [{"type": "text", "text": "{\"category\":\"incident\"}"}],
                "usage": {"input_tokens": 55, "output_tokens": 9},
            },
        }),
        json!({
            "recordId": "R0000000001",
            "error": {"errorCode": "ValidationException", "errorMessage": "input too long"},
        }),
    );
    s3.put(
        &object_store::path::Path::from(output_key.as_str()),
        out_lines.into_bytes().into(),
    )
    .await
    .unwrap();

    let results = provider.fetch_batch(&job_id).await.unwrap();
    assert_eq!(results.len(), 2);

    let ok = results.iter().find(|(key, _)| key == "wk-a").unwrap();
    let response = ok.1.as_ref().unwrap();
    assert_eq!(response.text, "{\"category\":\"incident\"}");
    assert_eq!(response.input_tokens, 55);
    assert_eq!(response.output_tokens, 9);

    let bad = results.iter().find(|(key, _)| key == "wk-b").unwrap();
    let failure = bad.1.as_ref().unwrap_err();
    assert!(failure.contains("input too long"), "{failure}");
}

// Pure control-plane behavior: no S3 involved, so no guard — this runs
// everywhere.
#[tokio::test(flavor = "multi_thread")]
async fn a_terminal_job_state_is_a_failed_status_not_a_hang() {
    let (endpoint, _captured) = pramen_testkit::http::one_shot_json(
        json!({
            "jobArn": JOB_ARN,
            "jobName": "pramen-x",
            "modelId": MODEL,
            "roleArn": ROLE,
            "status": "Failed",
            "message": "records per job below the service minimum",
            "submitTime": "2026-07-12T12:00:00Z",
        }),
        &[],
    );

    let provider = BedrockProvider::new(MODEL, Some("eu-central-1"), Some(&endpoint))
        .await
        .with_batch(BedrockBatchConfig {
            role_arn: ROLE.to_owned(),
            s3: "s3://unused/prefix".to_owned(),
        });
    match provider.poll_batch(JOB_ARN).await.unwrap() {
        BatchStatus::Failed(reason) => {
            assert!(reason.contains("Failed"), "{reason}");
            assert!(reason.contains("below the service minimum"), "{reason}");
        }
        other => panic!("expected Failed, got {other:?}"),
    }
}
