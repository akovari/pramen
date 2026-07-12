//! L1 protocol-stub tests (ADR 0005) for the OpenAI Files + Batches leg of
//! provider-batch execution: the real adapter against a local HTTP server
//! speaking the recorded protocol shapes — file upload, batch creation,
//! status polling, and JSONL result download — with zero cloud access.

#![allow(clippy::unwrap_used)]

use pramen_ai::provider::{BatchStatus, InferenceRequest, OpenAiCompatProvider, Provider};
use pramen_ai::{AiError, ProviderFault};
use pramen_testkit::http::{StubRequest, one_shot_raw, serve_router};
use serde_json::{Value, json};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

/// The canned batch protocol behind a router stub: upload → `file-in-1`;
/// create → `batch-1`; the first poll is `in_progress`, later ones
/// `completed` with an output and an error file; each result file holds
/// one item.
fn serve_batch_stub() -> (String, Arc<Mutex<Vec<StubRequest>>>) {
    let polls = AtomicUsize::new(0);
    let (base, captured) = serve_router(move |request| route(request, &polls));
    (format!("{base}/v1"), captured)
}

fn route(request: &StubRequest, polls: &AtomicUsize) -> Value {
    match (request.method.as_str(), request.path.as_str()) {
        ("POST", "/v1/files") => json!({"id": "file-in-1", "object": "file"}),
        ("POST", "/v1/batches") => json!({"id": "batch-1", "status": "validating"}),
        ("GET", "/v1/batches/batch-1") => {
            if polls.fetch_add(1, Ordering::SeqCst) == 0 {
                json!({"id": "batch-1", "status": "in_progress"})
            } else {
                json!({
                    "id": "batch-1",
                    "status": "completed",
                    "output_file_id": "file-out-1",
                    "error_file_id": "file-err-1",
                })
            }
        }
        ("GET", "/v1/files/file-out-1/content") => json!({
            "custom_id": "wk-ok",
            "response": {"status_code": 200, "body": {
                "id": "chatcmpl-b1",
                "choices": [{"message": {"role": "assistant",
                    "content": "```json\n{\"category\":\"billing\"}\n```"}}],
                "usage": {"prompt_tokens": 40, "completion_tokens": 6},
            }},
        }),
        ("GET", "/v1/files/file-err-1/content") => json!({
            "custom_id": "wk-bad",
            "error": {"code": "server_error", "message": "item exploded"},
        }),
        other => panic!("stub received an unexpected request: {other:?}"),
    }
}

fn request_for(text: &str) -> InferenceRequest {
    InferenceRequest {
        instruction: "classify the ticket".into(),
        inputs: json!({"description": text}),
        output_schema: json!({"type": "object"}),
        max_output_tokens: Some(64),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn batch_lifecycle_submits_polls_and_fetches_by_custom_id() {
    let (endpoint, captured) = serve_batch_stub();
    let provider = OpenAiCompatProvider::new(&endpoint, "test-model", Some("sk-test".into()));
    assert!(provider.capabilities().batch);

    let job_id = provider
        .submit_batch(&[
            ("wk-ok".to_owned(), request_for("invoice is wrong")),
            ("wk-bad".to_owned(), request_for("printer on fire")),
        ])
        .await
        .unwrap();
    assert_eq!(job_id, "batch-1");

    assert_eq!(
        provider.poll_batch(&job_id).await.unwrap(),
        BatchStatus::InProgress
    );
    assert_eq!(
        provider.poll_batch(&job_id).await.unwrap(),
        BatchStatus::Completed
    );

    let results = provider.fetch_batch(&job_id).await.unwrap();
    assert_eq!(results.len(), 2);

    let ok = results.iter().find(|(key, _)| key == "wk-ok").unwrap();
    let response = ok.1.as_ref().unwrap();
    assert_eq!(response.text, "{\"category\":\"billing\"}");
    assert_eq!(response.input_tokens, 40);
    assert_eq!(response.output_tokens, 6);
    assert_eq!(response.request_id, "chatcmpl-b1");

    let bad = results.iter().find(|(key, _)| key == "wk-bad").unwrap();
    let failure = bad.1.as_ref().unwrap_err();
    assert!(failure.contains("item exploded"), "{failure}");

    // What actually went over the wire.
    let requests = captured.lock().unwrap().clone();
    let upload = requests
        .iter()
        .find(|r| r.method == "POST" && r.path == "/v1/files")
        .unwrap();
    assert!(
        upload.body.contains("name=\"purpose\""),
        "multipart purpose"
    );
    assert!(upload.body.contains("\"custom_id\":\"wk-ok\""));
    assert!(upload.body.contains("\"custom_id\":\"wk-bad\""));
    assert!(
        upload.body.contains("\"url\":\"/v1/chat/completions\""),
        "every line targets chat completions"
    );

    let create = requests
        .iter()
        .find(|r| r.method == "POST" && r.path == "/v1/batches")
        .unwrap();
    let create_body: serde_json::Value = serde_json::from_str(&create.body).unwrap();
    assert_eq!(create_body["input_file_id"], "file-in-1");
    assert_eq!(create_body["completion_window"], "24h");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_terminal_provider_state_is_a_failed_status_not_a_hang() {
    let (base, _server) = pramen_testkit::http::one_shot_json(
        json!({
            "id": "batch-x",
            "status": "expired",
            "errors": {"data": [{"message": "completion window elapsed"}]},
        }),
        &[],
    );

    let provider = OpenAiCompatProvider::new(&format!("{base}/v1"), "m", None);
    match provider.poll_batch("batch-x").await.unwrap() {
        BatchStatus::Failed(reason) => {
            assert!(reason.contains("expired"), "{reason}");
            assert!(reason.contains("completion window elapsed"), "{reason}");
        }
        other => panic!("expected Failed, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn servers_without_a_batch_api_fail_submission_with_a_typed_fault() {
    // Ollama and plain vLLM answer 404 on /files: submission must surface
    // a typed Server fault immediately instead of pretending to queue work.
    let base = one_shot_raw(
        "HTTP/1.1 404 Not Found\r\nconnection: close\r\ncontent-length: 9\r\n\r\nnot found",
        None,
    );

    let provider = OpenAiCompatProvider::new(&format!("{base}/v1"), "m", None);
    let error = provider
        .submit_batch(&[("wk-1".to_owned(), request_for("hello"))])
        .await
        .unwrap_err();
    match error {
        AiError::Provider { fault, .. } => assert_eq!(fault, ProviderFault::Server),
        other => panic!("expected a typed provider error, got: {other}"),
    }
}
