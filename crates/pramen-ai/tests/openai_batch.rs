//! L1 protocol-stub tests (ADR 0005) for the OpenAI Files + Batches leg of
//! provider-batch execution: the real adapter against a local HTTP server
//! speaking the recorded protocol shapes — file upload, batch creation,
//! status polling, and JSONL result download — with zero cloud access.

#![allow(clippy::unwrap_used)]

use pramen_ai::provider::{BatchStatus, InferenceRequest, OpenAiCompatProvider, Provider};
use pramen_ai::{AiError, ProviderFault};
use serde_json::json;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

/// One captured request, for asserting what the adapter actually sent.
#[derive(Debug, Clone)]
struct Captured {
    method: String,
    path: String,
    body: String,
}

/// A minimal HTTP/1.1 stub that serves the OpenAI batch protocol. Every
/// response carries `Connection: close`, so each request arrives on a
/// fresh connection and the accept loop stays simple.
fn serve_batch_stub() -> (String, Arc<Mutex<Vec<Captured>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let captured = Arc::new(Mutex::new(Vec::new()));
    let seen = Arc::clone(&captured);
    let polls = AtomicUsize::new(0);

    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { break };
            let Some(request) = read_request(&mut stream) else {
                continue;
            };
            let payload = route(&request, &polls);
            seen.lock().unwrap().push(request);
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\nconnection: close\r\ncontent-length: {}\r\n\r\n{payload}",
                payload.len()
            );
            let _ = stream.write_all(response.as_bytes());
        }
    });
    (format!("http://{addr}/v1"), captured)
}

fn read_request(stream: &mut TcpStream) -> Option<Captured> {
    let mut reader = BufReader::new(stream);
    let mut request_line = String::new();
    reader.read_line(&mut request_line).ok()?;
    let mut parts = request_line.split_whitespace();
    let method = parts.next()?.to_owned();
    let path = parts.next()?.to_owned();

    let mut content_length = 0usize;
    loop {
        let mut line = String::new();
        reader.read_line(&mut line).ok()?;
        if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
            content_length = value.trim().parse().ok()?;
        }
        if line == "\r\n" {
            break;
        }
    }
    let mut body = vec![0u8; content_length];
    reader.read_exact(&mut body).ok()?;
    Some(Captured {
        method,
        path,
        body: String::from_utf8_lossy(&body).into_owned(),
    })
}

/// The canned protocol: upload → `file-in-1`; create → `batch-1`; the
/// first poll is `in_progress`, later ones `completed` with an output and
/// an error file; each result file holds one item.
fn route(request: &Captured, polls: &AtomicUsize) -> String {
    match (request.method.as_str(), request.path.as_str()) {
        ("POST", "/v1/files") => json!({"id": "file-in-1", "object": "file"}).to_string(),
        ("POST", "/v1/batches") => json!({"id": "batch-1", "status": "validating"}).to_string(),
        ("GET", "/v1/batches/batch-1") => {
            if polls.fetch_add(1, Ordering::SeqCst) == 0 {
                json!({"id": "batch-1", "status": "in_progress"}).to_string()
            } else {
                json!({
                    "id": "batch-1",
                    "status": "completed",
                    "output_file_id": "file-out-1",
                    "error_file_id": "file-err-1",
                })
                .to_string()
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
        })
        .to_string(),
        ("GET", "/v1/files/file-err-1/content") => json!({
            "custom_id": "wk-bad",
            "error": {"code": "server_error", "message": "item exploded"},
        })
        .to_string(),
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
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let _ = read_request(&mut stream);
        let payload = json!({
            "id": "batch-x",
            "status": "expired",
            "errors": {"data": [{"message": "completion window elapsed"}]},
        })
        .to_string();
        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\nconnection: close\r\ncontent-length: {}\r\n\r\n{payload}",
            payload.len()
        );
        let _ = stream.write_all(response.as_bytes());
    });

    let provider = OpenAiCompatProvider::new(&format!("http://{addr}/v1"), "m", None);
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
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let _ = read_request(&mut stream);
        let _ = stream.write_all(
            b"HTTP/1.1 404 Not Found\r\nconnection: close\r\ncontent-length: 9\r\n\r\nnot found",
        );
    });

    let provider = OpenAiCompatProvider::new(&format!("http://{addr}/v1"), "m", None);
    let error = provider
        .submit_batch(&[("wk-1".to_owned(), request_for("hello"))])
        .await
        .unwrap_err();
    match error {
        AiError::Provider { fault, .. } => assert_eq!(fault, ProviderFault::Server),
        other => panic!("expected a typed provider error, got: {other}"),
    }
}
