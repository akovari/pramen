//! Fault injection for provider adapters (P1.19), fully offline (L1 in
//! ADR 0005): each induced failure — timeout, throttle, server error,
//! malformed body, refused connection — must surface as the documented
//! typed [`ProviderFault`], never as a panic, a hang, or an untyped
//! string.

// Integration tests are test code; the no-unwrap production rule does not
// apply (mirrors the workspace's cfg(test) stance).
#![allow(clippy::unwrap_used)]

use pramen_ai::provider::{InferenceRequest, OpenAiCompatProvider, Provider};
use pramen_ai::{AiError, ProviderFault};
use serde_json::json;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::time::Duration;

fn request() -> InferenceRequest {
    InferenceRequest {
        instruction: "classify".into(),
        inputs: json!({"description": "printer on fire"}),
        output_schema: json!({"type": "object"}),
        max_output_tokens: Some(64),
    }
}

/// Serve one connection: consume the request, answer with `response`
/// (raw HTTP), or hang for `hold` first.
fn stub(response: &'static str, hold: Option<Duration>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        // Read the request without parsing; the tests only care about
        // the response side.
        let mut buffer = [0u8; 8192];
        let _ = stream.read(&mut buffer);
        if let Some(pause) = hold {
            std::thread::sleep(pause);
        }
        let _ = stream.write_all(response.as_bytes());
    });
    format!("http://{addr}/v1")
}

fn fault_of(error: AiError) -> ProviderFault {
    match error {
        AiError::Provider { fault, .. } => fault,
        other => panic!("expected a typed provider error, got: {other}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_hung_endpoint_is_a_typed_timeout() {
    let endpoint = stub("", Some(Duration::from_secs(5)));
    let provider =
        OpenAiCompatProvider::new(&endpoint, "m", None).with_timeout(Duration::from_millis(200));
    let started = std::time::Instant::now();
    let error = provider.invoke(&request()).await.unwrap_err();
    assert_eq!(fault_of(error), ProviderFault::Timeout);
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "the deadline must bound the wait"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn throttling_is_a_typed_throttled_fault() {
    let endpoint = stub(
        "HTTP/1.1 429 Too Many Requests\r\ncontent-length: 24\r\n\r\n{\"error\":\"slow down...\"}",
        None,
    );
    let provider = OpenAiCompatProvider::new(&endpoint, "m", None);
    let error = provider.invoke(&request()).await.unwrap_err();
    let message = error.to_string();
    assert_eq!(fault_of(error), ProviderFault::Throttled);
    assert!(message.contains("429"), "{message}");
}

#[tokio::test(flavor = "multi_thread")]
async fn server_failures_are_typed_server_faults() {
    let endpoint = stub(
        "HTTP/1.1 500 Internal Server Error\r\ncontent-length: 5\r\n\r\noops!",
        None,
    );
    let provider = OpenAiCompatProvider::new(&endpoint, "m", None);
    let error = provider.invoke(&request()).await.unwrap_err();
    assert_eq!(fault_of(error), ProviderFault::Server);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_malformed_body_is_a_typed_protocol_fault() {
    let endpoint = stub(
        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 19\r\n\r\nthis is not json at",
        None,
    );
    let provider = OpenAiCompatProvider::new(&endpoint, "m", None);
    let error = provider.invoke(&request()).await.unwrap_err();
    assert_eq!(fault_of(error), ProviderFault::Protocol);
}

#[tokio::test(flavor = "multi_thread")]
async fn protocol_shaped_json_without_choices_is_a_protocol_fault() {
    let endpoint = stub(
        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 15\r\n\r\n{\"choices\": []}",
        None,
    );
    let provider = OpenAiCompatProvider::new(&endpoint, "m", None);
    let error = provider.invoke(&request()).await.unwrap_err();
    assert_eq!(fault_of(error), ProviderFault::Protocol);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_refused_connection_is_a_typed_transport_fault() {
    // Bind a port and drop the listener so the port is closed but was
    // recently valid — the connection is refused, not hung.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    let provider = OpenAiCompatProvider::new(&format!("http://{addr}/v1"), "m", None)
        .with_timeout(Duration::from_secs(2));
    let error = provider.invoke(&request()).await.unwrap_err();
    assert_eq!(fault_of(error), ProviderFault::Transport);
}
