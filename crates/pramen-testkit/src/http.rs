//! Minimal local HTTP stubs for L1 protocol tests (ADR 0005): real
//! adapter code against canned responses on a loopback port. Deliberately
//! primitive — a blocking `TcpListener` and hand-rolled HTTP/1.1 — so the
//! fixture itself cannot hide protocol bugs behind a real server library.

use serde_json::Value;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

/// One request as the stub saw it.
#[derive(Debug, Clone)]
pub struct StubRequest {
    /// HTTP method (`GET`, `POST`, …).
    pub method: String,
    /// Request path including the query string.
    pub path: String,
    /// The raw request line, e.g. `POST /v1/chat/completions HTTP/1.1`.
    pub request_line: String,
    /// The request body, decoded lossily as UTF-8.
    pub body: String,
}

impl StubRequest {
    /// The body parsed as JSON. Panics on malformed JSON — in a test,
    /// that is the failure you want to see.
    #[must_use]
    pub fn json(&self) -> Value {
        serde_json::from_str(&self.body).expect("stub request body is JSON")
    }
}

/// Serve exactly one connection: consume the request without parsing it,
/// optionally hold for `hold` (for timeout tests), then answer with the
/// raw HTTP bytes given. Returns the `http://addr` base URL.
#[must_use]
pub fn one_shot_raw(response: &'static str, hold: Option<Duration>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind stub");
    let addr = listener.local_addr().expect("stub addr");
    std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept");
        let mut buffer = [0u8; 65536];
        let _ = stream.read(&mut buffer);
        if let Some(pause) = hold {
            std::thread::sleep(pause);
        }
        let _ = stream.write_all(response.as_bytes());
    });
    format!("http://{addr}")
}

/// Serve exactly one connection with a `200 OK` JSON payload (plus any
/// extra response headers), capturing the request. Returns the base URL
/// and a handle whose join yields the captured request.
#[must_use]
pub fn one_shot_json(
    payload: Value,
    extra_headers: &[(&str, &str)],
) -> (String, JoinHandle<StubRequest>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind stub");
    let addr = listener.local_addr().expect("stub addr");
    let headers = extra_headers
        .iter()
        .map(|(k, v)| format!("{k}: {v}\r\n"))
        .collect::<String>();
    let handle = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept");
        let request = read_request(&mut stream).expect("read request");
        let body = payload.to_string();
        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n{headers}content-length: {}\r\n\r\n{body}",
            body.len()
        );
        stream
            .write_all(response.as_bytes())
            .expect("write response");
        request
    });
    (format!("http://{addr}"), handle)
}

/// Serve connections until the listener is dropped, answering each
/// request with `200 OK` and the JSON payload the router returns for it,
/// and capturing every request in arrival order. Each response carries
/// `Connection: close`, keeping the accept loop single-request.
///
/// The router runs on the server thread; state it needs (counters, …)
/// must be owned or atomic.
#[must_use]
pub fn serve_router<F>(router: F) -> (String, Arc<Mutex<Vec<StubRequest>>>)
where
    F: Fn(&StubRequest) -> Value + Send + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind stub");
    let addr = listener.local_addr().expect("stub addr");
    let captured = Arc::new(Mutex::new(Vec::new()));
    let seen = Arc::clone(&captured);
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { break };
            let Some(request) = read_request(&mut stream) else {
                continue;
            };
            let body = router(&request).to_string();
            seen.lock().expect("capture lock").push(request);
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\nconnection: close\r\ncontent-length: {}\r\n\r\n{body}",
                body.len()
            );
            let _ = stream.write_all(response.as_bytes());
        }
    });
    (format!("http://{addr}"), captured)
}

/// Read one HTTP/1.1 request (request line, headers, `Content-Length`
/// body) off the stream.
fn read_request(stream: &mut TcpStream) -> Option<StubRequest> {
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
    Some(StubRequest {
        method,
        path,
        request_line: request_line.trim_end().to_owned(),
        body: String::from_utf8_lossy(&body).into_owned(),
    })
}
