//! Minimal single-threaded HTTP protocol stubs for offline provider tests
//! (ADR 0005, layer L1). Each stub serves canned JSON on a random localhost
//! port for a fixed number of requests, then stops.

use std::io::{Read, Write};
use std::net::TcpListener;
use std::thread::JoinHandle;

pub struct StubServer {
    pub url: String,
    handle: JoinHandle<Vec<String>>,
}

impl StubServer {
    /// Serve `body` as an HTTP 200 JSON response `expected_requests` times,
    /// capturing raw request bytes for assertions.
    pub fn serve_json(body: String, expected_requests: usize) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind localhost stub");
        let url = format!("http://{}", listener.local_addr().expect("stub addr"));
        let handle = std::thread::spawn(move || {
            let mut captured = Vec::new();
            for _ in 0..expected_requests {
                let (mut stream, _) = listener.accept().expect("accept");
                let mut buf = vec![0_u8; 65536];
                let n = stream.read(&mut buf).expect("read request");
                captured.push(String::from_utf8_lossy(&buf[..n]).into_owned());
                let response = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                stream.write_all(response.as_bytes()).expect("write response");
            }
            captured
        });
        Self { url, handle }
    }

    /// Stop and return the raw requests the stub saw.
    pub fn finish(self) -> Vec<String> {
        self.handle.join().expect("stub thread")
    }
}
