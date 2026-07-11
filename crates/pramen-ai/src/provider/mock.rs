//! The deterministic offline provider.

use super::{Capabilities, InferenceRequest, Provider, ProviderResponse};
use crate::error::AiError;
use crate::workkey::canonical_json;
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use std::sync::atomic::{AtomicU64, Ordering};

/// A provider that fabricates schema-conforming output deterministically
/// from the request content — no network, no cost, stable across runs.
///
/// Useful for pipeline dry-runs (`provider: mock` in the pipeline document)
/// and for every offline test of the governance machinery: because output
/// is a pure function of the request, ledger reuse and work-key semantics
/// behave exactly as with a real provider.
#[derive(Debug, Default)]
pub struct MockProvider {
    calls: AtomicU64,
}

impl MockProvider {
    /// A fresh mock with a zero call counter.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// How many invocations reached this provider (i.e. were not served
    /// from the ledger).
    #[must_use]
    pub fn calls(&self) -> u64 {
        self.calls.load(Ordering::SeqCst)
    }
}

#[async_trait::async_trait]
impl Provider for MockProvider {
    fn id(&self) -> &str {
        "mock"
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
        self.calls.fetch_add(1, Ordering::SeqCst);

        let mut hasher = Sha256::new();
        hasher.update(canonical_json(&request.inputs).as_bytes());
        hasher.update(request.instruction.as_bytes());
        let digest = hasher.finalize();
        let seed = u64::from_be_bytes([
            digest[0], digest[1], digest[2], digest[3], digest[4], digest[5], digest[6], digest[7],
        ]);

        // Fabricate a value per declared property, honoring its type.
        let mut output = Map::new();
        if let Some(properties) = request
            .output_schema
            .get("properties")
            .and_then(Value::as_object)
        {
            for (index, (name, prop)) in properties.iter().enumerate() {
                let salt = seed.wrapping_add(index as u64);
                let type_name = prop.get("type").and_then(Value::as_str).unwrap_or("string");
                let value = match type_name {
                    "integer" => json!((salt % 1000) as i64),
                    "number" => json!((salt % 1000) as f64 / 10.0),
                    "boolean" => json!(salt % 2 == 0),
                    _ => json!(format!("{name}-{:04x}", salt % 0xFFFF)),
                };
                output.insert(name.clone(), value);
            }
        }

        let text = Value::Object(output).to_string();
        let input_tokens =
            (request.instruction.len() + canonical_json(&request.inputs).len()) as u64 / 4;
        Ok(ProviderResponse {
            output_tokens: text.len() as u64 / 4,
            input_tokens,
            request_id: format!("mock-{seed:016x}"),
            text,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(description: &str) -> InferenceRequest {
        InferenceRequest {
            instruction: "classify".into(),
            inputs: json!({"description": description}),
            output_schema: json!({
                "type": "object",
                "properties": {
                    "category": {"type": "string"},
                    "score": {"type": "number"},
                    "urgent": {"type": "boolean"}
                }
            }),
            max_output_tokens: None,
        }
    }

    #[tokio::test]
    async fn output_is_deterministic_and_schema_shaped() {
        let provider = MockProvider::new();
        let a = provider.invoke(&request("printer on fire")).await.unwrap();
        let b = provider.invoke(&request("printer on fire")).await.unwrap();
        let c = provider.invoke(&request("printer fine")).await.unwrap();
        assert_eq!(a.text, b.text, "same input must give same output");
        assert_ne!(a.text, c.text, "different input must give different output");
        assert_eq!(provider.calls(), 3);

        let parsed: Value = serde_json::from_str(&a.text).unwrap();
        assert!(parsed.get("category").unwrap().is_string());
        assert!(parsed.get("score").unwrap().is_number());
        assert!(parsed.get("urgent").unwrap().is_boolean());
    }
}
