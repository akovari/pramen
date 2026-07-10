//! Work-key canonicalization: the content-addressed identity of a work item.
//!
//! The key material is everything that, if changed, must produce new work:
//! selected inputs, operation + prompt revision, output schema, provider,
//! model, and inference parameters. Canonical form is JSON with
//! lexicographically sorted object keys and no insignificant whitespace,
//! hashed with SHA-256.

use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};

/// Everything that identifies a unit of semantic work.
#[derive(Debug, Clone, Serialize)]
pub struct WorkSpec {
    pub operation: String,
    pub prompt_revision: String,
    pub inputs: Value,
    pub output_schema: Value,
    pub provider: String,
    pub model: String,
    pub params: Value,
}

impl WorkSpec {
    pub fn work_key(&self) -> String {
        let value = serde_json::to_value(self).expect("WorkSpec is always serializable");
        let canonical = canonical_json(&value);
        let mut hasher = Sha256::new();
        hasher.update(canonical.as_bytes());
        hex::encode(hasher.finalize())
    }
}

/// Serialize with object keys sorted at every level.
pub fn canonical_json(value: &Value) -> String {
    match value {
        Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            let inner: Vec<String> = keys
                .into_iter()
                .map(|k| {
                    format!(
                        "{}:{}",
                        serde_json::to_string(k).expect("string serializes"),
                        canonical_json(&map[k])
                    )
                })
                .collect();
            format!("{{{}}}", inner.join(","))
        }
        Value::Array(items) => {
            let inner: Vec<String> = items.iter().map(canonical_json).collect();
            format!("[{}]", inner.join(","))
        }
        other => serde_json::to_string(other).expect("scalar serializes"),
    }
}

mod hex {
    pub fn encode(bytes: impl AsRef<[u8]>) -> String {
        bytes.as_ref().iter().map(|b| format!("{b:02x}")).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn spec() -> WorkSpec {
        WorkSpec {
            operation: "ai.extract".into(),
            prompt_revision: "tickets-v1".into(),
            inputs: json!({"description": "printer on fire", "id": "t-1"}),
            output_schema: json!({"type": "object"}),
            provider: "mock".into(),
            model: "mock-1".into(),
            params: json!({"temperature": 0}),
        }
    }

    #[test]
    fn key_is_stable_across_object_key_order() {
        let a = json!({"b": 1, "a": {"y": 2, "x": 3}});
        let b = json!({"a": {"x": 3, "y": 2}, "b": 1});
        assert_eq!(canonical_json(&a), canonical_json(&b));

        let mut s1 = spec();
        s1.inputs = a;
        let mut s2 = spec();
        s2.inputs = b;
        assert_eq!(s1.work_key(), s2.work_key());
    }

    #[test]
    fn any_material_change_creates_new_work() {
        let base = spec().work_key();
        let mut changed = spec();
        changed.prompt_revision = "tickets-v2".into();
        assert_ne!(base, changed.work_key());

        let mut changed = spec();
        changed.model = "mock-2".into();
        assert_ne!(base, changed.work_key());

        let mut changed = spec();
        changed.inputs = serde_json::json!({"description": "printer fine", "id": "t-1"});
        assert_ne!(base, changed.work_key());
    }

    #[test]
    fn array_order_is_significant() {
        let a = json!({"evidence": ["x", "y"]});
        let b = json!({"evidence": ["y", "x"]});
        assert_ne!(canonical_json(&a), canonical_json(&b));
    }
}
