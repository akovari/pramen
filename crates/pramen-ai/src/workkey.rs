//! Work-key canonicalization: the content-addressed identity of a unit of
//! semantic work.
//!
//! The key material is everything that, if changed, must produce new work:
//! the selected input values, the operation and instruction, the declared
//! output schema, the provider, the model, and the inference parameters.
//! Canonical form is JSON with lexicographically sorted object keys and no
//! insignificant whitespace, hashed with SHA-256.
//!
//! Stability of this canonicalization is a compatibility contract: changing
//! it would orphan every recorded result. The tests in this module pin the
//! exact key for a fixed specification.

use serde_json::{Value, json};
use sha2::{Digest, Sha256};

/// Everything that identifies a unit of semantic work.
#[derive(Debug, Clone)]
pub struct WorkSpec {
    /// Operation name, e.g. `ai.extract`.
    pub operation: String,
    /// The full instruction text (serves as the prompt revision: any edit
    /// re-executes the affected work).
    pub instruction: String,
    /// Selected input values for one record, keyed by column name.
    pub inputs: Value,
    /// The declared output schema in canonical JSON form.
    pub output_schema: Value,
    /// Provider adapter identifier.
    pub provider: String,
    /// Provider-specific model identifier.
    pub model: String,
    /// Inference parameters that affect output (temperature, caps).
    pub params: Value,
}

impl WorkSpec {
    /// The SHA-256 hex digest of the canonical form of this specification.
    #[must_use]
    pub fn work_key(&self) -> String {
        let value = json!({
            "operation": self.operation,
            "instruction": self.instruction,
            "inputs": self.inputs,
            "output_schema": self.output_schema,
            "provider": self.provider,
            "model": self.model,
            "params": self.params,
        });
        let mut hasher = Sha256::new();
        hasher.update(canonical_json(&value).as_bytes());
        hex(&hasher.finalize())
    }

    /// The canonical JSON text of this specification (stored in the ledger
    /// for audit).
    #[must_use]
    pub fn canonical(&self) -> String {
        let value = json!({
            "operation": self.operation,
            "instruction": self.instruction,
            "inputs": self.inputs,
            "output_schema": self.output_schema,
            "provider": self.provider,
            "model": self.model,
            "params": self.params,
        });
        canonical_json(&value)
    }
}

/// Serialize a JSON value with object keys sorted at every nesting level.
///
/// Uses `Value`'s `Display` (compact JSON) for scalars and keys, which
/// cannot fail, so the canonical form is total.
#[must_use]
pub fn canonical_json(value: &Value) -> String {
    match value {
        Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            let inner: Vec<String> = keys
                .into_iter()
                .map(|k| format!("{}:{}", Value::String(k.clone()), canonical_json(&map[k])))
                .collect();
            format!("{{{}}}", inner.join(","))
        }
        Value::Array(items) => {
            let inner: Vec<String> = items.iter().map(canonical_json).collect();
            format!("[{}]", inner.join(","))
        }
        other => other.to_string(),
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec() -> WorkSpec {
        WorkSpec {
            operation: "ai.extract".into(),
            instruction: "classify the ticket".into(),
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
        changed.instruction = "classify the ticket carefully".into();
        assert_ne!(base, changed.work_key());

        let mut changed = spec();
        changed.model = "mock-2".into();
        assert_ne!(base, changed.work_key());

        let mut changed = spec();
        changed.inputs = json!({"description": "printer fine", "id": "t-1"});
        assert_ne!(base, changed.work_key());

        let mut changed = spec();
        changed.output_schema = json!({"type": "object", "required": ["category"]});
        assert_ne!(base, changed.work_key());
    }

    #[test]
    fn array_order_is_significant() {
        let a = json!({"evidence": ["x", "y"]});
        let b = json!({"evidence": ["y", "x"]});
        assert_ne!(canonical_json(&a), canonical_json(&b));
    }

    /// The canonicalization is a compatibility contract: this exact digest
    /// must never change for this fixed specification. If this test fails,
    /// a ledger migration is required (P1.6 migration story).
    #[test]
    fn key_is_pinned_for_compatibility() {
        assert_eq!(
            spec().work_key(),
            "f189c0c1011f2423210255a6fd3a553ce5d233eb20babf1d705bf3832da005bd"
        );
    }

    #[test]
    fn string_escaping_is_canonical() {
        let tricky = json!({"quote\"key": "line\nbreak"});
        assert_eq!(
            canonical_json(&tricky),
            "{\"quote\\\"key\":\"line\\nbreak\"}"
        );
    }
}
