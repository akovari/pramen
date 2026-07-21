//! Output-contract handling: JSON Schema generation from declared fields
//! and strict typed validation of model output.

use pramen_core::spec::{FieldSpec, FieldType};
use serde_json::{Map, Value, json};

/// The JSON Schema a model must satisfy, generated from the transform's
/// declared output fields. Sent to the model as part of the instruction
/// and used verbatim in the work key (so schema changes create new work).
///
/// UTF-8 fields with [`FieldSpec::max_chars`] include a `maxLength`
/// constraint so providers that honor JSON Schema see the bound, and so
/// the work key changes when bounds change.
#[must_use]
pub fn output_json_schema(fields: &[FieldSpec]) -> Value {
    let mut properties = Map::new();
    let mut required = Vec::new();
    for field in fields {
        let type_name = match field.field_type {
            FieldType::Utf8 => "string",
            FieldType::Int64 => "integer",
            FieldType::Float64 => "number",
            FieldType::Bool => "boolean",
            FieldType::Timestamp => "string",
        };
        let mut schema = if field.nullable {
            json!({"type": [type_name, "null"]})
        } else {
            json!({"type": type_name})
        };
        if field.field_type == FieldType::Utf8
            && let Some(max_chars) = field.max_chars
        {
            schema["maxLength"] = json!(max_chars);
        }
        properties.insert(field.name.clone(), schema);
        required.push(Value::String(field.name.clone()));
    }
    json!({
        "type": "object",
        "properties": Value::Object(properties),
        "required": Value::Array(required),
        "additionalProperties": false,
    })
}

/// Validate raw model output text against the declared fields.
///
/// Returns the normalized output object (fields in declaration order,
/// values type-checked) or a human-readable description of every problem.
/// Over-long UTF-8 values (exceeding `maxChars`) are rejected — never
/// truncated.
///
/// # Errors
///
/// Returns the full list of violations as one message; the caller applies
/// the transform's `onInvalid` policy.
pub fn validate_output(text: &str, fields: &[FieldSpec]) -> Result<Value, String> {
    let value: Value =
        serde_json::from_str(text).map_err(|e| format!("output is not valid JSON: {e}"))?;
    let Value::Object(object) = &value else {
        return Err("output is not a JSON object".to_owned());
    };

    let mut problems = Vec::new();
    let mut normalized = Map::new();

    for field in fields {
        match object.get(&field.name) {
            None => problems.push(format!("missing field `{}`", field.name)),
            Some(Value::Null) if !field.nullable => {
                problems.push(format!("field `{}` is null but not nullable", field.name));
            }
            Some(Value::Null) => {
                normalized.insert(field.name.clone(), Value::Null);
            }
            Some(actual) => {
                let ok = match field.field_type {
                    FieldType::Utf8 | FieldType::Timestamp => actual.is_string(),
                    FieldType::Int64 => actual.is_i64() || actual.is_u64(),
                    FieldType::Float64 => actual.is_number(),
                    FieldType::Bool => actual.is_boolean(),
                };
                if !ok {
                    problems.push(format!(
                        "field `{}` has wrong type (expected {:?}, got {})",
                        field.name,
                        field.field_type,
                        type_name(actual)
                    ));
                    continue;
                }
                if field.field_type == FieldType::Utf8
                    && let Some(max_chars) = field.max_chars
                    && let Some(text) = actual.as_str()
                {
                    let chars = text.chars().count();
                    if chars > max_chars as usize {
                        problems.push(format!(
                            "field `{}` length {chars} exceeds maxChars {max_chars}",
                            field.name
                        ));
                        continue;
                    }
                }
                normalized.insert(field.name.clone(), actual.clone());
            }
        }
    }
    for key in object.keys() {
        if !fields.iter().any(|f| &f.name == key) {
            problems.push(format!("unexpected field `{key}`"));
        }
    }

    if problems.is_empty() {
        Ok(Value::Object(normalized))
    } else {
        Err(problems.join("; "))
    }
}

fn type_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fields() -> Vec<FieldSpec> {
        vec![
            FieldSpec {
                name: "category".into(),
                field_type: FieldType::Utf8,
                nullable: false,
                max_chars: None,
            },
            FieldSpec {
                name: "score".into(),
                field_type: FieldType::Float64,
                nullable: true,
                max_chars: None,
            },
        ]
    }

    fn bounded_text() -> Vec<FieldSpec> {
        vec![FieldSpec {
            name: "summary".into(),
            field_type: FieldType::Utf8,
            nullable: false,
            max_chars: Some(16),
        }]
    }

    #[test]
    fn schema_shape_is_strict() {
        let schema = output_json_schema(&fields());
        assert_eq!(schema["properties"]["category"]["type"], "string");
        assert_eq!(schema["properties"]["score"]["type"][0], "number");
        assert_eq!(schema["properties"]["score"]["type"][1], "null");
        assert_eq!(schema["required"][0], "category");
        assert_eq!(schema["additionalProperties"], false);
        assert!(schema["properties"]["category"].get("maxLength").is_none());
    }

    #[test]
    fn schema_includes_max_length_for_bounded_utf8() {
        let schema = output_json_schema(&bounded_text());
        assert_eq!(schema["properties"]["summary"]["maxLength"], 16);
    }

    #[test]
    fn valid_output_is_normalized() {
        let normalized =
            validate_output(r#"{"score": 0.9, "category": "billing"}"#, &fields()).unwrap();
        assert_eq!(normalized["category"], "billing");
        assert_eq!(normalized["score"], 0.9);

        let with_null = validate_output(r#"{"category": "x", "score": null}"#, &fields()).unwrap();
        assert_eq!(with_null["score"], Value::Null);
    }

    #[test]
    fn every_violation_is_reported() {
        let error = validate_output(r#"{"category": 3, "extra": true}"#, &fields()).unwrap_err();
        assert!(error.contains("wrong type"), "{error}");
        assert!(error.contains("missing field `score`"), "{error}");
        assert!(error.contains("unexpected field `extra`"), "{error}");
    }

    #[test]
    fn non_json_and_null_violations() {
        assert!(validate_output("not json", &fields()).is_err());
        assert!(validate_output("[1,2]", &fields()).is_err());
        let error = validate_output(r#"{"category": null, "score": 1.0}"#, &fields()).unwrap_err();
        assert!(error.contains("not nullable"), "{error}");
    }

    #[test]
    fn over_long_utf8_is_rejected_not_truncated() {
        let ok = validate_output(r#"{"summary": "short enough"}"#, &bounded_text()).unwrap();
        assert_eq!(ok["summary"], "short enough");

        let error = validate_output(
            r#"{"summary": "this summary is definitely too long"}"#,
            &bounded_text(),
        )
        .unwrap_err();
        assert!(error.contains("exceeds maxChars"), "{error}");
        assert!(error.contains("summary"), "{error}");
    }
}
