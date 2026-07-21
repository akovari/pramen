//! Semantic validation of parsed pipeline documents.
//!
//! Validation reports *every* problem it finds, with a dotted path to the
//! offending element, so a user fixes a document in one round trip.

use super::error::ValidationIssue;
use super::types::{AiTransform, PipelineSpec, SinkMode, SinkSpec, SourceSpec, TransformSpec};
use std::collections::BTreeSet;

pub(super) fn validate(spec: &PipelineSpec) -> Vec<ValidationIssue> {
    let mut issues = Vec::new();
    let mut push = |path: &str, message: String| {
        issues.push(ValidationIssue {
            path: path.to_owned(),
            message,
        });
    };

    validate_name(&spec.metadata.name, &mut push);
    validate_models(spec, &mut push);
    validate_source(&spec.spec.source, &mut push);
    validate_transforms(spec, &mut push);
    validate_sink(&spec.spec.sink, &mut push);
    validate_runtime(spec, &mut push);

    issues
}

fn validate_name(name: &str, push: &mut impl FnMut(&str, String)) {
    let valid = !name.is_empty()
        && name.len() <= 63
        && name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
        && !name.starts_with('-')
        && !name.ends_with('-');
    if !valid {
        push(
            "metadata.name",
            format!(
                "`{name}` must be 1-63 characters of lowercase letters, digits, \
                 and interior hyphens"
            ),
        );
    }
}

fn validate_models(spec: &PipelineSpec, push: &mut impl FnMut(&str, String)) {
    for (name, model) in &spec.spec.models {
        let path = format!("spec.models.{name}");
        if model.provider.trim().is_empty() {
            push(&format!("{path}.provider"), "must not be empty".to_owned());
        }
        if model.model.trim().is_empty() {
            push(&format!("{path}.model"), "must not be empty".to_owned());
        }
        if let Some(batch) = &model.batch {
            if batch.role_arn.trim().is_empty() {
                push(
                    &format!("{path}.batch.roleArn"),
                    "must not be empty".to_owned(),
                );
            }
            if !batch.s3.starts_with("s3://") {
                push(
                    &format!("{path}.batch.s3"),
                    format!("`{}` must be an s3:// staging prefix", batch.s3),
                );
            }
        }
    }
}

fn validate_source(source: &SourceSpec, push: &mut impl FnMut(&str, String)) {
    match source {
        SourceSpec::ObjectStore { url, .. } => {
            if url.trim().is_empty() {
                push("spec.source.url", "must not be empty".to_owned());
            }
        }
    }
}

fn validate_transforms(spec: &PipelineSpec, push: &mut impl FnMut(&str, String)) {
    let mut seen_ids = BTreeSet::new();
    for (index, transform) in spec.spec.transforms.iter().enumerate() {
        let path = format!("spec.transforms[{index}]");
        let id = transform.id();
        if id.trim().is_empty() {
            push(&format!("{path}.id"), "must not be empty".to_owned());
        } else if !seen_ids.insert(id) {
            push(&format!("{path}.id"), format!("duplicate id `{id}`"));
        }

        match transform {
            TransformSpec::Sql(sql) => {
                if sql.query.trim().is_empty() {
                    push(&format!("{path}.query"), "must not be empty".to_owned());
                }
            }
            TransformSpec::AiExtract(ai) | TransformSpec::AiClassify(ai) => {
                validate_ai_transform(spec, ai, &path, false, push);
            }
            TransformSpec::AiGenerate(ai) => {
                validate_ai_transform(spec, ai, &path, true, push);
            }
            TransformSpec::Wasm(wasm) => {
                if wasm.component.trim().is_empty() {
                    push(&format!("{path}.component"), "must not be empty".to_owned());
                }
            }
        }
    }
}

fn validate_ai_transform(
    spec: &PipelineSpec,
    ai: &AiTransform,
    path: &str,
    generate: bool,
    push: &mut impl FnMut(&str, String),
) {
    use super::types::FieldType;

    if !spec.spec.models.contains_key(&ai.model) {
        let declared: Vec<&str> = spec.spec.models.keys().map(String::as_str).collect();
        push(
            &format!("{path}.model"),
            format!(
                "references undeclared model `{}`; declared models: [{}]",
                ai.model,
                declared.join(", ")
            ),
        );
    }
    if ai.inputs.is_empty() {
        push(
            &format!("{path}.inputs"),
            "must list at least one input column".to_owned(),
        );
    }
    if ai.instruction.trim().is_empty() {
        push(
            &format!("{path}.instruction"),
            "must not be empty".to_owned(),
        );
    }
    if ai.output.fields.is_empty() {
        push(
            &format!("{path}.output.fields"),
            "must declare at least one output field".to_owned(),
        );
    }
    let mut seen_fields = BTreeSet::new();
    for (field_index, field) in ai.output.fields.iter().enumerate() {
        let field_base = format!("{path}.output.fields[{field_index}]");
        let field_path = format!("{field_base}.name");
        if field.name.trim().is_empty() {
            push(&field_path, "must not be empty".to_owned());
        } else if !seen_fields.insert(field.name.as_str()) {
            push(&field_path, format!("duplicate field `{}`", field.name));
        }
        if generate && field.field_type != FieldType::Utf8 {
            push(
                &format!("{field_base}.type"),
                format!(
                    "`ai.generate` output fields must be utf8 (field `{}` is {:?})",
                    field.name, field.field_type
                ),
            );
        }
        match field.max_chars {
            Some(0) => push(
                &format!("{field_base}.maxChars"),
                "must be positive".to_owned(),
            ),
            Some(_) if field.field_type != FieldType::Utf8 => push(
                &format!("{field_base}.maxChars"),
                "only valid on utf8 fields".to_owned(),
            ),
            None if generate => push(
                &format!("{field_base}.maxChars"),
                "`ai.generate` requires maxChars on every output field".to_owned(),
            ),
            _ => {}
        }
    }
    if let Some(budget) = &ai.budget {
        if budget.max_input_tokens_per_record == Some(0) {
            push(
                &format!("{path}.budget.maxInputTokensPerRecord"),
                "must be positive".to_owned(),
            );
        }
        if budget.max_output_tokens_per_record == Some(0) {
            push(
                &format!("{path}.budget.maxOutputTokensPerRecord"),
                "must be positive".to_owned(),
            );
        }
        if budget.max_run_tokens == Some(0) {
            push(
                &format!("{path}.budget.maxRunTokens"),
                "must be positive".to_owned(),
            );
        }
    }
    if generate {
        match ai
            .budget
            .as_ref()
            .and_then(|b| b.max_output_tokens_per_record)
        {
            None => push(
                &format!("{path}.budget.maxOutputTokensPerRecord"),
                "`ai.generate` requires a positive maxOutputTokensPerRecord \
                 (provider request cap + post-validation)"
                    .to_owned(),
            ),
            Some(0) => {
                // Already reported above when the budget block is present.
            }
            Some(_) => {}
        }
    }
    if ai.breaker.max_consecutive_invalid == 0 {
        push(
            &format!("{path}.breaker.maxConsecutiveInvalid"),
            "must be positive (the breaker is always armed)".to_owned(),
        );
    }
}

fn validate_sink(sink: &SinkSpec, push: &mut impl FnMut(&str, String)) {
    match sink {
        SinkSpec::Postgres {
            target,
            mode,
            keys,
            dsn_env,
        } => {
            let parts: Vec<&str> = target.split('.').collect();
            if parts.len() != 2 || parts.iter().any(|p| p.trim().is_empty()) {
                push(
                    "spec.sink.target",
                    format!("`{target}` must be a qualified `schema.table` name"),
                );
            }
            if dsn_env.trim().is_empty() {
                push("spec.sink.dsnEnv", "must not be empty".to_owned());
            }
            match mode {
                SinkMode::Upsert if keys.is_empty() => push(
                    "spec.sink.keys",
                    "mode `upsert` requires at least one merge-key column".to_owned(),
                ),
                SinkMode::Append if !keys.is_empty() => push(
                    "spec.sink.keys",
                    "only meaningful with mode `upsert`".to_owned(),
                ),
                _ => {}
            }
            let mut seen = std::collections::BTreeSet::new();
            for key in keys {
                if key.trim().is_empty() {
                    push("spec.sink.keys", "key columns must not be empty".to_owned());
                } else if !seen.insert(key) {
                    push("spec.sink.keys", format!("duplicate key column `{key}`"));
                }
            }
        }
    }
}

fn validate_runtime(spec: &PipelineSpec, push: &mut impl FnMut(&str, String)) {
    let runtime = &spec.spec.runtime;
    if runtime.target_batch_bytes == 0 {
        push(
            "spec.runtime.targetBatchBytes",
            "must be positive".to_owned(),
        );
    }
    if runtime.max_inflight_bytes < runtime.target_batch_bytes {
        push(
            "spec.runtime.maxInflightBytes",
            format!(
                "({}) must be at least targetBatchBytes ({})",
                runtime.max_inflight_bytes, runtime.target_batch_bytes
            ),
        );
    }
    if let Some(checkpoint) = &runtime.checkpoint
        && checkpoint.url.trim().is_empty()
    {
        push(
            "spec.runtime.checkpoint.url",
            "must not be empty".to_owned(),
        );
    }
}
