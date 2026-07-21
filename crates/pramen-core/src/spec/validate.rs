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
    validate_source(spec, &mut push);
    validate_transforms(spec, &mut push);
    validate_sink(&spec.spec.sink, &mut push);
    validate_runtime(spec, &mut push);
    validate_residency(spec, &mut push);

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

fn validate_source(spec: &PipelineSpec, push: &mut impl FnMut(&str, String)) {
    match &spec.spec.source {
        SourceSpec::ObjectStore { url, location, .. } => {
            if url.trim().is_empty() {
                push("spec.source.url", "must not be empty".to_owned());
                return;
            }
            match classify_source_url(url) {
                SourceUrlKind::Unsupported(scheme) => push(
                    "spec.source.url",
                    format!(
                        "unsupported scheme `{scheme}`; supported: local paths, file://, \
                         s3://, gs://, az://, azure://, adl://, abfs://, abfss://, and \
                         Azure https://{{account}}.blob|dfs.core.windows.net URLs"
                    ),
                ),
                SourceUrlKind::Cloud { .. } | SourceUrlKind::Local => {}
            }
            if let Some(location) = location
                && location.trim().is_empty()
            {
                push(
                    "spec.source.location",
                    "must not be empty when set".to_owned(),
                );
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
                validate_ai_transform(spec, ai, &path, push);
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
    push: &mut impl FnMut(&str, String),
) {
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
        let field_path = format!("{path}.output.fields[{field_index}].name");
        if field.name.trim().is_empty() {
            push(&field_path, "must not be empty".to_owned());
        } else if !seen_fields.insert(field.name.as_str()) {
            push(&field_path, format!("duplicate field `{}`", field.name));
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

/// Enforce [`RuntimeSpec::residency`] against declared source and model
/// locations — declaration-only, no live cloud lookups (ADR 0005).
fn validate_residency(spec: &PipelineSpec, push: &mut impl FnMut(&str, String)) {
    let Some(residency) = &spec.spec.runtime.residency else {
        return;
    };

    if residency.allowed_locations.is_empty() {
        push(
            "spec.runtime.residency.allowedLocations",
            "must list at least one allowed location".to_owned(),
        );
        return;
    }
    let allowed: BTreeSet<&str> = residency
        .allowed_locations
        .iter()
        .map(String::as_str)
        .collect();
    for (index, location) in residency.allowed_locations.iter().enumerate() {
        if location.trim().is_empty() {
            push(
                &format!("spec.runtime.residency.allowedLocations[{index}]"),
                "must not be empty".to_owned(),
            );
        }
    }

    if let Some(schemes) = &residency.allowed_schemes {
        if schemes.is_empty() {
            push(
                "spec.runtime.residency.allowedSchemes",
                "must list at least one scheme when set".to_owned(),
            );
        }
        for (index, scheme) in schemes.iter().enumerate() {
            if scheme.trim().is_empty() {
                push(
                    &format!("spec.runtime.residency.allowedSchemes[{index}]"),
                    "must not be empty".to_owned(),
                );
            }
        }
    }

    for (name, model) in &spec.spec.models {
        if let Some(region) = &model.region {
            if region.trim().is_empty() {
                push(
                    &format!("spec.models.{name}.region"),
                    "must not be empty when set".to_owned(),
                );
            } else if !allowed.contains(region.as_str()) {
                push(
                    &format!("spec.models.{name}.region"),
                    format!(
                        "`{region}` is outside runtime.residency.allowedLocations [{}]",
                        residency.allowed_locations.join(", ")
                    ),
                );
            }
        }
    }

    let SourceSpec::ObjectStore { url, location, .. } = &spec.spec.source;
    let kind = classify_source_url(url);
    if let SourceUrlKind::Cloud { scheme } = &kind {
        if let Some(allowed_schemes) = &residency.allowed_schemes {
            let allowed_schemes: BTreeSet<String> = allowed_schemes
                .iter()
                .map(|s| s.trim().to_ascii_lowercase())
                .filter(|s| !s.is_empty())
                .collect();
            if !allowed_schemes.is_empty() && !allowed_schemes.contains(scheme) {
                push(
                    "spec.source.url",
                    format!(
                        "scheme `{scheme}` is outside runtime.residency.allowedSchemes [{}]",
                        allowed_schemes.into_iter().collect::<Vec<_>>().join(", ")
                    ),
                );
            }
        }
        match location {
            None => push(
                "spec.source.location",
                "required when runtime.residency is set and the source URL is a \
                 cloud scheme (declare the bucket/container region offline; Pramen \
                 does not look it up at plan time)"
                    .to_owned(),
            ),
            Some(loc) if !loc.trim().is_empty() && !allowed.contains(loc.as_str()) => push(
                "spec.source.location",
                format!(
                    "`{loc}` is outside runtime.residency.allowedLocations [{}]",
                    residency.allowed_locations.join(", ")
                ),
            ),
            Some(_) => {}
        }
    }
}

/// Classification of a source URL for validation (and docs).
#[derive(Debug, PartialEq, Eq)]
enum SourceUrlKind {
    Local,
    Cloud { scheme: String },
    Unsupported(String),
}

fn classify_source_url(url: &str) -> SourceUrlKind {
    let trimmed = url.trim();
    if !trimmed.contains("://") {
        return SourceUrlKind::Local;
    }
    let scheme = trimmed
        .split_once("://")
        .map(|(s, _)| s.to_ascii_lowercase())
        .unwrap_or_default();
    match scheme.as_str() {
        "file" => SourceUrlKind::Local,
        "s3" | "gs" | "az" | "azure" | "adl" | "abfs" | "abfss" => SourceUrlKind::Cloud { scheme },
        "https" if is_azure_https_url(trimmed) => SourceUrlKind::Cloud { scheme },
        other => SourceUrlKind::Unsupported(other.to_owned()),
    }
}

fn is_azure_https_url(url: &str) -> bool {
    let Some((_, rest)) = url.split_once("://") else {
        return false;
    };
    let host = rest.split('/').next().unwrap_or_default();
    host.ends_with(".blob.core.windows.net")
        || host.ends_with(".dfs.core.windows.net")
        || host.ends_with(".blob.fabric.microsoft.com")
        || host.ends_with(".dfs.fabric.microsoft.com")
}

#[cfg(test)]
mod url_tests {
    use super::*;

    #[test]
    fn classifies_supported_cloud_schemes() {
        assert!(matches!(
            classify_source_url("gs://bucket/prefix/"),
            SourceUrlKind::Cloud { scheme } if scheme == "gs"
        ));
        assert!(matches!(
            classify_source_url("az://container/prefix/"),
            SourceUrlKind::Cloud { scheme } if scheme == "az"
        ));
        assert!(matches!(
            classify_source_url("abfss://fs@acct.dfs.core.windows.net/p/"),
            SourceUrlKind::Cloud { scheme } if scheme == "abfss"
        ));
        assert!(matches!(
            classify_source_url("https://acct.blob.core.windows.net/c/p"),
            SourceUrlKind::Cloud { scheme } if scheme == "https"
        ));
        assert!(matches!(
            classify_source_url("file:///tmp/in/"),
            SourceUrlKind::Local
        ));
        assert!(matches!(
            classify_source_url("/tmp/in/"),
            SourceUrlKind::Local
        ));
        assert!(matches!(
            classify_source_url("http://example.com/x"),
            SourceUrlKind::Unsupported(_)
        ));
    }
}
