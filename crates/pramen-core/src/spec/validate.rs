//! Semantic validation of parsed pipeline documents.
//!
//! Validation reports *every* problem it finds, with a dotted path to the
//! offending element, so a user fixes a document in one round trip.

use super::component_ref::{ComponentRef, ComponentRefError};
use super::error::ValidationIssue;
use super::types::{
    AiTransform, PipelineSpec, SinkMode, SinkSpec, SourceSpec, TransformSpec, SOURCE_STAGE_ID,
};
use std::collections::{BTreeMap, BTreeSet};

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
    validate_sinks(spec, &mut push);
    validate_topology(spec, &mut push);
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
                validate_ai_transform(spec, ai, &path, false, push);
            }
            TransformSpec::AiGenerate(ai) => {
                validate_ai_transform(spec, ai, &path, true, push);
            }
            TransformSpec::Wasm(wasm) => {
                if wasm.component.trim().is_empty() {
                    push(&format!("{path}.component"), "must not be empty".to_owned());
                } else if let Err(error) = ComponentRef::parse(&wasm.component) {
                    let message = match error {
                        ComponentRefError::DigestRequired => error.to_string(),
                        ComponentRefError::Invalid(detail) => detail,
                    };
                    push(&format!("{path}.component"), message);
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

fn validate_sinks(spec: &PipelineSpec, push: &mut impl FnMut(&str, String)) {
    let has_sink = spec.spec.sink.is_some();
    let has_sinks = !spec.spec.sinks.is_empty();
    match (has_sink, has_sinks) {
        (false, false) => push(
            "spec",
            "must declare `sink` or a non-empty `sinks` list".to_owned(),
        ),
        (true, true) => push(
            "spec",
            "declare either `sink` or `sinks`, not both".to_owned(),
        ),
        (true, false) => {
            if let Some(sink) = &spec.spec.sink {
                validate_sink_body(sink, "spec.sink", push);
            }
        }
        (false, true) => {
            let mut seen_ids = BTreeSet::new();
            for (index, bound) in spec.spec.sinks.iter().enumerate() {
                let path = format!("spec.sinks[{index}]");
                if bound.id.trim().is_empty() {
                    push(&format!("{path}.id"), "must not be empty".to_owned());
                } else if !seen_ids.insert(bound.id.as_str()) {
                    push(
                        &format!("{path}.id"),
                        format!("duplicate id `{}`", bound.id),
                    );
                }
                if let Some(from) = &bound.from
                    && from.trim().is_empty()
                {
                    push(&format!("{path}.from"), "must not be empty when set".to_owned());
                }
                validate_sink_body(&bound.sink, &path, push);
            }
        }
    }
}

fn validate_sink_body(sink: &SinkSpec, path: &str, push: &mut impl FnMut(&str, String)) {
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
                    &format!("{path}.target"),
                    format!("`{target}` must be a qualified `schema.table` name"),
                );
            }
            if dsn_env.trim().is_empty() {
                push(&format!("{path}.dsnEnv"), "must not be empty".to_owned());
            }
            match mode {
                SinkMode::Upsert if keys.is_empty() => push(
                    &format!("{path}.keys"),
                    "mode `upsert` requires at least one merge-key column".to_owned(),
                ),
                SinkMode::Append if !keys.is_empty() => push(
                    &format!("{path}.keys"),
                    "only meaningful with mode `upsert`".to_owned(),
                ),
                _ => {}
            }
            let mut seen = BTreeSet::new();
            for key in keys {
                if key.trim().is_empty() {
                    push(&format!("{path}.keys"), "key columns must not be empty".to_owned());
                } else if !seen.insert(key) {
                    push(
                        &format!("{path}.keys"),
                        format!("duplicate key column `{key}`"),
                    );
                }
            }
        }
        SinkSpec::FlightSql {
            endpoint,
            target,
            mode,
            token_env,
        } => {
            if endpoint.trim().is_empty() {
                push(&format!("{path}.endpoint"), "must not be empty".to_owned());
            } else if !(endpoint.starts_with("http://") || endpoint.starts_with("https://")) {
                push(
                    &format!("{path}.endpoint"),
                    format!("`{endpoint}` must be an http:// or https:// URI"),
                );
            }
            let parts: Vec<&str> = target.split('.').collect();
            if !(2..=3).contains(&parts.len()) || parts.iter().any(|p| p.trim().is_empty()) {
                push(
                    &format!("{path}.target"),
                    format!(
                        "`{target}` must be `schema.table` or `catalog.schema.table`"
                    ),
                );
            }
            if *mode != SinkMode::Append {
                push(
                    &format!("{path}.mode"),
                    "Flight SQL sinks support only `append` in v1alpha1 (ADR 0008)".to_owned(),
                );
            }
            if token_env.trim().is_empty() {
                push(&format!("{path}.tokenEnv"), "must not be empty".to_owned());
            }
        }
    }
}

fn validate_topology(spec: &PipelineSpec, push: &mut impl FnMut(&str, String)) {
    // Skip detailed graph checks when sink declaration is already invalid.
    if spec.spec.sink.is_some() == !spec.spec.sinks.is_empty() {
        return;
    }

    let transform_edges = spec.spec.resolved_transform_edges();
    let sinks = spec.spec.resolved_sinks();

    let mut producers: BTreeSet<&str> = BTreeSet::new();
    producers.insert(SOURCE_STAGE_ID);
    for transform in &spec.spec.transforms {
        producers.insert(transform.id());
    }

    // Incoming edge count (fan-in detection) and adjacency for reachability.
    let mut incoming: BTreeMap<&str, u32> = BTreeMap::new();
    let mut children: BTreeMap<&str, Vec<&str>> = BTreeMap::new();

    for (index, (id, from)) in transform_edges.iter().enumerate() {
        let path = format!("spec.transforms[{index}]");
        if from.trim().is_empty() {
            push(&format!("{path}.from"), "must not be empty".to_owned());
            continue;
        }
        if !producers.contains(from.as_str()) {
            push(
                &format!("{path}.from"),
                format!("unknown stage `{from}` (use `source` or a transform id)"),
            );
        }
        if id == from {
            push(
                &format!("{path}.from"),
                format!("stage `{id}` cannot depend on itself"),
            );
        }
        *incoming.entry(id.as_str()).or_insert(0) += 1;
        children.entry(from.as_str()).or_default().push(id.as_str());
    }

    for (index, resolved) in sinks.iter().enumerate() {
        let path = if spec.spec.sinks.is_empty() {
            "spec.sink".to_owned()
        } else {
            format!("spec.sinks[{index}]")
        };
        if resolved.from.trim().is_empty() {
            push(&format!("{path}.from"), "must not be empty".to_owned());
            continue;
        }
        if !producers.contains(resolved.from) {
            push(
                &format!("{path}.from"),
                format!(
                    "unknown stage `{}` (use `source` or a transform id)",
                    resolved.from
                ),
            );
        }
        *incoming.entry(resolved.id).or_insert(0) += 1;
        children
            .entry(resolved.from)
            .or_default()
            .push(resolved.id);
    }

    for (node, count) in &incoming {
        if *count > 1 {
            push(
                "spec",
                format!(
                    "fan-in is not supported in v1alpha1: stage `{node}` has {count} upstreams \
                     (ADR 0007)"
                ),
            );
        }
    }

    // Cycle detection among transforms (sinks have no outgoing edges).
    let transform_ids: BTreeSet<&str> = spec.spec.transforms.iter().map(TransformSpec::id).collect();
    let mut visiting = BTreeSet::new();
    let mut visited = BTreeSet::new();
    for id in &transform_ids {
        if !visited.contains(id)
            && dfs_cycle(id, &children, &transform_ids, &mut visiting, &mut visited)
        {
            push(
                "spec.transforms",
                format!("cycle detected involving transform `{id}`"),
            );
            break;
        }
    }

    // Reachability from source.
    let mut reachable = BTreeSet::new();
    let mut stack = vec![SOURCE_STAGE_ID];
    while let Some(node) = stack.pop() {
        if !reachable.insert(node) {
            continue;
        }
        if let Some(kids) = children.get(node) {
            for kid in kids {
                stack.push(kid);
            }
        }
    }
    for transform in &spec.spec.transforms {
        if !reachable.contains(transform.id()) {
            push(
                "spec.transforms",
                format!(
                    "transform `{}` is not reachable from `source`",
                    transform.id()
                ),
            );
        }
        if children
            .get(transform.id())
            .is_none_or(|kids| kids.is_empty())
        {
            push(
                "spec.transforms",
                format!(
                    "transform `{}` has no downstream stage (fan-out leaf must be a sink)",
                    transform.id()
                ),
            );
        }
    }
    for resolved in &sinks {
        if !reachable.contains(resolved.id) {
            push(
                "spec",
                format!("sink `{}` is not reachable from `source`", resolved.id),
            );
        }
    }
}

fn dfs_cycle<'a>(
    node: &'a str,
    children: &BTreeMap<&str, Vec<&'a str>>,
    transform_ids: &BTreeSet<&str>,
    visiting: &mut BTreeSet<&'a str>,
    visited: &mut BTreeSet<&'a str>,
) -> bool {
    if visiting.contains(node) {
        return true;
    }
    if visited.contains(node) {
        return false;
    }
    visiting.insert(node);
    if let Some(kids) = children.get(node) {
        for kid in kids {
            if transform_ids.contains(kid) && dfs_cycle(kid, children, transform_ids, visiting, visited)
            {
                return true;
            }
        }
    }
    visiting.remove(node);
    visited.insert(node);
    false
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
