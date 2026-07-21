use super::*;

const EXAMPLE_RAW: &str = include_str!("../../../../examples/governed-enrichment.yaml");

/// Canonical example with LF newlines.
///
/// Git on Windows may check out the YAML with CRLF (`core.autocrlf`), which
/// makes `.replace("…\\n…")` mutations no-ops and silently weakens tests.
fn example() -> String {
    EXAMPLE_RAW.replace("\r\n", "\n")
}

#[test]
fn example_mutations_survive_crlf_checkouts() {
    // Simulate a Windows autocrlf working tree: include_str! content has CRLF,
    // but example() normalizes so LF-needle replaces still apply.
    let crlf = example().replace('\n', "\r\n");
    let normalized = crlf.replace("\r\n", "\n");
    assert!(normalized.contains("      inputs: [description]\n"));
    let mutated = normalized.replacen(
        "      inputs: [description]\n",
        "      dispatch:\n        expectedRecords: 1\n      inputs: [description]\n",
        1,
    );
    assert!(mutated.contains("expectedRecords: 1"));
}

fn issues_of(yaml: &str) -> Vec<String> {
    match parse(yaml) {
        Err(SpecError::Invalid(issues)) => issues.iter().map(ToString::to_string).collect(),
        Err(SpecError::Parse(message)) => {
            panic!("expected validation issues, got parse error: {message}")
        }
        Ok(_) => Vec::new(),
    }
}

#[test]
fn canonical_example_parses_and_validates() {
    let spec = parse(&example()).unwrap();
    assert_eq!(spec.metadata.name, "governed-semantic-enrichment");
    assert_eq!(spec.spec.transforms.len(), 2);
    assert_eq!(spec.spec.transforms[0].id(), "normalize");
    assert_eq!(spec.spec.transforms[1].id(), "classify");
    let TransformSpec::AiExtract(ai) = &spec.spec.transforms[1] else {
        panic!("expected ai.extract");
    };
    assert_eq!(ai.execution, ExecutionMode::Auto);
    assert!(ai.dispatch.is_none());
    assert_eq!(ai.output.fields.len(), 2);
    assert_eq!(
        ai.output.fields[0].field_type.arrow_type(),
        arrow::datatypes::DataType::Utf8
    );
}

#[test]
fn auto_dispatch_hints_parse() {
    let yaml = example().replacen(
        "      inputs: [description]\n",
        concat!(
            "      dispatch:\n",
            "        expectedRecords: 10000\n",
            "        deadlineSeconds: 3600\n",
            "        rateCard: mock\n",
            "      inputs: [description]\n",
        ),
        1,
    );
    let spec = parse(&yaml).unwrap();
    let TransformSpec::AiExtract(ai) = &spec.spec.transforms[1] else {
        panic!("expected ai.extract");
    };
    let hints = ai.dispatch.as_ref().expect("dispatch hints");
    assert_eq!(hints.expected_records, Some(10_000));
    assert_eq!(hints.deadline_seconds, Some(3_600));
    assert_eq!(hints.rate_card.as_deref(), Some("mock"));
}

#[test]
fn round_trips_through_serde() {
    let spec = parse(&example()).unwrap();
    let yaml = serde_yaml_ng::to_string(&spec).unwrap();
    let reparsed = parse(&yaml).unwrap();
    assert_eq!(
        serde_json::to_value(&spec).unwrap(),
        serde_json::to_value(&reparsed).unwrap()
    );
}

#[test]
fn unknown_fields_are_rejected() {
    let yaml = example().replace(
        "  name: governed-semantic-enrichment",
        "  name: x\n  surprise: 1",
    );
    let error = parse(&yaml).unwrap_err();
    let SpecError::Parse(message) = error else {
        panic!("expected parse error");
    };
    assert!(message.contains("surprise"), "message was: {message}");
}

#[test]
fn undeclared_model_reference_is_reported_with_path() {
    let yaml = example().replace("model: enrichment", "model: missing");
    let issues = issues_of(&yaml);
    assert_eq!(issues.len(), 1, "issues: {issues:?}");
    assert!(issues[0].starts_with("spec.transforms[1].model:"));
    assert!(issues[0].contains("`missing`"));
    assert!(issues[0].contains("enrichment"));
}

#[test]
fn all_issues_are_reported_at_once() {
    let yaml = example()
        .replace("name: governed-semantic-enrichment", "name: Bad_Name")
        .replace("model: enrichment", "model: missing")
        .replace("target: analytics.events", "target: events");
    let issues = issues_of(&yaml);
    assert_eq!(issues.len(), 3, "issues: {issues:?}");
}

#[test]
fn duplicate_transform_ids_are_reported() {
    let yaml = example().replace("- id: classify", "- id: normalize");
    let issues = issues_of(&yaml);
    assert!(
        issues
            .iter()
            .any(|i| i.contains("duplicate id `normalize`")),
        "issues: {issues:?}"
    );
}

#[test]
fn zero_budget_is_rejected() {
    let yaml = example().replace(
        "maxOutputTokensPerRecord: 256",
        "maxOutputTokensPerRecord: 0",
    );
    let issues = issues_of(&yaml);
    assert_eq!(issues.len(), 1, "issues: {issues:?}");
    assert!(issues[0].contains("maxOutputTokensPerRecord"));
}

#[test]
fn inflight_smaller_than_batch_is_rejected() {
    let yaml = example().replace("maxInflightBytes: 268435456", "maxInflightBytes: 1024");
    let issues = issues_of(&yaml);
    assert_eq!(issues.len(), 1, "issues: {issues:?}");
    assert!(issues[0].contains("targetBatchBytes"));
}

#[test]
fn unsupported_source_scheme_is_rejected() {
    let yaml = example().replace(
        "url: s3://example-input/events/",
        "url: http://example.com/x",
    );
    let issues = issues_of(&yaml);
    assert!(
        issues.iter().any(|i| i.contains("unsupported scheme")),
        "issues: {issues:?}"
    );
}

#[test]
fn residency_rejects_model_region_outside_allow_list() {
    let yaml = example().replace(
        "    checkpoint:\n      url: file:///var/lib/pramen/checkpoints/\n",
        "    residency:\n      allowedLocations: [eu-west-1]\n    checkpoint:\n      url: file:///var/lib/pramen/checkpoints/\n",
    );
    // Source is cloud and residency is set — also needs location; inject both
    // expectations via a fuller document.
    let yaml = yaml.replace(
        "    url: s3://example-input/events/\n    format:",
        "    url: s3://example-input/events/\n    location: eu-west-1\n    format:",
    );
    let issues = issues_of(&yaml);
    assert!(
        issues
            .iter()
            .any(|i| i.contains("spec.models.enrichment.region") && i.contains("eu-central-1")),
        "issues: {issues:?}"
    );
}

#[test]
fn residency_requires_source_location_for_cloud_urls() {
    let yaml = example().replace(
        "    checkpoint:\n      url: file:///var/lib/pramen/checkpoints/\n",
        "    residency:\n      allowedLocations: [eu-central-1]\n    checkpoint:\n      url: file:///var/lib/pramen/checkpoints/\n",
    );
    let issues = issues_of(&yaml);
    assert!(
        issues
            .iter()
            .any(|i| i.starts_with("spec.source.location:") && i.contains("required")),
        "issues: {issues:?}"
    );
}

#[test]
fn residency_rejects_disallowed_source_scheme() {
    let yaml = "\
apiVersion: pramen.dev/v1alpha1
kind: Pipeline
metadata:
  name: residency-gcs
spec:
  source:
    type: object_store
    url: gs://eu-data/events/
    location: europe-west1
    format:
      type: parquet
  sink:
    type: postgres
    target: public.out
  runtime:
    residency:
      allowedLocations: [europe-west1]
      allowedSchemes: [s3]
";
    let issues = issues_of(yaml);
    assert!(
        issues
            .iter()
            .any(|i| i.contains("spec.source.url") && i.contains("gs")),
        "issues: {issues:?}"
    );
}

#[test]
fn residency_accepts_matching_cloud_source_and_model() {
    let yaml = example()
        .replace(
            "    url: s3://example-input/events/\n    format:",
            "    url: s3://example-input/events/\n    location: eu-central-1\n    format:",
        )
        .replace(
            "    checkpoint:\n      url: file:///var/lib/pramen/checkpoints/\n",
            "    residency:\n      allowedLocations: [eu-central-1]\n      allowedSchemes: [s3]\n    checkpoint:\n      url: file:///var/lib/pramen/checkpoints/\n",
        );
    assert!(issues_of(&yaml).is_empty(), "{:?}", issues_of(&yaml));
}

#[test]
fn defaults_are_applied() {
    let yaml = "\
apiVersion: pramen.dev/v1alpha1
kind: Pipeline
metadata:
  name: minimal
spec:
  source:
    type: object_store
    url: file:///tmp/in/
    format:
      type: ndjson
  sink:
    type: postgres
    target: public.out
";
    let spec = parse(yaml).unwrap();
    assert_eq!(spec.spec.runtime.target_batch_bytes, 8 * 1024 * 1024);
    assert_eq!(spec.spec.runtime.max_inflight_bytes, 256 * 1024 * 1024);
    assert!(spec.spec.transforms.is_empty());
    let SinkSpec::Postgres { mode, dsn_env, .. } = spec.spec.sink.as_ref().unwrap();
    assert_eq!(*mode, SinkMode::Append);
    assert_eq!(dsn_env, "PRAMEN_POSTGRES_DSN");
}

#[test]
fn fanout_sinks_parse_and_resolve_from() {
    let yaml = "\
apiVersion: pramen.dev/v1alpha1
kind: Pipeline
metadata:
  name: fanout
spec:
  source:
    type: object_store
    url: file:///tmp/in/
    format:
      type: ndjson
  transforms:
    - id: project
      type: sql
      query: SELECT * FROM input
  sinks:
    - id: primary
      type: postgres
      target: public.primary
    - id: archive
      from: project
      type: postgres
      target: public.archive
";
    let spec = parse(yaml).unwrap();
    let sinks = spec.spec.resolved_sinks();
    assert_eq!(sinks.len(), 2);
    assert_eq!(sinks[0].id, "primary");
    assert_eq!(sinks[0].from, "project");
    assert_eq!(sinks[1].id, "archive");
    assert_eq!(sinks[1].from, "project");
}

#[test]
fn duplicate_sink_ids_rejected() {
    let yaml = "\
apiVersion: pramen.dev/v1alpha1
kind: Pipeline
metadata:
  name: dup-sink
spec:
  source:
    type: object_store
    url: file:///tmp/in/
    format:
      type: ndjson
  sinks:
    - id: same
      type: postgres
      target: public.a
    - id: same
      type: postgres
      target: public.b
";
    let err = parse(yaml).unwrap_err();
    let SpecError::Invalid(issues) = err else {
        panic!("expected invalid, got {err}");
    };
    assert!(
        issues.iter().any(|i| i.message.contains("duplicate id")),
        "{issues:?}"
    );
}

#[test]
fn both_sink_and_sinks_rejected() {
    let yaml = "\
apiVersion: pramen.dev/v1alpha1
kind: Pipeline
metadata:
  name: both
spec:
  source:
    type: object_store
    url: file:///tmp/in/
    format:
      type: ndjson
  sink:
    type: postgres
    target: public.out
  sinks:
    - id: other
      type: postgres
      target: public.other
";
    let err = parse(yaml).unwrap_err();
    let SpecError::Invalid(issues) = err else {
        panic!("expected invalid, got {err}");
    };
    assert!(
        issues
            .iter()
            .any(|i| i.message.contains("either `sink` or `sinks`")),
        "{issues:?}"
    );
}

#[test]
fn committed_schema_artifact_is_current() {
    let committed = include_str!("../../../../docs/schema/pipeline.v1alpha1.schema.json");
    let committed: serde_json::Value = serde_json::from_str(committed).unwrap();
    assert_eq!(
        committed,
        json_schema(),
        "docs/schema/pipeline.v1alpha1.schema.json is stale; regenerate with \
         `cargo run -p pramen-core --example generate_schema --quiet 2>/dev/null \
          > docs/schema/pipeline.v1alpha1.schema.json`"
    );
}

#[test]
fn json_schema_is_generated_and_strict() {
    let schema = json_schema();
    let text = schema.to_string();
    assert!(text.contains("pramen.dev/v1alpha1"));
    assert!(text.contains("ai.extract"));
    assert!(text.contains("ai.generate"));
    // deny_unknown_fields must surface as additionalProperties: false.
    assert!(text.contains("\"additionalProperties\":false"));
}

#[test]
fn ai_generate_requires_utf8_bounds_and_output_token_cap() {
    const VALID: &str = r#"
apiVersion: pramen.dev/v1alpha1
kind: Pipeline
metadata:
  name: generate-demo
spec:
  models:
    writer:
      provider: mock
      model: mock-1
  source:
    type: object_store
    url: file:///tmp/in/
    format:
      type: ndjson
  transforms:
    - id: summarize
      type: ai.generate
      model: writer
      inputs: [description]
      instruction: Summarize the ticket in one sentence.
      output:
        fields:
          - name: summary
            type: utf8
            maxChars: 120
      budget:
        maxOutputTokensPerRecord: 64
  sink:
    type: postgres
    target: public.out
"#;
    let spec = parse(VALID).unwrap();
    let TransformSpec::AiGenerate(ai) = &spec.spec.transforms[0] else {
        panic!("expected ai.generate");
    };
    assert_eq!(ai.output.fields[0].max_chars, Some(120));

    let missing_cap = VALID.replace("      budget:\n        maxOutputTokensPerRecord: 64\n", "");
    let issues = issues_of(&missing_cap);
    assert!(
        issues
            .iter()
            .any(|i| i.contains("maxOutputTokensPerRecord")),
        "issues: {issues:?}"
    );

    let missing_chars = VALID.replace("            maxChars: 120\n", "");
    let issues = issues_of(&missing_chars);
    assert!(
        issues.iter().any(|i| i.contains("maxChars")),
        "issues: {issues:?}"
    );

    let wrong_type = VALID.replace(
        "            type: utf8\n            maxChars: 120\n",
        "            type: int64\n            maxChars: 120\n",
    );
    let issues = issues_of(&wrong_type);
    assert!(
        issues.iter().any(|i| i.contains("must be utf8")),
        "issues: {issues:?}"
    );
}

#[test]
fn wasm_oci_tag_only_component_is_rejected() {
    let yaml = "\
apiVersion: pramen.dev/v1alpha1
kind: Pipeline
metadata:
  name: wasm-oci
spec:
  source:
    type: object_store
    url: file:///tmp/in/
    format:
      type: ndjson
  transforms:
    - id: enrich
      type: wasm
      component: oci://ghcr.io/acme/enrich:latest
  sink:
    type: postgres
    target: public.out
";
    let issues = issues_of(yaml);
    assert_eq!(issues.len(), 1, "issues: {issues:?}");
    assert!(issues[0].starts_with("spec.transforms[0].component:"));
    assert!(issues[0].contains("sha256"));
}

#[test]
fn wasm_oci_digest_component_validates() {
    let yaml = "\
apiVersion: pramen.dev/v1alpha1
kind: Pipeline
metadata:
  name: wasm-oci
spec:
  source:
    type: object_store
    url: file:///tmp/in/
    format:
      type: ndjson
  transforms:
    - id: enrich
      type: wasm
      component: oci://ghcr.io/acme/enrich@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
  runtime:
    wasmOciAllowlist:
      - sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
  sink:
    type: postgres
    target: public.out
";
    parse(yaml).unwrap();
}
