use super::*;

const EXAMPLE: &str = include_str!("../../../../examples/governed-enrichment.yaml");

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
    let spec = parse(EXAMPLE).unwrap();
    assert_eq!(spec.metadata.name, "governed-semantic-enrichment");
    assert_eq!(spec.spec.transforms.len(), 2);
    assert_eq!(spec.spec.transforms[0].id(), "normalize");
    assert_eq!(spec.spec.transforms[1].id(), "classify");
    let TransformSpec::AiExtract(ai) = &spec.spec.transforms[1] else {
        panic!("expected ai.extract");
    };
    assert_eq!(ai.execution, ExecutionMode::Auto);
    assert_eq!(ai.output.fields.len(), 2);
    assert_eq!(
        ai.output.fields[0].field_type.arrow_type(),
        arrow::datatypes::DataType::Utf8
    );
}

#[test]
fn round_trips_through_serde() {
    let spec = parse(EXAMPLE).unwrap();
    let yaml = serde_yaml_ng::to_string(&spec).unwrap();
    let reparsed = parse(&yaml).unwrap();
    assert_eq!(
        serde_json::to_value(&spec).unwrap(),
        serde_json::to_value(&reparsed).unwrap()
    );
}

#[test]
fn unknown_fields_are_rejected() {
    let yaml = EXAMPLE.replace(
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
    let yaml = EXAMPLE.replace("model: enrichment", "model: missing");
    let issues = issues_of(&yaml);
    assert_eq!(issues.len(), 1, "issues: {issues:?}");
    assert!(issues[0].starts_with("spec.transforms[1].model:"));
    assert!(issues[0].contains("`missing`"));
    assert!(issues[0].contains("enrichment"));
}

#[test]
fn all_issues_are_reported_at_once() {
    let yaml = EXAMPLE
        .replace("name: governed-semantic-enrichment", "name: Bad_Name")
        .replace("model: enrichment", "model: missing")
        .replace("target: analytics.events", "target: events");
    let issues = issues_of(&yaml);
    assert_eq!(issues.len(), 3, "issues: {issues:?}");
}

#[test]
fn duplicate_transform_ids_are_reported() {
    let yaml = EXAMPLE.replace("- id: classify", "- id: normalize");
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
    let yaml = EXAMPLE.replace(
        "maxOutputTokensPerRecord: 256",
        "maxOutputTokensPerRecord: 0",
    );
    let issues = issues_of(&yaml);
    assert_eq!(issues.len(), 1, "issues: {issues:?}");
    assert!(issues[0].contains("maxOutputTokensPerRecord"));
}

#[test]
fn inflight_smaller_than_batch_is_rejected() {
    let yaml = EXAMPLE.replace("maxInflightBytes: 268435456", "maxInflightBytes: 1024");
    let issues = issues_of(&yaml);
    assert_eq!(issues.len(), 1, "issues: {issues:?}");
    assert!(issues[0].contains("targetBatchBytes"));
}

#[test]
fn unsupported_source_scheme_is_rejected() {
    let yaml = EXAMPLE.replace(
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
    let yaml = EXAMPLE.replace(
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
    let yaml = EXAMPLE.replace(
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
    let yaml = EXAMPLE
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
    let SinkSpec::Postgres { mode, dsn_env, .. } = &spec.spec.sink;
    assert_eq!(*mode, SinkMode::Append);
    assert_eq!(dsn_env, "PRAMEN_POSTGRES_DSN");
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
