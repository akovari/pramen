//! Offline checks for X2.2 deploy artifacts (no AWS).
//!
//! Mirrors `scripts/validate-deploy.sh` so `cargo nextest` fails if the
//! systemd units, Grafana dashboard, or example pipeline drift.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::path::PathBuf;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("repo root")
}

fn read(rel: &str) -> String {
    let path = repo_root().join(rel);
    std::fs::read_to_string(&path).unwrap_or_else(|error| {
        panic!("read {}: {error}", path.display());
    })
}

#[test]
fn systemd_unit_has_required_stanzas() {
    let unit = read("deploy/systemd/pramen.service");
    for needle in [
        "[Unit]",
        "[Service]",
        "[Install]",
        "ExecStart=",
        "EnvironmentFile=",
        "Type=oneshot",
    ] {
        assert!(unit.contains(needle), "pramen.service missing {needle}");
    }
    let timer = read("deploy/systemd/pramen.timer");
    assert!(timer.contains("[Timer]"));
    assert!(timer.contains("Unit=pramen.service"));
}

#[test]
fn env_templates_expose_runtime_knobs_without_secrets() {
    for rel in [
        "deploy/systemd/pramen.env.example",
        "deploy/examples/env.example",
    ] {
        let text = read(rel);
        assert!(text.contains("PRAMEN_POSTGRES_DSN="), "{rel}");
        assert!(text.contains("PRAMEN_LEDGER_PATH="), "{rel}");
        assert!(text.contains("PRAMEN_OTLP_ENDPOINT="), "{rel}");
        assert!(text.contains("AWS_REGION="), "{rel}");
        assert!(
            !text.contains("AKIA"),
            "{rel} must not contain access key material"
        );
        assert!(
            !text.contains("PRIVATE KEY"),
            "{rel} must not contain private keys"
        );
    }
}

#[test]
fn grafana_dashboard_parses_and_names_real_otlp_metrics() {
    let raw = read("deploy/grafana/pramen-runtime.json");
    let value: serde_json::Value = serde_json::from_str(&raw).expect("grafana JSON must parse");
    assert_eq!(value["uid"], "pramen-runtime-otlp");
    assert!(value["panels"].as_array().is_some_and(|p| p.len() >= 4));
    for metric in [
        "pramen.rows_in",
        "pramen.rows_out",
        "pramen.batches_in",
        "pramen.batches_out",
        "pramen.bytes_in",
        "pramen.bytes_out",
        "pramen.run_duration",
    ] {
        assert!(
            raw.contains(metric),
            "dashboard must document OTLP metric {metric}"
        );
    }
    // Gap documentation must stay present so panels stay honest.
    assert!(raw.contains("§13") || raw.contains("Gaps vs architecture"));
}

#[test]
fn example_pipeline_is_placeholder_aws_vertical() {
    let yaml = read("deploy/examples/aws-s3-to-aurora.yaml");
    assert!(yaml.contains("apiVersion: pramen.dev/v1alpha1"));
    assert!(yaml.contains("provider: bedrock"));
    assert!(yaml.contains("eu-central-1"));
    assert!(yaml.contains("CHANGE_ME"));
    assert!(yaml.contains("type: postgres"));
}
