// Criterion's macros expand to undocumented items, and panicking on a
// broken fixture is the correct behavior in a benchmark harness.
#![allow(missing_docs, clippy::expect_used)]
//! Micro-benchmarks for the AI-governance hot paths: work-key
//! canonicalization and the SQLite inference ledger.
//!
//! Run with `cargo bench -p pramen-ai`. These are the per-record fixed
//! costs a governed semantic transform pays on top of the model call
//! itself, so they bound how cheap ledger reuse can make a replay.

use criterion::{Criterion, criterion_group, criterion_main};
use pramen_ai::ledger::{Ledger, RecordedResult};
use pramen_ai::workkey::WorkSpec;
use serde_json::json;
use std::hint::black_box;
use std::path::PathBuf;

fn spec(seq: u64) -> WorkSpec {
    WorkSpec {
        operation: "ai.classify".to_owned(),
        instruction: "Classify the support ticket into category, severity, \
                      and whether a refund is requested."
            .to_owned(),
        inputs: json!({
            "subject": format!("Order #{seq} arrived damaged"),
            "body": "The package was crushed and the item inside no longer \
                     powers on. I would like a replacement or my money back.",
        }),
        output_schema: json!({
            "category": {"type": "string", "enum": ["billing", "shipping", "product", "account"]},
            "severity": {"type": "string", "enum": ["low", "medium", "high"]},
            "refund_requested": {"type": "boolean"},
        }),
        provider: "mock".to_owned(),
        model: "mock-classifier-v1".to_owned(),
        params: json!({"temperature": 0.0, "maxOutputTokens": 256}),
    }
}

fn result() -> RecordedResult {
    RecordedResult {
        output: json!({"category": "shipping", "severity": "high", "refund_requested": true}),
        provider: "mock".to_owned(),
        model: "mock-classifier-v1".to_owned(),
        request_id: "bench-request".to_owned(),
        input_tokens: 120,
        output_tokens: 24,
        validation: "valid".to_owned(),
    }
}

fn bench_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("pramen-bench-{}-{}", name, std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create bench dir");
    dir
}

fn work_key(c: &mut Criterion) {
    let spec = spec(42);
    c.bench_function("workkey/canonicalize_and_hash", |b| {
        b.iter(|| black_box(&spec).work_key());
    });
}

fn ledger_cold(c: &mut Criterion) {
    let dir = bench_dir("ledger-cold");
    let ledger = Ledger::open(&dir.join("ledger.db")).expect("open ledger");
    let recorded = result();
    let mut seq: u64 = 0;
    c.bench_function("ledger/cold_record_new_result", |b| {
        b.iter(|| {
            seq += 1;
            let spec = spec(seq);
            let key = spec.work_key();
            ledger
                .upsert_pending(&key, &spec.canonical())
                .expect("upsert");
            ledger.complete(&key, &recorded).expect("complete");
        });
    });
    let _ = std::fs::remove_dir_all(&dir);
}

fn ledger_warm(c: &mut Criterion) {
    let dir = bench_dir("ledger-warm");
    let ledger = Ledger::open(&dir.join("ledger.db")).expect("open ledger");
    // Populate a realistic working set so the lookup is not against an
    // empty table.
    let recorded = result();
    let mut keys = Vec::with_capacity(10_000);
    for seq in 0..10_000 {
        let spec = spec(seq);
        let key = spec.work_key();
        ledger
            .upsert_pending(&key, &spec.canonical())
            .expect("upsert");
        ledger.complete(&key, &recorded).expect("complete");
        keys.push(key);
    }
    let mut cursor = 0usize;
    c.bench_function("ledger/warm_reuse_existing_result", |b| {
        b.iter(|| {
            cursor = (cursor + 1) % keys.len();
            ledger.state(black_box(&keys[cursor])).expect("state")
        });
    });
    let _ = std::fs::remove_dir_all(&dir);
}

criterion_group!(benches, work_key, ledger_cold, ledger_warm);
criterion_main!(benches);
