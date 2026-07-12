//! Integration tests for the WASM host and artifact cache.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use arrow::array::{ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use pramen_wasm::{
    ArtifactCache, InvocationLimits, PreparedComponent, ResourceLimits, S1_4_FIXTURE, WasmError,
};
use std::sync::Arc;

fn fixture_batch(rows: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("amount", DataType::Float64, false),
        Field::new("note", DataType::Utf8, true),
    ]));
    let ids: ArrayRef = Arc::new(Int64Array::from_iter_values(0..rows as i64));
    let amounts: ArrayRef = Arc::new(Float64Array::from_iter_values(
        (0..rows).map(|i| i as f64 * 1.5),
    ));
    let notes: ArrayRef = Arc::new(StringArray::from_iter((0..rows).map(|i| {
        if i % 5 == 0 {
            None
        } else {
            Some(format!("note for row {i}"))
        }
    })));
    RecordBatch::try_new(schema, vec![ids, amounts, notes]).unwrap()
}

#[test]
fn round_trips_arrow_ipc_through_s1_4_fixture() {
    let prepared = PreparedComponent::from_path(S1_4_FIXTURE).expect("load fixture");
    let input = pramen_wasm::encode_batch(&fixture_batch(512)).expect("encode");
    let output = prepared
        .invoke(&input, &InvocationLimits::default())
        .expect("invoke");
    let batches = pramen_wasm::decode_stream(&output).expect("decode");
    assert_eq!(batches[0].num_rows(), 512);
    assert_eq!(batches[0].schema().field(3).name(), "amount_gross");
}

#[test]
fn artifact_cache_reuses_prepared_component_for_same_digest() {
    let cache = ArtifactCache::new();
    let first = cache.load_path(S1_4_FIXTURE).expect("first load");
    let second = cache.load_path(S1_4_FIXTURE).expect("second load");
    assert_eq!(cache.len(), 1);
    assert!(Arc::ptr_eq(&first, &second));
}

#[test]
fn fuel_limit_traps_deterministically() {
    let prepared = PreparedComponent::from_path(S1_4_FIXTURE).expect("load");
    let input = pramen_wasm::encode_batch(&fixture_batch(256)).expect("encode");
    let limits = InvocationLimits {
        resource: ResourceLimits {
            memory_bytes: None,
            fuel: Some(1_000),
        },
        max_input_bytes: 64 * 1024 * 1024,
        max_output_bytes: 64 * 1024 * 1024,
    };
    let error = prepared.invoke(&input, &limits).unwrap_err();
    assert!(matches!(error, WasmError::Trap(_)), "{error}");
}
