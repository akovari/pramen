// Criterion's macros expand to undocumented items, and panicking on a
// broken fixture is the correct behavior in a benchmark harness.
#![allow(missing_docs, clippy::expect_used)]
//! Micro-benchmark for the Arrow → PostgreSQL binary `COPY` encoder.
//!
//! Run with `cargo bench -p pramen-io`. This is the CPU cost Pramen adds
//! per batch on the load path; the wire and server dominate a real load,
//! so the encoder must stay far below them.

use arrow::array::{
    ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use bytes::BytesMut;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use pramen_io::encode_batch;
use std::hint::black_box;
use std::sync::Arc;

/// The six-type column mix validated in spike S1.3.
fn batch(rows: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("amount", DataType::Float64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("active", DataType::Boolean, false),
        Field::new(
            "created_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("note", DataType::Utf8, true),
    ]));
    let ids: ArrayRef = Arc::new(Int64Array::from_iter_values(0..rows as i64));
    let amounts: ArrayRef = Arc::new(Float64Array::from_iter_values(
        (0..rows).map(|i| i as f64 * 1.5),
    ));
    let names: ArrayRef = Arc::new(StringArray::from_iter_values(
        (0..rows).map(|i| format!("customer-{i:08}")),
    ));
    let actives: ArrayRef = Arc::new(BooleanArray::from_iter((0..rows).map(|i| Some(i % 3 == 0))));
    let created: ArrayRef = Arc::new(
        TimestampMicrosecondArray::from_iter_values(
            (0..rows).map(|i| 1_700_000_000_000_000 + i as i64),
        )
        .with_timezone("UTC"),
    );
    let notes: ArrayRef = Arc::new(StringArray::from_iter((0..rows).map(|i| {
        if i % 5 == 0 {
            None
        } else {
            Some(format!("note for row {i}"))
        }
    })));
    RecordBatch::try_new(schema, vec![ids, amounts, names, actives, created, notes])
        .expect("valid batch")
}

fn copy_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("copy_encode");
    for rows in [1_024usize, 8_192, 65_536] {
        let batch = batch(rows);
        group.throughput(Throughput::Elements(rows as u64));
        group.bench_with_input(BenchmarkId::from_parameter(rows), &batch, |b, batch| {
            let mut buf = BytesMut::with_capacity(4 * 1024 * 1024);
            b.iter(|| {
                buf.clear();
                encode_batch(black_box(batch), &mut buf).expect("encode");
                black_box(buf.len());
            });
        });
    }
    group.finish();
}

criterion_group!(benches, copy_encode);
criterion_main!(benches);
