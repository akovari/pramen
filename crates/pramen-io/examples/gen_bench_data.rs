// A benchmark fixture generator: panicking with a clear message is the
// right failure mode, so `expect` is fine here.
#![allow(clippy::expect_used)]
//! Deterministic Parquet dataset generator for the benchmark suite
//! (`scripts/bench.sh`).
//!
//! Usage:
//!
//! ```text
//! cargo run --release -p pramen-io --example gen_bench_data -- <out_dir> <rows> <files>
//! ```
//!
//! The schema is the six-type column mix validated in spike S1.3 plus a
//! low-cardinality `category` column used by the benchmark's SQL filter.
//! Generation is a pure function of the row index, so any two runs (on any
//! machine) produce byte-identical inputs.

use arrow::array::{
    ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use datafusion::parquet::arrow::ArrowWriter;
use std::fs::File;
use std::sync::Arc;

const CATEGORIES: [&str; 5] = ["alpha", "beta", "gamma", "delta", "epsilon"];
const BATCH_ROWS: usize = 65_536;

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("category", DataType::Utf8, false),
        Field::new("amount", DataType::Float64, false),
        Field::new("active", DataType::Boolean, false),
        Field::new(
            "created_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("note", DataType::Utf8, true),
    ]))
}

fn batch(schema: &Arc<Schema>, start: i64, rows: usize) -> RecordBatch {
    let range = start..start + rows as i64;
    let ids: ArrayRef = Arc::new(Int64Array::from_iter_values(range.clone()));
    let categories: ArrayRef = Arc::new(StringArray::from_iter_values(
        range
            .clone()
            .map(|i| CATEGORIES[(i as usize) % CATEGORIES.len()]),
    ));
    let amounts: ArrayRef = Arc::new(Float64Array::from_iter_values(
        range.clone().map(|i| (i % 100_000) as f64 / 100.0),
    ));
    let actives: ArrayRef = Arc::new(BooleanArray::from_iter(
        range.clone().map(|i| Some(i % 3 == 0)),
    ));
    let created: ArrayRef = Arc::new(
        TimestampMicrosecondArray::from_iter_values(
            range.clone().map(|i| 1_700_000_000_000_000 + i * 1_000),
        )
        .with_timezone("UTC"),
    );
    let notes: ArrayRef = Arc::new(StringArray::from_iter(range.map(|i| {
        if i % 5 == 0 {
            None
        } else {
            Some(format!("record {i} generated for the pramen bench suite"))
        }
    })));
    RecordBatch::try_new(
        Arc::clone(schema),
        vec![ids, categories, amounts, actives, created, notes],
    )
    .expect("batch construction is infallible for generated arrays")
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let [_, out_dir, rows, files] = args.as_slice() else {
        eprintln!("usage: gen_bench_data <out_dir> <rows> <files>");
        std::process::exit(2);
    };
    let rows: usize = rows.parse().expect("rows must be an integer");
    let files: usize = files.parse().expect("files must be an integer");
    assert!(files > 0 && rows >= files, "need at least one row per file");

    std::fs::create_dir_all(out_dir).expect("create output directory");
    let schema = schema();
    let per_file = rows / files;
    let mut written = 0usize;
    for file_index in 0..files {
        // The last file absorbs the remainder so totals are exact.
        let file_rows = if file_index == files - 1 {
            rows - written
        } else {
            per_file
        };
        let path = format!("{out_dir}/part-{file_index:04}.parquet");
        let file = File::create(&path).expect("create parquet file");
        let mut writer =
            ArrowWriter::try_new(file, Arc::clone(&schema), None).expect("create writer");
        let mut offset = 0usize;
        while offset < file_rows {
            let chunk = BATCH_ROWS.min(file_rows - offset);
            writer
                .write(&batch(&schema, (written + offset) as i64, chunk))
                .expect("write batch");
            offset += chunk;
        }
        writer.close().expect("close writer");
        written += file_rows;
    }
    println!("wrote {written} rows across {files} files to {out_dir}");
}
