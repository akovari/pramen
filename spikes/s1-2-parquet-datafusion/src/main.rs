//! Spike S1.2: Parquet source + DataFusion SQL under a bounded memory pool.
//!
//! Generates a deterministic multi-file Parquet dataset, then streams a
//! filter+projection+derivation SQL query over it with a hard DataFusion
//! memory limit, recording throughput and peak RSS. Running it at 1x and 2x
//! dataset size demonstrates that peak memory does not scale with input.
//!
//! Usage: s1-2-parquet-datafusion [gen|query] [--files N] [--rows-per-file N]
//!          [--dir PATH] [--mem-limit-mb N]

use anyhow::{Context, Result};
use datafusion::arrow::array::{Float64Array, Int64Array, StringArray, TimestampMicrosecondArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::memory_pool::FairSpillPool;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::basic::Compression;
use datafusion::parquet::file::properties::WriterProperties;
use datafusion::prelude::*;
use futures_util::StreamExt;
use std::sync::Arc;
use std::time::Instant;

const CATEGORIES: &[&str] = &["alpha", "beta", "gamma", "delta", "epsilon"];

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("amount", DataType::Float64, false),
        Field::new("category", DataType::Utf8, false),
        Field::new(
            "created_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("payload", DataType::Utf8, false),
    ]))
}

fn make_batch(start: i64, rows: usize) -> RecordBatch {
    let ids: Vec<i64> = (start..start + rows as i64).collect();
    let amounts: Vec<f64> = ids.iter().map(|i| (i % 100_000) as f64 / 100.0).collect();
    let categories: Vec<&str> = ids
        .iter()
        .map(|i| CATEGORIES[(*i as usize) % CATEGORIES.len()])
        .collect();
    let timestamps: Vec<i64> = ids
        .iter()
        .map(|i| 1_700_000_000_000_000 + i * 250_000)
        .collect();
    let payloads: Vec<String> = ids
        .iter()
        .map(|i| format!("row {i} synthetic payload {}", "y".repeat((*i % 64) as usize)))
        .collect();
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Float64Array::from(amounts)),
            Arc::new(StringArray::from(categories)),
            Arc::new(TimestampMicrosecondArray::from(timestamps).with_timezone("UTC")),
            Arc::new(StringArray::from(payloads)),
        ],
    )
    .expect("schema and arrays agree")
}

fn arg(name: &str, default: &str) -> String {
    let args: Vec<String> = std::env::args().collect();
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1))
        .cloned()
        .unwrap_or_else(|| default.to_owned())
}

fn generate(dir: &str, files: usize, rows_per_file: usize) -> Result<()> {
    std::fs::create_dir_all(dir)?;
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let started = Instant::now();
    for file_index in 0..files {
        let path = format!("{dir}/part-{file_index:04}.parquet");
        let file = std::fs::File::create(&path).with_context(|| path.clone())?;
        let mut writer = ArrowWriter::try_new(file, schema(), Some(props.clone()))?;
        let base = (file_index * rows_per_file) as i64;
        let mut written = 0;
        while written < rows_per_file {
            let rows = 65_536.min(rows_per_file - written);
            writer.write(&make_batch(base + written as i64, rows))?;
            written += rows;
        }
        writer.close()?;
    }
    let total_bytes: u64 = std::fs::read_dir(dir)?
        .filter_map(|e| e.ok()?.metadata().ok())
        .map(|m| m.len())
        .sum();
    println!(
        "generated {files} files x {rows_per_file} rows = {} rows, {:.1} MiB on disk, in {:?}",
        files * rows_per_file,
        total_bytes as f64 / 1_048_576.0,
        started.elapsed()
    );
    Ok(())
}

const QUERY: &str = "SELECT id, category, amount, amount * 1.21 AS amount_gross, \
     date_trunc('hour', created_at) AS hour, length(payload) AS payload_len \
     FROM events WHERE amount > 500.0 AND category <> 'epsilon'";

async fn query(dir: &str, mem_limit_mb: usize) -> Result<()> {
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::new(FairSpillPool::new(mem_limit_mb * 1024 * 1024)))
        .build_arc()?;
    let config = SessionConfig::new().with_batch_size(8192);
    let ctx = SessionContext::new_with_config_rt(config, runtime);
    ctx.register_parquet("events", dir, ParquetReadOptions::default())
        .await?;

    let started = Instant::now();
    let dataframe = ctx.sql(QUERY).await?;
    let mut stream = dataframe.execute_stream().await?;

    let mut rows: u64 = 0;
    let mut batches: u64 = 0;
    let mut peak_rss: usize = 0;
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        rows += batch.num_rows() as u64;
        batches += 1;
        if batches % 64 == 0 {
            if let Some(usage) = memory_stats::memory_stats() {
                peak_rss = peak_rss.max(usage.physical_mem);
            }
        }
    }
    if let Some(usage) = memory_stats::memory_stats() {
        peak_rss = peak_rss.max(usage.physical_mem);
    }
    let elapsed = started.elapsed();

    let total_bytes: u64 = std::fs::read_dir(dir)?
        .filter_map(|e| e.ok()?.metadata().ok())
        .map(|m| m.len())
        .sum();
    println!(
        "query: {rows} rows out in {batches} batches, {elapsed:?}\n  {:.2} MiB/s parquet scanned, {:.0} rows/s out\n  peak RSS {:.0} MiB (pool limit {mem_limit_mb} MiB)",
        total_bytes as f64 / 1_048_576.0 / elapsed.as_secs_f64(),
        rows as f64 / elapsed.as_secs_f64(),
        peak_rss as f64 / 1_048_576.0,
    );
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let command = std::env::args().nth(1).unwrap_or_else(|| "query".into());
    let dir = arg("--dir", "/tmp/s1-2-dataset");
    match command.as_str() {
        "gen" => generate(
            &dir,
            arg("--files", "32").parse()?,
            arg("--rows-per-file", "500000").parse()?,
        ),
        "query" => query(&dir, arg("--mem-limit-mb", "512").parse()?).await,
        other => anyhow::bail!("unknown command `{other}` (use: gen | query)"),
    }
}
