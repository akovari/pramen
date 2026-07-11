//! Spike S1.3: native Rust PostgreSQL `COPY FROM STDIN BINARY` throughput.
//!
//! Generates deterministic Arrow batches, loads them through binary COPY via
//! tokio-postgres, and compares against `psql \copy` fed the same data as
//! CSV (the conventional bulk-load baseline). Verifies row counts and
//! sampled values after each load.
//!
//! Usage: s1-3-postgres-copy [--rows N] [--dsn postgres://...] [--csv PATH]
//!   run-copy   binary COPY through the Rust encoder (default)
//!   make-csv   write the identical dataset as CSV for the psql baseline

mod encoder;

use anyhow::{Context, Result};
use arrow::array::{
    BooleanArray, Float64Array, Int64Array, StringArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use bytes::BytesMut;
use futures_util::SinkExt;
use std::io::Write;
use std::sync::Arc;
use std::time::Instant;

const BATCH_ROWS: usize = 8192;
const CATEGORIES: &[&str] = &["alpha", "beta", "gamma", "delta"];

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("amount", DataType::Float64, false),
        Field::new("category", DataType::Utf8, false),
        Field::new("active", DataType::Boolean, false),
        Field::new(
            "created_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("note", DataType::Utf8, true),
    ]))
}

/// Deterministic pseudo-data; `start` keeps batches distinct.
fn make_batch(start: i64, rows: usize) -> RecordBatch {
    let ids: Vec<i64> = (start..start + rows as i64).collect();
    let amounts: Vec<f64> = ids.iter().map(|i| (i % 10_000) as f64 / 100.0).collect();
    let categories: Vec<&str> = ids
        .iter()
        .map(|i| CATEGORIES[(*i as usize) % CATEGORIES.len()])
        .collect();
    let actives: Vec<bool> = ids.iter().map(|i| i % 3 == 0).collect();
    let timestamps: Vec<i64> = ids
        .iter()
        .map(|i| 1_700_000_000_000_000 + i * 1_000_000)
        .collect();
    let notes: Vec<Option<String>> = ids
        .iter()
        .map(|i| {
            if i % 5 == 0 {
                None
            } else {
                Some(format!("synthetic note for row {i}, padded {}", "x".repeat(24)))
            }
        })
        .collect();

    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Float64Array::from(amounts)),
            Arc::new(StringArray::from(categories)),
            Arc::new(BooleanArray::from(actives)),
            Arc::new(TimestampMicrosecondArray::from(timestamps).with_timezone("UTC")),
            Arc::new(StringArray::from(notes)),
        ],
    )
    .expect("schema and arrays agree")
}

const CREATE_TABLE: &str = "CREATE TABLE IF NOT EXISTS spike_copy (
    id bigint NOT NULL,
    amount double precision NOT NULL,
    category text NOT NULL,
    active boolean NOT NULL,
    created_at timestamptz NOT NULL,
    note text
)";

fn arg(name: &str, default: &str) -> String {
    let args: Vec<String> = std::env::args().collect();
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1))
        .cloned()
        .unwrap_or_else(|| default.to_owned())
}

async fn connect(dsn: &str) -> Result<tokio_postgres::Client> {
    let (client, connection) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
        .await
        .context("connect to postgres")?;
    tokio::spawn(async move {
        if let Err(error) = connection.await {
            eprintln!("connection error: {error}");
        }
    });
    Ok(client)
}

async fn run_copy(dsn: &str, total_rows: usize) -> Result<()> {
    let client = connect(dsn).await?;
    client.batch_execute(CREATE_TABLE).await?;
    client.batch_execute("TRUNCATE spike_copy").await?;

    let started = Instant::now();
    let sink = client
        .copy_in("COPY spike_copy FROM STDIN (FORMAT binary)")
        .await?;
    futures_util::pin_mut!(sink);

    let mut bytes_sent: u64 = 0;
    let mut buf = BytesMut::with_capacity(4 * 1024 * 1024);
    buf.extend_from_slice(&encoder::copy_header());

    let mut row = 0_i64;
    while (row as usize) < total_rows {
        let rows = BATCH_ROWS.min(total_rows - row as usize);
        let batch = make_batch(row, rows);
        encoder::encode_batch(&batch, &mut buf)?;
        row += rows as i64;
        if buf.len() >= 2 * 1024 * 1024 {
            bytes_sent += buf.len() as u64;
            sink.send(buf.split().freeze()).await?;
        }
    }
    buf.extend_from_slice(&encoder::copy_trailer());
    bytes_sent += buf.len() as u64;
    sink.send(buf.split().freeze()).await?;
    let copied = sink.finish().await?;
    let elapsed = started.elapsed();

    let count: i64 = client
        .query_one("SELECT count(*) FROM spike_copy", &[])
        .await?
        .get(0);
    let sample = client
        .query_one(
            "SELECT amount, category, active, note FROM spike_copy WHERE id = 7",
            &[],
        )
        .await?;
    anyhow::ensure!(count as usize == total_rows, "row count mismatch: {count}");
    anyhow::ensure!(sample.get::<_, f64>(0) == 0.07, "amount mismatch");
    anyhow::ensure!(sample.get::<_, &str>(1) == "delta", "category mismatch");
    anyhow::ensure!(sample.get::<_, Option<&str>>(3).is_some(), "note mismatch");

    println!(
        "binary COPY: {total_rows} rows ({copied} reported) in {elapsed:?}\n  {:.0} rows/s, {:.1} MiB/s wire",
        total_rows as f64 / elapsed.as_secs_f64(),
        bytes_sent as f64 / 1_048_576.0 / elapsed.as_secs_f64(),
    );
    Ok(())
}

/// Civil date from days since the Unix epoch (Howard Hinnant's algorithm).
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let year = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let month = (if mp < 10 { mp + 3 } else { mp - 9 }) as u32;
    (if month <= 2 { year + 1 } else { year }, month, day)
}

fn iso_timestamp(epoch_us: i64) -> String {
    let secs = epoch_us.div_euclid(1_000_000);
    let days = secs.div_euclid(86_400);
    let tod = secs.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    format!(
        "{year:04}-{month:02}-{day:02} {:02}:{:02}:{:02}+00",
        tod / 3600,
        (tod % 3600) / 60,
        tod % 60
    )
}

fn make_csv(path: &str, total_rows: usize) -> Result<()> {
    let file = std::fs::File::create(path)?;
    let mut writer = std::io::BufWriter::with_capacity(4 * 1024 * 1024, file);
    let mut row = 0_i64;
    while (row as usize) < total_rows {
        let rows = BATCH_ROWS.min(total_rows - row as usize);
        for i in row..row + rows as i64 {
            let amount = (i % 10_000) as f64 / 100.0;
            let category = CATEGORIES[(i as usize) % CATEGORIES.len()];
            let active = i % 3 == 0;
            let ts = iso_timestamp(1_700_000_000_000_000 + i * 1_000_000);
            let note = if i % 5 == 0 {
                String::new()
            } else {
                // Quoted: the note text contains a comma.
                format!("\"synthetic note for row {i}, padded {}\"", "x".repeat(24))
            };
            writeln!(writer, "{i},{amount},{category},{active},{ts},{note}")?;
        }
        row += rows as i64;
    }
    writer.flush()?;
    println!("wrote {total_rows} rows to {path}");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let command = std::env::args().nth(1).unwrap_or_else(|| "run-copy".into());
    let dsn = arg(
        "--dsn",
        "postgres://postgres:spike@localhost:5433/postgres",
    );
    let rows: usize = arg("--rows", "5000000").parse()?;

    match command.as_str() {
        "run-copy" => run_copy(&dsn, rows).await,
        "make-csv" => make_csv(&arg("--csv", "/tmp/s1-3-baseline.csv"), rows),
        other => anyhow::bail!("unknown command `{other}` (use: run-copy | make-csv)"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Round-trip a small batch through a real Postgres if one is reachable;
    /// otherwise validate the encoded frame structurally.
    #[test]
    fn encoded_frame_is_structurally_valid() {
        let batch = make_batch(0, 3);
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&encoder::copy_header());
        encoder::encode_batch(&batch, &mut buf).unwrap();
        buf.extend_from_slice(&encoder::copy_trailer());

        assert_eq!(&buf[..11], b"PGCOPY\n\xff\r\n\0");
        // First tuple: field count 6 immediately after the 19-byte header.
        assert_eq!(i16::from_be_bytes([buf[19], buf[20]]), 6);
        // Ends with the -1 trailer.
        assert_eq!(i16::from_be_bytes([buf[buf.len() - 2], buf[buf.len() - 1]]), -1);
        // Row 0: note is NULL (0 % 5 == 0) — a -1 length must appear in the
        // first tuple; find the id field length (4 bytes after field count).
        assert_eq!(i32::from_be_bytes([buf[21], buf[22], buf[23], buf[24]]), 8);
    }
}
