// A benchmark baseline binary: panicking with a clear message is the
// right failure mode, so `expect` is fine here.
#![allow(clippy::expect_used)]
//! Engine-only baseline for the benchmark suite (`scripts/bench.sh`).
//!
//! Runs the same SQL as the benchmark pipeline directly through
//! DataFusion's streaming execution and drains the result without any
//! sink. This is the scan-and-transform ceiling: the difference between
//! this number and the end-to-end Pramen run is the cost of the runtime,
//! the binary `COPY` encoder, the wire, and the PostgreSQL server.
//!
//! Usage:
//!
//! ```text
//! cargo run --release -p pramen-io --example datafusion_direct -- <data_dir>
//! ```

use datafusion::prelude::{ParquetReadOptions, SessionContext};
use futures::StreamExt;
use std::time::Instant;

const QUERY: &str = "SELECT id, category, amount, amount * 1.21 AS amount_gross, \
                     active, created_at, note \
                     FROM input WHERE category <> 'epsilon'";

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let [_, data_dir] = args.as_slice() else {
        eprintln!("usage: datafusion_direct <data_dir>");
        std::process::exit(2);
    };

    let ctx = SessionContext::new();
    ctx.register_parquet("input", data_dir, ParquetReadOptions::default())
        .await
        .expect("register parquet directory");

    let started = Instant::now();
    let mut stream = ctx
        .sql(QUERY)
        .await
        .expect("plan query")
        .execute_stream()
        .await
        .expect("execute query");
    let mut rows = 0usize;
    while let Some(batch) = stream.next().await {
        rows += batch.expect("stream batch").num_rows();
    }
    let elapsed = started.elapsed();
    println!(
        "datafusion_direct rows={rows} wall_s={:.3} rows_per_s={:.0}",
        elapsed.as_secs_f64(),
        rows as f64 / elapsed.as_secs_f64()
    );
}
