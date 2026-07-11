//! Observability spine: log initialization and the run metrics registry.
//!
//! Logging is structured `tracing` with three operator-selectable formats
//! (`pretty`, `json`, `silent`). Metrics are a cheap atomic registry that
//! stages increment on the hot path and the CLI snapshots for reporting.

use serde::Serialize;
use std::sync::atomic::{AtomicU64, Ordering};

/// Log output formats selectable via `--log-format`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum LogFormat {
    /// Human-oriented, colored, single-line output. The default.
    #[default]
    Pretty,
    /// One JSON object per line, for collectors.
    Json,
    /// No log output at all.
    Silent,
}

impl std::str::FromStr for LogFormat {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "pretty" => Ok(Self::Pretty),
            "json" => Ok(Self::Json),
            "silent" => Ok(Self::Silent),
            other => Err(format!(
                "unknown log format `{other}` (expected: pretty, json, silent)"
            )),
        }
    }
}

/// Install the global `tracing` subscriber for the selected format.
///
/// Respects `RUST_LOG` for filtering, defaulting to `info`. Calling this
/// more than once is an error only for the caller that lost the race; the
/// first subscriber stays installed and the result reports it.
///
/// # Errors
///
/// Returns an error when a global subscriber is already installed.
pub fn init_logging(format: LogFormat) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use tracing_subscriber::EnvFilter;

    if format == LogFormat::Silent {
        return Ok(());
    }
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let builder = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr);
    match format {
        LogFormat::Pretty => builder.try_init()?,
        LogFormat::Json => builder.json().flatten_event(true).try_init()?,
        LogFormat::Silent => {}
    }
    Ok(())
}

/// Shared counters incremented by pipeline stages during a run.
///
/// All counters are monotonic within one run. Increments use relaxed
/// ordering: the values are statistics, not synchronization.
#[derive(Debug, Default)]
pub struct RunMetrics {
    /// Batches emitted by the source.
    pub batches_in: AtomicU64,
    /// Batches accepted by the sink.
    pub batches_out: AtomicU64,
    /// Rows emitted by the source.
    pub rows_in: AtomicU64,
    /// Rows accepted by the sink.
    pub rows_out: AtomicU64,
    /// Arrow buffer bytes emitted by the source.
    pub bytes_in: AtomicU64,
    /// Arrow buffer bytes accepted by the sink.
    pub bytes_out: AtomicU64,
}

impl RunMetrics {
    /// Add `rows` and `bytes` for one batch on the source side.
    pub fn record_in(&self, rows: u64, bytes: u64) {
        self.batches_in.fetch_add(1, Ordering::Relaxed);
        self.rows_in.fetch_add(rows, Ordering::Relaxed);
        self.bytes_in.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Add `rows` and `bytes` for one batch on the sink side.
    pub fn record_out(&self, rows: u64, bytes: u64) {
        self.batches_out.fetch_add(1, Ordering::Relaxed);
        self.rows_out.fetch_add(rows, Ordering::Relaxed);
        self.bytes_out.fetch_add(bytes, Ordering::Relaxed);
    }

    /// A point-in-time copy of every counter.
    #[must_use]
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            batches_in: self.batches_in.load(Ordering::Relaxed),
            batches_out: self.batches_out.load(Ordering::Relaxed),
            rows_in: self.rows_in.load(Ordering::Relaxed),
            rows_out: self.rows_out.load(Ordering::Relaxed),
            bytes_in: self.bytes_in.load(Ordering::Relaxed),
            bytes_out: self.bytes_out.load(Ordering::Relaxed),
        }
    }
}

/// A point-in-time copy of [`RunMetrics`], serializable for reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct MetricsSnapshot {
    /// Batches emitted by the source.
    pub batches_in: u64,
    /// Batches accepted by the sink.
    pub batches_out: u64,
    /// Rows emitted by the source.
    pub rows_in: u64,
    /// Rows accepted by the sink.
    pub rows_out: u64,
    /// Arrow buffer bytes emitted by the source.
    pub bytes_in: u64,
    /// Arrow buffer bytes accepted by the sink.
    pub bytes_out: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn log_format_parses() {
        assert_eq!("pretty".parse::<LogFormat>().unwrap(), LogFormat::Pretty);
        assert_eq!("json".parse::<LogFormat>().unwrap(), LogFormat::Json);
        assert_eq!("silent".parse::<LogFormat>().unwrap(), LogFormat::Silent);
        assert!("verbose".parse::<LogFormat>().is_err());
    }

    #[test]
    fn metrics_accumulate_and_snapshot() {
        let metrics = RunMetrics::default();
        metrics.record_in(10, 1000);
        metrics.record_in(5, 500);
        metrics.record_out(15, 1500);
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.batches_in, 2);
        assert_eq!(snapshot.rows_in, 15);
        assert_eq!(snapshot.bytes_in, 1500);
        assert_eq!(snapshot.batches_out, 1);
        assert_eq!(snapshot.rows_out, 15);
    }
}
