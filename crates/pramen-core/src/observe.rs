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
        LogFormat::Json => builder
            .json()
            .flatten_event(true)
            .with_current_span(false)
            .with_span_list(false)
            .try_init()?,
        LogFormat::Silent => {}
    }
    Ok(())
}

/// The envelope fields every `--log-format json` event line carries, in
/// addition to the event's own flattened fields.
///
/// This is the contract collectors may rely on; the schema test pins it,
/// and changing it is a breaking change to the JSON log format.
pub const JSON_EVENT_ENVELOPE: [&str; 4] = ["timestamp", "level", "target", "message"];

/// Push one run's final metrics to an OTLP collector over HTTP/protobuf
/// (`endpoint` is the collector base URL, e.g. `http://localhost:4318`).
///
/// This is a one-shot export at run end — the §13 signal list as OTLP
/// counters (`pramen.rows_in`, `pramen.rows_out`, `pramen.batches_in`,
/// `pramen.batches_out`, `pramen.bytes_in`, `pramen.bytes_out`) plus a
/// `pramen.run_duration` gauge in seconds, all attributed with the
/// pipeline name. It uses a blocking HTTP client and must not be called
/// from inside an async runtime.
///
/// # Errors
///
/// Returns a message when the exporter cannot be built or the flush to
/// the collector fails.
pub fn export_metrics_otlp(
    endpoint: &str,
    pipeline: &str,
    snapshot: &MetricsSnapshot,
    duration_seconds: f64,
) -> Result<(), String> {
    use opentelemetry::KeyValue;
    use opentelemetry::metrics::MeterProvider as _;
    use opentelemetry_otlp::WithExportConfig as _;

    let exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_http()
        .with_protocol(opentelemetry_otlp::Protocol::HttpBinary)
        .with_endpoint(format!(
            "{}/v1/metrics",
            endpoint
                .trim_end_matches('/')
                .trim_end_matches("/v1/metrics")
        ))
        .build()
        .map_err(|error| format!("otlp exporter: {error}"))?;
    let provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
        .with_periodic_exporter(exporter)
        .with_resource(
            opentelemetry_sdk::Resource::builder()
                .with_service_name("pramen")
                .build(),
        )
        .build();

    let meter = provider.meter("pramen");
    let attributes = [KeyValue::new("pipeline", pipeline.to_owned())];
    for (name, value) in [
        ("pramen.rows_in", snapshot.rows_in),
        ("pramen.rows_out", snapshot.rows_out),
        ("pramen.batches_in", snapshot.batches_in),
        ("pramen.batches_out", snapshot.batches_out),
        ("pramen.bytes_in", snapshot.bytes_in),
        ("pramen.bytes_out", snapshot.bytes_out),
    ] {
        meter.u64_counter(name).build().add(value, &attributes);
    }
    meter
        .f64_gauge("pramen.run_duration")
        .with_unit("s")
        .build()
        .record(duration_seconds, &attributes);

    provider
        .shutdown()
        .map_err(|error| format!("otlp export: {error}"))
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

    /// Pins the JSONL event schema (F3): the exact key set a
    /// `--log-format json` line carries for an event with fields. The
    /// subscriber below is configured identically to [`init_logging`]'s
    /// JSON branch; if this test fails, the JSON log contract changed
    /// and collectors will notice too.
    #[test]
    fn json_event_schema_is_pinned() {
        use std::sync::{Arc, Mutex};

        #[derive(Clone, Default)]
        struct Capture(Arc<Mutex<Vec<u8>>>);
        impl std::io::Write for Capture {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.0.lock().map_or(Ok(0), |mut b| {
                    b.extend_from_slice(buf);
                    Ok(buf.len())
                })
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Capture {
            type Writer = Capture;
            fn make_writer(&'a self) -> Capture {
                self.clone()
            }
        }

        let capture = Capture::default();
        let subscriber = tracing_subscriber::fmt()
            .json()
            .flatten_event(true)
            .with_current_span(false)
            .with_span_list(false)
            .with_writer(capture.clone())
            .finish();
        tracing::subscriber::with_default(subscriber, || {
            tracing::info!(rows = 42u64, stage = "sink", "batch committed");
        });

        let bytes = capture.0.lock().unwrap().clone();
        let line = String::from_utf8(bytes).unwrap();
        let event: serde_json::Value = serde_json::from_str(line.trim()).unwrap();

        let mut keys: Vec<&str> = event
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        keys.sort_unstable();
        // The pinned schema: the envelope plus the event's own flattened
        // fields — nothing else.
        assert_eq!(
            keys,
            ["level", "message", "rows", "stage", "target", "timestamp"]
        );
        for field in JSON_EVENT_ENVELOPE {
            assert!(event.get(field).is_some(), "envelope field `{field}`");
        }
        assert_eq!(event["message"], "batch committed");
        assert_eq!(event["rows"], 42);
        assert_eq!(event["stage"], "sink");
        assert_eq!(event["level"], "INFO");
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
