use super::*;
use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

fn test_batch(start: i64, rows: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
    let values: Vec<i64> = (start..start + rows as i64).collect();
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values))]).unwrap()
}

/// Emits `total` batches of `rows` rows, counting how many were emitted.
struct CountingSource {
    total: usize,
    rows: usize,
    emitted: Arc<AtomicU64>,
}

#[async_trait::async_trait]
impl Source for CountingSource {
    async fn next_batch(&mut self) -> Result<Option<RecordBatch>, StageError> {
        let emitted = self.emitted.fetch_add(1, Ordering::SeqCst) as usize;
        if emitted >= self.total {
            self.emitted.fetch_sub(1, Ordering::SeqCst);
            return Ok(None);
        }
        Ok(Some(test_batch((emitted * self.rows) as i64, self.rows)))
    }
}

/// Passes batches through, optionally failing on the nth batch.
struct FailingTransform {
    seen: usize,
    fail_on: Option<usize>,
}

#[async_trait::async_trait]
impl Transform for FailingTransform {
    async fn apply(&mut self, batch: RecordBatch) -> Result<Vec<RecordBatch>, StageError> {
        self.seen += 1;
        if self.fail_on == Some(self.seen) {
            return Err(StageError::InvalidData("boom".to_owned()));
        }
        Ok(vec![batch])
    }
}

/// Splits every batch in two and emits a final marker batch on finish.
struct SplittingTransform;

#[async_trait::async_trait]
impl Transform for SplittingTransform {
    async fn apply(&mut self, batch: RecordBatch) -> Result<Vec<RecordBatch>, StageError> {
        let half = batch.num_rows() / 2;
        Ok(vec![
            batch.slice(0, half),
            batch.slice(half, batch.num_rows() - half),
        ])
    }

    async fn finish(&mut self) -> Result<Vec<RecordBatch>, StageError> {
        Ok(vec![test_batch(-1, 1)])
    }
}

/// Counts rows; optionally holds every write until permitted.
struct CollectingSink {
    rows: Arc<AtomicU64>,
    committed: Arc<AtomicU64>,
    hold: Option<Arc<tokio::sync::Semaphore>>,
    fail_write: bool,
}

#[async_trait::async_trait]
impl Sink for CollectingSink {
    async fn write(&mut self, batch: RecordBatch) -> Result<(), StageError> {
        if self.fail_write {
            return Err(StageError::InvalidData("sink boom".to_owned()));
        }
        if let Some(hold) = &self.hold {
            let permit = hold.acquire().await;
            drop(permit);
        }
        self.rows
            .fetch_add(batch.num_rows() as u64, Ordering::SeqCst);
        Ok(())
    }

    async fn commit(&mut self) -> Result<(), StageError> {
        self.committed.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

fn collecting_sink() -> (Box<CollectingSink>, Arc<AtomicU64>, Arc<AtomicU64>) {
    let rows = Arc::new(AtomicU64::new(0));
    let committed = Arc::new(AtomicU64::new(0));
    let sink = Box::new(CollectingSink {
        rows: Arc::clone(&rows),
        committed: Arc::clone(&committed),
        hold: None,
        fail_write: false,
    });
    (sink, rows, committed)
}

#[tokio::test]
async fn linear_pipeline_moves_every_row_and_commits() {
    let source = Box::new(CountingSource {
        total: 5,
        rows: 100,
        emitted: Arc::new(AtomicU64::new(0)),
    });
    let (sink, rows, committed) = collecting_sink();
    let metrics = Arc::new(RunMetrics::default());

    let summary = run_pipeline(
        source,
        vec![(
            "split".to_owned(),
            "source".to_owned(),
            Box::new(SplittingTransform),
        )],
        vec![("sink".to_owned(), "split".to_owned(), sink)],
        RunOptions::default(),
        Arc::clone(&metrics),
        CancellationToken::new(),
    )
    .await
    .unwrap();

    // 5 x 100 rows plus the 1-row finish marker.
    assert_eq!(rows.load(Ordering::SeqCst), 501);
    assert_eq!(committed.load(Ordering::SeqCst), 1);
    assert_eq!(summary.metrics.rows_in, 500);
    assert_eq!(summary.metrics.rows_out, 501);
    assert_eq!(summary.metrics.batches_in, 5);
    // Each source batch split in two, plus the finish marker.
    assert_eq!(summary.metrics.batches_out, 11);
}

#[tokio::test]
async fn fanout_two_sinks_receive_identical_rows_and_both_commit() {
    let source = Box::new(CountingSource {
        total: 3,
        rows: 10,
        emitted: Arc::new(AtomicU64::new(0)),
    });
    let (sink_a, rows_a, committed_a) = collecting_sink();
    let (sink_b, rows_b, committed_b) = collecting_sink();

    run_pipeline(
        source,
        vec![],
        vec![
            ("a".to_owned(), "source".to_owned(), sink_a),
            ("b".to_owned(), "source".to_owned(), sink_b),
        ],
        RunOptions::default(),
        Arc::new(RunMetrics::default()),
        CancellationToken::new(),
    )
    .await
    .unwrap();

    assert_eq!(rows_a.load(Ordering::SeqCst), 30);
    assert_eq!(rows_b.load(Ordering::SeqCst), 30);
    assert_eq!(committed_a.load(Ordering::SeqCst), 1);
    assert_eq!(committed_b.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn fanout_sink_failure_prevents_all_commits() {
    let source = Box::new(CountingSource {
        total: 2,
        rows: 5,
        emitted: Arc::new(AtomicU64::new(0)),
    });
    let (ok_sink, _, committed_ok) = collecting_sink();
    let rows = Arc::new(AtomicU64::new(0));
    let committed_fail = Arc::new(AtomicU64::new(0));
    let fail_sink = Box::new(CollectingSink {
        rows: Arc::clone(&rows),
        committed: Arc::clone(&committed_fail),
        hold: None,
        fail_write: true,
    });

    let error = run_pipeline(
        source,
        vec![],
        vec![
            ("ok".to_owned(), "source".to_owned(), ok_sink),
            ("bad".to_owned(), "source".to_owned(), fail_sink),
        ],
        RunOptions::default(),
        Arc::new(RunMetrics::default()),
        CancellationToken::new(),
    )
    .await
    .unwrap_err();

    assert!(matches!(error, RunError::Stage { .. }));
    assert_eq!(committed_ok.load(Ordering::SeqCst), 0);
    assert_eq!(committed_fail.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn transform_error_fails_the_run_with_stage_id() {
    let source = Box::new(CountingSource {
        total: 100,
        rows: 10,
        emitted: Arc::new(AtomicU64::new(0)),
    });
    let (sink, _, committed) = collecting_sink();

    let error = run_pipeline(
        source,
        vec![(
            "explode".to_owned(),
            "source".to_owned(),
            Box::new(FailingTransform {
                seen: 0,
                fail_on: Some(3),
            }),
        )],
        vec![("sink".to_owned(), "explode".to_owned(), sink)],
        RunOptions::default(),
        Arc::new(RunMetrics::default()),
        CancellationToken::new(),
    )
    .await
    .unwrap_err();

    let RunError::Stage { stage, source } = error else {
        panic!("expected stage error, got: {error}");
    };
    assert_eq!(stage, "explode");
    assert!(matches!(source, StageError::InvalidData(_)));
    assert_eq!(committed.load(Ordering::SeqCst), 0, "must not commit");
}

#[tokio::test]
async fn external_cancellation_stops_an_endless_run_promptly() {
    let source = Box::new(CountingSource {
        total: usize::MAX,
        rows: 10,
        emitted: Arc::new(AtomicU64::new(0)),
    });
    let (sink, _, committed) = collecting_sink();
    let cancel = CancellationToken::new();

    let run = tokio::spawn(run_pipeline(
        source,
        vec![],
        vec![("sink".to_owned(), "source".to_owned(), sink)],
        RunOptions::default(),
        Arc::new(RunMetrics::default()),
        cancel.clone(),
    ));
    tokio::time::sleep(Duration::from_millis(50)).await;
    cancel.cancel();

    let outcome = tokio::time::timeout(Duration::from_secs(2), run)
        .await
        .expect("run must stop promptly after cancellation")
        .unwrap();
    assert!(matches!(outcome, Err(RunError::Cancelled)));
    assert_eq!(committed.load(Ordering::SeqCst), 0, "must not commit");
}

#[tokio::test]
async fn bounded_channels_apply_backpressure_to_the_source() {
    let emitted = Arc::new(AtomicU64::new(0));
    let source = Box::new(CountingSource {
        total: usize::MAX,
        rows: 10,
        emitted: Arc::clone(&emitted),
    });
    let hold = Arc::new(tokio::sync::Semaphore::new(0));
    let rows = Arc::new(AtomicU64::new(0));
    let committed = Arc::new(AtomicU64::new(0));
    let sink = Box::new(CollectingSink {
        rows: Arc::clone(&rows),
        committed: Arc::clone(&committed),
        hold: Some(Arc::clone(&hold)),
        fail_write: false,
    });
    let cancel = CancellationToken::new();

    let run = tokio::spawn(run_pipeline(
        source,
        vec![],
        vec![("sink".to_owned(), "source".to_owned(), sink)],
        RunOptions {
            channel_capacity: 2,
        },
        Arc::new(RunMetrics::default()),
        cancel.clone(),
    ));

    // With the sink blocked, the source can run at most capacity + the one
    // batch held by each stage ahead of the block.
    tokio::time::sleep(Duration::from_millis(100)).await;
    let produced = emitted.load(Ordering::SeqCst);
    assert!(
        produced <= 5,
        "source ran {produced} batches ahead of a blocked sink"
    );

    cancel.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(2), run)
        .await
        .expect("run must stop promptly after cancellation");
}
