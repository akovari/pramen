//! Offline sink conformance helpers (E1.4).

use crate::runtime::{Sink, StageError};
use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Assert the ADR 0007 commit-barrier contract for a sink.
///
/// `visible_rows` returns how many rows the destination currently exposes.
/// After `write` and before `commit` it must stay at the baseline; after
/// `commit` it must equal baseline + written rows. Dropping a sink that has
/// written but not committed must leave visibility unchanged.
///
/// # Errors
///
/// Returns a message when the sink or the visibility probe violates the
/// contract.
pub async fn assert_sink_commit_barrier<S, F>(
    mut make_sink: impl FnMut() -> S,
    visible_rows: F,
) -> Result<(), String>
where
    S: Sink,
    F: Fn() -> u64,
{
    let baseline = visible_rows();
    let mut sink = make_sink();
    sink.write(fixture_batch(10)?)
        .await
        .map_err(|error| format!("write: {error}"))?;
    sink.write(fixture_batch(5)?)
        .await
        .map_err(|error| format!("write: {error}"))?;
    let after_write = visible_rows();
    if after_write != baseline {
        return Err(format!(
            "commit barrier violated: visible rows moved from {baseline} to {after_write} before commit"
        ));
    }
    sink.commit()
        .await
        .map_err(|error| format!("commit: {error}"))?;
    let after_commit = visible_rows();
    if after_commit != baseline + 15 {
        return Err(format!(
            "expected {} visible rows after commit, got {after_commit}",
            baseline + 15
        ));
    }

    let baseline = visible_rows();
    {
        let mut sink = make_sink();
        sink.write(fixture_batch(7)?)
            .await
            .map_err(|error| format!("write (no commit): {error}"))?;
        drop(sink);
    }
    let after_drop = visible_rows();
    if after_drop != baseline {
        return Err(format!(
            "drop without commit leaked rows: baseline {baseline}, now {after_drop}"
        ));
    }
    Ok(())
}

fn fixture_batch(rows: i64) -> Result<RecordBatch, String> {
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
    let values: Vec<i64> = (0..rows).collect();
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values))])
        .map_err(|error| format!("fixture batch: {error}"))
}

/// In-memory sink used to pin the conformance harness itself.
///
/// Rows become visible only on [`Sink::commit`].
pub struct RecordingSink {
    pending: Vec<RecordBatch>,
    committed_rows: Arc<AtomicU64>,
}

impl RecordingSink {
    /// Create a sink that publishes into `committed_rows` on commit.
    #[must_use]
    pub fn new(committed_rows: Arc<AtomicU64>) -> Self {
        Self {
            pending: Vec::new(),
            committed_rows,
        }
    }
}

#[async_trait::async_trait]
impl Sink for RecordingSink {
    async fn write(&mut self, batch: RecordBatch) -> Result<(), StageError> {
        self.pending.push(batch);
        Ok(())
    }

    async fn commit(&mut self) -> Result<(), StageError> {
        let rows: u64 = self
            .pending
            .drain(..)
            .map(|batch| batch.num_rows() as u64)
            .sum();
        self.committed_rows.fetch_add(rows, Ordering::SeqCst);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn recording_sink_satisfies_commit_barrier() {
        let rows = Arc::new(AtomicU64::new(0));
        let probe = Arc::clone(&rows);
        assert_sink_commit_barrier(
            || RecordingSink::new(Arc::clone(&rows)),
            move || probe.load(Ordering::SeqCst),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn harness_detects_eager_visibility() {
        let rows = Arc::new(AtomicU64::new(0));
        let probe = Arc::clone(&rows);
        let result = assert_sink_commit_barrier(
            || EagerSink {
                rows: Arc::clone(&rows),
            },
            move || probe.load(Ordering::SeqCst),
        )
        .await;
        assert!(result.is_err(), "eager sink must fail conformance");
    }

    struct EagerSink {
        rows: Arc<AtomicU64>,
    }

    #[async_trait::async_trait]
    impl Sink for EagerSink {
        async fn write(&mut self, batch: RecordBatch) -> Result<(), StageError> {
            self.rows
                .fetch_add(batch.num_rows() as u64, Ordering::SeqCst);
            Ok(())
        }

        async fn commit(&mut self) -> Result<(), StageError> {
            Ok(())
        }
    }
}
