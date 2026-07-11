//! The dataflow runtime: stage traits and the bounded-channel runner.
//!
//! A pipeline is executed as one tokio task per stage, connected by bounded
//! `mpsc` channels of Arrow [`RecordBatch`]es. Backpressure is structural:
//! a fast source blocks on `send` when a downstream stage is slower, so
//! memory in flight is bounded by channel capacity times batch size.
//!
//! Shutdown is cooperative, and failure ordering matters: a failing stage
//! cancels the shared [`CancellationToken`] *before* its channel ends are
//! dropped, so a downstream stage that sees its input close can reliably
//! distinguish "upstream finished" from "upstream failed" and never commit
//! a partial run. External cancellation (Ctrl-C) uses the same token and
//! surfaces as [`RunError::Cancelled`].

mod stages;

pub use stages::{Sink, Source, StageError, Transform};

use crate::observe::{MetricsSnapshot, RunMetrics};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender, channel};
use tokio_util::sync::CancellationToken;

/// Tuning knobs for [`run_pipeline`].
#[derive(Debug, Clone)]
pub struct RunOptions {
    /// Capacity, in batches, of each inter-stage channel.
    pub channel_capacity: usize,
}

impl Default for RunOptions {
    fn default() -> Self {
        Self {
            channel_capacity: 4,
        }
    }
}

/// Why a run did not complete.
#[derive(Debug, thiserror::Error)]
pub enum RunError {
    /// A stage failed; the offending stage id is included.
    #[error("stage `{stage}` failed: {source}")]
    Stage {
        /// Identifier of the failing stage.
        stage: String,
        /// The underlying stage error.
        source: StageError,
    },
    /// The run was cancelled from outside before completing.
    #[error("run cancelled")]
    Cancelled,
    /// A stage task panicked or was aborted.
    #[error("stage task aborted: {0}")]
    Join(String),
}

/// The result of a completed run.
#[derive(Debug, Clone, Copy)]
pub struct RunSummary {
    /// Final counter values for the run.
    pub metrics: MetricsSnapshot,
}

fn batch_bytes(batch: &RecordBatch) -> u64 {
    batch.get_array_memory_size() as u64
}

/// Await `future`, aborting with [`RunError::Cancelled`] if `cancel` fires
/// first. Every potentially-blocking await in a stage goes through this so
/// no stage can stall shutdown.
async fn cancellable<T>(
    cancel: &CancellationToken,
    future: impl Future<Output = T>,
) -> Result<T, RunError> {
    tokio::select! {
        () = cancel.cancelled() => Err(RunError::Cancelled),
        value = future => Ok(value),
    }
}

async fn source_loop(
    source: &mut dyn Source,
    tx: &Sender<RecordBatch>,
    metrics: &RunMetrics,
    cancel: &CancellationToken,
) -> Result<(), RunError> {
    loop {
        let next = cancellable(cancel, source.next_batch()).await?;
        let batch = match next {
            Ok(Some(batch)) => batch,
            Ok(None) => return Ok(()),
            Err(source) => {
                return Err(RunError::Stage {
                    stage: "source".to_owned(),
                    source,
                });
            }
        };
        metrics.record_in(batch.num_rows() as u64, batch_bytes(&batch));
        if cancellable(cancel, tx.send(batch)).await?.is_err() {
            // Downstream closed: it failed (reported there) or the run is
            // tearing down. Stop quietly either way.
            return Ok(());
        }
    }
}

async fn transform_loop(
    stage_id: &str,
    transform: &mut dyn Transform,
    input: &mut Receiver<RecordBatch>,
    tx: &Sender<RecordBatch>,
    cancel: &CancellationToken,
) -> Result<(), RunError> {
    let fail = |source| RunError::Stage {
        stage: stage_id.to_owned(),
        source,
    };
    loop {
        let Some(batch) = cancellable(cancel, input.recv()).await? else {
            break;
        };
        let outputs = cancellable(cancel, transform.apply(batch))
            .await?
            .map_err(fail)?;
        for out in outputs {
            if cancellable(cancel, tx.send(out)).await?.is_err() {
                return Ok(());
            }
        }
    }
    // Input closed. A failed upstream cancels before dropping its sender,
    // so a clean close with a cancelled token means failure, not EOF.
    if cancel.is_cancelled() {
        return Err(RunError::Cancelled);
    }
    let outputs = cancellable(cancel, transform.finish())
        .await?
        .map_err(fail)?;
    for out in outputs {
        if cancellable(cancel, tx.send(out)).await?.is_err() {
            return Ok(());
        }
    }
    Ok(())
}

async fn sink_loop(
    sink: &mut dyn Sink,
    input: &mut Receiver<RecordBatch>,
    metrics: &RunMetrics,
    cancel: &CancellationToken,
) -> Result<(), RunError> {
    let fail = |source| RunError::Stage {
        stage: "sink".to_owned(),
        source,
    };
    loop {
        let Some(batch) = cancellable(cancel, input.recv()).await? else {
            break;
        };
        let (rows, bytes) = (batch.num_rows() as u64, batch_bytes(&batch));
        cancellable(cancel, sink.write(batch))
            .await?
            .map_err(fail)?;
        metrics.record_out(rows, bytes);
    }
    // A commit must never make a failed run's partial output visible.
    if cancel.is_cancelled() {
        return Err(RunError::Cancelled);
    }
    cancellable(cancel, sink.commit()).await?.map_err(fail)?;
    Ok(())
}

/// Execute a linear pipeline to completion.
///
/// Consumes the stages, wires them with bounded channels, runs each in its
/// own task, and waits for the sink to commit. `cancel` may be triggered at
/// any time to abort the run cooperatively.
///
/// # Errors
///
/// Returns the first stage error, [`RunError::Cancelled`] when `cancel`
/// fired before completion, or [`RunError::Join`] if a stage panicked.
pub async fn run_pipeline(
    mut source: Box<dyn Source>,
    transforms: Vec<(String, Box<dyn Transform>)>,
    mut sink: Box<dyn Sink>,
    options: RunOptions,
    metrics: Arc<RunMetrics>,
    cancel: CancellationToken,
) -> Result<RunSummary, RunError> {
    let capacity = options.channel_capacity.max(1);
    let mut tasks: tokio::task::JoinSet<Result<(), RunError>> = tokio::task::JoinSet::new();

    let (source_tx, mut upstream_rx) = channel::<RecordBatch>(capacity);
    {
        let cancel = cancel.clone();
        let metrics = Arc::clone(&metrics);
        tasks.spawn(async move {
            let result = source_loop(source.as_mut(), &source_tx, &metrics, &cancel).await;
            if result.is_err() {
                // Cancel before source_tx drops (at end of this block).
                cancel.cancel();
            }
            result
        });
    }

    for (stage_id, mut transform) in transforms {
        let (tx, rx) = channel::<RecordBatch>(capacity);
        let mut input = std::mem::replace(&mut upstream_rx, rx);
        let cancel = cancel.clone();
        tasks.spawn(async move {
            let result =
                transform_loop(&stage_id, transform.as_mut(), &mut input, &tx, &cancel).await;
            if result.is_err() {
                // Cancel before tx and input drop (at end of this block).
                cancel.cancel();
            }
            result
        });
    }

    {
        let cancel = cancel.clone();
        let metrics = Arc::clone(&metrics);
        let mut input = upstream_rx;
        tasks.spawn(async move {
            let result = sink_loop(sink.as_mut(), &mut input, &metrics, &cancel).await;
            if result.is_err() {
                cancel.cancel();
            }
            result
        });
    }

    // Collect every outcome; a concrete stage failure beats Cancelled so
    // the user sees the root cause, not the teardown.
    let mut outcome: Result<(), RunError> = Ok(());
    while let Some(joined) = tasks.join_next().await {
        let result = match joined {
            Ok(result) => result,
            Err(join_error) => Err(RunError::Join(join_error.to_string())),
        };
        match (&outcome, &result) {
            (Ok(()) | Err(RunError::Cancelled), Err(error))
                if !matches!(error, RunError::Cancelled) =>
            {
                outcome = result;
            }
            (Ok(()), Err(RunError::Cancelled)) => outcome = result,
            _ => {}
        }
    }

    outcome.map(|()| RunSummary {
        metrics: metrics.snapshot(),
    })
}

#[cfg(test)]
mod tests;
