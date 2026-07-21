//! The dataflow runtime: stage traits and the bounded-channel runner.
//!
//! A pipeline is executed as one tokio task per stage, connected by bounded
//! `mpsc` channels of Arrow [`RecordBatch`]es. Backpressure is structural:
//! a fast source blocks on `send` when a downstream stage is slower, so
//! memory in flight is bounded by channel capacity times batch size.
//!
//! Fan-out (ADR 0007) tees batches by cloning to every downstream channel.
//! Fan-in is rejected at validation time. Sinks complete their write phase
//! first; commits run only after every stage succeeds (all-sinks-then-
//! checkpoint).
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
use std::collections::HashMap;
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

/// A planned transform: `(id, from, operator)`.
pub type PlannedTransform = (String, String, Box<dyn Transform>);
/// A planned sink: `(id, from, operator)`.
pub type PlannedSink = (String, String, Box<dyn Sink>);

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

async fn fanout_send(
    txs: &[Sender<RecordBatch>],
    batch: RecordBatch,
    cancel: &CancellationToken,
) -> Result<(), RunError> {
    for tx in txs {
        if cancellable(cancel, tx.send(batch.clone())).await?.is_err() {
            return Ok(());
        }
    }
    Ok(())
}

async fn source_loop(
    source: &mut dyn Source,
    txs: &[Sender<RecordBatch>],
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
        fanout_send(txs, batch, cancel).await?;
    }
}

async fn transform_loop(
    stage_id: &str,
    transform: &mut dyn Transform,
    input: &mut Receiver<RecordBatch>,
    txs: &[Sender<RecordBatch>],
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
            fanout_send(txs, out, cancel).await?;
        }
    }
    if cancel.is_cancelled() {
        return Err(RunError::Cancelled);
    }
    let outputs = cancellable(cancel, transform.finish())
        .await?
        .map_err(fail)?;
    for out in outputs {
        fanout_send(txs, out, cancel).await?;
    }
    Ok(())
}

async fn sink_write_loop(
    stage_id: &str,
    sink: &mut dyn Sink,
    input: &mut Receiver<RecordBatch>,
    metrics: &RunMetrics,
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
        let (rows, bytes) = (batch.num_rows() as u64, batch_bytes(&batch));
        cancellable(cancel, sink.write(batch))
            .await?
            .map_err(fail)?;
        metrics.record_out(rows, bytes);
    }
    if cancel.is_cancelled() {
        return Err(RunError::Cancelled);
    }
    Ok(())
}

fn merge_outcome(outcome: &mut Result<(), RunError>, result: Result<(), RunError>) {
    match (&*outcome, &result) {
        (Ok(()) | Err(RunError::Cancelled), Err(error)) if !matches!(error, RunError::Cancelled) => {
            *outcome = result;
        }
        (Ok(()), Err(RunError::Cancelled)) => *outcome = result,
        _ => {}
    }
}

/// Execute a pipeline graph to completion (linear or fan-out).
///
/// Each transform and sink names its upstream via `from` (`"source"` or a
/// transform id). Fan-out tees batches to every downstream; validation must
/// have rejected fan-in. Sink `commit` runs only after every write phase
/// succeeds (ADR 0007).
///
/// # Errors
///
/// Returns the first stage error, [`RunError::Cancelled`] when `cancel`
/// fired before completion, or [`RunError::Join`] if a stage panicked.
pub async fn run_pipeline(
    mut source: Box<dyn Source>,
    transforms: Vec<PlannedTransform>,
    sinks: Vec<PlannedSink>,
    options: RunOptions,
    metrics: Arc<RunMetrics>,
    cancel: CancellationToken,
) -> Result<RunSummary, RunError> {
    let capacity = options.channel_capacity.max(1);

    let mut out_txs: HashMap<String, Vec<Sender<RecordBatch>>> = HashMap::new();
    let mut in_rxs: HashMap<String, Receiver<RecordBatch>> = HashMap::new();

    for (id, from, _) in &transforms {
        let (tx, rx) = channel(capacity);
        out_txs.entry(from.clone()).or_default().push(tx);
        in_rxs.insert(id.clone(), rx);
    }
    for (id, from, _) in &sinks {
        let (tx, rx) = channel(capacity);
        out_txs.entry(from.clone()).or_default().push(tx);
        in_rxs.insert(id.clone(), rx);
    }

    let source_txs = out_txs.remove("source").unwrap_or_default();
    let mut tasks: tokio::task::JoinSet<Result<(), RunError>> = tokio::task::JoinSet::new();

    {
        let cancel = cancel.clone();
        let metrics = Arc::clone(&metrics);
        tasks.spawn(async move {
            let result = source_loop(source.as_mut(), &source_txs, &metrics, &cancel).await;
            if result.is_err() {
                cancel.cancel();
            }
            result
        });
    }

    for (stage_id, _from, mut transform) in transforms {
        let txs = out_txs.remove(&stage_id).unwrap_or_default();
        let Some(mut input) = in_rxs.remove(&stage_id) else {
            return Err(RunError::Join(format!(
                "missing input channel for transform `{stage_id}`"
            )));
        };
        let cancel = cancel.clone();
        tasks.spawn(async move {
            let result =
                transform_loop(&stage_id, transform.as_mut(), &mut input, &txs, &cancel).await;
            if result.is_err() {
                cancel.cancel();
            }
            result
        });
    }

    let mut sink_tasks: tokio::task::JoinSet<(String, Box<dyn Sink>, Result<(), RunError>)> =
        tokio::task::JoinSet::new();
    for (stage_id, _from, mut sink) in sinks {
        let Some(mut input) = in_rxs.remove(&stage_id) else {
            return Err(RunError::Join(format!(
                "missing input channel for sink `{stage_id}`"
            )));
        };
        let cancel = cancel.clone();
        let metrics = Arc::clone(&metrics);
        sink_tasks.spawn(async move {
            let result =
                sink_write_loop(&stage_id, sink.as_mut(), &mut input, &metrics, &cancel).await;
            if result.is_err() {
                cancel.cancel();
            }
            (stage_id, sink, result)
        });
    }

    let mut outcome: Result<(), RunError> = Ok(());
    while let Some(joined) = tasks.join_next().await {
        let result = match joined {
            Ok(result) => result,
            Err(join_error) => Err(RunError::Join(join_error.to_string())),
        };
        merge_outcome(&mut outcome, result);
    }

    let mut ready_sinks: Vec<(String, Box<dyn Sink>)> = Vec::new();
    while let Some(joined) = sink_tasks.join_next().await {
        match joined {
            Ok((id, sink, result)) => {
                let wrote_ok = result.is_ok();
                merge_outcome(&mut outcome, result);
                if wrote_ok {
                    ready_sinks.push((id, sink));
                }
            }
            Err(join_error) => {
                merge_outcome(&mut outcome, Err(RunError::Join(join_error.to_string())));
            }
        }
    }

    if let Ok(()) = &outcome {
        for (id, mut sink) in ready_sinks {
            if cancel.is_cancelled() {
                outcome = Err(RunError::Cancelled);
                break;
            }
            if let Err(source) = sink.commit().await {
                outcome = Err(RunError::Stage {
                    stage: id,
                    source,
                });
                cancel.cancel();
                break;
            }
        }
    }

    outcome.map(|()| RunSummary {
        metrics: metrics.snapshot(),
    })
}

#[cfg(test)]
mod tests;
