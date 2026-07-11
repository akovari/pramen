//! The stage traits every source, transform, and sink implements.

use arrow::record_batch::RecordBatch;

/// An error raised by a stage during a run.
///
/// Stages classify their failures so the runner and operators can tell
/// user-fixable problems from environmental ones.
#[derive(Debug, thiserror::Error)]
pub enum StageError {
    /// The stage's input (schema, value, or configuration) is unusable.
    #[error("invalid data: {0}")]
    InvalidData(String),
    /// An external system (object store, database, provider) failed.
    #[error("external system: {0}")]
    External(#[from] Box<dyn std::error::Error + Send + Sync>),
}

impl StageError {
    /// Convenience constructor for external failures.
    pub fn external<E>(error: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        Self::External(Box::new(error))
    }
}

/// A producer of Arrow batches.
#[async_trait::async_trait]
pub trait Source: Send {
    /// Produce the next batch, or `None` when the source is exhausted.
    ///
    /// # Errors
    ///
    /// Returns a [`StageError`] when the underlying data cannot be read or
    /// decoded; the runner fails the run.
    async fn next_batch(&mut self) -> Result<Option<RecordBatch>, StageError>;
}

/// A batch-to-batches operator.
#[async_trait::async_trait]
pub trait Transform: Send {
    /// Transform one input batch into zero or more output batches.
    ///
    /// # Errors
    ///
    /// Returns a [`StageError`] when the batch cannot be processed; the
    /// runner fails the run.
    async fn apply(&mut self, batch: RecordBatch) -> Result<Vec<RecordBatch>, StageError>;

    /// Emit any buffered output after the input stream ends.
    ///
    /// The default implementation emits nothing; stateful operators
    /// (windowed aggregations, provider-batch flushes) override it.
    ///
    /// # Errors
    ///
    /// Returns a [`StageError`] when the buffered state cannot be flushed.
    async fn finish(&mut self) -> Result<Vec<RecordBatch>, StageError> {
        Ok(Vec::new())
    }
}

/// A consumer of Arrow batches.
#[async_trait::async_trait]
pub trait Sink: Send {
    /// Accept one batch.
    ///
    /// # Errors
    ///
    /// Returns a [`StageError`] when the destination rejects the batch; the
    /// runner fails the run.
    async fn write(&mut self, batch: RecordBatch) -> Result<(), StageError>;

    /// Finalize the load after the input stream ends.
    ///
    /// This is where staged data becomes visible (transaction commit,
    /// staging-table merge). A run only succeeds after `commit` returns.
    ///
    /// # Errors
    ///
    /// Returns a [`StageError`] when finalization fails; the run fails even
    /// though every batch was written.
    async fn commit(&mut self) -> Result<(), StageError>;
}
