//! The `type: wasm` transform operator.

use crate::cache::ArtifactCache;
use crate::host::PreparedComponent;
use crate::ipc;
use crate::limits::InvocationLimits;
use arrow::record_batch::RecordBatch;
use pramen_core::runtime::{StageError, Transform};
use std::sync::Arc;

/// Applies a WebAssembly component transform to each incoming batch.
///
/// The guest implements the S1.4 WIT ABI: Arrow IPC bytes in, Arrow IPC
/// bytes out. Invocation runs on a blocking thread because Wasmtime is
/// synchronous.
pub struct WasmTransform {
    prepared: Arc<PreparedComponent>,
    limits: InvocationLimits,
}

impl WasmTransform {
    /// Build the operator from a prepared artifact and invocation limits.
    #[must_use]
    pub fn new(prepared: Arc<PreparedComponent>, limits: InvocationLimits) -> Self {
        Self { prepared, limits }
    }

    /// Load a component through `cache` and build the operator.
    ///
    /// # Errors
    ///
    /// Returns [`StageError::External`] when the component cannot be loaded.
    pub fn from_cache(
        cache: &ArtifactCache,
        path: &std::path::Path,
        limits: InvocationLimits,
    ) -> Result<Self, StageError> {
        let prepared = cache.load_path(path).map_err(StageError::external)?;
        Ok(Self::new(prepared, limits))
    }
}

#[async_trait::async_trait]
impl Transform for WasmTransform {
    async fn apply(&mut self, batch: RecordBatch) -> Result<Vec<RecordBatch>, StageError> {
        let prepared = Arc::clone(&self.prepared);
        let limits = self.limits.clone();
        let input_ipc = ipc::encode_batch(&batch).map_err(StageError::external)?;
        let output_ipc = tokio::task::spawn_blocking(move || prepared.invoke(&input_ipc, &limits))
            .await
            .map_err(StageError::external)?
            .map_err(StageError::external)?;
        ipc::decode_stream(&output_ipc).map_err(|error| match error {
            crate::error::WasmError::Guest(message) => StageError::InvalidData(message),
            other => StageError::external(other),
        })
    }
}
