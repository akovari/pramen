//! Arrow IPC helpers for the WIT `list<u8>` payload.

use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;

use crate::error::WasmError;

/// Encode one batch as an Arrow IPC stream (single-batch payload).
///
/// # Errors
///
/// Returns [`WasmError::Ipc`] when the batch cannot be serialized.
pub fn encode_batch(batch: &RecordBatch) -> Result<Vec<u8>, WasmError> {
    let mut writer = StreamWriter::try_new(Vec::new(), &batch.schema())
        .map_err(|e| WasmError::ipc(e.to_string()))?;
    writer
        .write(batch)
        .map_err(|e| WasmError::ipc(e.to_string()))?;
    writer
        .into_inner()
        .map_err(|e| WasmError::ipc(e.to_string()))
}

/// Decode every batch from an Arrow IPC stream payload.
///
/// # Errors
///
/// Returns [`WasmError::Ipc`] when the bytes are not a valid IPC stream.
pub fn decode_stream(bytes: &[u8]) -> Result<Vec<RecordBatch>, WasmError> {
    let reader = StreamReader::try_new(bytes, None).map_err(|e| WasmError::ipc(e.to_string()))?;
    reader
        .map(|item| item.map_err(|e| WasmError::ipc(e.to_string())))
        .collect()
}
