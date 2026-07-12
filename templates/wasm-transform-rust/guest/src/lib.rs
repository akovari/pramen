//! Pramen WASM transform guest template.
//!
//! Customize [`transform_batch`] — decode Arrow IPC, transform columns,
//! encode Arrow IPC back. The host calls the exported `run` function once
//! per micro-batch.

use arrow::array::{ArrayRef, Float64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use std::sync::Arc;

wit_bindgen::generate!({
    path: "../wit",
    world: "transform",
});

struct Component;

impl Guest for Component {
    fn run(batch: Vec<u8>) -> Result<Vec<u8>, String> {
        let reader = StreamReader::try_new(batch.as_slice(), None)
            .map_err(|e| format!("ipc decode: {e}"))?;

        let mut out: Option<StreamWriter<Vec<u8>>> = None;
        for item in reader {
            let input = item.map_err(|e| format!("ipc decode: {e}"))?;
            let derived = transform_batch(&input)?;
            let writer = match &mut out {
                Some(w) => w,
                None => out.insert(
                    StreamWriter::try_new(Vec::new(), &derived.schema())
                        .map_err(|e| format!("ipc encode: {e}"))?,
                ),
            };
            writer
                .write(&derived)
                .map_err(|e| format!("ipc encode: {e}"))?;
        }
        let writer = out.ok_or_else(|| "empty ipc stream".to_owned())?;
        writer.into_inner().map_err(|e| format!("ipc encode: {e}"))
    }
}

/// Columnar transform for one batch — edit this function for your pipeline.
fn transform_batch(batch: &RecordBatch) -> Result<RecordBatch, String> {
    let index = batch
        .schema()
        .index_of("amount")
        .map_err(|e| e.to_string())?;
    let amounts = batch
        .column(index)
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| "column `amount` is not float64".to_owned())?;
    let gross: Float64Array = amounts.iter().map(|v| v.map(|a| a * 1.21)).collect();

    let mut fields: Vec<Field> = batch
        .schema()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    fields.push(Field::new("amount_gross", DataType::Float64, true));
    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
    columns.push(Arc::new(gross));
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).map_err(|e| e.to_string())
}

export!(Component);
