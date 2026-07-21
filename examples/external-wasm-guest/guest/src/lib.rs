//! ACME gross-up transform — authored as if outside the Pramen repository.
//!
//! This guest only consumes the published WIT world (`pramen:transform@0.1.0`)
//! and public cookbook docs. It is the X2.1 extensibility proof: a third-party
//! component that passes `pramen transform test`.

use arrow::array::{ArrayRef, Float64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use std::sync::Arc;

wit_bindgen::generate!({
    path: "../wit",
    world: "transform",
});

/// VAT multiplier applied to the `amount` column (21%).
const GROSS_FACTOR: f64 = 1.21;

struct AcmeGross;

impl Guest for AcmeGross {
    fn run(batch: Vec<u8>) -> Result<Vec<u8>, String> {
        let reader = StreamReader::try_new(batch.as_slice(), None)
            .map_err(|e| format!("ipc decode: {e}"))?;

        let mut out: Option<StreamWriter<Vec<u8>>> = None;
        for item in reader {
            let input = item.map_err(|e| format!("ipc decode: {e}"))?;
            let derived = append_amount_gross(&input)?;
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

/// Append `amount_gross = amount * GROSS_FACTOR` for the conformance fixture.
fn append_amount_gross(batch: &RecordBatch) -> Result<RecordBatch, String> {
    let index = batch
        .schema()
        .index_of("amount")
        .map_err(|e| e.to_string())?;
    let amounts = batch
        .column(index)
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| "column `amount` is not float64".to_owned())?;
    let gross: Float64Array = amounts
        .iter()
        .map(|v| v.map(|a| a * GROSS_FACTOR))
        .collect();

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

export!(AcmeGross);
