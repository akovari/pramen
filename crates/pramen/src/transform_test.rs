//! The `pramen transform test` command (X1.3).

use arrow::array::{ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use pramen_core::runtime::Transform;
use pramen_wasm::{ArtifactCache, InvocationLimits, S1_4_FIXTURE, WasmTransform};
use std::path::PathBuf;
use std::sync::Arc;

/// Arguments for `pramen transform test`.
pub struct TransformTestArgs {
    /// Path to the `.wasm` component artifact.
    pub component: PathBuf,
    /// Rows in the synthetic fixture batch.
    pub rows: usize,
}

/// Run the conformance fixture through production limits and verify output.
///
/// # Errors
///
/// Returns a human-readable message when loading, invoking, or validating fails.
pub fn execute(args: &TransformTestArgs) -> Result<(), String> {
    let batch = fixture_batch(args.rows)?;
    let cache = ArtifactCache::new();
    let limits = InvocationLimits::default();
    let mut transform =
        WasmTransform::from_cache(&cache, &args.component, limits).map_err(|e| e.to_string())?;
    let runtime = tokio::runtime::Runtime::new()
        .map_err(|error| format!("failed to start async runtime: {error}"))?;
    let output = runtime
        .block_on(transform.apply(batch))
        .map_err(|error| error.to_string())?;
    let rows: usize = output.iter().map(|batch| batch.num_rows()).sum();
    if rows != args.rows {
        return Err(format!(
            "row count mismatch: expected {}, got {rows}",
            args.rows
        ));
    }
    let schema = output[0].schema();
    if schema.index_of("amount_gross").is_err() {
        return Err(format!(
            "output schema missing `amount_gross`; fields: {}",
            schema
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    println!(
        "OK: component `{}` transformed {rows} row(s); output has `amount_gross`",
        args.component.display()
    );
    Ok(())
}

/// Default component for `transform test` when `--component` is omitted.
#[must_use]
pub fn default_component() -> PathBuf {
    PathBuf::from(S1_4_FIXTURE)
}

fn fixture_batch(rows: usize) -> Result<RecordBatch, String> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("amount", DataType::Float64, false),
        Field::new("note", DataType::Utf8, true),
    ]));
    let ids: ArrayRef = Arc::new(Int64Array::from_iter_values(0..rows as i64));
    let amounts: ArrayRef = Arc::new(Float64Array::from_iter_values(
        (0..rows).map(|i| i as f64 * 1.5),
    ));
    let notes: ArrayRef = Arc::new(StringArray::from_iter((0..rows).map(|i| {
        if i % 5 == 0 {
            None
        } else {
            Some(format!("note for row {i}"))
        }
    })));
    RecordBatch::try_new(schema, vec![ids, amounts, notes])
        .map_err(|error| format!("fixture batch: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn s1_4_fixture_passes_conformance() {
        execute(&TransformTestArgs {
            component: PathBuf::from(S1_4_FIXTURE),
            rows: 256,
        })
        .expect("conformance");
    }
}
