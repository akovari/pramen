//! The per-batch SQL transform operator.

use datafusion::prelude::{SessionConfig, SessionContext};
use pramen_core::runtime::{StageError, Transform};

/// Applies a SQL statement to each incoming batch.
///
/// The incoming batch is visible as the table `input`. v1 semantics are
/// deliberately per-batch: row-wise filters, projections, and derivations
/// behave identically to whole-stream execution, while cross-batch
/// aggregations are out of scope until the engine grows blocking operators
/// (tracked with fan-out DAGs in E1.3).
pub struct SqlTransform {
    ctx: SessionContext,
    query: String,
}

impl SqlTransform {
    /// Create the operator for `query`.
    #[must_use]
    pub fn new(query: &str) -> Self {
        let ctx = SessionContext::new_with_config(SessionConfig::new());
        Self {
            ctx,
            query: query.to_owned(),
        }
    }
}

#[async_trait::async_trait]
impl Transform for SqlTransform {
    async fn apply(
        &mut self,
        batch: arrow::record_batch::RecordBatch,
    ) -> Result<Vec<arrow::record_batch::RecordBatch>, StageError> {
        // Replace the previous batch under the fixed `input` name.
        self.ctx
            .deregister_table("input")
            .map_err(StageError::external)?;
        self.ctx
            .register_batch("input", batch)
            .map_err(StageError::external)?;
        let frame = self
            .ctx
            .sql(&self.query)
            .await
            .map_err(|error| StageError::InvalidData(format!("SQL error: {error}")))?;
        frame.collect().await.map_err(StageError::external)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    fn batch(values: &[i64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("v", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let names: Vec<String> = values.iter().map(|v| format!("row-{v}")).collect();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(values.to_vec())),
                Arc::new(StringArray::from(names)),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn filters_and_derives_per_batch() {
        let mut transform = SqlTransform::new(
            "SELECT v, v * 2 AS doubled, upper(name) AS name FROM input WHERE v >= 2",
        );
        let out = transform.apply(batch(&[1, 2, 3])).await.unwrap();
        let rows: usize = out.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 2);
        assert_eq!(out[0].schema().field(1).name(), "doubled");

        // A second batch replaces the first under the `input` name.
        let out = transform.apply(batch(&[10])).await.unwrap();
        let rows: usize = out.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 1);
    }

    #[tokio::test]
    async fn bad_sql_is_reported_as_invalid_data() {
        let mut transform = SqlTransform::new("SELECT nonexistent FROM input");
        let error = transform.apply(batch(&[1])).await.unwrap_err();
        assert!(matches!(error, StageError::InvalidData(_)), "{error}");
    }
}
