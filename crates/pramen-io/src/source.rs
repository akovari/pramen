//! The file sources: Parquet and NDJSON.

use datafusion::execution::SendableRecordBatchStream;
use datafusion::execution::memory_pool::FairSpillPool;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::{NdJsonReadOptions, ParquetReadOptions, SessionConfig, SessionContext};
use futures::StreamExt;
use pramen_core::runtime::{Source, StageError};
use std::sync::Arc;

/// Streams Arrow batches from Parquet files under a URL.
///
/// Accepts local paths and `file://` URLs today; remote object stores
/// (`s3://` and friends) register onto the same `SessionContext` when the
/// object-store configuration work (P1.1 remainder) lands.
///
/// Execution is DataFusion's streaming scan under a [`FairSpillPool`], so
/// memory stays bounded regardless of dataset size (validated in spike
/// S1.2).
pub struct ParquetSource {
    stream: SendableRecordBatchStream,
}

impl ParquetSource {
    /// Open the Parquet data under `url`, bounding scan memory to
    /// `memory_limit_bytes` and emitting batches of at most
    /// `batch_size_rows`.
    ///
    /// # Errors
    ///
    /// Returns a [`StageError`] when the URL scheme is unsupported or the
    /// files cannot be opened or planned.
    pub async fn open(
        url: &str,
        memory_limit_bytes: usize,
        batch_size_rows: usize,
    ) -> Result<Self, StageError> {
        let path = local_path(url)?;
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::new(FairSpillPool::new(
                memory_limit_bytes.max(16 * 1024 * 1024),
            )))
            .build_arc()
            .map_err(StageError::external)?;
        let config = SessionConfig::new().with_batch_size(batch_size_rows.max(1));
        let ctx = SessionContext::new_with_config_rt(config, runtime);
        let frame = ctx
            .read_parquet(path, ParquetReadOptions::default())
            .await
            .map_err(StageError::external)?;
        let stream = frame.execute_stream().await.map_err(StageError::external)?;
        Ok(Self { stream })
    }
}

/// Streams Arrow batches from newline-delimited JSON files under a URL.
///
/// The schema is inferred from a bounded prefix of the data (the first
/// 1000 records), so inference cost does not grow with dataset size.
/// Memory bounding and URL handling match [`ParquetSource`].
pub struct NdjsonSource {
    stream: SendableRecordBatchStream,
}

impl NdjsonSource {
    /// Number of leading records used for schema inference.
    const SCHEMA_INFER_RECORDS: usize = 1000;

    /// Open the NDJSON data under `url`, bounding scan memory to
    /// `memory_limit_bytes` and emitting batches of at most
    /// `batch_size_rows`.
    ///
    /// A directory URL scans files with the `.ndjson` extension; a file
    /// URL uses the file's own extension (`.ndjson`, `.jsonl`, `.json`).
    ///
    /// # Errors
    ///
    /// Returns a [`StageError`] when the URL scheme is unsupported or the
    /// files cannot be opened or planned.
    pub async fn open(
        url: &str,
        memory_limit_bytes: usize,
        batch_size_rows: usize,
    ) -> Result<Self, StageError> {
        let path = local_path(url)?;
        let extension = std::path::Path::new(&path)
            .extension()
            .and_then(|e| e.to_str())
            .map_or_else(|| ".ndjson".to_owned(), |e| format!(".{e}"));
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::new(FairSpillPool::new(
                memory_limit_bytes.max(16 * 1024 * 1024),
            )))
            .build_arc()
            .map_err(StageError::external)?;
        let config = SessionConfig::new().with_batch_size(batch_size_rows.max(1));
        let ctx = SessionContext::new_with_config_rt(config, runtime);
        let mut options = NdJsonReadOptions::default().file_extension(&extension);
        options.schema_infer_max_records = Self::SCHEMA_INFER_RECORDS;
        let frame = ctx
            .read_json(path, options)
            .await
            .map_err(StageError::external)?;
        let stream = frame.execute_stream().await.map_err(StageError::external)?;
        Ok(Self { stream })
    }
}

#[async_trait::async_trait]
impl Source for NdjsonSource {
    async fn next_batch(&mut self) -> Result<Option<arrow::record_batch::RecordBatch>, StageError> {
        match self.stream.next().await {
            None => Ok(None),
            Some(Ok(batch)) => Ok(Some(batch)),
            Some(Err(error)) => Err(StageError::external(error)),
        }
    }
}

/// Resolve a source URL to a local filesystem path.
fn local_path(url: &str) -> Result<String, StageError> {
    if let Some(path) = url.strip_prefix("file://") {
        return Ok(path.to_owned());
    }
    if url.contains("://") {
        return Err(StageError::InvalidData(format!(
            "unsupported source URL scheme in `{url}`; v1 supports local \
             paths and file:// (remote object stores are tracked in P1.1)"
        )));
    }
    Ok(url.to_owned())
}

#[async_trait::async_trait]
impl Source for ParquetSource {
    async fn next_batch(&mut self) -> Result<Option<arrow::record_batch::RecordBatch>, StageError> {
        match self.stream.next().await {
            None => Ok(None),
            Some(Ok(batch)) => Ok(Some(batch)),
            Some(Err(error)) => Err(StageError::external(error)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::parquet::arrow::ArrowWriter;

    fn write_parquet(dir: &std::path::Path, name: &str, start: i64, rows: usize) {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let values: Vec<i64> = (start..start + rows as i64).collect();
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(values))]).unwrap();
        let file = std::fs::File::create(dir.join(name)).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    #[tokio::test]
    async fn streams_all_rows_from_multiple_files() {
        let dir = std::env::temp_dir().join(format!("pramen-src-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        write_parquet(&dir, "a.parquet", 0, 1000);
        write_parquet(&dir, "b.parquet", 1000, 1000);

        let mut source = ParquetSource::open(dir.to_str().unwrap(), 64 << 20, 128)
            .await
            .unwrap();
        let mut rows = 0;
        while let Some(batch) = source.next_batch().await.unwrap() {
            assert!(batch.num_rows() <= 128, "batch size bound violated");
            rows += batch.num_rows();
        }
        assert_eq!(rows, 2000);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn ndjson_streams_rows_with_inferred_schema() {
        let dir = std::env::temp_dir().join(format!("pramen-ndjson-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let mut lines = String::new();
        for i in 0..500 {
            lines.push_str(&format!(
                "{{\"id\": {i}, \"description\": \"ticket {i}\", \"amount\": {}.5}}\n",
                i * 2
            ));
        }
        std::fs::write(dir.join("data.ndjson"), lines).unwrap();

        let mut source = NdjsonSource::open(dir.to_str().unwrap(), 64 << 20, 128)
            .await
            .unwrap();
        let mut rows = 0;
        let mut columns = 0;
        while let Some(batch) = source.next_batch().await.unwrap() {
            assert!(batch.num_rows() <= 128, "batch size bound violated");
            rows += batch.num_rows();
            columns = batch.num_columns();
        }
        assert_eq!(rows, 500);
        assert_eq!(columns, 3, "id, description, amount inferred");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn ndjson_single_file_uses_its_own_extension() {
        let dir = std::env::temp_dir().join(format!("pramen-jsonl-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("data.jsonl");
        std::fs::write(&file, "{\"v\": 1}\n{\"v\": 2}\n").unwrap();

        let mut source = NdjsonSource::open(file.to_str().unwrap(), 64 << 20, 128)
            .await
            .unwrap();
        let mut rows = 0;
        while let Some(batch) = source.next_batch().await.unwrap() {
            rows += batch.num_rows();
        }
        assert_eq!(rows, 2);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn remote_schemes_are_rejected_with_guidance() {
        let Err(error) = ParquetSource::open("s3://bucket/prefix/", 64 << 20, 128).await else {
            panic!("expected an error for s3:// in v1");
        };
        assert!(error.to_string().contains("unsupported source URL scheme"));
    }
}
