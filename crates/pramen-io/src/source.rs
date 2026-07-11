//! The file sources: Parquet and NDJSON.

use datafusion::execution::SendableRecordBatchStream;
use datafusion::execution::memory_pool::FairSpillPool;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::{NdJsonReadOptions, ParquetReadOptions, SessionConfig, SessionContext};
use futures::StreamExt;
use pramen_core::checkpoint::WorkUnit;
use pramen_core::runtime::{Source, StageError};
use std::sync::Arc;

/// Enumerate the source files under a local URL as checkpointable work
/// units (architecture §11: one immutable file = one unit).
///
/// A file URL yields one unit; a directory yields one unit per contained
/// file whose extension is in `extensions` (non-recursive, matching
/// DataFusion's single-level listing behavior).
///
/// # Errors
///
/// Returns a [`StageError`] when the URL is remote (checkpointed remote
/// enumeration lands with the rest of P1.1) or cannot be read.
pub fn list_work_units(url: &str, extensions: &[&str]) -> Result<Vec<WorkUnit>, StageError> {
    let path = local_path(url)?;
    let root = std::path::Path::new(&path);
    let metadata = std::fs::metadata(root)
        .map_err(|e| StageError::InvalidData(format!("source `{path}`: {e}")))?;

    let mut files: Vec<std::path::PathBuf> = if metadata.is_file() {
        vec![root.to_path_buf()]
    } else {
        let mut found = Vec::new();
        let entries = std::fs::read_dir(root)
            .map_err(|e| StageError::InvalidData(format!("source `{path}`: {e}")))?;
        for entry in entries {
            let entry = entry.map_err(StageError::external)?;
            let entry_path = entry.path();
            let matches = entry_path
                .extension()
                .and_then(|e| e.to_str())
                .is_some_and(|e| extensions.iter().any(|x| x.eq_ignore_ascii_case(e)));
            if entry_path.is_file() && matches {
                found.push(entry_path);
            }
        }
        found
    };
    files.sort();

    let mut units = Vec::with_capacity(files.len());
    for file in files {
        let meta = std::fs::metadata(&file).map_err(StageError::external)?;
        let modified_millis = meta
            .modified()
            .ok()
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
            .unwrap_or_default();
        units.push(WorkUnit {
            url: file.to_string_lossy().into_owned(),
            size: meta.len(),
            modified_millis,
        });
    }
    Ok(units)
}

/// Streams Arrow batches from Parquet files under a URL.
///
/// Accepts local paths, `file://` URLs, and `s3://` URLs (including
/// S3-compatible services like MinIO, configured via the standard `AWS_*`
/// environment). Azure Blob and GCS are tracked in X1.5.
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
        Self::open_files(vec![url.to_owned()], memory_limit_bytes, batch_size_rows).await
    }

    /// Open an explicit list of Parquet files or directories — the
    /// checkpointed entry point, where the caller has already filtered out
    /// completed work units.
    ///
    /// # Errors
    ///
    /// Returns a [`StageError`] when the files cannot be opened or planned.
    pub async fn open_files(
        paths: Vec<String>,
        memory_limit_bytes: usize,
        batch_size_rows: usize,
    ) -> Result<Self, StageError> {
        let ctx = session(memory_limit_bytes, batch_size_rows)?;
        let paths = register_remote_stores(&ctx, paths)?;
        let frame = ctx
            .read_parquet(paths, ParquetReadOptions::default())
            .await
            .map_err(StageError::external)?;
        let stream = frame.execute_stream().await.map_err(StageError::external)?;
        Ok(Self { stream })
    }
}

/// Register object stores for any remote URLs among `paths`, returning the
/// paths with local ones normalized (`file://` stripped).
///
/// S3 (and S3-compatible services like MinIO) is configured entirely from
/// the standard `AWS_*` environment: `AWS_ACCESS_KEY_ID`,
/// `AWS_SECRET_ACCESS_KEY`, `AWS_REGION` / `AWS_DEFAULT_REGION`,
/// `AWS_ENDPOINT` (for S3-compatible endpoints), and `AWS_ALLOW_HTTP=true`
/// for plaintext local endpoints. Credentials never appear in the pipeline
/// document.
fn register_remote_stores(
    ctx: &SessionContext,
    paths: Vec<String>,
) -> Result<Vec<String>, StageError> {
    let mut normalized = Vec::with_capacity(paths.len());
    for path in paths {
        if let Some(rest) = path.strip_prefix("s3://") {
            let bucket = rest.split('/').next().unwrap_or_default();
            if bucket.is_empty() {
                return Err(StageError::InvalidData(format!(
                    "source URL `{path}` has no bucket"
                )));
            }
            let base = url::Url::parse(&format!("s3://{bucket}"))
                .map_err(|e| StageError::InvalidData(format!("source URL `{path}`: {e}")))?;
            let store = object_store::aws::AmazonS3Builder::from_env()
                .with_bucket_name(bucket)
                .build()
                .map_err(|e| {
                    StageError::InvalidData(format!(
                        "S3 configuration for `{path}`: {e} (set AWS_ACCESS_KEY_ID, \
                         AWS_SECRET_ACCESS_KEY, AWS_REGION, and optionally AWS_ENDPOINT / \
                         AWS_ALLOW_HTTP for S3-compatible services)"
                    ))
                })?;
            ctx.register_object_store(&base, Arc::new(store));
            normalized.push(path);
        } else if let Some(local) = path.strip_prefix("file://") {
            normalized.push(local.to_owned());
        } else if path.contains("://") {
            return Err(StageError::InvalidData(format!(
                "unsupported source URL scheme in `{path}`; v1 supports local paths, file://, \
                 and s3:// (Azure Blob and GCS are tracked in X1.5)"
            )));
        } else {
            normalized.push(path);
        }
    }
    Ok(normalized)
}

/// A DataFusion session with the standard bounded memory pool and batch
/// size shared by every file source.
fn session(
    memory_limit_bytes: usize,
    batch_size_rows: usize,
) -> Result<SessionContext, StageError> {
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::new(FairSpillPool::new(
            memory_limit_bytes.max(16 * 1024 * 1024),
        )))
        .build_arc()
        .map_err(StageError::external)?;
    let config = SessionConfig::new().with_batch_size(batch_size_rows.max(1));
    Ok(SessionContext::new_with_config_rt(config, runtime))
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
        Self::open_files(vec![url.to_owned()], memory_limit_bytes, batch_size_rows).await
    }

    /// Open an explicit list of NDJSON files or directories — the
    /// checkpointed entry point, where the caller has already filtered out
    /// completed work units.
    ///
    /// # Errors
    ///
    /// Returns a [`StageError`] when the files cannot be opened or planned.
    pub async fn open_files(
        paths: Vec<String>,
        memory_limit_bytes: usize,
        batch_size_rows: usize,
    ) -> Result<Self, StageError> {
        let ctx = session(memory_limit_bytes, batch_size_rows)?;
        let paths = register_remote_stores(&ctx, paths)?;
        let extension = paths
            .first()
            .and_then(|p| std::path::Path::new(p).extension())
            .and_then(|e| e.to_str())
            .map_or_else(|| ".ndjson".to_owned(), |e| format!(".{e}"));
        let mut options = NdJsonReadOptions::default().file_extension(&extension);
        options.schema_infer_max_records = Self::SCHEMA_INFER_RECORDS;
        let frame = ctx
            .read_json(paths, options)
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

/// Resolve a URL to a local filesystem path, for operations that are
/// local-only today (checkpointed work-unit enumeration).
fn local_path(url: &str) -> Result<String, StageError> {
    if let Some(path) = url.strip_prefix("file://") {
        return Ok(path.to_owned());
    }
    if url.contains("://") {
        return Err(StageError::InvalidData(format!(
            "`{url}`: checkpointed enumeration supports local paths and file:// only today; \
             remote work-unit enumeration is tracked in P1.1"
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
    async fn work_units_enumerate_matching_files_with_identity() {
        let dir = std::env::temp_dir().join(format!("pramen-units-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        write_parquet(&dir, "b.parquet", 0, 10);
        write_parquet(&dir, "a.parquet", 0, 10);
        std::fs::write(dir.join("notes.txt"), "not data").unwrap();

        let units = list_work_units(dir.to_str().unwrap(), &["parquet"]).unwrap();
        assert_eq!(units.len(), 2, "non-matching extensions excluded");
        assert!(units[0].url.ends_with("a.parquet"), "deterministic order");
        assert!(units[1].url.ends_with("b.parquet"));
        assert!(units[0].size > 0);
        assert!(units[0].modified_millis > 0);

        // Opening only a subset streams only that subset.
        let mut source = ParquetSource::open_files(vec![units[0].url.clone()], 64 << 20, 128)
            .await
            .unwrap();
        let mut rows = 0;
        while let Some(batch) = source.next_batch().await.unwrap() {
            rows += batch.num_rows();
        }
        assert_eq!(rows, 10);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn unsupported_schemes_are_rejected_with_guidance() {
        let Err(error) = ParquetSource::open("az://container/prefix/", 64 << 20, 128).await else {
            panic!("expected an error for az:// in v1");
        };
        assert!(error.to_string().contains("X1.5"), "{error}");
    }

    #[tokio::test]
    async fn checkpoint_enumeration_is_local_only_for_now() {
        let Err(error) = list_work_units("s3://bucket/prefix/", &["parquet"]) else {
            panic!("expected an error for remote enumeration");
        };
        assert!(error.to_string().contains("P1.1"), "{error}");
    }

    /// L2 test (ADR 0005): the real S3 code path against MinIO. Guarded by
    /// `PRAMEN_TEST_S3_URL` (e.g. `s3://pramen-test/e2e/`) with standard
    /// `AWS_*` variables pointing at the local endpoint; skipped when unset.
    #[tokio::test]
    async fn reads_parquet_from_s3_compatible_store() {
        let Ok(url) = std::env::var("PRAMEN_TEST_S3_URL") else {
            eprintln!("skipping: PRAMEN_TEST_S3_URL not set");
            return;
        };
        let mut source = ParquetSource::open(&url, 64 << 20, 128).await.unwrap();
        let mut rows = 0;
        while let Some(batch) = source.next_batch().await.unwrap() {
            rows += batch.num_rows();
        }
        assert!(rows > 0, "expected rows from {url}");
    }
}
