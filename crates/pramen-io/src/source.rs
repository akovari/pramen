//! The file sources: Parquet and NDJSON.

use datafusion::execution::SendableRecordBatchStream;
use datafusion::execution::memory_pool::FairSpillPool;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::{NdJsonReadOptions, ParquetReadOptions, SessionConfig, SessionContext};
use futures::StreamExt;
use object_store::ObjectStore;
use pramen_core::checkpoint::WorkUnit;
use pramen_core::runtime::{Source, StageError};
use std::sync::Arc;

/// Enumerate the source files under a URL as checkpointable work units
/// (architecture §11: one immutable file = one unit).
///
/// A file URL yields one unit; a directory (or cloud prefix) yields one
/// unit per contained file whose extension is in `extensions`
/// (non-recursive, matching DataFusion's single-level listing behavior).
/// Cloud identity comes from the object listing itself: key, size, and
/// last-modified — one `LIST` per run, no per-object requests.
///
/// # Errors
///
/// Returns a [`StageError`] when the URL scheme is unsupported or the
/// location cannot be read.
pub async fn list_work_units(url: &str, extensions: &[&str]) -> Result<Vec<WorkUnit>, StageError> {
    if let Some(cloud) = CloudUrl::parse(url)? {
        return list_cloud_work_units(&cloud, extensions).await;
    }
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

/// Parsed cloud object-store URL (S3, GCS, Azure Blob).
#[derive(Debug, Clone)]
struct CloudUrl {
    /// Original URL string (for builders and errors).
    raw: String,
    /// Provider family.
    kind: CloudKind,
    /// Object key / prefix after the bucket or container.
    key: String,
    /// Base URL registered with DataFusion (`scheme://authority`).
    base: url::Url,
    /// Prefix used when reconstructing listed object URLs.
    list_prefix: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CloudKind {
    S3,
    Gcs,
    Azure,
}

impl CloudUrl {
    /// Parse a supported cloud URL, or `None` for local/`file://` paths.
    fn parse(url: &str) -> Result<Option<Self>, StageError> {
        if !url.contains("://") {
            return Ok(None);
        }
        let parsed = url::Url::parse(url)
            .map_err(|e| StageError::InvalidData(format!("source URL `{url}`: {e}")))?;
        let scheme = parsed.scheme();
        let kind = match scheme {
            "s3" => CloudKind::S3,
            "gs" => CloudKind::Gcs,
            "az" | "azure" | "adl" | "abfs" | "abfss" => CloudKind::Azure,
            "https" if is_azure_https_host(parsed.host_str()) => CloudKind::Azure,
            "file" => return Ok(None),
            other => {
                return Err(StageError::InvalidData(format!(
                    "unsupported source URL scheme `{other}` in `{url}`; v1 supports local \
                     paths, file://, s3://, gs://, az://, azure://, adl://, abfs://, abfss://, \
                     and Azure https://{{account}}.blob|dfs.core.windows.net URLs"
                )));
            }
        };

        let (key, list_prefix, base) = match kind {
            CloudKind::S3 | CloudKind::Gcs => {
                let bucket = parsed.host_str().filter(|h| !h.is_empty()).ok_or_else(|| {
                    StageError::InvalidData(format!("source URL `{url}` has no bucket"))
                })?;
                let key = parsed.path().trim_start_matches('/').to_owned();
                let list_prefix = format!("{scheme}://{bucket}");
                let base = url::Url::parse(&list_prefix)
                    .map_err(|e| StageError::InvalidData(format!("source URL `{url}`: {e}")))?;
                (key, list_prefix, base)
            }
            CloudKind::Azure => {
                let (_container, key, list_prefix, base) = parse_azure_parts(url, &parsed)?;
                (key, list_prefix, base)
            }
        };

        Ok(Some(Self {
            raw: url.to_owned(),
            kind,
            key,
            base,
            list_prefix,
        }))
    }
}

fn is_azure_https_host(host: Option<&str>) -> bool {
    let Some(host) = host else {
        return false;
    };
    host.ends_with(".blob.core.windows.net")
        || host.ends_with(".dfs.core.windows.net")
        || host.ends_with(".blob.fabric.microsoft.com")
        || host.ends_with(".dfs.fabric.microsoft.com")
}

/// Extract container, key, list prefix, and DataFusion base URL for Azure.
fn parse_azure_parts(
    url: &str,
    parsed: &url::Url,
) -> Result<(String, String, String, url::Url), StageError> {
    let scheme = parsed.scheme();
    match scheme {
        "az" | "azure" | "adl" | "abfs" | "abfss" => {
            // Forms:
            //   az://container/path
            //   abfss://container@account.dfs.core.windows.net/path
            let host = parsed.host_str().unwrap_or_default();
            let user = parsed.username();
            let (container, list_prefix) = if !user.is_empty() {
                // filesystem@account.dfs...
                let container = user.to_owned();
                let list_prefix = format!("{scheme}://{user}@{host}");
                (container, list_prefix)
            } else {
                let container = host.to_owned();
                if container.is_empty() {
                    return Err(StageError::InvalidData(format!(
                        "source URL `{url}` has no container"
                    )));
                }
                let list_prefix = format!("{scheme}://{container}");
                (container, list_prefix)
            };
            let key = parsed.path().trim_start_matches('/').to_owned();
            let base = url::Url::parse(&list_prefix)
                .map_err(|e| StageError::InvalidData(format!("source URL `{url}`: {e}")))?;
            Ok((container, key, list_prefix, base))
        }
        "https" => {
            // https://account.blob.core.windows.net/container/path
            let mut segments = parsed
                .path_segments()
                .map(|s| s.filter(|p| !p.is_empty()).map(str::to_owned))
                .ok_or_else(|| {
                    StageError::InvalidData(format!("source URL `{url}` has no container"))
                })?;
            let container = segments.next().ok_or_else(|| {
                StageError::InvalidData(format!("source URL `{url}` has no container"))
            })?;
            let key = segments.collect::<Vec<_>>().join("/");
            let host = parsed.host_str().unwrap_or_default();
            let list_prefix = format!("https://{host}/{container}");
            let base = url::Url::parse(&list_prefix)
                .map_err(|e| StageError::InvalidData(format!("source URL `{url}`: {e}")))?;
            Ok((container, key, list_prefix, base))
        }
        other => Err(StageError::InvalidData(format!(
            "unsupported Azure URL scheme `{other}` in `{url}`"
        ))),
    }
}

fn build_object_store(cloud: &CloudUrl) -> Result<Arc<dyn ObjectStore>, StageError> {
    match cloud.kind {
        CloudKind::S3 => {
            let bucket = cloud.base.host_str().ok_or_else(|| {
                StageError::InvalidData(format!("source URL `{}` has no bucket", cloud.raw))
            })?;
            let store = object_store::aws::AmazonS3Builder::from_env()
                .with_bucket_name(bucket)
                .build()
                .map_err(|e| {
                    StageError::InvalidData(format!(
                        "S3 configuration for `{}`: {e} (set AWS_ACCESS_KEY_ID, \
                         AWS_SECRET_ACCESS_KEY, AWS_REGION, and optionally AWS_ENDPOINT / \
                         AWS_ALLOW_HTTP for S3-compatible services)",
                        cloud.raw
                    ))
                })?;
            Ok(Arc::new(store))
        }
        CloudKind::Gcs => {
            let store = object_store::gcp::GoogleCloudStorageBuilder::from_env()
                .with_url(&cloud.raw)
                .build()
                .map_err(|e| {
                    StageError::InvalidData(format!(
                        "GCS configuration for `{}`: {e} (set GOOGLE_SERVICE_ACCOUNT, \
                         GOOGLE_SERVICE_ACCOUNT_PATH, or GOOGLE_SERVICE_ACCOUNT_KEY; for \
                         emulators put gcs_base_url / disable_oauth in the service-account \
                         JSON)",
                        cloud.raw
                    ))
                })?;
            Ok(Arc::new(store))
        }
        CloudKind::Azure => {
            let store = object_store::azure::MicrosoftAzureBuilder::from_env()
                .with_url(&cloud.raw)
                .build()
                .map_err(|e| {
                    StageError::InvalidData(format!(
                        "Azure Blob configuration for `{}`: {e} (set \
                         AZURE_STORAGE_ACCOUNT_NAME and AZURE_STORAGE_ACCOUNT_KEY / \
                         AZURE_STORAGE_ACCESS_KEY, or service-principal \
                         AZURE_STORAGE_CLIENT_ID / AZURE_STORAGE_CLIENT_SECRET / \
                         AZURE_STORAGE_TENANT_ID; for Azurite set AZURE_STORAGE_ENDPOINT \
                         and AZURE_ALLOW_HTTP=true)",
                        cloud.raw
                    ))
                })?;
            Ok(Arc::new(store))
        }
    }
}

/// Enumerate cloud work units from a single object listing: key, size, and
/// last-modified — no per-object round trips. A URL whose final segment
/// carries a matching extension names one object; otherwise it is a prefix.
async fn list_cloud_work_units(
    cloud: &CloudUrl,
    extensions: &[&str],
) -> Result<Vec<WorkUnit>, StageError> {
    let store = build_object_store(cloud)?;

    let matches = |location: &object_store::path::Path| {
        location
            .extension()
            .is_some_and(|e| extensions.iter().any(|x| x.eq_ignore_ascii_case(e)))
    };

    let key_path = object_store::path::Path::from(cloud.key.as_str());
    let mut metas = Vec::new();
    if matches(&key_path) {
        let meta = store.head(&key_path).await.map_err(StageError::external)?;
        metas.push(meta);
    } else {
        let prefix = (!cloud.key.is_empty()).then_some(&key_path);
        let listing = store
            .list_with_delimiter(prefix)
            .await
            .map_err(StageError::external)?;
        metas.extend(listing.objects.into_iter().filter(|m| matches(&m.location)));
    }
    metas.sort_by(|a, b| a.location.cmp(&b.location));
    Ok(metas
        .into_iter()
        .map(|meta| WorkUnit {
            url: format!("{}/{}", cloud.list_prefix, meta.location),
            size: meta.size,
            modified_millis: meta.last_modified.timestamp_millis(),
        })
        .collect())
}

/// Streams Arrow batches from Parquet files under a URL.
///
/// Accepts local paths, `file://` URLs, `s3://`, `gs://`, and Azure Blob
/// URLs (`az://`, `abfs(s)://`, …), configured via the standard provider
/// environment variables. Credentials never appear in the pipeline document.
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
/// Cloud stores are configured from the standard provider environment:
/// - S3 / MinIO: `AWS_*`
/// - GCS: `GOOGLE_SERVICE_ACCOUNT*` / `GOOGLE_SERVICE_ACCOUNT_KEY`
/// - Azure Blob / Azurite: `AZURE_STORAGE_*` (and `AZURE_ALLOW_HTTP` for
///   plaintext local endpoints)
///
/// Credentials never appear in the pipeline document.
fn register_remote_stores(
    ctx: &SessionContext,
    paths: Vec<String>,
) -> Result<Vec<String>, StageError> {
    let mut normalized = Vec::with_capacity(paths.len());
    for path in paths {
        if let Some(cloud) = CloudUrl::parse(&path)? {
            let store = build_object_store(&cloud)?;
            ctx.register_object_store(&cloud.base, store);
            normalized.push(path);
        } else if let Some(local) = path.strip_prefix("file://") {
            normalized.push(local.to_owned());
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

/// Resolve a URL to a local filesystem path (cloud schemes are handled
/// before this is called).
fn local_path(url: &str) -> Result<String, StageError> {
    if let Some(path) = url.strip_prefix("file://") {
        return Ok(path.to_owned());
    }
    if url.contains("://") {
        // CloudUrl::parse should have claimed supported schemes; anything
        // else is unsupported.
        return Err(StageError::InvalidData(format!(
            "unsupported source URL scheme in `{url}`; v1 supports local paths, file://, \
             s3://, gs://, az://, azure://, adl://, abfs://, and abfss://"
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

        let units = list_work_units(dir.to_str().unwrap(), &["parquet"])
            .await
            .unwrap();
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

    #[test]
    fn parses_azure_and_gcs_url_shapes() {
        let gs = CloudUrl::parse("gs://my-bucket/prefix/").unwrap().unwrap();
        assert_eq!(gs.kind, CloudKind::Gcs);
        assert_eq!(gs.key, "prefix/");
        assert_eq!(gs.list_prefix, "gs://my-bucket");

        let az = CloudUrl::parse("az://container/prefix/").unwrap().unwrap();
        assert_eq!(az.kind, CloudKind::Azure);
        assert_eq!(az.key, "prefix/");
        assert_eq!(az.list_prefix, "az://container");

        let abfs = CloudUrl::parse("abfss://fs@acct.dfs.core.windows.net/data/")
            .unwrap()
            .unwrap();
        assert_eq!(abfs.kind, CloudKind::Azure);
        assert_eq!(abfs.key, "data/");
        assert_eq!(abfs.list_prefix, "abfss://fs@acct.dfs.core.windows.net");

        assert!(CloudUrl::parse("/tmp/local").unwrap().is_none());
        let err = CloudUrl::parse("http://example.com/x").unwrap_err();
        assert!(err.to_string().contains("unsupported"), "{err}");
    }

    #[test]
    fn unsupported_http_scheme_is_rejected() {
        let err = CloudUrl::parse("http://example.com/x").unwrap_err();
        assert!(err.to_string().contains("unsupported"), "{err}");
    }

    /// L2 test (ADR 0005): the real S3 code path against MinIO. Guarded by
    /// `PRAMEN_TEST_S3_URL` (e.g. `s3://pramen-test/e2e/`) with standard
    /// `AWS_*` variables pointing at the local endpoint; skipped when unset.
    #[tokio::test]
    async fn reads_parquet_from_s3_compatible_store() {
        let Some(url) = pramen_testkit::env::s3_url() else {
            return;
        };
        let mut source = ParquetSource::open(&url, 64 << 20, 128).await.unwrap();
        let mut rows = 0;
        while let Some(batch) = source.next_batch().await.unwrap() {
            rows += batch.num_rows();
        }
        assert!(rows > 0, "expected rows from {url}");
    }

    /// L2 test (ADR 0005): checkpointed enumeration against MinIO — the
    /// listing yields identity (key, size, last-modified) and opening one
    /// enumerated unit streams only that unit. Guarded like the test above.
    #[tokio::test]
    async fn enumerates_work_units_from_s3_compatible_store() {
        let Some(url) = pramen_testkit::env::s3_url() else {
            return;
        };
        let units = list_work_units(&url, &["parquet"]).await.unwrap();
        assert!(!units.is_empty(), "expected work units under {url}");
        for unit in &units {
            assert!(unit.url.starts_with("s3://"), "{}", unit.url);
            assert!(unit.size > 0);
            assert!(unit.modified_millis > 0, "listing carries last-modified");
        }
        let mut sorted = units.clone();
        sorted.sort_by(|a, b| a.url.cmp(&b.url));
        assert_eq!(
            units.iter().map(|u| &u.url).collect::<Vec<_>>(),
            sorted.iter().map(|u| &u.url).collect::<Vec<_>>(),
            "deterministic order"
        );

        let mut source = ParquetSource::open_files(vec![units[0].url.clone()], 64 << 20, 128)
            .await
            .unwrap();
        let mut rows = 0;
        while let Some(batch) = source.next_batch().await.unwrap() {
            rows += batch.num_rows();
        }
        assert!(rows > 0, "expected rows from {}", units[0].url);
    }

    /// L2 test (ADR 0005): Azure Blob against Azurite. Guarded by
    /// `PRAMEN_TEST_AZURE_URL` (e.g. `az://pramen-test/e2e/`) with
    /// `AZURE_STORAGE_*` (and typically `AZURE_ALLOW_HTTP=true` +
    /// `AZURE_STORAGE_ENDPOINT` for the emulator); skipped when unset.
    #[tokio::test]
    async fn enumerates_work_units_from_azure_emulator() {
        let Some(url) = pramen_testkit::env::azure_url() else {
            return;
        };
        let units = list_work_units(&url, &["parquet"]).await.unwrap();
        assert!(!units.is_empty(), "expected work units under {url}");
        for unit in &units {
            assert!(
                unit.url.contains("://"),
                "cloud URL expected, got {}",
                unit.url
            );
            assert!(unit.size > 0);
            assert!(unit.modified_millis > 0);
        }
    }

    /// L2 test (ADR 0005): GCS against a local emulator (e.g. fake-gcs).
    /// Guarded by `PRAMEN_TEST_GCS_URL` (e.g. `gs://pramen-test/e2e/`) with
    /// a service-account JSON that points at the emulator; skipped when unset.
    #[tokio::test]
    async fn enumerates_work_units_from_gcs_emulator() {
        let Some(url) = pramen_testkit::env::gcs_url() else {
            return;
        };
        let units = list_work_units(&url, &["parquet"]).await.unwrap();
        assert!(!units.is_empty(), "expected work units under {url}");
        for unit in &units {
            assert!(unit.url.starts_with("gs://"), "{}", unit.url);
            assert!(unit.size > 0);
            assert!(unit.modified_millis > 0);
        }
    }
}
