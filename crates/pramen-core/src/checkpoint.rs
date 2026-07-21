//! Durable work-unit checkpoints (architecture §11).
//!
//! The checkpoint unit is an immutable source object (one file). A durable
//! record ties the unit's identity — URL, size, modification time — to the
//! pipeline that consumed it. Processing order per unit:
//!
//! 1. claim the work unit (durably, before reading);
//! 2. read and transform it;
//! 3. load into the sink;
//! 4. commit at the sink;
//! 5. durably mark the unit complete.
//!
//! A crash between steps 4 and 5 duplicates the unit on the next run; that
//! window is part of the at-least-once delivery contract and is documented
//! wherever the contract is (ADR 0006, the sink's docs). Completed units
//! are never re-processed.
//!
//! [`FileCheckpointStore`] is the local-default backend: an append-only JSONL
//! log with fsync'd writes, replayed on open, tolerant of a torn final line.
//! [`PostgresCheckpointStore`] is the shared fleet backend (X1.8): the same
//! claim/complete contract on a `pramen_checkpoints` table, selected when the
//! checkpoint URL uses a `postgres://` or `postgresql://` scheme.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::io::{BufRead, Write};
use std::path::{Path, PathBuf};

/// Errors from checkpoint storage.
#[derive(Debug, thiserror::Error)]
pub enum CheckpointError {
    /// The checkpoint log could not be read or written.
    #[error("checkpoint store: {0}")]
    Io(#[from] std::io::Error),
    /// The checkpoint log contains an undecodable (non-final) record.
    #[error("checkpoint store corrupt at {path}, line {line}: {reason}")]
    Corrupt {
        /// Log file path.
        path: PathBuf,
        /// One-based line number.
        line: usize,
        /// What failed to decode.
        reason: String,
    },
    /// The shared (Postgres) backend could not be reached or queried.
    #[error("checkpoint store: {0}")]
    Backend(String),
}

impl From<tokio_postgres::Error> for CheckpointError {
    fn from(error: tokio_postgres::Error) -> Self {
        Self::Backend(error.to_string())
    }
}

/// The identity of one immutable source object.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkUnit {
    /// Source object URL or path.
    pub url: String,
    /// Object size in bytes.
    pub size: u64,
    /// Last-modified time, milliseconds since the Unix epoch.
    pub modified_millis: i64,
}

impl WorkUnit {
    /// The stable key of this unit within `pipeline`.
    ///
    /// Size and modification time are part of the identity: a rewritten
    /// file is new work, an untouched file is not.
    #[must_use]
    pub fn key(&self, pipeline: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(pipeline.as_bytes());
        hasher.update([0]);
        hasher.update(self.url.as_bytes());
        hasher.update([0]);
        hasher.update(self.size.to_be_bytes());
        hasher.update(self.modified_millis.to_be_bytes());
        hasher
            .finalize()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect()
    }
}

/// One durable log record.
#[derive(Debug, Serialize, Deserialize)]
#[serde(
    tag = "event",
    rename_all = "snake_case",
    rename_all_fields = "camelCase"
)]
enum LogRecord {
    Claimed {
        key: String,
        unit: WorkUnit,
        run_id: String,
        at_millis: i64,
    },
    Completed {
        key: String,
        run_id: String,
        at_millis: i64,
    },
}

/// A durable store of work-unit claims and completions.
pub trait CheckpointStore: Send {
    /// Whether a unit has been durably completed by any prior run.
    fn is_complete(&mut self, key: &str) -> bool;

    /// Durably record that `run_id` is about to process `unit`.
    ///
    /// Claims are advisory in the single-process v1 runtime (a stale claim
    /// from a crashed run is simply re-claimed); they exist so a later
    /// coordinator can lease work without a protocol change. The Postgres
    /// backend uses row upserts so concurrent writers cannot corrupt
    /// completed rows, but it does not yet fence leases — treat multi-worker
    /// claim races like the file store: at-least-once, not exclusive.
    ///
    /// # Errors
    ///
    /// Returns [`CheckpointError::Io`] or [`CheckpointError::Backend`] when
    /// the record cannot be made durable.
    fn claim(&mut self, unit: &WorkUnit, key: &str, run_id: &str) -> Result<(), CheckpointError>;

    /// Durably record that `key` was loaded and committed at the sink.
    /// Idempotent.
    ///
    /// # Errors
    ///
    /// Returns [`CheckpointError::Io`] or [`CheckpointError::Backend`] when
    /// the record cannot be made durable.
    fn complete(&mut self, key: &str, run_id: &str) -> Result<(), CheckpointError>;
}

/// Whether `url` selects the shared Postgres checkpoint backend.
#[must_use]
pub fn is_postgres_url(url: &str) -> bool {
    match url.split_once("://") {
        Some((scheme, _)) => {
            let scheme = scheme.to_ascii_lowercase();
            scheme == "postgres" || scheme == "postgresql"
        }
        None => false,
    }
}

/// The v1 file-backed checkpoint store: `checkpoints.jsonl` inside a
/// directory, append-only, fsync per record.
pub struct FileCheckpointStore {
    log_path: PathBuf,
    file: std::fs::File,
    completed: HashSet<String>,
    claimed: HashMap<String, String>,
}

impl FileCheckpointStore {
    /// Open (creating if necessary) the store in `dir` and replay its log.
    ///
    /// A torn final line — the signature of a crash mid-append — is
    /// tolerated and truncated away; a torn line anywhere else is
    /// corruption and is reported, never silently skipped.
    ///
    /// # Errors
    ///
    /// Returns [`CheckpointError::Io`] on filesystem failure and
    /// [`CheckpointError::Corrupt`] for a damaged non-final record.
    pub fn open(dir: &Path) -> Result<Self, CheckpointError> {
        std::fs::create_dir_all(dir)?;
        let log_path = dir.join("checkpoints.jsonl");
        let mut completed = HashSet::new();
        let mut claimed = HashMap::new();
        let mut valid_bytes: u64 = 0;

        if log_path.exists() {
            let reader = std::io::BufReader::new(std::fs::File::open(&log_path)?);
            let mut lines = reader.lines().enumerate().peekable();
            while let Some((index, line)) = lines.next() {
                let line = line?;
                let is_last = lines.peek().is_none();
                match serde_json::from_str::<LogRecord>(&line) {
                    Ok(LogRecord::Claimed { key, run_id, .. }) => {
                        claimed.insert(key, run_id);
                        valid_bytes += line.len() as u64 + 1;
                    }
                    Ok(LogRecord::Completed { key, .. }) => {
                        claimed.remove(&key);
                        completed.insert(key);
                        valid_bytes += line.len() as u64 + 1;
                    }
                    Err(error) if is_last => {
                        tracing::warn!(
                            path = %log_path.display(),
                            line = index + 1,
                            %error,
                            "discarding torn final checkpoint record (crash during append)"
                        );
                    }
                    Err(error) => {
                        return Err(CheckpointError::Corrupt {
                            path: log_path,
                            line: index + 1,
                            reason: error.to_string(),
                        });
                    }
                }
            }
        }

        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&log_path)?;
        // Truncate away any torn final line so the next append starts on a
        // record boundary.
        file.set_len(valid_bytes)?;
        let mut file = file;
        use std::io::Seek;
        file.seek(std::io::SeekFrom::End(0))?;

        Ok(Self {
            log_path,
            file,
            completed,
            claimed,
        })
    }

    /// Units currently claimed but not completed (stale after a crash).
    #[must_use]
    pub fn claimed_units(&self) -> Vec<(String, String)> {
        self.claimed
            .iter()
            .map(|(k, r)| (k.clone(), r.clone()))
            .collect()
    }

    /// Number of completed units.
    #[must_use]
    pub fn completed_count(&self) -> usize {
        self.completed.len()
    }

    fn append(&mut self, record: &LogRecord) -> Result<(), CheckpointError> {
        let mut line = serde_json::to_string(record).map_err(|error| CheckpointError::Corrupt {
            path: self.log_path.clone(),
            line: 0,
            reason: error.to_string(),
        })?;
        line.push('\n');
        self.file.write_all(line.as_bytes())?;
        self.file.sync_data()?;
        Ok(())
    }
}

fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or_default()
}

const POSTGRES_CHECKPOINT_SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS pramen_checkpoints (
    key TEXT PRIMARY KEY,
    status TEXT NOT NULL CHECK (status IN ('claimed', 'completed')),
    url TEXT NOT NULL,
    size BIGINT NOT NULL,
    modified_millis BIGINT NOT NULL,
    run_id TEXT NOT NULL,
    claimed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ
);
";

/// Shared Postgres checkpoint store for fleet deployments (X1.8).
///
/// Concurrency: correct for a single writer and safe against lost updates on
/// completed keys when multiple workers race (completed rows are never
/// demoted). Claims are not exclusively leased — two workers may claim the
/// same incomplete key; both may process it (at-least-once). Exclusive
/// leasing is a later coordinator concern.
pub struct PostgresCheckpointStore {
    client: tokio_postgres::Client,
    handle: tokio::runtime::Handle,
    /// Retains a runtime when [`Self::open`] created one outside async.
    _runtime: Option<std::sync::Arc<tokio::runtime::Runtime>>,
}

impl PostgresCheckpointStore {
    /// Connect and bootstrap schema using the current Tokio runtime.
    ///
    /// # Errors
    ///
    /// Returns [`CheckpointError::Backend`] when the DSN is unreachable or
    /// migration fails.
    pub async fn connect(dsn: &str) -> Result<Self, CheckpointError> {
        let (client, connection) =
            tokio_postgres::connect(dsn, tokio_postgres::NoTls).await?;
        tokio::spawn(async move {
            if let Err(error) = connection.await {
                tracing::error!(%error, "postgres checkpoint connection closed");
            }
        });
        client.batch_execute(POSTGRES_CHECKPOINT_SCHEMA).await?;
        Ok(Self {
            client,
            handle: tokio::runtime::Handle::current(),
            _runtime: None,
        })
    }

    /// Connect from a synchronous context (creates a private runtime when
    /// none is running). Prefer [`Self::connect`] inside `async` code.
    ///
    /// # Errors
    ///
    /// Returns [`CheckpointError::Backend`] on connection or migration
    /// failure, or [`CheckpointError::Io`] when a runtime cannot be built.
    pub fn open(dsn: &str) -> Result<Self, CheckpointError> {
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            return tokio::task::block_in_place(|| handle.block_on(Self::connect(dsn)));
        }
        let runtime = std::sync::Arc::new(tokio::runtime::Runtime::new()?);
        let handle = runtime.handle().clone();
        let store = handle.block_on(async {
            let (client, connection) =
                tokio_postgres::connect(dsn, tokio_postgres::NoTls).await?;
            handle.spawn(async move {
                if let Err(error) = connection.await {
                    tracing::error!(%error, "postgres checkpoint connection closed");
                }
            });
            client.batch_execute(POSTGRES_CHECKPOINT_SCHEMA).await?;
            Ok::<_, CheckpointError>(client)
        })?;
        Ok(Self {
            client: store,
            handle,
            _runtime: Some(runtime),
        })
    }

}

impl CheckpointStore for PostgresCheckpointStore {
    fn is_complete(&mut self, key: &str) -> bool {
        let handle = self.handle.clone();
        let future = async {
            match self
                .client
                .query_opt(
                    "SELECT 1 FROM pramen_checkpoints WHERE key = $1 AND status = 'completed'",
                    &[&key],
                )
                .await
            {
                Ok(row) => row.is_some(),
                Err(error) => {
                    tracing::error!(%error, "postgres checkpoint is_complete failed");
                    false
                }
            }
        };
        if tokio::runtime::Handle::try_current().is_ok() {
            tokio::task::block_in_place(|| handle.block_on(future))
        } else {
            handle.block_on(future)
        }
    }

    fn claim(&mut self, unit: &WorkUnit, key: &str, run_id: &str) -> Result<(), CheckpointError> {
        let url = unit.url.clone();
        let size = i64::try_from(unit.size).unwrap_or(i64::MAX);
        let modified = unit.modified_millis;
        let key = key.to_owned();
        let run_id = run_id.to_owned();
        let handle = self.handle.clone();
        let future = async {
            self.client
                .execute(
                    "INSERT INTO pramen_checkpoints
                        (key, status, url, size, modified_millis, run_id)
                     VALUES ($1, 'claimed', $2, $3, $4, $5)
                     ON CONFLICT (key) DO UPDATE SET
                        url = EXCLUDED.url,
                        size = EXCLUDED.size,
                        modified_millis = EXCLUDED.modified_millis,
                        run_id = EXCLUDED.run_id,
                        claimed_at = now()
                     WHERE pramen_checkpoints.status <> 'completed'",
                    &[&key, &url, &size, &modified, &run_id],
                )
                .await?;
            Ok(())
        };
        if tokio::runtime::Handle::try_current().is_ok() {
            tokio::task::block_in_place(|| handle.block_on(future))
        } else {
            handle.block_on(future)
        }
    }

    fn complete(&mut self, key: &str, run_id: &str) -> Result<(), CheckpointError> {
        let key = key.to_owned();
        let run_id = run_id.to_owned();
        let handle = self.handle.clone();
        // Idempotent: a completed row keeps its first completed_at and
        // run_id; claimed rows are promoted. Never demotes completed.
        let future = async {
            self.client
                .execute(
                    "INSERT INTO pramen_checkpoints
                        (key, status, url, size, modified_millis, run_id, completed_at)
                     VALUES ($1, 'completed', '', 0, 0, $2, now())
                     ON CONFLICT (key) DO UPDATE SET
                        status = 'completed',
                        completed_at = COALESCE(pramen_checkpoints.completed_at, now()),
                        run_id = CASE
                            WHEN pramen_checkpoints.status = 'completed'
                                THEN pramen_checkpoints.run_id
                            ELSE EXCLUDED.run_id
                        END",
                    &[&key, &run_id],
                )
                .await?;
            Ok(())
        };
        if tokio::runtime::Handle::try_current().is_ok() {
            tokio::task::block_in_place(|| handle.block_on(future))
        } else {
            handle.block_on(future)
        }
    }
}

impl CheckpointStore for FileCheckpointStore {
    fn is_complete(&mut self, key: &str) -> bool {
        self.completed.contains(key)
    }

    fn claim(&mut self, unit: &WorkUnit, key: &str, run_id: &str) -> Result<(), CheckpointError> {
        self.append(&LogRecord::Claimed {
            key: key.to_owned(),
            unit: unit.clone(),
            run_id: run_id.to_owned(),
            at_millis: now_millis(),
        })?;
        self.claimed.insert(key.to_owned(), run_id.to_owned());
        Ok(())
    }

    fn complete(&mut self, key: &str, run_id: &str) -> Result<(), CheckpointError> {
        if self.completed.contains(key) {
            return Ok(());
        }
        self.append(&LogRecord::Completed {
            key: key.to_owned(),
            run_id: run_id.to_owned(),
            at_millis: now_millis(),
        })?;
        self.claimed.remove(key);
        self.completed.insert(key.to_owned());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(name: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("pramen-ckpt-test-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    fn unit(url: &str) -> WorkUnit {
        WorkUnit {
            url: url.to_owned(),
            size: 1234,
            modified_millis: 1_700_000_000_000,
        }
    }

    #[test]
    fn unit_identity_covers_content_metadata() {
        let base = unit("/data/a.parquet").key("pipe");
        assert_ne!(base, unit("/data/b.parquet").key("pipe"));
        assert_ne!(base, unit("/data/a.parquet").key("other-pipe"));

        let mut resized = unit("/data/a.parquet");
        resized.size = 999;
        assert_ne!(base, resized.key("pipe"), "rewritten file is new work");

        let mut touched = unit("/data/a.parquet");
        touched.modified_millis += 1;
        assert_ne!(base, touched.key("pipe"));

        assert_eq!(base, unit("/data/a.parquet").key("pipe"), "stable");
    }

    #[test]
    fn claim_complete_lifecycle_survives_reopen() {
        let dir = temp_dir("lifecycle");
        let a = unit("/data/a.parquet");
        let b = unit("/data/b.parquet");
        let (ka, kb) = (a.key("pipe"), b.key("pipe"));

        {
            let mut store = FileCheckpointStore::open(&dir).unwrap();
            assert!(!store.is_complete(&ka));
            store.claim(&a, &ka, "run-1").unwrap();
            store.claim(&b, &kb, "run-1").unwrap();
            store.complete(&ka, "run-1").unwrap();
            // b stays claimed: simulates a crash before completion.
        }

        let mut store = FileCheckpointStore::open(&dir).unwrap();
        assert!(store.is_complete(&ka), "completion is durable");
        assert!(!store.is_complete(&kb), "unfinished work is not completed");
        assert_eq!(
            store.claimed_units(),
            vec![(kb.clone(), "run-1".to_owned())],
            "stale claim is visible for diagnosis"
        );
        assert_eq!(store.completed_count(), 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn completion_is_idempotent() {
        let dir = temp_dir("idempotent");
        let a = unit("/data/a.parquet");
        let ka = a.key("pipe");
        let mut store = FileCheckpointStore::open(&dir).unwrap();
        store.claim(&a, &ka, "run-1").unwrap();
        store.complete(&ka, "run-1").unwrap();
        store.complete(&ka, "run-2").unwrap();

        let log = std::fs::read_to_string(dir.join("checkpoints.jsonl")).unwrap();
        assert_eq!(
            log.lines().filter(|l| l.contains("completed")).count(),
            1,
            "second completion writes nothing"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn torn_final_line_is_discarded_and_truncated() {
        let dir = temp_dir("torn");
        let a = unit("/data/a.parquet");
        let ka = a.key("pipe");
        {
            let mut store = FileCheckpointStore::open(&dir).unwrap();
            store.claim(&a, &ka, "run-1").unwrap();
            store.complete(&ka, "run-1").unwrap();
        }
        // Simulate a crash mid-append: a partial record at the end.
        let log_path = dir.join("checkpoints.jsonl");
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&log_path)
            .unwrap();
        file.write_all(b"{\"event\":\"completed\",\"key\":\"trunc")
            .unwrap();
        drop(file);

        let mut store = FileCheckpointStore::open(&dir).unwrap();
        assert!(store.is_complete(&ka), "intact records survive");
        assert_eq!(store.completed_count(), 1);

        // The torn bytes are gone; the log is clean for future appends.
        let log = std::fs::read_to_string(&log_path).unwrap();
        assert!(log.ends_with('\n'));
        assert!(!log.contains("trunc"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn corruption_in_the_middle_is_an_error_not_data_loss() {
        let dir = temp_dir("corrupt");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("checkpoints.jsonl"),
            "not json at all\n{\"event\":\"completed\",\"key\":\"k\",\"runId\":\"r\",\"atMillis\":1}\n",
        )
        .unwrap();
        let error = match FileCheckpointStore::open(&dir) {
            Err(error) => error,
            Ok(_) => panic!("expected corruption error"),
        };
        assert!(matches!(error, CheckpointError::Corrupt { line: 1, .. }));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn postgres_url_detection() {
        assert!(is_postgres_url("postgres://localhost/db"));
        assert!(is_postgres_url("postgresql://user@host/db"));
        assert!(is_postgres_url("POSTGRES://localhost/db"));
        assert!(!is_postgres_url("file:///var/lib/pramen/checkpoints"));
        assert!(!is_postgres_url("/var/lib/pramen/checkpoints"));
    }

    /// L2: claim/complete against real PostgreSQL when
    /// `PRAMEN_TEST_POSTGRES_DSN` is set (ADR 0005).
    #[tokio::test(flavor = "multi_thread")]
    async fn postgres_claim_complete_lifecycle() {
        let Some(dsn) = pramen_testkit::env::postgres_dsn() else {
            return;
        };
        let (setup, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
            .await
            .unwrap();
        tokio::spawn(connection);
        setup
            .batch_execute("DROP TABLE IF EXISTS pramen_checkpoints")
            .await
            .unwrap();

        let a = unit("/data/a.parquet");
        let ka = a.key("pipe");
        {
            let mut store = PostgresCheckpointStore::connect(&dsn).await.unwrap();
            assert!(!store.is_complete(&ka));
            store.claim(&a, &ka, "run-1").unwrap();
            store.complete(&ka, "run-1").unwrap();
            store.complete(&ka, "run-2").unwrap();
            assert!(store.is_complete(&ka));
        }

        let mut reopened = PostgresCheckpointStore::connect(&dsn).await.unwrap();
        assert!(reopened.is_complete(&ka), "completion survives reconnect");

        // A re-claim must not demote a completed unit.
        reopened.claim(&a, &ka, "run-3").unwrap();
        assert!(reopened.is_complete(&ka));
        let status: String = setup
            .query_one(
                "SELECT status FROM pramen_checkpoints WHERE key = $1",
                &[&ka],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(status, "completed");
    }
}
