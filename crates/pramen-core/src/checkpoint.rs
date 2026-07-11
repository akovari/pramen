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
//! [`FileCheckpointStore`] is the v1 backend: an append-only JSONL log with
//! fsync'd writes, replayed on open, tolerant of a torn final line. The
//! [`CheckpointStore`] trait is the seam where the Postgres-backed fleet
//! store (X1.8) slots in later.

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
    fn is_complete(&self, key: &str) -> bool;

    /// Durably record that `run_id` is about to process `unit`.
    ///
    /// Claims are advisory in the single-process v1 runtime (a stale claim
    /// from a crashed run is simply re-claimed); they exist so a later
    /// coordinator can lease work without a protocol change.
    ///
    /// # Errors
    ///
    /// Returns [`CheckpointError::Io`] when the record cannot be made
    /// durable.
    fn claim(&mut self, unit: &WorkUnit, key: &str, run_id: &str) -> Result<(), CheckpointError>;

    /// Durably record that `key` was loaded and committed at the sink.
    /// Idempotent.
    ///
    /// # Errors
    ///
    /// Returns [`CheckpointError::Io`] when the record cannot be made
    /// durable.
    fn complete(&mut self, key: &str, run_id: &str) -> Result<(), CheckpointError>;
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

impl CheckpointStore for FileCheckpointStore {
    fn is_complete(&self, key: &str) -> bool {
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

        let store = FileCheckpointStore::open(&dir).unwrap();
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

        let store = FileCheckpointStore::open(&dir).unwrap();
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
}
