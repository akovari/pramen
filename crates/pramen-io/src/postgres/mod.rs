//! The PostgreSQL binary-`COPY` sink.

mod encode;

use bytes::BytesMut;
use futures::SinkExt;
use pramen_core::runtime::{Sink, StageError};
use pramen_core::spec::SinkMode;
use std::pin::Pin;
use tokio_postgres::{Client, CopyInSink};

/// Bytes buffered before a frame is flushed to the server.
const FLUSH_BYTES: usize = 2 * 1024 * 1024;

/// Session-local staging table name used by `upsert` mode.
const STAGE_TABLE: &str = "pramen_stage";

/// Ordinal column added to the staging table so that when the run itself
/// contains several rows with the same merge key, the last one written
/// wins deterministically.
const STAGE_SEQ: &str = "pramen_seq";

/// Loads Arrow batches into a PostgreSQL table via binary `COPY` inside a
/// single transaction.
///
/// `append` copies straight into the target. `upsert` copies into a
/// session-local temporary staging table, then merges into the target with
/// `INSERT … ON CONFLICT (keys) DO UPDATE` at commit — replays become
/// idempotent on the merge key, which requires a unique index over exactly
/// those columns on the target.
///
/// The `COPY` starts lazily on the first batch (its schema defines the
/// column list) and the transaction commits only from [`Sink::commit`], so
/// a failed run leaves the target table untouched. The binary encoder was
/// validated at 3.1x `psql \copy` throughput in spike S1.3.
pub struct PostgresCopySink {
    client: Client,
    target: String,
    mode: SinkMode,
    /// Merge-key columns (upsert mode only).
    keys: Vec<String>,
    /// Column names of the first batch, captured for the merge statement.
    columns: Vec<String>,
    copy: Option<Pin<Box<CopyInSink<bytes::Bytes>>>>,
    buffer: BytesMut,
}

impl PostgresCopySink {
    /// Connect to `dsn` and prepare to load into `target`
    /// (a qualified `schema.table` name).
    ///
    /// For [`SinkMode::Upsert`], `keys` names the merge-key columns; the
    /// target table needs a unique index over exactly these columns.
    ///
    /// # Errors
    ///
    /// Returns a [`StageError`] when the connection cannot be established
    /// or the staging table cannot be created.
    pub async fn connect(
        dsn: &str,
        target: &str,
        mode: SinkMode,
        keys: &[String],
    ) -> Result<Self, StageError> {
        let (client, connection) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
            .await
            .map_err(StageError::external)?;
        tokio::spawn(async move {
            if let Err(error) = connection.await {
                tracing::error!(%error, "postgres connection task ended with error");
            }
        });
        client
            .batch_execute("BEGIN")
            .await
            .map_err(StageError::external)?;
        if mode == SinkMode::Upsert {
            // Temporary tables are session-local, so the name cannot
            // collide across concurrent runs; ON COMMIT DROP ties its
            // lifetime to this transaction.
            let staging = format!(
                "CREATE TEMPORARY TABLE {STAGE_TABLE} \
                 (LIKE {} INCLUDING DEFAULTS) ON COMMIT DROP; \
                 ALTER TABLE {STAGE_TABLE} ADD COLUMN {STAGE_SEQ} bigint \
                 GENERATED ALWAYS AS IDENTITY",
                quote_target(target)
            );
            client
                .batch_execute(&staging)
                .await
                .map_err(StageError::external)?;
        }
        Ok(Self {
            client,
            target: target.to_owned(),
            mode,
            keys: keys.to_vec(),
            columns: Vec::new(),
            copy: None,
            buffer: BytesMut::with_capacity(2 * FLUSH_BYTES),
        })
    }

    async fn start_copy(&mut self, schema: &arrow::datatypes::Schema) -> Result<(), StageError> {
        self.columns = schema
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect();
        let quoted: Vec<String> = self.columns.iter().map(|c| quote_ident(c)).collect();
        let copy_into = match self.mode {
            SinkMode::Append => quote_target(&self.target),
            SinkMode::Upsert => STAGE_TABLE.to_owned(),
        };
        let statement = format!(
            "COPY {copy_into} ({}) FROM STDIN (FORMAT binary)",
            quoted.join(", ")
        );
        let sink = self
            .client
            .copy_in::<_, bytes::Bytes>(&statement)
            .await
            .map_err(StageError::external)?;
        self.copy = Some(Box::pin(sink));
        self.buffer.extend_from_slice(&encode::copy_header());
        Ok(())
    }

    /// The `INSERT … ON CONFLICT` statement merging staged rows into the
    /// target: within-run duplicates collapse to the last-written row
    /// (highest staging ordinal), replayed keys update in place.
    fn merge_statement(&self) -> Result<String, StageError> {
        let quoted: Vec<String> = self.columns.iter().map(|c| quote_ident(c)).collect();
        let key_set: std::collections::BTreeSet<&str> =
            self.keys.iter().map(String::as_str).collect();
        for key in &self.keys {
            if !self.columns.contains(key) {
                return Err(StageError::InvalidData(format!(
                    "upsert key column `{key}` is not produced by the pipeline \
                     (columns: {})",
                    self.columns.join(", ")
                )));
            }
        }
        let keys_quoted: Vec<String> = self.keys.iter().map(|k| quote_ident(k)).collect();
        let updates: Vec<String> = self
            .columns
            .iter()
            .filter(|c| !key_set.contains(c.as_str()))
            .map(|c| {
                let q = quote_ident(c);
                format!("{q} = EXCLUDED.{q}")
            })
            .collect();
        let on_conflict = if updates.is_empty() {
            "DO NOTHING".to_owned()
        } else {
            format!("DO UPDATE SET {}", updates.join(", "))
        };
        Ok(format!(
            "INSERT INTO {target} ({cols}) \
             SELECT DISTINCT ON ({keys}) {cols} FROM {STAGE_TABLE} \
             ORDER BY {keys}, {STAGE_SEQ} DESC \
             ON CONFLICT ({keys}) {on_conflict}",
            target = quote_target(&self.target),
            cols = quoted.join(", "),
            keys = keys_quoted.join(", "),
        ))
    }

    async fn flush(&mut self) -> Result<(), StageError> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let frame = self.buffer.split().freeze();
        if let Some(copy) = self.copy.as_mut() {
            copy.send(frame).await.map_err(StageError::external)?;
        }
        Ok(())
    }
}

/// Quote one SQL identifier.
fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Quote a qualified `schema.table` target.
fn quote_target(target: &str) -> String {
    target
        .split('.')
        .map(quote_ident)
        .collect::<Vec<_>>()
        .join(".")
}

#[async_trait::async_trait]
impl Sink for PostgresCopySink {
    async fn write(&mut self, batch: arrow::record_batch::RecordBatch) -> Result<(), StageError> {
        if self.copy.is_none() {
            self.start_copy(&batch.schema()).await?;
        }
        encode::encode_batch(&batch, &mut self.buffer)?;
        if self.buffer.len() >= FLUSH_BYTES {
            self.flush().await?;
        }
        Ok(())
    }

    async fn commit(&mut self) -> Result<(), StageError> {
        if let Some(mut copy) = self.copy.take() {
            self.buffer.extend_from_slice(&encode::copy_trailer());
            let frame = self.buffer.split().freeze();
            copy.send(frame).await.map_err(StageError::external)?;
            copy.as_mut().finish().await.map_err(StageError::external)?;
            if self.mode == SinkMode::Upsert {
                let merge = self.merge_statement()?;
                let merged = self
                    .client
                    .execute(&merge, &[])
                    .await
                    .map_err(StageError::external)?;
                tracing::info!(rows = merged, target = %self.target, "upsert merge applied");
            }
        }
        self.client
            .batch_execute("COMMIT")
            .await
            .map_err(StageError::external)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    #[test]
    fn identifiers_are_quoted() {
        assert_eq!(quote_ident("plain"), "\"plain\"");
        assert_eq!(quote_ident("we\"ird"), "\"we\"\"ird\"");
        assert_eq!(quote_target("analytics.events"), "\"analytics\".\"events\"");
    }

    /// End-to-end load against a real PostgreSQL, exercised when
    /// `PRAMEN_TEST_POSTGRES_DSN` is set (L2 in ADR 0005). Offline runs
    /// skip it and rely on the encoder unit tests.
    #[tokio::test]
    async fn loads_and_commits_against_real_postgres() {
        let Ok(dsn) = std::env::var("PRAMEN_TEST_POSTGRES_DSN") else {
            eprintln!("skipping: PRAMEN_TEST_POSTGRES_DSN not set");
            return;
        };
        let (setup, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
            .await
            .unwrap();
        tokio::spawn(connection);
        setup
            .batch_execute(
                "DROP TABLE IF EXISTS public.pramen_sink_test;
                 CREATE TABLE public.pramen_sink_test (id bigint NOT NULL, label text)",
            )
            .await
            .unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("label", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])),
            ],
        )
        .unwrap();

        let mut sink =
            PostgresCopySink::connect(&dsn, "public.pramen_sink_test", SinkMode::Append, &[])
                .await
                .unwrap();
        sink.write(batch.clone()).await.unwrap();
        sink.write(batch).await.unwrap();
        sink.commit().await.unwrap();

        let count: i64 = setup
            .query_one("SELECT count(*) FROM public.pramen_sink_test", &[])
            .await
            .unwrap()
            .get(0);
        assert_eq!(count, 6);
        let nulls: i64 = setup
            .query_one(
                "SELECT count(*) FROM public.pramen_sink_test WHERE label IS NULL",
                &[],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(nulls, 2);
    }

    /// P1.14 (L2): the delivery contract, pinned. A replayed `append` run
    /// duplicates rows — that is the documented at-least-once window — and
    /// the same replay under `upsert` is idempotent.
    #[tokio::test]
    async fn delivery_contract_append_duplicates_upsert_does_not() {
        let Ok(dsn) = std::env::var("PRAMEN_TEST_POSTGRES_DSN") else {
            eprintln!("skipping: PRAMEN_TEST_POSTGRES_DSN not set");
            return;
        };
        let (setup, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
            .await
            .unwrap();
        tokio::spawn(connection);
        setup
            .batch_execute(
                "DROP TABLE IF EXISTS public.pramen_delivery_test;
                 CREATE TABLE public.pramen_delivery_test
                     (id bigint NOT NULL UNIQUE, label text)",
            )
            .await
            .unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("label", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec![Some("a"), Some("b")])),
            ],
        )
        .unwrap();
        async fn count(setup: &Client) -> i64 {
            setup
                .query_one("SELECT count(*) FROM public.pramen_delivery_test", &[])
                .await
                .unwrap()
                .get(0)
        }

        // Upsert replay: idempotent. (Run upsert first: append would
        // create key conflicts for it.)
        for _ in 0..2 {
            let mut sink = PostgresCopySink::connect(
                &dsn,
                "public.pramen_delivery_test",
                SinkMode::Upsert,
                &["id".to_owned()],
            )
            .await
            .unwrap();
            sink.write(batch.clone()).await.unwrap();
            sink.commit().await.unwrap();
        }
        assert_eq!(count(&setup).await, 2, "upsert replays are idempotent");

        // Append replay: duplicates, by contract (ADR 0006).
        setup
            .batch_execute(
                "DROP TABLE IF EXISTS public.pramen_delivery_test;
                 CREATE TABLE public.pramen_delivery_test (id bigint NOT NULL, label text)",
            )
            .await
            .unwrap();
        for _ in 0..2 {
            let mut sink = PostgresCopySink::connect(
                &dsn,
                "public.pramen_delivery_test",
                SinkMode::Append,
                &[],
            )
            .await
            .unwrap();
            sink.write(batch.clone()).await.unwrap();
            sink.commit().await.unwrap();
        }
        assert_eq!(
            count(&setup).await,
            4,
            "append replays duplicate — the documented at-least-once window"
        );
    }

    /// P1.4 + P1.14 (L2): upsert is idempotent on the merge key across
    /// replays, updates changed rows in place, and collapses within-run
    /// duplicates to the last-written row.
    #[tokio::test]
    async fn upsert_is_idempotent_and_last_write_wins() {
        let Ok(dsn) = std::env::var("PRAMEN_TEST_POSTGRES_DSN") else {
            eprintln!("skipping: PRAMEN_TEST_POSTGRES_DSN not set");
            return;
        };
        let (setup, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
            .await
            .unwrap();
        tokio::spawn(connection);
        setup
            .batch_execute(
                "DROP TABLE IF EXISTS public.pramen_upsert_test;
                 CREATE TABLE public.pramen_upsert_test
                     (id bigint NOT NULL UNIQUE, label text)",
            )
            .await
            .unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("label", DataType::Utf8, true),
        ]));
        let keys = vec!["id".to_owned()];
        let load = |rows: Vec<(i64, &'static str)>| {
            let dsn = dsn.clone();
            let schema = Arc::clone(&schema);
            let keys = keys.clone();
            async move {
                let batch = RecordBatch::try_new(
                    schema,
                    vec![
                        Arc::new(Int64Array::from(
                            rows.iter().map(|r| r.0).collect::<Vec<_>>(),
                        )),
                        Arc::new(StringArray::from(
                            rows.iter().map(|r| Some(r.1)).collect::<Vec<_>>(),
                        )),
                    ],
                )
                .unwrap();
                let mut sink = PostgresCopySink::connect(
                    &dsn,
                    "public.pramen_upsert_test",
                    SinkMode::Upsert,
                    &keys,
                )
                .await
                .unwrap();
                sink.write(batch).await.unwrap();
                sink.commit().await.unwrap();
            }
        };

        // Within-run duplicate on id=2: the later row must win.
        load(vec![(1, "one"), (2, "two-stale"), (2, "two")]).await;
        // Replay with one changed and one new row: no duplicates, in-place
        // update, new row inserted.
        load(vec![(1, "one-updated"), (3, "three")]).await;

        let rows = setup
            .query(
                "SELECT id, label FROM public.pramen_upsert_test ORDER BY id",
                &[],
            )
            .await
            .unwrap();
        let got: Vec<(i64, String)> = rows.iter().map(|r| (r.get(0), r.get(1))).collect();
        assert_eq!(
            got,
            vec![
                (1, "one-updated".to_owned()),
                (2, "two".to_owned()),
                (3, "three".to_owned()),
            ]
        );
    }
}
