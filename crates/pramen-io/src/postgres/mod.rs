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

/// Loads Arrow batches into a PostgreSQL table via binary `COPY` inside a
/// single transaction.
///
/// The `COPY` starts lazily on the first batch (its schema defines the
/// column list) and the transaction commits only from [`Sink::commit`], so
/// a failed run leaves the target table untouched. The binary encoder was
/// validated at 3.1x `psql \copy` throughput in spike S1.3.
pub struct PostgresCopySink {
    client: Client,
    target: String,
    copy: Option<Pin<Box<CopyInSink<bytes::Bytes>>>>,
    buffer: BytesMut,
}

impl PostgresCopySink {
    /// Connect to `dsn` and prepare to load into `target`
    /// (a qualified `schema.table` name).
    ///
    /// Only [`SinkMode::Append`] is implemented; `upsert` staging lands
    /// with the delivery-contract work (P1.4 remainder).
    ///
    /// # Errors
    ///
    /// Returns a [`StageError`] when the connection cannot be established
    /// or the mode is unsupported.
    pub async fn connect(dsn: &str, target: &str, mode: SinkMode) -> Result<Self, StageError> {
        if mode != SinkMode::Append {
            return Err(StageError::InvalidData(
                "sink mode `upsert` is not implemented yet (P1.4)".to_owned(),
            ));
        }
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
        Ok(Self {
            client,
            target: target.to_owned(),
            copy: None,
            buffer: BytesMut::with_capacity(2 * FLUSH_BYTES),
        })
    }

    async fn start_copy(&mut self, schema: &arrow::datatypes::Schema) -> Result<(), StageError> {
        let columns: Vec<String> = schema
            .fields()
            .iter()
            .map(|field| quote_ident(field.name()))
            .collect();
        let statement = format!(
            "COPY {} ({}) FROM STDIN (FORMAT binary)",
            quote_target(&self.target),
            columns.join(", ")
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

        let mut sink = PostgresCopySink::connect(&dsn, "public.pramen_sink_test", SinkMode::Append)
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
}
