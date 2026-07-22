//! Flight SQL append sink (E1.2 / ADR 0008).

use arrow::record_batch::RecordBatch;
use arrow_flight::sql::client::FlightSqlServiceClient;
use arrow_flight::sql::{
    CommandStatementIngest, TableDefinitionOptions, TableExistsOption, TableNotExistOption,
};
use futures::stream;
use pramen_core::runtime::{Sink, StageError};
use tonic::transport::Channel;

/// Loads Arrow batches into a Flight SQL table via `CommandStatementIngest`.
///
/// Batches are buffered in memory on [`Sink::write`] and sent only from
/// [`Sink::commit`], so a failed run never mutates the destination (ADR 0007
/// commit barrier / ADR 0008).
pub struct FlightSqlSink {
    endpoint: String,
    catalog: Option<String>,
    schema: Option<String>,
    table: String,
    token: Option<String>,
    batches: Vec<RecordBatch>,
}

impl FlightSqlSink {
    /// Prepare an append-only Flight SQL sink.
    ///
    /// `target` is `schema.table` or `catalog.schema.table`. `token` is an
    /// optional Bearer token (already read from the configured env var).
    ///
    /// # Errors
    ///
    /// Returns [`StageError::InvalidData`] when `target` is not a 2- or
    /// 3-part identifier, or `endpoint` is empty.
    pub fn new(endpoint: &str, target: &str, token: Option<String>) -> Result<Self, StageError> {
        if endpoint.trim().is_empty() {
            return Err(StageError::InvalidData(
                "Flight SQL endpoint must not be empty".to_owned(),
            ));
        }
        let (catalog, schema, table) = parse_target(target)?;
        Ok(Self {
            endpoint: endpoint.trim().to_owned(),
            catalog,
            schema,
            table,
            token,
            batches: Vec::new(),
        })
    }
}

fn parse_target(target: &str) -> Result<(Option<String>, Option<String>, String), StageError> {
    let parts: Vec<&str> = target.split('.').collect();
    match parts.as_slice() {
        [schema, table] if !schema.trim().is_empty() && !table.trim().is_empty() => {
            Ok((None, Some((*schema).to_owned()), (*table).to_owned()))
        }
        [catalog, schema, table]
            if !catalog.trim().is_empty()
                && !schema.trim().is_empty()
                && !table.trim().is_empty() =>
        {
            Ok((
                Some((*catalog).to_owned()),
                Some((*schema).to_owned()),
                (*table).to_owned(),
            ))
        }
        _ => Err(StageError::InvalidData(format!(
            "`{target}` must be `schema.table` or `catalog.schema.table`"
        ))),
    }
}

#[async_trait::async_trait]
impl Sink for FlightSqlSink {
    async fn write(&mut self, batch: RecordBatch) -> Result<(), StageError> {
        if let Some(first) = self.batches.first()
            && first.schema() != batch.schema()
        {
            return Err(StageError::InvalidData(
                "Flight SQL sink received a batch with a different schema".to_owned(),
            ));
        }
        self.batches.push(batch);
        Ok(())
    }

    async fn commit(&mut self) -> Result<(), StageError> {
        if self.batches.is_empty() {
            return Ok(());
        }
        let channel = Channel::from_shared(self.endpoint.clone())
            .map_err(|error| StageError::InvalidData(format!("Flight SQL endpoint: {error}")))?
            .connect()
            .await
            .map_err(StageError::external)?;
        let mut client = FlightSqlServiceClient::new(channel);
        if let Some(token) = &self.token
            && !token.is_empty()
        {
            client.set_token(token.clone());
        }

        let command = CommandStatementIngest {
            table_definition_options: Some(TableDefinitionOptions {
                if_not_exist: TableNotExistOption::Fail as i32,
                if_exists: TableExistsOption::Append as i32,
            }),
            table: self.table.clone(),
            schema: self.schema.clone(),
            catalog: self.catalog.clone(),
            temporary: false,
            transaction_id: None,
            options: Default::default(),
        };

        let batches = std::mem::take(&mut self.batches);
        let stream = stream::iter(batches.into_iter().map(Ok));
        client
            .execute_ingest(command, stream)
            .await
            .map_err(|error| StageError::external(error))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow_flight::decode::FlightRecordBatchStream;
    use arrow_flight::error::FlightError;
    use arrow_flight::flight_service_server::FlightServiceServer;
    use arrow_flight::sql::SqlInfo;
    use arrow_flight::sql::server::{FlightSqlService, PeekableFlightDataStream};
    use futures::TryStreamExt;
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use tokio::sync::Mutex;
    use tonic::{Request, Status};

    fn batch(rows: i64) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let values: Vec<i64> = (0..rows).collect();
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values))]).unwrap()
    }

    struct MockFlightSql {
        rows: Arc<AtomicU64>,
        table: Arc<Mutex<Option<String>>>,
    }

    #[tonic::async_trait]
    impl FlightSqlService for MockFlightSql {
        type FlightService = Self;

        async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}

        async fn do_put_statement_ingest(
            &self,
            ticket: CommandStatementIngest,
            request: Request<PeekableFlightDataStream>,
        ) -> Result<i64, Status> {
            {
                let mut guard = self.table.lock().await;
                *guard = Some(format!(
                    "{}.{}.{}",
                    ticket.catalog.as_deref().unwrap_or("-"),
                    ticket.schema.as_deref().unwrap_or("-"),
                    ticket.table
                ));
            }
            let stream = request.into_inner();
            let batches: Vec<RecordBatch> = FlightRecordBatchStream::new_from_flight_data(
                stream.map_err(|status| FlightError::Tonic(Box::new(status))),
            )
            .try_collect()
            .await
            .map_err(|error| Status::internal(error.to_string()))?;
            let mut total = 0_i64;
            for batch in batches {
                total += batch.num_rows() as i64;
            }
            self.rows.fetch_add(total as u64, Ordering::SeqCst);
            Ok(total)
        }
    }

    async fn serve_mock(rows: Arc<AtomicU64>, table: Arc<Mutex<Option<String>>>) -> SocketAddr {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
        let service = FlightServiceServer::new(MockFlightSql { rows, table });
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(service)
                .serve_with_incoming(incoming)
                .await
                .unwrap();
        });
        // Give the server a moment to accept.
        tokio::task::yield_now().await;
        addr
    }

    #[tokio::test]
    async fn flight_sql_passes_connector_conformance() {
        let rows = Arc::new(AtomicU64::new(0));
        let table = Arc::new(Mutex::new(None));
        let addr = serve_mock(Arc::clone(&rows), Arc::clone(&table)).await;
        let endpoint = format!("http://{addr}");
        let probe = Arc::clone(&rows);
        pramen_core::connector::assert_sink_commit_barrier(
            || FlightSqlSink::new(&endpoint, "public.events", None).unwrap(),
            move || probe.load(Ordering::SeqCst),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn commit_ingests_buffered_batches() {
        let rows = Arc::new(AtomicU64::new(0));
        let table = Arc::new(Mutex::new(None));
        let addr = serve_mock(Arc::clone(&rows), Arc::clone(&table)).await;
        let endpoint = format!("http://{addr}");

        let mut sink = FlightSqlSink::new(&endpoint, "public.events", None).unwrap();
        sink.write(batch(10)).await.unwrap();
        sink.write(batch(5)).await.unwrap();
        assert_eq!(
            rows.load(Ordering::SeqCst),
            0,
            "must not send before commit"
        );
        sink.commit().await.unwrap();

        assert_eq!(rows.load(Ordering::SeqCst), 15);
        let recorded = table.lock().await.clone().unwrap();
        assert_eq!(recorded, "-.public.events");
    }

    #[tokio::test]
    async fn no_commit_leaves_server_empty() {
        let rows = Arc::new(AtomicU64::new(0));
        let table = Arc::new(Mutex::new(None));
        let addr = serve_mock(Arc::clone(&rows), Arc::clone(&table)).await;
        let endpoint = format!("http://{addr}");

        let mut sink = FlightSqlSink::new(&endpoint, "analytics.t", None).unwrap();
        sink.write(batch(3)).await.unwrap();
        drop(sink);
        assert_eq!(rows.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn parse_target_accepts_two_and_three_parts() {
        let (c, s, t) = parse_target("public.events").unwrap();
        assert!(c.is_none());
        assert_eq!(s.as_deref(), Some("public"));
        assert_eq!(t, "events");

        let (c, s, t) = parse_target("hive.public.events").unwrap();
        assert_eq!(c.as_deref(), Some("hive"));
        assert_eq!(s.as_deref(), Some("public"));
        assert_eq!(t, "events");

        assert!(parse_target("events").is_err());
    }
}
