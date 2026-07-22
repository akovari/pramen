//! File formats, object-store sources, SQL transforms, and database sinks.
//!
//! This crate provides the concrete [`pramen_core::runtime`] stage
//! implementations for the lean v1 profile:
//!
//! - [`ParquetSource`] / [`NdjsonSource`]: stream Arrow batches out of
//!   Parquet or newline-delimited JSON files via DataFusion's bounded,
//!   spillable execution;
//! - [`SqlTransform`]: per-batch DataFusion SQL where the incoming batch is
//!   visible as the table `input`;
//! - [`PostgresCopySink`]: native binary `COPY` into PostgreSQL inside a
//!   single transaction, committed only when the run succeeds;
//! - [`FlightSqlSink`]: append Arrow batches to a Flight SQL endpoint via
//!   `CommandStatementIngest` (ADR 0008).

mod flight_sql;
mod postgres;
mod source;
mod sql;

pub use flight_sql::FlightSqlSink;
pub use postgres::PostgresCopySink;
#[doc(hidden)]
pub use postgres::encode_batch;
pub use source::{NdjsonSource, ParquetSource, list_work_units};
pub use sql::SqlTransform;
