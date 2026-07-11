//! Governed semantic operators for Pramen.
//!
//! This crate turns language-model calls into contract-bound operations:
//!
//! - [`workkey`]: the content-addressed identity of a unit of semantic work;
//! - [`ledger`]: the durable SQLite (WAL) inference ledger — completed work
//!   is recorded before use and reused on replay, never re-billed;
//! - [`provider`]: the provider abstraction with mock and OpenAI-compatible
//!   adapters (Bedrock arrives with P1.7);
//! - [`schema`]: JSON Schema generation from declared output fields and
//!   strict typed validation of model output;
//! - [`budget`]: token budgets enforced *before* dispatch;
//! - [`operator`]: the `ai.extract` / `ai.classify` transform that plugs
//!   into the [`pramen_core::runtime`] dataflow.

pub mod budget;
pub mod error;
pub mod ledger;
pub mod operator;
pub mod provider;
pub mod schema;
pub mod workkey;

pub use error::AiError;
