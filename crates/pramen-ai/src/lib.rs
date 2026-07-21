//! Governed semantic operators for Pramen.
//!
//! This crate turns language-model calls into contract-bound operations:
//!
//! - [`workkey`]: the content-addressed identity of a unit of semantic work;
//! - [`ledger`]: the durable SQLite (WAL) inference ledger — completed work
//!   is recorded before use and reused on replay, never re-billed;
//! - [`provider`]: the provider abstraction with mock, OpenAI-compatible,
//!   and Bedrock Converse adapters, plus the provider-batch surface;
//! - [`schema`]: JSON Schema generation from declared output fields and
//!   strict typed validation of model output;
//! - [`budget`]: token budgets enforced *before* dispatch;
//! - [`operator`]: the `ai.extract` / `ai.classify` / `ai.generate`
//!   transform that plugs into the [`pramen_core::runtime`] dataflow;
//! - [`review`]: the durable review queue — records routed by
//!   `onInvalid: review` await a human decision; accepted corrections
//!   re-enter the ledger, rejections drop permanently (`pramen ai review`);
//! - [`eval`]: golden-corpus evaluation — quality, cost, and latency of a
//!   model on a versioned labelled corpus (`pramen ai evaluate`).

pub mod budget;
pub mod error;
pub mod eval;
pub mod ledger;
pub mod operator;
pub mod provider;
pub mod review;
pub mod schema;
pub mod workkey;

pub use error::{AiError, ProviderFault};
