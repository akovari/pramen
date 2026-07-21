//! Governed semantic operators for Pramen.
//!
//! This crate turns language-model calls into contract-bound operations:
//!
//! - [`workkey`]: the content-addressed identity of a unit of semantic work;
//! - [`ledger`]: the durable inference ledger (SQLite WAL locally, Postgres
//!   for fleets) — completed work is recorded before use and reused on
//!   replay, never re-billed;
//! - [`provider`]: the provider abstraction with mock, OpenAI-compatible,
//!   and Bedrock Converse adapters, plus the provider-batch surface;
//! - [`schema`]: JSON Schema generation from declared output fields and
//!   strict typed validation of model output;
//! - [`budget`]: token budgets enforced *before* dispatch;
//! - [`dispatch`]: online vs provider-batch cost model for `execution: auto`
//!   and `pramen ai dispatch-plan` frontier sweeps (E2.1 / RQ1);
//! - [`operator`]: the `ai.extract` / `ai.classify` / `ai.generate`
//!   transform that plugs into the [`pramen_core::runtime`] dataflow;
//! - [`review`]: the durable review queue — records routed by
//!   `onInvalid: review` await a human decision; accepted corrections
//!   re-enter the ledger, rejections drop permanently (`pramen ai review`);
//! - [`eval`]: golden-corpus evaluation — quality, cost, and latency of a
//!   model on a versioned labelled corpus (`pramen ai evaluate`);
//! - [`reuse`]: offline RQ2 memoization measurement suite (task E2.2) —
//!   crash/replay, incremental re-enrichment, duplicate-heavy savings.

pub mod budget;
pub mod dispatch;
pub mod error;
pub mod eval;
pub mod ledger;
pub mod operator;
pub mod provider;
pub mod reuse;
pub mod review;
pub mod schema;
pub mod workkey;

pub use error::{AiError, ProviderFault};
