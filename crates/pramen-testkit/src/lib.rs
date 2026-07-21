//! Shared test fixtures for the Pramen workspace (T1.6, ADR 0005).
//!
//! Two things every crate's tests kept reimplementing live here once:
//!
//! - [`http`]: minimal local HTTP stub servers for L1 protocol tests —
//!   real adapters against canned provider responses, zero cloud access.
//! - [`mod@env`]: the L2 opt-in guards (`PRAMEN_TEST_POSTGRES_DSN`,
//!   `PRAMEN_TEST_S3_URL`, `PRAMEN_TEST_AZURE_URL`, `PRAMEN_TEST_GCS_URL`)
//!   with their uniform skip messages.
//!
//! This crate is a dev-dependency only and is never published; panicking
//! on a broken fixture is correct behavior here, hence the crate-level
//! allowance below.

#![allow(clippy::expect_used)]

pub mod env;
pub mod http;
