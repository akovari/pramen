//! Core Pramen types: pipeline specifications, plans, the dataflow runtime,
//! bounded channels, checkpoints, and observability.
//!
//! See `docs/architecture.md` for the design this crate implements. The
//! crate is a skeleton until the Phase 1 foundation tasks (F1–F3) land.

pub mod observe;
pub mod runtime;
pub mod spec;

/// The version of the Pramen workspace this crate was built from.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

#[cfg(test)]
mod tests {
    use super::VERSION;

    #[test]
    fn version_is_nonempty() {
        assert!(!VERSION.is_empty());
    }
}
