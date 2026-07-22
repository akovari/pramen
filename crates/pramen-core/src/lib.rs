//! Core Pramen types: pipeline specifications, plans, the dataflow runtime,
//! bounded channels, checkpoints, observability, and connector descriptors.

pub mod checkpoint;
pub mod connector;
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
