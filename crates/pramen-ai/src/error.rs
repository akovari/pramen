//! Error types for the semantic operator stack.

/// Failures in the AI workstream.
#[derive(Debug, thiserror::Error)]
pub enum AiError {
    /// The inference ledger could not be read or written.
    #[error("ledger: {0}")]
    Ledger(#[from] rusqlite::Error),
    /// A provider call failed (network, authentication, throttling).
    #[error("provider `{provider}`: {message}")]
    Provider {
        /// Provider adapter identifier.
        provider: String,
        /// Human-readable failure description.
        message: String,
    },
    /// A record exceeded a configured budget before dispatch.
    #[error("budget exceeded: {0}")]
    BudgetExceeded(String),
    /// Model output failed validation against the declared schema.
    #[error("invalid model output: {0}")]
    InvalidOutput(String),
    /// The pipeline requests something this build does not support yet.
    #[error("unsupported: {0}")]
    Unsupported(String),
    /// Input data could not be converted for the model.
    #[error("input: {0}")]
    Input(String),
}
