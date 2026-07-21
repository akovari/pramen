//! Error types for the semantic operator stack.

/// How a provider call failed — the documented, typed classification
/// every adapter maps its transport and protocol failures onto (P1.19).
///
/// The class matters operationally: `Timeout`, `Throttled`, and
/// `Transport` are environmental and safely retryable (the ledger's
/// claim/complete protocol makes re-dispatch cost-free bookkeeping),
/// while `Protocol` and `Server` usually mean a misconfigured endpoint,
/// model, or a provider-side incident worth a human look.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderFault {
    /// The request exceeded its deadline.
    Timeout,
    /// The provider rejected the request for rate or quota reasons
    /// (HTTP 429 and equivalents).
    Throttled,
    /// The connection failed (refused, reset, DNS).
    Transport,
    /// The provider answered, but not in the shape the protocol promises.
    Protocol,
    /// The provider reported a failure (HTTP 5xx and other non-success).
    Server,
}

impl std::fmt::Display for ProviderFault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Timeout => "timeout",
            Self::Throttled => "throttled",
            Self::Transport => "transport",
            Self::Protocol => "protocol",
            Self::Server => "server",
        })
    }
}

/// Failures in the AI workstream.
#[derive(Debug, thiserror::Error)]
pub enum AiError {
    /// The inference ledger could not be read or written.
    #[error("ledger: {0}")]
    Ledger(String),
    /// A provider call failed; `fault` carries the typed classification.
    #[error("provider `{provider}` ({fault}): {message}")]
    Provider {
        /// Provider adapter identifier.
        provider: String,
        /// Typed failure class.
        fault: ProviderFault,
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

impl From<rusqlite::Error> for AiError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Ledger(error.to_string())
    }
}

impl From<tokio_postgres::Error> for AiError {
    fn from(error: tokio_postgres::Error) -> Self {
        Self::Ledger(error.to_string())
    }
}
