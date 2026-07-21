//! Typed errors from the WASM host boundary.

/// A failure loading, invoking, or decoding a WASM component transform.
#[derive(Debug, thiserror::Error)]
pub enum WasmError {
    /// The component bytes could not be parsed or linked.
    #[error("component load failed: {0}")]
    Load(String),
    /// A host-enforced limit was exceeded before or during invocation.
    #[error("limit exceeded: {0}")]
    Limit(String),
    /// The guest returned a structured error string (batch failure).
    #[error("guest error: {0}")]
    Guest(String),
    /// The guest trapped (fuel, memory, epoch deadline, or wasm fault).
    #[error("guest trap: {0}")]
    Trap(String),
    /// Arrow IPC encode or decode failed.
    #[error("arrow ipc: {0}")]
    Ipc(String),
    /// OCI pull failed (network, manifest, or empty artifact).
    #[error("OCI pull failed: {0}")]
    Oci(String),
    /// The OCI reference is not on the configured allow-list.
    #[error("OCI component `{reference}` is not on the WASM OCI allow-list")]
    NotAllowlisted {
        /// The rejected `oci://…@sha256:…` reference.
        reference: String,
    },
    /// Signature verification hook rejected the artifact.
    #[error("OCI signature verification failed: {0}")]
    Signature(String),
}

impl WasmError {
    pub(crate) fn load(message: impl Into<String>) -> Self {
        Self::Load(message.into())
    }

    pub(crate) fn limit(message: impl Into<String>) -> Self {
        Self::Limit(message.into())
    }

    pub(crate) fn ipc(message: impl Into<String>) -> Self {
        Self::Ipc(message.into())
    }

    pub(crate) fn trap_message(message: impl Into<String>) -> Self {
        Self::Trap(message.into())
    }

    pub(crate) fn oci(message: impl Into<String>) -> Self {
        Self::Oci(message.into())
    }
}
