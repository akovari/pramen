//! Signature verification hook for OCI-distributed WASM components.
//!
//! Production can inject a real cosign/sigstore verifier later; the default
//! [`AllowAllSignatureVerifier`] accepts every artifact after allow-list and
//! pull succeed. Tests use [`RejectSignatureVerifier`] to prove the hook runs.

use crate::error::WasmError;
use pramen_core::spec::OciReference;

/// Verifies an OCI-pulled artifact before it enters the artifact cache.
///
/// Implementations must not log artifact bytes (may contain sensitive guest
/// logic); failures should be typed via [`WasmError::Signature`].
pub trait SignatureVerifier: Send + Sync {
    /// Verify `artifact` bytes for `reference`.
    ///
    /// # Errors
    ///
    /// Returns [`WasmError::Signature`] when verification fails.
    fn verify(&self, reference: &OciReference, artifact: &[u8]) -> Result<(), WasmError>;
}

/// Default verifier: accepts every artifact (signature check deferred).
#[derive(Debug, Default, Clone, Copy)]
pub struct AllowAllSignatureVerifier;

impl SignatureVerifier for AllowAllSignatureVerifier {
    fn verify(&self, _reference: &OciReference, _artifact: &[u8]) -> Result<(), WasmError> {
        Ok(())
    }
}

/// Test/stub verifier that always rejects, proving the hook is invoked.
#[derive(Debug, Clone)]
pub struct RejectSignatureVerifier {
    /// Human-readable rejection reason.
    pub reason: String,
}

impl RejectSignatureVerifier {
    /// Create a reject-all verifier with `reason`.
    #[must_use]
    pub fn new(reason: impl Into<String>) -> Self {
        Self {
            reason: reason.into(),
        }
    }
}

impl SignatureVerifier for RejectSignatureVerifier {
    fn verify(&self, reference: &OciReference, _artifact: &[u8]) -> Result<(), WasmError> {
        Err(WasmError::Signature(format!(
            "{} ({reference})",
            self.reason
        )))
    }
}
