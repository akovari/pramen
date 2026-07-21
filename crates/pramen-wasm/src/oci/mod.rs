//! OCI distribution for WASM components (task X1.4).
//!
//! Pipeline `component` values may be `oci://registry/repo@sha256:…`. The host
//! allow-lists digests/prefixes, pulls once into the digest-keyed artifact
//! cache, and runs a [`SignatureVerifier`] hook before load. Full
//! cosign/sigstore verification is intentionally a later integration; the
//! hook is the production injection seam.

mod allowlist;
mod pull;
mod signature;

pub use allowlist::{OciAllowlist, WASM_OCI_ALLOWLIST_ENV};
pub use pull::{MockOciFetcher, OciClientFetcher, OciFetcher};
pub use signature::{AllowAllSignatureVerifier, RejectSignatureVerifier, SignatureVerifier};

use crate::cache::ArtifactCache;
use crate::error::WasmError;
use crate::host::PreparedComponent;
use allowlist::OciAllowlist as Allowlist;
use pramen_core::spec::{ComponentRef, OciReference};
use std::path::Path;
use std::sync::Arc;

/// Inputs required to resolve an OCI (or path) component reference.
pub struct OciLoadOptions {
    /// Digest / repository-prefix allow-list (fail closed when empty for OCI).
    pub allowlist: Allowlist,
    /// Registry pull implementation.
    pub fetcher: Arc<dyn OciFetcher>,
    /// Signature verification hook (default [`AllowAllSignatureVerifier`]).
    pub verifier: Arc<dyn SignatureVerifier>,
}

impl OciLoadOptions {
    /// Build options from an allow-list with the default HTTPS fetcher and
    /// allow-all signature verifier.
    #[must_use]
    pub fn new(allowlist: Allowlist) -> Self {
        Self {
            allowlist,
            fetcher: Arc::new(OciClientFetcher::new()),
            verifier: Arc::new(AllowAllSignatureVerifier),
        }
    }
}

/// Resolve `component` (path or OCI digest) and load through `cache`.
///
/// # Errors
///
/// Returns [`WasmError`] when the reference is invalid, not allow-listed,
/// the pull/signature step fails, or the artifact cannot be prepared.
pub async fn load_component(
    cache: &ArtifactCache,
    pipeline_dir: &Path,
    component: &str,
    oci: &OciLoadOptions,
) -> Result<Arc<PreparedComponent>, WasmError> {
    match ComponentRef::parse(component).map_err(|e| WasmError::load(e.to_string()))? {
        ComponentRef::Path(path) => {
            let resolved = crate::cache::resolve_component_path(pipeline_dir, &path);
            cache.load_path(resolved)
        }
        ComponentRef::Oci(reference) => load_oci(cache, &reference, oci).await,
    }
}

async fn load_oci(
    cache: &ArtifactCache,
    reference: &OciReference,
    oci: &OciLoadOptions,
) -> Result<Arc<PreparedComponent>, WasmError> {
    if !oci.allowlist.permits(reference) {
        return Err(WasmError::NotAllowlisted {
            reference: reference.as_oci_url(),
        });
    }
    let bytes = oci.fetcher.pull(reference).await?;
    oci.verifier.verify(reference, &bytes)?;
    cache.load_bytes(&bytes)
}
