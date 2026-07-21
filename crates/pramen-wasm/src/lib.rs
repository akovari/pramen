//! WebAssembly component transforms: WIT ABI, resource limits, the
//! precompiled artifact cache, and OCI distribution (tasks X1.1–X1.4).

#![forbid(unsafe_code)]

mod cache;
mod error;
mod host;
mod ipc;
mod limits;
mod oci;
mod transform;

pub use cache::{ArtifactCache, digest_bytes, resolve_component_path};
pub use error::WasmError;
pub use host::PreparedComponent;
pub use ipc::{decode_stream, encode_batch};
pub use limits::{InvocationLimits, ResourceLimits};
pub use oci::{
    AllowAllSignatureVerifier, MockOciFetcher, OciAllowlist, OciClientFetcher, OciFetcher,
    OciLoadOptions, RejectSignatureVerifier, SignatureVerifier, WASM_OCI_ALLOWLIST_ENV,
    load_component,
};
pub use transform::WasmTransform;

/// Path to the S1.4 conformance fixture checked into this crate.
pub const S1_4_FIXTURE: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/fixtures/s1_4_guest.wasm");
