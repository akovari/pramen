//! WebAssembly component transforms: WIT ABI, resource limits, and the
//! precompiled artifact cache (tasks X1.1–X1.2).

#![forbid(unsafe_code)]

mod cache;
mod error;
mod host;
mod ipc;
mod limits;
mod transform;

pub use cache::{ArtifactCache, digest_bytes, resolve_component_path};
pub use error::WasmError;
pub use host::PreparedComponent;
pub use ipc::{decode_stream, encode_batch};
pub use limits::{InvocationLimits, ResourceLimits};
pub use transform::WasmTransform;

/// Path to the S1.4 conformance fixture checked into this crate.
pub const S1_4_FIXTURE: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/fixtures/s1_4_guest.wasm");
