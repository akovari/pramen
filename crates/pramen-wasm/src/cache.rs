//! Precompiled artifact cache keyed by component digest and engine version.

use crate::error::WasmError;
use crate::host::PreparedComponent;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

/// Engine identity for cache keys — digest alone is insufficient across
/// wasmtime upgrades or limit-configuration changes.
const ENGINE_TAG: &str = concat!("wasmtime-38:", env!("CARGO_PKG_VERSION"));

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CacheKey {
    digest: [u8; 32],
    engine_tag: &'static str,
}

/// A digest-keyed cache of [`PreparedComponent`] artifacts.
pub struct ArtifactCache {
    entries: Mutex<HashMap<CacheKey, Arc<PreparedComponent>>>,
}

impl ArtifactCache {
    /// Create an empty cache.
    #[must_use]
    pub fn new() -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
        }
    }

    /// Load a component from disk, reusing a cached [`PreparedComponent`] when
    /// the digest and engine tag match.
    ///
    /// # Errors
    ///
    /// Returns [`WasmError::Load`] when the file cannot be read or parsed.
    pub fn load_path(&self, path: impl AsRef<Path>) -> Result<Arc<PreparedComponent>, WasmError> {
        let bytes = std::fs::read(path.as_ref())
            .map_err(|e| WasmError::load(format!("read {}: {e}", path.as_ref().display())))?;
        self.load_bytes(&bytes)
    }

    /// Load from bytes with digest-keyed caching.
    ///
    /// # Errors
    ///
    /// Returns [`WasmError::Load`] when parsing fails.
    pub fn load_bytes(&self, wasm: &[u8]) -> Result<Arc<PreparedComponent>, WasmError> {
        let digest = digest_bytes(wasm);
        let key = CacheKey {
            digest,
            engine_tag: ENGINE_TAG,
        };
        let mut entries = self
            .entries
            .lock()
            .map_err(|_| WasmError::load("artifact cache lock poisoned"))?;
        if let Some(hit) = entries.get(&key) {
            return Ok(Arc::clone(hit));
        }
        let prepared = Arc::new(PreparedComponent::from_bytes(wasm)?);
        entries.insert(key, Arc::clone(&prepared));
        Ok(prepared)
    }

    /// Number of distinct prepared artifacts currently cached.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries
            .lock()
            .map(|entries| entries.len())
            .unwrap_or(0)
    }

    /// Whether the cache has no entries.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Default for ArtifactCache {
    fn default() -> Self {
        Self::new()
    }
}

/// SHA-256 digest of component bytes.
#[must_use]
pub fn digest_bytes(wasm: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(wasm);
    hasher.finalize().into()
}

/// Resolve a component path relative to a pipeline document directory.
///
/// Absolute paths pass through unchanged; relative paths join against
/// `pipeline_dir`.
#[must_use]
pub fn resolve_component_path(pipeline_dir: &Path, component: &str) -> PathBuf {
    let path = Path::new(component);
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        pipeline_dir.join(path)
    }
}
