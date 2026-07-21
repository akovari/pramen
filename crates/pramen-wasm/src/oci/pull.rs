//! OCI Distribution pull of WASM component layers.

use crate::error::WasmError;
use async_trait::async_trait;
use oci_client::Reference;
use oci_client::client::{Client, ClientConfig, ClientProtocol};
use oci_client::manifest::{IMAGE_LAYER_MEDIA_TYPE, WASM_LAYER_MEDIA_TYPE};
use oci_client::secrets::RegistryAuth;
use pramen_core::spec::OciReference;
use std::collections::HashMap;
use std::sync::Mutex;

/// Accepted layer media types for WASM / generic OCI artifacts.
const ACCEPTED_LAYERS: &[&str] = &[
    WASM_LAYER_MEDIA_TYPE,
    "application/vnd.wasm.component.v1.wasm",
    "application/vnd.wasm.module.v1.wasm",
    "application/vnd.wasm.content.layer.v1+wasm",
    IMAGE_LAYER_MEDIA_TYPE,
    "application/vnd.oci.image.layer.v1.tar",
    "application/vnd.oci.image.layer.v1.tar+gzip",
    "application/octet-stream",
];

/// Pulls WASM component bytes for an [`OciReference`].
#[async_trait]
pub trait OciFetcher: Send + Sync {
    /// Fetch component bytes for `reference` (already digest-pinned).
    async fn pull(&self, reference: &OciReference) -> Result<Vec<u8>, WasmError>;
}

/// Production fetcher backed by [`oci_client::Client`].
pub struct OciClientFetcher {
    /// Force HTTP (for loopback registries in tests).
    http_for_loopback: bool,
}

impl OciClientFetcher {
    /// HTTPS client; loopback hosts (`localhost`, `127.0.0.1`) use HTTP.
    #[must_use]
    pub fn new() -> Self {
        Self {
            http_for_loopback: true,
        }
    }

    fn client_for(&self, reference: &OciReference) -> Client {
        let loopback = reference.registry.starts_with("localhost")
            || reference.registry.starts_with("127.0.0.1");
        let protocol = if self.http_for_loopback && loopback {
            ClientProtocol::Http
        } else {
            ClientProtocol::Https
        };
        Client::new(ClientConfig {
            protocol,
            ..ClientConfig::default()
        })
    }
}

impl Default for OciClientFetcher {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl OciFetcher for OciClientFetcher {
    async fn pull(&self, reference: &OciReference) -> Result<Vec<u8>, WasmError> {
        let image_ref: Reference = reference
            .as_distribution_reference()
            .parse()
            .map_err(|e| WasmError::oci(format!("invalid OCI reference: {e}")))?;
        let client = self.client_for(reference);
        let auth = RegistryAuth::Anonymous;
        // Ensure the registry agrees the digest exists before layer pull.
        let _ = client
            .fetch_manifest_digest(&image_ref, &auth)
            .await
            .map_err(|e| WasmError::oci(format!("manifest digest fetch failed: {e}")))?;
        let image = client
            .pull(&image_ref, &auth, ACCEPTED_LAYERS.to_vec())
            .await
            .map_err(|e| WasmError::oci(format!("OCI pull failed: {e}")))?;
        let layer = image
            .layers
            .into_iter()
            .next()
            .ok_or_else(|| WasmError::oci("OCI artifact has no layers".to_owned()))?;
        Ok(layer.data.to_vec())
    }
}

/// In-memory fetcher for offline unit tests.
#[derive(Debug, Default)]
pub struct MockOciFetcher {
    artifacts: Mutex<HashMap<String, Vec<u8>>>,
}

impl MockOciFetcher {
    /// Empty mock.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register bytes served for `reference`.
    pub fn insert(&self, reference: &OciReference, bytes: Vec<u8>) {
        self.artifacts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(reference.as_distribution_reference(), bytes);
    }
}

#[async_trait]
impl OciFetcher for MockOciFetcher {
    async fn pull(&self, reference: &OciReference) -> Result<Vec<u8>, WasmError> {
        self.artifacts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&reference.as_distribution_reference())
            .cloned()
            .ok_or_else(|| {
                WasmError::oci(format!(
                    "mock registry miss for {}",
                    reference.as_distribution_reference()
                ))
            })
    }
}
