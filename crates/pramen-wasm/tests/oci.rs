//! Offline OCI distribution tests (X1.4): mock pull, allow-list, signature hook.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use pramen_core::spec::OciReference;
use pramen_wasm::{
    ArtifactCache, MockOciFetcher, OciAllowlist, OciLoadOptions, RejectSignatureVerifier,
    S1_4_FIXTURE, WasmError, load_component,
};
use std::path::Path;
use std::sync::Arc;

fn fixture_bytes() -> Vec<u8> {
    std::fs::read(S1_4_FIXTURE).expect("read fixture")
}

fn sample_reference() -> OciReference {
    OciReference {
        registry: "localhost:5000".to_owned(),
        repository: "pramen/s1-4".to_owned(),
        digest: "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
            .to_owned(),
    }
}

#[tokio::test]
async fn mock_oci_pull_loads_into_artifact_cache() {
    let reference = sample_reference();
    let fetcher = Arc::new(MockOciFetcher::new());
    fetcher.insert(&reference, fixture_bytes());
    let allowlist = OciAllowlist::from_entries([reference.digest.as_str()]);
    let options = OciLoadOptions {
        allowlist,
        fetcher,
        verifier: Arc::new(pramen_wasm::AllowAllSignatureVerifier),
    };
    let cache = ArtifactCache::new();
    let prepared = load_component(&cache, Path::new("."), &reference.as_oci_url(), &options)
        .await
        .expect("load oci component");
    assert_eq!(cache.len(), 1);
    assert_eq!(
        prepared.digest(),
        pramen_wasm::digest_bytes(&fixture_bytes())
    );
}

#[tokio::test]
async fn allowlist_rejects_unlisted_digest() {
    let reference = sample_reference();
    let fetcher = Arc::new(MockOciFetcher::new());
    fetcher.insert(&reference, fixture_bytes());
    let options = OciLoadOptions {
        allowlist: OciAllowlist::empty(),
        fetcher,
        verifier: Arc::new(pramen_wasm::AllowAllSignatureVerifier),
    };
    let cache = ArtifactCache::new();
    let error =
        match load_component(&cache, Path::new("."), &reference.as_oci_url(), &options).await {
            Err(error) => error,
            Ok(_) => panic!("expected allow-list rejection"),
        };
    assert!(matches!(error, WasmError::NotAllowlisted { .. }), "{error}");
}

#[tokio::test]
async fn signature_hook_is_invoked_and_can_reject() {
    let reference = sample_reference();
    let fetcher = Arc::new(MockOciFetcher::new());
    fetcher.insert(&reference, fixture_bytes());
    let options = OciLoadOptions {
        allowlist: OciAllowlist::from_entries(["localhost:5000/pramen"]),
        fetcher,
        verifier: Arc::new(RejectSignatureVerifier::new("cosign stub reject")),
    };
    let cache = ArtifactCache::new();
    let error =
        match load_component(&cache, Path::new("."), &reference.as_oci_url(), &options).await {
            Err(error) => error,
            Ok(_) => panic!("expected signature rejection"),
        };
    match error {
        WasmError::Signature(message) => {
            assert!(message.contains("cosign stub reject"), "{message}");
        }
        other => panic!("expected Signature, got {other}"),
    }
}

#[tokio::test]
async fn local_path_still_loads_without_allowlist() {
    let options = OciLoadOptions::new(OciAllowlist::empty());
    let cache = ArtifactCache::new();
    let prepared = load_component(&cache, Path::new("."), S1_4_FIXTURE, &options)
        .await
        .expect("load path");
    assert_eq!(
        prepared.digest(),
        pramen_wasm::digest_bytes(&fixture_bytes())
    );
}
