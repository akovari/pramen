//! Parsing of `type: wasm` `component` values: local paths or OCI digests.
//!
//! OCI references must be content-addressed (`@sha256:…`). Tag-only refs are
//! rejected so pipelines cannot drift across mutable tags (architecture §15).

use std::fmt;

/// Where a WASM component artifact is loaded from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ComponentRef {
    /// Filesystem path, absolute or relative to the pipeline document.
    Path(String),
    /// OCI artifact pinned by digest.
    Oci(OciReference),
}

/// An OCI registry reference pinned to a content digest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OciReference {
    /// Registry host and optional port (`ghcr.io`, `localhost:5000`).
    pub registry: String,
    /// Repository path within the registry (`org/wasm-transform`).
    pub repository: String,
    /// Digest in canonical form `sha256:` + 64 lowercase hex digits.
    pub digest: String,
}

impl OciReference {
    /// `registry/repository` without the digest (for allow-list prefix checks).
    #[must_use]
    pub fn repository_path(&self) -> String {
        format!("{}/{}", self.registry, self.repository)
    }

    /// Canonical `oci://…@sha256:…` form.
    #[must_use]
    pub fn as_oci_url(&self) -> String {
        format!(
            "oci://{}/{}@{}",
            self.registry, self.repository, self.digest
        )
    }

    /// Form accepted by OCI clients (`registry/repo@sha256:…`).
    #[must_use]
    pub fn as_distribution_reference(&self) -> String {
        format!("{}/{}@{}", self.registry, self.repository, self.digest)
    }
}

impl fmt::Display for OciReference {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.as_oci_url())
    }
}

/// Failure to parse a `component` string as a path or OCI digest reference.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ComponentRefError {
    /// An `oci://` reference did not pin a sha256 digest.
    #[error(
        "OCI component references must pin a sha256 digest \
         (oci://registry/repo@sha256:…); tag-only refs are not allowed"
    )]
    DigestRequired,
    /// An `oci://` reference was malformed beyond a missing digest.
    #[error("invalid OCI component reference: {0}")]
    Invalid(String),
}

impl ComponentRef {
    /// Parse a pipeline `component` string.
    ///
    /// Values starting with `oci://` are OCI references and must include
    /// `@sha256:` + 64 hex digits. Everything else is treated as a local path.
    ///
    /// # Errors
    ///
    /// Returns [`ComponentRefError`] when an `oci://` value is not digest-pinned
    /// or is otherwise malformed.
    pub fn parse(component: &str) -> Result<Self, ComponentRefError> {
        let trimmed = component.trim();
        if trimmed.is_empty() {
            return Ok(Self::Path(String::new()));
        }
        if let Some(rest) = trimmed.strip_prefix("oci://") {
            Ok(Self::Oci(parse_oci(rest)?))
        } else {
            Ok(Self::Path(trimmed.to_owned()))
        }
    }
}

fn parse_oci(rest: &str) -> Result<OciReference, ComponentRefError> {
    let Some((image, digest_part)) = rest.rsplit_once('@') else {
        return Err(ComponentRefError::DigestRequired);
    };
    let digest = normalize_digest(digest_part)?;
    let (registry, name) = image
        .split_once('/')
        .ok_or_else(|| ComponentRefError::Invalid("missing repository path".to_owned()))?;
    if registry.is_empty() {
        return Err(ComponentRefError::Invalid(
            "missing registry host".to_owned(),
        ));
    }
    // Optional `:tag` before `@digest` is ignored; the digest is authoritative.
    let repository = match name.rsplit_once(':') {
        Some((repo, _tag)) if !repo.is_empty() => repo,
        _ => name,
    };
    if repository.is_empty() {
        return Err(ComponentRefError::Invalid(
            "missing repository path".to_owned(),
        ));
    }
    Ok(OciReference {
        registry: registry.to_owned(),
        repository: repository.to_owned(),
        digest,
    })
}

fn normalize_digest(raw: &str) -> Result<String, ComponentRefError> {
    let raw = raw.trim();
    let hex = raw
        .strip_prefix("sha256:")
        .or_else(|| raw.strip_prefix("SHA256:"))
        .ok_or(ComponentRefError::DigestRequired)?;
    if hex.len() != 64 || !hex.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(ComponentRefError::Invalid(
            "digest must be sha256: followed by 64 hex digits".to_owned(),
        ));
    }
    Ok(format!("sha256:{}", hex.to_ascii_lowercase()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_local_path() {
        assert_eq!(
            ComponentRef::parse("./guest.wasm").unwrap(),
            ComponentRef::Path("./guest.wasm".to_owned())
        );
    }

    #[test]
    fn parses_oci_digest_ref() {
        let reference = ComponentRef::parse(
            "oci://ghcr.io/acme/enrich@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        )
        .unwrap();
        let ComponentRef::Oci(oci) = reference else {
            panic!("expected oci");
        };
        assert_eq!(oci.registry, "ghcr.io");
        assert_eq!(oci.repository, "acme/enrich");
        assert!(oci.digest.starts_with("sha256:"));
        assert_eq!(oci.digest.len(), "sha256:".len() + 64);
    }

    #[test]
    fn rejects_tag_only_oci_ref() {
        let err = ComponentRef::parse("oci://ghcr.io/acme/enrich:latest").unwrap_err();
        assert_eq!(err, ComponentRefError::DigestRequired);
    }

    #[test]
    fn accepts_tag_with_digest_and_ignores_tag() {
        let reference = ComponentRef::parse(
            "oci://ghcr.io/acme/enrich:1.2.3@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        )
        .unwrap();
        let ComponentRef::Oci(oci) = reference else {
            panic!("expected oci");
        };
        assert_eq!(oci.repository, "acme/enrich");
    }
}
