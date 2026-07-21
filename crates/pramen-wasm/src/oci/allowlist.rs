//! Allow-list of OCI digests and registry/repository prefixes.

use pramen_core::spec::OciReference;

/// Environment variable merged with `runtime.wasmOciAllowlist`.
pub const WASM_OCI_ALLOWLIST_ENV: &str = "PRAMEN_WASM_OCI_ALLOWLIST";

/// Digests and/or `registry/repository` prefixes permitted for OCI pulls.
///
/// An empty allow-list denies every OCI pull (fail closed). Entries are:
/// - digests: `sha256:` + 64 hex digits (or bare 64-hex);
/// - prefixes: `ghcr.io/acme/` or `ghcr.io/acme/enrich` matching
///   [`OciReference::repository_path`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OciAllowlist {
    entries: Vec<AllowEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum AllowEntry {
    Digest(String),
    Prefix(String),
}

impl OciAllowlist {
    /// Empty allow-list (denies all OCI pulls).
    #[must_use]
    pub fn empty() -> Self {
        Self::default()
    }

    /// Parse entries from a list of allow-list strings (spec + tests).
    #[must_use]
    pub fn from_entries<I, S>(entries: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut parsed = Vec::new();
        for entry in entries {
            if let Some(item) = parse_entry(entry.as_ref()) {
                parsed.push(item);
            }
        }
        Self { entries: parsed }
    }

    /// Merge pipeline `runtime.wasmOciAllowlist` with [`WASM_OCI_ALLOWLIST_ENV`].
    #[must_use]
    pub fn from_runtime_and_env(runtime_entries: &[String]) -> Self {
        let mut combined: Vec<String> = runtime_entries.to_vec();
        if let Ok(env) = std::env::var(WASM_OCI_ALLOWLIST_ENV) {
            for part in env.split(',') {
                let trimmed = part.trim();
                if !trimmed.is_empty() {
                    combined.push(trimmed.to_owned());
                }
            }
        }
        Self::from_entries(combined)
    }

    /// Whether any entry permits `reference`.
    #[must_use]
    pub fn permits(&self, reference: &OciReference) -> bool {
        let path = reference.repository_path();
        self.entries.iter().any(|entry| match entry {
            AllowEntry::Digest(digest) => digest == &reference.digest,
            AllowEntry::Prefix(prefix) => {
                path == *prefix || path.starts_with(&format!("{prefix}/"))
            }
        })
    }

    /// Number of configured entries.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether no entries are configured.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

fn parse_entry(raw: &str) -> Option<AllowEntry> {
    let raw = raw.trim();
    if raw.is_empty() {
        return None;
    }
    if let Some(hex) = raw.strip_prefix("sha256:")
        && hex.len() == 64
        && hex.chars().all(|c| c.is_ascii_hexdigit())
    {
        return Some(AllowEntry::Digest(format!(
            "sha256:{}",
            hex.to_ascii_lowercase()
        )));
    }
    if raw.len() == 64 && raw.chars().all(|c| c.is_ascii_hexdigit()) {
        return Some(AllowEntry::Digest(format!(
            "sha256:{}",
            raw.to_ascii_lowercase()
        )));
    }
    Some(AllowEntry::Prefix(raw.trim_end_matches('/').to_owned()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_ref() -> OciReference {
        OciReference {
            registry: "ghcr.io".to_owned(),
            repository: "acme/enrich".to_owned(),
            digest: "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                .to_owned(),
        }
    }

    #[test]
    fn digest_entry_permits_matching_ref() {
        let list = OciAllowlist::from_entries([
            "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        ]);
        assert!(list.permits(&sample_ref()));
    }

    #[test]
    fn prefix_entry_permits_repo() {
        let list = OciAllowlist::from_entries(["ghcr.io/acme"]);
        assert!(list.permits(&sample_ref()));
    }

    #[test]
    fn empty_allowlist_denies() {
        assert!(!OciAllowlist::empty().permits(&sample_ref()));
    }

    #[test]
    fn unrelated_digest_denied() {
        let list = OciAllowlist::from_entries([
            "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
        ]);
        assert!(!list.permits(&sample_ref()));
    }
}
