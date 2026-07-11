//! Error types for specification parsing and validation.

/// A semantic problem found in an otherwise well-formed document.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidationIssue {
    /// Dotted path to the offending element, e.g. `spec.transforms[1].model`.
    pub path: String,
    /// Human-readable description of the problem.
    pub message: String,
}

impl std::fmt::Display for ValidationIssue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.path, self.message)
    }
}

/// Failure to accept a pipeline document.
#[derive(Debug, thiserror::Error)]
pub enum SpecError {
    /// The document is not valid YAML or does not match the schema.
    #[error("pipeline document is not valid: {0}")]
    Parse(String),
    /// The document parsed but is semantically invalid.
    #[error("pipeline document has {} validation issue(s)", .0.len())]
    Invalid(Vec<ValidationIssue>),
}

impl SpecError {
    pub(crate) fn from_parse(error: serde_yaml_ng::Error) -> Self {
        // The serde_yaml_ng error Display already includes line/column
        // location where available.
        Self::Parse(error.to_string())
    }

    /// All validation issues, or an empty slice for parse errors.
    #[must_use]
    pub fn issues(&self) -> &[ValidationIssue] {
        match self {
            Self::Parse(_) => &[],
            Self::Invalid(issues) => issues,
        }
    }
}
