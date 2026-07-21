//! Pipeline specification: the versioned, human-authored YAML surface.
//!
//! The specification is an API. Parsing is strict (unknown fields are
//! errors), validation is semantic and reports every problem with a path,
//! and the accepted surface is published as a generated JSON Schema.
//!
//! The v1alpha1 schema describes a linear pipeline — one source, an ordered
//! list of transforms, one sink — while the internal plan remains free to
//! become a DAG later without changing this surface.

mod component_ref;
mod error;
mod types;
mod validate;

pub use component_ref::{ComponentRef, ComponentRefError, OciReference};
pub use error::{SpecError, ValidationIssue};
pub use types::{
    AiBreaker, AiBudget, AiOutput, AiTransform, AiValidation, ApiVersion, AutoDispatchHints,
    CheckpointSpec, ExecutionMode, FieldSpec, FieldType, FormatSpec, InvalidPolicy, Kind, Metadata,
    ModelBatchSpec, ModelSpec, PipelineSpec, PipelineSpecBody, ResidencySpec, RuntimeSpec,
    SinkMode, SinkSpec, SourceSpec, SqlTransform, TransformSpec, WasmLimitsSpec, WasmTransform,
};

/// Parse a YAML document into a validated [`PipelineSpec`].
///
/// This is the single entry point the CLI and runtime use: it performs
/// strict deserialization followed by semantic validation, so a returned
/// spec is always safe to plan.
///
/// # Errors
///
/// Returns [`SpecError::Parse`] when the document is not valid YAML or does
/// not match the schema, and [`SpecError::Invalid`] with every semantic
/// issue found otherwise.
pub fn parse(yaml: &str) -> Result<PipelineSpec, SpecError> {
    let spec: PipelineSpec = serde_yaml_ng::from_str(yaml).map_err(SpecError::from_parse)?;
    let issues = validate::validate(&spec);
    if issues.is_empty() {
        Ok(spec)
    } else {
        Err(SpecError::Invalid(issues))
    }
}

/// The generated JSON Schema for the v1alpha1 pipeline document.
///
/// Published as an artifact so editors and CI can validate pipeline files
/// without running Pramen.
#[must_use]
pub fn json_schema() -> serde_json::Value {
    serde_json::to_value(schemars::schema_for!(PipelineSpec)).unwrap_or_default()
}

#[cfg(test)]
mod tests;
