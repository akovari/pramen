//! The `pramen inspect` command family (E1.4).

use pramen_core::connector::{ConnectorDescriptor, builtin, builtins};

/// Format the built-in connector list for human-readable inspect output.
#[must_use]
pub fn format_connector_list() -> String {
    let mut lines = Vec::with_capacity(builtins().len() + 1);
    lines.push(format!("connectors: {} built-in", builtins().len()));
    for connector in builtins() {
        lines.push(format_connector_line(connector));
    }
    lines.join("\n")
}

/// Format one connector for human-readable inspect output.
#[must_use]
pub fn format_connector_detail(connector: &ConnectorDescriptor) -> String {
    let mut lines = vec![
        format!("connector: {}", connector.id),
        format!("  kind: {}", connector.kind.as_str()),
        format!("  support: {}", connector.support_level.as_str()),
        format!("  summary: {}", connector.summary),
        format!("  delivery: {}", connector.delivery.summary()),
    ];
    if !connector.modes.is_empty() {
        lines.push(format!("  modes: {}", connector.modes.join(", ")));
    }
    if !connector.schemes.is_empty() {
        lines.push(format!("  schemes: {}", connector.schemes.join(", ")));
    }
    lines.push(format!("  notes: {}", connector.notes));
    lines.join("\n")
}

fn format_connector_line(connector: &ConnectorDescriptor) -> String {
    format!(
        "  {} ({}, {}) — {}",
        connector.id,
        connector.kind.as_str(),
        connector.support_level.as_str(),
        connector.summary
    )
}

/// Resolve inspect connector output (text or JSON).
///
/// # Errors
///
/// Returns a message when `id` is set but unknown.
pub fn inspect_connector(id: Option<&str>, json: bool) -> Result<String, String> {
    if let Some(id) = id {
        let connector = builtin(id).ok_or_else(|| {
            format!(
                "unknown connector `{id}`; known ids: {}",
                builtins()
                    .iter()
                    .map(|c| c.id)
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        })?;
        if json {
            serde_json::to_string_pretty(connector)
                .map_err(|error| format!("failed to render JSON: {error}"))
        } else {
            Ok(format_connector_detail(connector))
        }
    } else if json {
        serde_json::to_string_pretty(builtins())
            .map_err(|error| format!("failed to render JSON: {error}"))
    } else {
        Ok(format_connector_list())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn list_includes_postgres_and_flight() {
        let text = format_connector_list();
        assert!(text.contains("sink.postgres"));
        assert!(text.contains("sink.flightSql"));
        assert!(text.contains("source.objectStore"));
    }

    #[test]
    fn detail_unknown_is_error() {
        let err = inspect_connector(Some("nope"), false).unwrap_err();
        assert!(err.contains("unknown connector"));
    }

    #[test]
    fn json_list_is_array() {
        let text = inspect_connector(None, true).unwrap();
        let value: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert!(value.is_array());
        assert!(value.as_array().unwrap().len() >= 3);
    }

    #[test]
    fn json_detail_has_id() {
        let text = inspect_connector(Some("sink.postgres"), true).unwrap();
        let value: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(value["id"], "sink.postgres");
        assert_eq!(value["supportLevel"], "supported");
    }
}
