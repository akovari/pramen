//! Orchestrates work items through the ledger: reuse completed results,
//! reconcile submitted ones, dispatch only what is genuinely new, and
//! validate every output against the declared JSON Schema.

use crate::ledger::{Ledger, RecordedResult, WorkState};
use crate::provider::Provider;
use crate::workkey::WorkSpec;
use anyhow::Result;

#[derive(Debug, Default)]
pub struct RunStats {
    pub reused: u64,
    pub dispatched: u64,
    pub reconciled: u64,
    pub invalid: u64,
    pub failed: u64,
    pub input_tokens: u64,
    pub output_tokens: u64,
}

pub fn process(ledger: &Ledger, provider: &dyn Provider, specs: &[WorkSpec]) -> Result<RunStats> {
    let mut stats = RunStats::default();

    for spec in specs {
        let key = spec.work_key();
        ledger.upsert_pending(&key, &serde_json::to_string(spec)?)?;

        match ledger.state(&key)? {
            Some(WorkState::Completed(_)) => {
                stats.reused += 1;
                continue;
            }
            Some(WorkState::Submitted { request_id }) => {
                // Online requests cannot be looked up after a crash; the
                // honest contract is to surface the ambiguity and re-dispatch.
                // Provider batch jobs (S2.1) reconcile by job/record ID here.
                stats.reconciled += 1;
                eprintln!("reconciling ambiguous submission {request_id} for {key}");
            }
            _ => {}
        }

        // Record intent-to-dispatch before the call so a crash leaves a trace.
        let intent_id = format!("intent-{}", uuid::Uuid::new_v4());
        ledger.mark_submitted(&key, &intent_id)?;

        match provider.invoke(spec) {
            Ok(response) => {
                stats.dispatched += 1;
                stats.input_tokens += response.input_tokens;
                stats.output_tokens += response.output_tokens;

                let validation = match validate(&spec.output_schema, &response.output) {
                    Ok(()) => "valid".to_owned(),
                    Err(reason) => {
                        stats.invalid += 1;
                        format!("invalid: {reason}")
                    }
                };
                ledger.complete(
                    &key,
                    &RecordedResult {
                        output: response.output,
                        provider: provider.name().to_owned(),
                        model: spec.model.clone(),
                        request_id: response.request_id,
                        input_tokens: response.input_tokens,
                        output_tokens: response.output_tokens,
                        validation,
                    },
                )?;
            }
            Err(error) => {
                stats.failed += 1;
                ledger.mark_failed(&key, &error.to_string())?;
            }
        }
    }

    Ok(stats)
}

fn validate(schema: &serde_json::Value, output: &serde_json::Value) -> Result<(), String> {
    let compiled = jsonschema::validator_for(schema).map_err(|e| e.to_string())?;
    let errors: Vec<String> = compiled
        .iter_errors(output)
        .map(|e| e.to_string())
        .collect();
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors.join("; "))
    }
}

/// The golden output schema for the support-ticket extraction task.
pub fn ticket_schema() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["category", "priority", "rationale"],
        "properties": {
            "category": {"type": "string", "enum": ["incident", "billing", "question"]},
            "priority": {"type": "string", "enum": ["high", "normal", "low"]},
            "rationale": {"type": "string", "maxLength": 2000}
        }
    })
}

pub fn ticket_specs(provider: &str, model: &str, tickets: &[(&str, &str)]) -> Vec<WorkSpec> {
    tickets
        .iter()
        .map(|(id, description)| WorkSpec {
            operation: "ai.extract".into(),
            prompt_revision: "tickets-v1".into(),
            inputs: serde_json::json!({"id": id, "description": description}),
            output_schema: ticket_schema(),
            provider: provider.into(),
            model: model.into(),
            params: serde_json::json!({"temperature": 0}),
        })
        .collect()
}
