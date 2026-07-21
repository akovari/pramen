//! The `pramen ai review` workflow (X1.6): list and export queued
//! records, and re-ingest human decisions into the ledger.

use pramen_ai::ledger::Ledger;
use pramen_ai::review::ReviewItem;
use pramen_core::checkpoint::is_postgres_url;
use serde_json::{Value, json};
use std::path::Path;

/// Resolve a work key from a possibly-shortened prefix, requiring
/// uniqueness so a decision can never land on the wrong record.
fn resolve_key(items: &[ReviewItem], prefix: &str) -> Result<String, String> {
    let matches: Vec<&ReviewItem> = items
        .iter()
        .filter(|item| item.work_key.starts_with(prefix))
        .collect();
    match matches.as_slice() {
        [one] => Ok(one.work_key.clone()),
        [] => Err(format!(
            "no pending review item matches `{prefix}`; `pramen ai review list` shows the queue"
        )),
        many => Err(format!(
            "`{prefix}` is ambiguous ({} pending items match); use more characters",
            many.len()
        )),
    }
}

/// `pramen ai review list`: the pending queue, oldest first.
///
/// # Errors
///
/// Returns a message when the ledger cannot be read.
pub fn list(ledger_location: &str) -> Result<(), String> {
    let ledger = open(ledger_location)?;
    let items = ledger.pending_reviews().map_err(|e| e.to_string())?;
    let (pending, accepted, rejected) = ledger.review_counts().map_err(|e| e.to_string())?;
    if items.is_empty() {
        println!("review queue is empty ({accepted} accepted, {rejected} rejected all-time)");
        return Ok(());
    }
    println!("{pending} pending ({accepted} accepted, {rejected} rejected all-time)");
    for item in items {
        println!();
        println!("  key:       {}", item.work_key);
        println!(
            "  transform: {}  queued: {}",
            item.transform_id, item.created_at
        );
        println!("  reason:    {}", item.reason);
        println!("  inputs:    {}", compact(&item.spec["inputs"], 120));
        if let Some(raw) = &item.raw_output {
            println!("  model out: {}", truncate(raw, 120));
        }
    }
    println!();
    println!(
        "decide with `pramen ai review accept --key <k> --output '<json>'` or \
         `pramen ai review reject --key <k>` (unique key prefixes are accepted)"
    );
    Ok(())
}

/// `pramen ai review export`: the full pending queue as JSONL on stdout —
/// one self-contained object per item (spec, reason, raw output), ready
/// for labeling tools.
///
/// # Errors
///
/// Returns a message when the ledger cannot be read.
pub fn export(ledger_location: &str) -> Result<(), String> {
    let ledger = open(ledger_location)?;
    for item in ledger.pending_reviews().map_err(|e| e.to_string())? {
        let line = json!({
            "workKey": item.work_key,
            "transform": item.transform_id,
            "reason": item.reason,
            "rawOutput": item.raw_output,
            "queuedAt": item.created_at,
            "spec": item.spec,
        });
        println!("{line}");
    }
    Ok(())
}

/// `pramen ai review accept`: validate a corrected output against the
/// item's declared schema and record it as a completed, human-attributed
/// ledger result.
///
/// # Errors
///
/// Returns a message when the key is unknown/ambiguous, the correction
/// is not valid JSON, or it violates the declared schema.
pub fn accept(ledger_location: &str, key_prefix: &str, output: &str) -> Result<(), String> {
    let corrected: Value =
        serde_json::from_str(output).map_err(|e| format!("--output is not valid JSON: {e}"))?;
    let ledger = open(ledger_location)?;
    let items = ledger.pending_reviews().map_err(|e| e.to_string())?;
    let key = resolve_key(&items, key_prefix)?;
    ledger
        .accept_review(&key, &corrected)
        .map_err(|e| e.to_string())?;
    println!(
        "accepted {key}: recorded as a completed human-review result; \
         the next run emits this record from the ledger at zero model cost"
    );
    Ok(())
}

/// `pramen ai review reject`: permanently drop a queued record.
///
/// # Errors
///
/// Returns a message when the key is unknown or ambiguous.
pub fn reject(ledger_location: &str, key_prefix: &str) -> Result<(), String> {
    let ledger = open(ledger_location)?;
    let items = ledger.pending_reviews().map_err(|e| e.to_string())?;
    let key = resolve_key(&items, key_prefix)?;
    ledger.reject_review(&key).map_err(|e| e.to_string())?;
    println!("rejected {key}: the record is permanently dropped (replays never re-dispatch it)");
    Ok(())
}

fn open(location: &str) -> Result<Ledger, String> {
    if !is_postgres_url(location) && !Path::new(location).exists() {
        return Err(format!("no ledger at {location} (nothing recorded yet)"));
    }
    Ledger::open_location(location).map_err(|e| e.to_string())
}

fn compact(value: &Value, max: usize) -> String {
    truncate(&value.to_string(), max)
}

fn truncate(text: &str, max: usize) -> String {
    if text.chars().count() <= max {
        text.to_owned()
    } else {
        let cut: String = text.chars().take(max).collect();
        format!("{cut}…")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn item(key: &str) -> ReviewItem {
        ReviewItem {
            work_key: key.to_owned(),
            transform_id: "t".into(),
            reason: "r".into(),
            raw_output: None,
            status: pramen_ai::review::ReviewStatus::Pending,
            created_at: "2026-07-12T00:00:00Z".into(),
            spec: Value::Null,
        }
    }

    #[test]
    fn key_prefixes_resolve_only_when_unique() {
        let items = vec![item("abc123"), item("abd456")];
        assert_eq!(resolve_key(&items, "abc").unwrap(), "abc123");
        assert!(resolve_key(&items, "ab").unwrap_err().contains("ambiguous"));
        assert!(
            resolve_key(&items, "zz")
                .unwrap_err()
                .contains("no pending")
        );
    }

    #[test]
    fn truncation_is_char_safe() {
        assert_eq!(truncate("héllo", 3), "hél…");
        assert_eq!(truncate("hi", 10), "hi");
    }
}
