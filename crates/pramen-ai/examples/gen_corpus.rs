//! Regenerate the golden support-ticket corpus (S2.2).
//!
//! The corpus is synthetic and labelled *by construction*: every ticket is
//! composed from a category-specific template whose slots determine the
//! expected output, so ground truth is exact and no production data is
//! involved. Generation is fully deterministic — same code, same corpus —
//! and the result is versioned in-repo at
//! `corpora/support-tickets.v1.yaml`. Bump `version` when templates or
//! labelling rules change.
//!
//! ```console
//! cargo run -p pramen-ai --example gen_corpus > corpora/support-tickets.v1.yaml
//! ```

use pramen_ai::eval::{Corpus, EvalItem, EvalTask};
use pramen_core::spec::{FieldSpec, FieldType};
use serde_json::{Map, Value, json};
use std::collections::BTreeMap;

/// Items to generate; ≥500 per the S2.2 exit criteria.
const ITEMS: usize = 520;

/// One category's template material.
struct Template {
    category: &'static str,
    /// (symptom text, product or None, is urgent)
    cases: &'static [(&'static str, Option<&'static str>, bool)],
}

const TEMPLATES: &[Template] = &[
    Template {
        category: "hardware",
        cases: &[
            (
                "the office printer is jamming on every duplex job",
                Some("printer"),
                false,
            ),
            (
                "printer in building 7 is smoking and smells of burning plastic",
                Some("printer"),
                true,
            ),
            (
                "my laptop battery drains from full to empty in twenty minutes",
                Some("laptop"),
                false,
            ),
            (
                "laptop screen shattered right before the customer demo",
                Some("laptop"),
                true,
            ),
            (
                "the conference room display flickers whenever someone presents",
                None,
                false,
            ),
        ],
    },
    Template {
        category: "network",
        cases: &[
            (
                "vpn drops every ten minutes and has to be restarted manually",
                Some("vpn"),
                false,
            ),
            (
                "the whole third floor lost network and production deploys are blocked",
                Some("router"),
                true,
            ),
            (
                "wifi is unusably slow in the east wing since the office move",
                Some("router"),
                false,
            ),
            (
                "vpn refuses connections for the entire on-call team right now",
                Some("vpn"),
                true,
            ),
            (
                "intermittent packet loss to the data center during backups",
                None,
                false,
            ),
        ],
    },
    Template {
        category: "billing",
        cases: &[
            (
                "the invoice total for order 4411 does not match the quote we approved",
                None,
                false,
            ),
            (
                "we were charged twice this month and finance escalated to the CFO",
                None,
                true,
            ),
            (
                "the tax line on the March invoice uses last year's rate",
                None,
                false,
            ),
            (
                "subscription renewed at the wrong tier and the card was already charged",
                Some("crm"),
                false,
            ),
            (
                "credit note promised six weeks ago still has not arrived",
                None,
                false,
            ),
        ],
    },
    Template {
        category: "account",
        cases: &[
            (
                "i cannot reset my password, the email link never arrives",
                None,
                false,
            ),
            (
                "an ex-employee's account still has admin access to the crm",
                Some("crm"),
                true,
            ),
            (
                "two-factor codes stopped working after i changed phones",
                None,
                false,
            ),
            (
                "my account was locked out in the middle of quarter-end closing",
                Some("crm"),
                true,
            ),
            (
                "need my display name corrected in the directory",
                None,
                false,
            ),
        ],
    },
    Template {
        category: "software",
        cases: &[
            (
                "the crm export to spreadsheet times out for any range over a week",
                Some("crm"),
                false,
            ),
            (
                "yesterday's update broke saving, everyone's edits are silently lost",
                Some("crm"),
                true,
            ),
            (
                "the mobile app crashes on launch since the OS upgrade",
                None,
                false,
            ),
            (
                "report totals differ between the dashboard and the export",
                Some("crm"),
                false,
            ),
            (
                "search returns no results for records created after midnight",
                None,
                false,
            ),
        ],
    },
];

/// Deterministic filler prefixes to vary surface form without touching
/// the label-bearing content.
const OPENERS: &[&str] = &[
    "Hi team,",
    "Hello support,",
    "Urgent for us:",
    "As discussed on the phone,",
    "Repeated issue:",
    "New ticket:",
    "FYI —",
    "Please advise:",
];

fn main() {
    let task = EvalTask {
        instruction: "Classify the support ticket. Return the business category \
                      (hardware, network, billing, account, or software), the \
                      operational priority (urgent or normal), the affected \
                      product if one is clearly named (printer, laptop, router, \
                      vpn, or crm; otherwise null), and whether the ticket \
                      requires human review (true when the priority is urgent)."
            .to_owned(),
        fields: vec![
            FieldSpec {
                name: "category".into(),
                field_type: FieldType::Utf8,
                nullable: false,
            },
            FieldSpec {
                name: "priority".into(),
                field_type: FieldType::Utf8,
                nullable: false,
            },
            FieldSpec {
                name: "product".into(),
                field_type: FieldType::Utf8,
                nullable: true,
            },
            FieldSpec {
                name: "requires_review".into(),
                field_type: FieldType::Bool,
                nullable: false,
            },
        ],
        weights: BTreeMap::from([
            ("category".to_owned(), 3.0),
            ("priority".to_owned(), 2.0),
            ("product".to_owned(), 1.0),
            ("requires_review".to_owned(), 1.0),
        ]),
    };

    let mut items = Vec::with_capacity(ITEMS);
    for index in 0..ITEMS {
        let template = &TEMPLATES[index % TEMPLATES.len()];
        let (symptom, product, urgent) =
            template.cases[(index / TEMPLATES.len()) % template.cases.len()];
        let opener = OPENERS[index % OPENERS.len()];
        let priority = if urgent { "urgent" } else { "normal" };

        let mut input = Map::new();
        input.insert("id".to_owned(), json!(index as i64 + 1));
        input.insert(
            "description".to_owned(),
            json!(format!("{opener} {symptom} (ref #{:05})", index + 1)),
        );

        let mut expected = Map::new();
        expected.insert("category".to_owned(), json!(template.category));
        expected.insert("priority".to_owned(), json!(priority));
        expected.insert(
            "product".to_owned(),
            product.map_or(Value::Null, |p| json!(p)),
        );
        expected.insert("requires_review".to_owned(), json!(urgent));

        items.push(EvalItem {
            id: format!("st-{:05}", index + 1),
            input,
            expected,
        });
    }

    let corpus = Corpus {
        name: "support-tickets".to_owned(),
        version: 1,
        task,
        items,
    };
    match corpus.to_yaml() {
        Ok(yaml) => {
            println!(
                "# Generated by `cargo run -p pramen-ai --example gen_corpus`; do not hand-edit."
            );
            print!("{yaml}");
        }
        Err(error) => {
            eprintln!("corpus generation failed: {error}");
            std::process::exit(1);
        }
    }
}
