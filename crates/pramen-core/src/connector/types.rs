//! Support levels and connector capability descriptors.

use serde::Serialize;

/// How far a connector is carried in product support (architecture §2 / E1.4).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SupportLevel {
    /// First-party; offline conformance / CI coverage; documented delivery.
    Supported,
    /// Shipped with explicit limits (for example append-only Flight SQL).
    Preview,
    /// Named on the matrix; not in the default binary yet.
    Planned,
}

impl SupportLevel {
    /// Stable slug for docs and CLI.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Supported => "supported",
            Self::Preview => "preview",
            Self::Planned => "planned",
        }
    }
}

/// Whether the connector is a source, sink, or transform.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectorKind {
    /// Produces Arrow batches.
    Source,
    /// Consumes Arrow batches.
    Sink,
    /// Batch-to-batches operator.
    Transform,
}

impl ConnectorKind {
    /// Stable slug for docs and CLI.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Source => "source",
            Self::Sink => "sink",
            Self::Transform => "transform",
        }
    }
}

/// Delivery semantics operators must see (architecture §11).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryContract {
    /// Sink commits only after every write succeeds; crash before commit
    /// leaves the destination unchanged (ADR 0007 barrier).
    CommitBarrierAtLeastOnce,
    /// Source enumeration / read; checkpoint marks at-least-once progress.
    SourceCheckpointAtLeastOnce,
    /// Pure in-process transform; no external delivery.
    InProcess,
    /// Semantic work items are ledger-backed; reuse avoids re-billing.
    LedgerBackedSemantic,
}

impl DeliveryContract {
    /// One-line operator-facing summary.
    #[must_use]
    pub const fn summary(self) -> &'static str {
        match self {
            Self::CommitBarrierAtLeastOnce => {
                "commit barrier; at-least-once across the post-commit checkpoint window"
            }
            Self::SourceCheckpointAtLeastOnce => {
                "work-unit checkpoints; at-least-once on crash between sink commit and mark-complete"
            }
            Self::InProcess => "in-process; no external delivery contract",
            Self::LedgerBackedSemantic => {
                "inference ledger records results; replays reuse without re-billing"
            }
        }
    }
}

/// Inspectable metadata for one connector (architecture: `pramen inspect connector`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectorDescriptor {
    /// Stable id, e.g. `sink.postgres`.
    pub id: &'static str,
    /// Source, sink, or transform.
    pub kind: ConnectorKind,
    /// Product support level.
    pub support_level: SupportLevel,
    /// Short human summary.
    pub summary: &'static str,
    /// Sink load modes (`append`, `upsert`); empty when not a sink.
    pub modes: &'static [&'static str],
    /// Source URL schemes; empty when not a source.
    pub schemes: &'static [&'static str],
    /// Delivery contract.
    pub delivery: DeliveryContract,
    /// Limits, secret env vars, and other operator notes.
    pub notes: &'static str,
}
