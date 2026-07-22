//! Connector descriptors, support levels, and offline conformance helpers (E1.4).

mod conformance;
mod registry;
mod types;

pub use conformance::{RecordingSink, assert_sink_commit_barrier};
pub use registry::{builtin, builtins, matrix_markdown};
pub use types::{ConnectorDescriptor, ConnectorKind, DeliveryContract, SupportLevel};
