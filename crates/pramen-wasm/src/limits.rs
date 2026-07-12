//! Resource limits enforced on every component invocation.

use pramen_core::spec::WasmLimitsSpec;

/// Guest memory and fuel ceilings for one store.
#[derive(Debug, Clone, Copy)]
pub struct ResourceLimits {
    /// Maximum guest linear memory in bytes (`None` = unlimited).
    pub memory_bytes: Option<usize>,
    /// Fuel budget for the invocation (`None` = metering disabled).
    pub fuel: Option<u64>,
}

/// Host-side limits for one `run` call.
#[derive(Debug, Clone)]
pub struct InvocationLimits {
    /// Wasmtime store limits.
    pub resource: ResourceLimits,
    /// Maximum Arrow IPC input bytes accepted.
    pub max_input_bytes: usize,
    /// Maximum Arrow IPC output bytes accepted.
    pub max_output_bytes: usize,
}

impl Default for InvocationLimits {
    fn default() -> Self {
        Self {
            resource: ResourceLimits {
                memory_bytes: Some(256 * 1024 * 1024),
                fuel: Some(10_000_000_000),
            },
            max_input_bytes: 64 * 1024 * 1024,
            max_output_bytes: 64 * 1024 * 1024,
        }
    }
}

impl InvocationLimits {
    /// Map pipeline-document limits onto production defaults.
    #[must_use]
    pub fn from_spec(spec: &WasmLimitsSpec) -> Self {
        let mut limits = Self::default();
        if let Some(mb) = spec.memory_mb {
            limits.resource.memory_bytes = Some((mb as usize) * 1024 * 1024);
        }
        if let Some(fuel) = spec.fuel {
            limits.resource.fuel = Some(fuel);
        }
        if let Some(mb) = spec.max_input_mb {
            limits.max_input_bytes = (mb as usize) * 1024 * 1024;
        }
        if let Some(mb) = spec.max_output_mb {
            limits.max_output_bytes = (mb as usize) * 1024 * 1024;
        }
        limits
    }
}
