//! Wasmtime component host: WIT `run` over Arrow IPC bytes.

use crate::error::WasmError;
use crate::limits::InvocationLimits;
use std::path::Path;
use wasmtime::component::{Component, Linker, ResourceTable};
use wasmtime::{Config, Engine, Store, StoreLimits, StoreLimitsBuilder};
use wasmtime_wasi::WasiCtx;
use wasmtime_wasi::WasiCtxBuilder;

struct Ctx {
    wasi: WasiCtx,
    table: ResourceTable,
    limits: StoreLimits,
}

impl wasmtime_wasi::WasiView for Ctx {
    fn ctx(&mut self) -> wasmtime_wasi::WasiCtxView<'_> {
        wasmtime_wasi::WasiCtxView {
            ctx: &mut self.wasi,
            table: &mut self.table,
        }
    }
}

/// A loaded component ready for repeated invocations via [`PreparedComponent::invoke`].
pub struct PreparedComponent {
    engine: Engine,
    component: Component,
    linker: Linker<Ctx>,
    digest: [u8; 32],
}

impl PreparedComponent {
    /// Load a component from bytes and prepare it for invocation.
    ///
    /// # Errors
    ///
    /// Returns [`WasmError::Load`] when parsing or linking fails.
    pub fn from_bytes(wasm: &[u8]) -> Result<Self, WasmError> {
        let digest = crate::cache::digest_bytes(wasm);
        let mut config = Config::new();
        config.consume_fuel(true);
        config.epoch_interruption(true);
        let engine = Engine::new(&config).map_err(|e| WasmError::load(e.to_string()))?;
        let component =
            Component::from_binary(&engine, wasm).map_err(|e| WasmError::load(e.to_string()))?;
        let mut linker = Linker::new(&engine);
        wasmtime_wasi::p2::add_to_linker_sync(&mut linker)
            .map_err(|e| WasmError::load(e.to_string()))?;
        Ok(Self {
            engine,
            component,
            linker,
            digest,
        })
    }

    /// Load a component from a filesystem path.
    ///
    /// # Errors
    ///
    /// Returns [`WasmError::Load`] when the file cannot be read or parsed.
    pub fn from_path(path: impl AsRef<Path>) -> Result<Self, WasmError> {
        let bytes = std::fs::read(path.as_ref())
            .map_err(|e| WasmError::load(format!("read {}: {e}", path.as_ref().display())))?;
        Self::from_bytes(&bytes)
    }

    /// SHA-256 digest of the component bytes used as the artifact cache key.
    #[must_use]
    pub fn digest(&self) -> [u8; 32] {
        self.digest
    }

    /// Invoke the exported `run` function with Arrow IPC input bytes.
    ///
    /// # Errors
    ///
    /// Returns [`WasmError`] for limit violations, guest errors, traps, or
    /// host failures.
    pub fn invoke(
        &self,
        input_ipc: &[u8],
        limits: &InvocationLimits,
    ) -> Result<Vec<u8>, WasmError> {
        if input_ipc.len() > limits.max_input_bytes {
            return Err(WasmError::limit(format!(
                "input IPC {} bytes exceeds max {}",
                input_ipc.len(),
                limits.max_input_bytes
            )));
        }

        let mut store = self.store(limits)?;
        let instance = self
            .linker
            .instantiate(&mut store, &self.component)
            .map_err(|e| WasmError::trap_message(e.to_string()))?;
        let func = instance
            .get_typed_func::<(Vec<u8>,), (Result<Vec<u8>, String>,)>(&mut store, "run")
            .map_err(|e| WasmError::load(e.to_string()))?;
        let (result,) = func
            .call(&mut store, (input_ipc.to_vec(),))
            .map_err(|e| WasmError::trap_message(e.to_string()))?;
        func.post_return(&mut store)
            .map_err(|e| WasmError::trap_message(e.to_string()))?;

        let output = match result {
            Ok(bytes) => bytes,
            Err(message) => return Err(WasmError::Guest(message)),
        };
        if output.len() > limits.max_output_bytes {
            return Err(WasmError::limit(format!(
                "output IPC {} bytes exceeds max {}",
                output.len(),
                limits.max_output_bytes
            )));
        }
        Ok(output)
    }

    fn store(&self, limits: &InvocationLimits) -> Result<Store<Ctx>, WasmError> {
        let mut builder = StoreLimitsBuilder::new();
        if let Some(bytes) = limits.resource.memory_bytes {
            builder = builder.memory_size(bytes);
        }
        let ctx = Ctx {
            wasi: WasiCtxBuilder::new().build(),
            table: ResourceTable::new(),
            limits: builder.build(),
        };
        let mut store = Store::new(&self.engine, ctx);
        store.limiter(|ctx| &mut ctx.limits);
        if let Some(fuel) = limits.resource.fuel {
            store
                .set_fuel(fuel)
                .map_err(|e| WasmError::load(e.to_string()))?;
        }
        store.set_epoch_deadline(u64::MAX / 2);
        Ok(store)
    }
}
