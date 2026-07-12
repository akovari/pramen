//! S1.4 spike host: measure the WASM–Arrow boundary (WIT component,
//! Arrow IPC in/out) and prove that memory, fuel, and deadline limits
//! behave deterministically.
//!
//! Usage: `s1-4-host <component.wasm>`

use anyhow::{Context, Result, bail};
use arrow::array::{ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use std::sync::Arc;
use std::time::Instant;
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

/// The benchmark row shape: id, amount, nullable note — a slice of the
/// suite's six-type mix that keeps the guest transform simple.
fn batch(rows: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("amount", DataType::Float64, false),
        Field::new("note", DataType::Utf8, true),
    ]));
    let ids: ArrayRef = Arc::new(Int64Array::from_iter_values(0..rows as i64));
    let amounts: ArrayRef = Arc::new(Float64Array::from_iter_values(
        (0..rows).map(|i| i as f64 * 1.5),
    ));
    let notes: ArrayRef = Arc::new(StringArray::from_iter((0..rows).map(|i| {
        if i % 5 == 0 {
            None
        } else {
            Some(format!("note for row {i}"))
        }
    })));
    RecordBatch::try_new(schema, vec![ids, amounts, notes]).expect("valid batch")
}

fn to_ipc(batch: &RecordBatch) -> Vec<u8> {
    let mut writer = StreamWriter::try_new(Vec::new(), &batch.schema()).expect("writer");
    writer.write(batch).expect("write");
    writer.into_inner().expect("finish")
}

fn from_ipc(bytes: &[u8]) -> RecordBatch {
    let mut reader = StreamReader::try_new(bytes, None).expect("reader");
    reader.next().expect("one batch").expect("valid batch")
}

/// The native equivalent of the guest transform, IPC round trip included
/// — so wasm/native ratios compare identical work.
fn native_roundtrip(bytes: &[u8]) -> Vec<u8> {
    let input = from_ipc(bytes);
    let amounts = input
        .column(1)
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("amount");
    let gross: Float64Array = amounts.iter().map(|v| v.map(|a| a * 1.21)).collect();
    let mut fields: Vec<Field> = input
        .schema()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    fields.push(Field::new("amount_gross", DataType::Float64, true));
    let mut columns: Vec<ArrayRef> = input.columns().to_vec();
    columns.push(Arc::new(gross));
    let out = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("batch");
    to_ipc(&out)
}

struct Host {
    engine: Engine,
    component: Component,
    linker: Linker<Ctx>,
}

impl Host {
    fn new(component_path: &str, fuel: bool) -> Result<Self> {
        let mut config = Config::new();
        config.consume_fuel(fuel);
        config.epoch_interruption(true);
        let engine = Engine::new(&config)?;
        let component = Component::from_file(&engine, component_path)
            .context("load component (build the guest first)")?;
        let mut linker = Linker::new(&engine);
        wasmtime_wasi::p2::add_to_linker_sync(&mut linker)?;
        Ok(Self {
            engine,
            component,
            linker,
        })
    }

    fn store(&self, memory_limit: Option<usize>, fuel: Option<u64>) -> Result<Store<Ctx>> {
        let mut builder = StoreLimitsBuilder::new();
        if let Some(bytes) = memory_limit {
            builder = builder.memory_size(bytes);
        }
        let ctx = Ctx {
            wasi: WasiCtxBuilder::new().build(),
            table: ResourceTable::new(),
            limits: builder.build(),
        };
        let mut store = Store::new(&self.engine, ctx);
        store.limiter(|ctx| &mut ctx.limits);
        if let Some(amount) = fuel {
            store.set_fuel(amount)?;
        }
        // A deadline far in the future; the deadline test lowers it.
        store.set_epoch_deadline(u64::MAX / 2);
        Ok(store)
    }

    fn call(&self, store: &mut Store<Ctx>, input: &[u8]) -> Result<Result<Vec<u8>, String>> {
        let instance = self.linker.instantiate(&mut *store, &self.component)?;
        let func = instance
            .get_typed_func::<(Vec<u8>,), (Result<Vec<u8>, String>,)>(&mut *store, "run")?;
        let (result,) = func.call(&mut *store, (input.to_vec(),))?;
        func.post_return(&mut *store)?;
        Ok(result)
    }
}

fn measure(host: &Host, rows: usize, iterations: usize) -> Result<()> {
    let input = to_ipc(&batch(rows));
    let mut store = host.store(None, None)?;

    // Correctness first.
    let output = host
        .call(&mut store, &input)?
        .map_err(|e| anyhow::anyhow!(e))?;
    let decoded = from_ipc(&output);
    assert_eq!(decoded.num_rows(), rows);
    assert_eq!(decoded.schema().field(3).name(), "amount_gross");

    // Reuse one store/instance pattern per call, as the runtime would.
    let started = Instant::now();
    for _ in 0..iterations {
        let out = host.call(&mut store, &input)?;
        assert!(out.is_ok());
    }
    let wasm_elapsed = started.elapsed();

    let started = Instant::now();
    for _ in 0..iterations {
        std::hint::black_box(native_roundtrip(&input));
    }
    let native_elapsed = started.elapsed();

    let per_call_us = wasm_elapsed.as_secs_f64() * 1e6 / iterations as f64;
    let native_us = native_elapsed.as_secs_f64() * 1e6 / iterations as f64;
    println!(
        "rows={rows:>6}  ipc_in={:>9} B  wasm={per_call_us:>9.1} µs/call ({:>6.1} ns/row, {:>7.1} MiB/s)  native+ipc={native_us:>8.1} µs/call  ratio={:.2}x",
        input.len(),
        per_call_us * 1000.0 / rows as f64,
        input.len() as f64 / 1_048_576.0 / (per_call_us / 1e6),
        per_call_us / native_us,
    );
    Ok(())
}

fn limits(host_fuel: &Host, component_path: &str) -> Result<()> {
    let input = to_ipc(&batch(8_192));

    // Fuel: a tiny budget must trap deterministically, and identical
    // inputs must consume identical fuel.
    let mut store = host_fuel.store(None, Some(1_000))?;
    match host_fuel.call(&mut store, &input) {
        Err(error) => println!("fuel: 1k budget trapped as expected: {}", first_line(&error)),
        Ok(_) => bail!("fuel: expected a trap under a 1k budget"),
    }
    let consumed = {
        let mut store = host_fuel.store(None, Some(10_000_000_000))?;
        let before = store.get_fuel()?;
        host_fuel
            .call(&mut store, &input)?
            .map_err(|e| anyhow::anyhow!(e))?;
        before - store.get_fuel()?
    };
    let consumed_again = {
        let mut store = host_fuel.store(None, Some(10_000_000_000))?;
        let before = store.get_fuel()?;
        host_fuel
            .call(&mut store, &input)?
            .map_err(|e| anyhow::anyhow!(e))?;
        before - store.get_fuel()?
    };
    if consumed != consumed_again {
        bail!("fuel consumption was not deterministic: {consumed} vs {consumed_again}");
    }
    println!("fuel: identical input consumed identical fuel twice ({consumed} units)");

    // Memory: a ceiling below the guest's needs must fail cleanly.
    let host = Host::new(component_path, false)?;
    let mut store = host.store(Some(2 * 1024 * 1024), None)?;
    match host.call(&mut store, &input) {
        Err(error) => println!(
            "memory: 2 MiB ceiling failed deterministically: {}",
            first_line(&error)
        ),
        Ok(_) => bail!("memory: expected failure under a 2 MiB ceiling"),
    }

    // Deadline: an already-elapsed epoch deadline traps on entry.
    let mut store = host.store(None, None)?;
    store.set_epoch_deadline(0);
    host.engine.increment_epoch();
    match host.call(&mut store, &input) {
        Err(error) => println!(
            "deadline: elapsed epoch trapped on entry: {}",
            first_line(&error)
        ),
        Ok(_) => bail!("deadline: expected an epoch trap"),
    }
    Ok(())
}

fn first_line(error: &anyhow::Error) -> String {
    error.to_string().lines().next().unwrap_or_default().to_owned()
}

fn main() -> Result<()> {
    let component_path = std::env::args()
        .nth(1)
        .context("usage: s1-4-host <component.wasm>")?;

    println!("== throughput (per-call instantiate, as the runtime would per batch) ==");
    let host = Host::new(&component_path, false)?;

    // Attribute the fixed cost: how much of each call is instantiation?
    {
        let mut store = host.store(None, None)?;
        let started = Instant::now();
        for _ in 0..200 {
            let _ = host.linker.instantiate(&mut store, &host.component)?;
        }
        println!(
            "instantiate only: {:.1} µs/instance",
            started.elapsed().as_secs_f64() * 1e6 / 200.0
        );
    }

    for rows in [1_024usize, 8_192, 65_536] {
        let iterations = if rows >= 65_536 { 30 } else { 200 };
        measure(&host, rows, iterations)?;
    }

    println!("\n== limits ==");
    let host_fuel = Host::new(&component_path, true)?;
    limits(&host_fuel, &component_path)?;

    println!("\nspike complete");
    Ok(())
}
