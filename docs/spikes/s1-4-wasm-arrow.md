# Spike S1.4 — WASM–Arrow boundary (WIT component, IPC round trip, limits)

Status: complete. Gates Phase 2 only (X1.1/X1.2); nothing in v1 depends
on it. Verdict: **the candidate ABI is viable — the boundary sustains
~1.1 GiB/s at production batch sizes with a ~2x total overhead against
native, and memory, fuel, and deadline limits all fail deterministically
with typed traps.**

## What was built

`spikes/s1-4-wasm-arrow` — three parts:

- `wit/transform.wit` — the candidate v2 transform ABI (architecture §8):
  one exported function, `run: func(batch: list<u8>) -> result<list<u8>,
  string>`, where the bytes are an Arrow IPC stream. Schema negotiation,
  limits, and identity stay host-side; the guest is a pure
  batch-to-batch function.
- `guest/` — a Rust component (`wasm32-wasip2`, `wit-bindgen` 0.46,
  arrow 56 with only the `ipc` feature) implementing a representative
  columnar transform: decode IPC, append `amount_gross = amount * 1.21`,
  encode IPC. The same derivation as the benchmark suite's SQL step, so
  ratios are comparable to real deterministic work. Release component:
  **1.1 MiB**.
- `host/` — a wasmtime 38 component host that instantiates per call (as
  the runtime would per batch), measures the full boundary round trip
  (host IPC encode → guest decode → transform → guest encode → host
  decode) against the identical transform-plus-IPC path natively, and
  proves limit behavior with `StoreLimits` (memory), fuel, and epoch
  deadlines.

## Measurement

Apple M3, 16 GiB, macOS; Rust 1.97.0 (pinned toolchain); wasmtime 38
(Cranelift); three-column batch (int64, float64, nullable utf8), 200
iterations (30 at 64k rows). Warm figures after a correctness-checked
first call.

| Rows/batch | IPC in | WASM µs/call | ns/row | Boundary MiB/s | Native+IPC µs/call | Ratio |
| --- | --- | --- | --- | --- | --- | --- |
| 1,024 | 34 KiB | 67.7 | 66.1 | 487 | 14.5 | 4.7x |
| 8,192 | 272 KiB | 350.0 | 42.7 | 758 | 86.2 | 4.1x |
| 65,536 | 2.2 MiB | 1,982.6 | 30.3 | 1,092 | 973.8 | 2.0x |

Instantiation alone (component instantiate, no call): **26.3 µs** — at
the default 8k-row batch size that is ~8% of a call; an `InstancePre` or
pooling allocator removes most of it if it ever matters.

Limit behavior (all deterministic, all typed traps, process unharmed):

| Limit | Behavior |
| --- | --- |
| Fuel, 1k units | Traps immediately with a wasm backtrace |
| Fuel accounting | Identical input consumed identical fuel twice: 2,052,247 units for an 8k-row batch |
| Memory, 2 MiB `StoreLimits` ceiling | Guest allocation fails; call errors cleanly |
| Epoch deadline already elapsed | Traps on entry before any guest work |

## Conclusions

- **The IPC-over-WIT ABI clears the bar.** At the runtime's default
  batch size (8k rows) the full boundary costs ~43 ns/row; against the
  measured end-to-end load path (~434–590k rows/s, i.e. ~2 µs/row) a
  WASM transform would add ~2% wall overhead. The 4x ratio against
  native is dominated by double IPC serialization (guest encode + host
  decode on both sides) and shrinks to 2x as batches grow — acceptable
  for v2 extensibility, not for replacing built-in SQL.
- **Sandboxing is enforceable and deterministic.** Memory, fuel, and
  deadline all fail typed and reproducibly — the properties X1.1's
  limits design assumed. Fuel metering is exactly reproducible for
  identical input, which makes per-record fuel budgets a viable
  governance surface, mirroring the token budgets on semantic
  transforms.
- **The toolchain is unremarkable in a good way**: stock
  `wasm32-wasip2` target + `wit-bindgen` produce a working component
  from ~70 lines of guest code; arrow 56 compiles to wasip2 with the
  `ipc` feature alone.

## Follow-ups feeding X1.1/X1.2 (Phase 2)

- Use `InstancePre` (or the pooling allocator) to amortize the 26 µs
  instantiation; decide per-batch vs per-work-unit instance lifetime.
- Zero-copy interest: the `list<u8>` lowering copies in and out of guest
  memory; investigate shared linear-memory windows only if a workload
  shows the ~1 GiB/s boundary as the bottleneck (measure first).
- Schema negotiation (declared input/output schemas checked at plan
  time) and the conformance suite around traps, limits, and malformed
  IPC belong to X1.2.
- Fuel-per-record budgets as the deterministic-transform analog of
  token budgets.
