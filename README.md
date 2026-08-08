# mcapdecode-rs

[![CI](https://github.com/eduidl/mcapdecode-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/eduidl/mcapdecode-rs/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/mcapdecode.svg)](https://crates.io/crates/mcapdecode)

Rust workspace for decoding MCAP data into a normalized schema/value model, with optional Apache Arrow integration.

## Crates

- [`mcapdecode`](mcapdecode/mcapdecode): library entry point for decoding MCAP into structured messages, with optional Arrow `RecordBatch` output
- [`transmcap`](tools/transmcap): CLI for converting MCAP to `jsonl/csv/parquet`
- [`mcaptui`](tools/mcaptui): terminal UI for browsing topics, decoded messages, and derived schemas interactively
- `mcapdecode-*`: internal/support crates used by `mcapdecode`
- [`mcapbench`](dev/mcapbench): unpublished dev crate that generates benchmark fixtures

## Start Here

- CLI usage and options: [`tools/transmcap/README.md`](tools/transmcap/README.md)
- TUI usage and key bindings: [`tools/mcaptui/README.md`](tools/mcaptui/README.md)
- Library usage and feature flags: [`mcapdecode/mcapdecode/README.md`](mcapdecode/mcapdecode/README.md)

`mcapdecode` defaults to the schema/value API plus built-in decoders. Arrow support is opt-in via the `arrow` feature.

## Quick CLI Usage (`transmcap`)

```bash
cargo run -p transmcap -- convert <input.mcap> --topic <topic> --format jsonl
cargo run -p transmcap -- schema <input.mcap> --topic <topic>
```

Use `-o/--output` to write files (`parquet` requires `-o`).

## Quick Commands

```bash
cargo build -p transmcap
cargo build -p mcaptui
cargo test --workspace
```

## Performance benchmarks

Benchmarks run against MCAP files that `mcapbench` generates on demand into a cache
directory, so nothing has to be checked in or downloaded first. Generation is
deterministic: a fixed LCG seed feeds CDR encoding for ROS 2 payloads and
`prost-reflect::DynamicMessage` with a generated `FileDescriptorSet` for protobuf.

```bash
# Create a shareable fixture locally (not checked in).
cargo run -p mcapbench -- --case bytes --encoding ros2idl --output /tmp/bytes.mcap

# Decoder / reader / Arrow / parquet stages.
cargo bench -p mcapdecode --features arrow
cargo bench -p transmcap

# Run every benchmark once without measuring, to check they all still execute.
cargo bench -p mcapdecode --features arrow -- --test

# Save and compare local baselines; CI intentionally does not gate elapsed time.
cargo bench -p mcapdecode --features arrow -- --save-baseline before-change
cargo bench -p mcapdecode --features arrow -- --baseline before-change
```

Decoder benchmarks keep payload shape separate from encoding: `flat`, `nested`, `bytes`
and `numeric_array` run for ROS 2 IDL, ROS 2 msg and protobuf; `strings` is ROS 2 IDL
only. Reader benchmarks vary one axis at a time from a fixed baseline: selectivity
(1%, 50%, 100%) combined with clustered or interleaved layout, none/zstd/lz4 compression,
chunk size, and sequential/parallel decoding. Layout matters because only a clustered
topic lets a reader skip chunks; an interleaved one appears in every chunk. Criterion is
configured with ten samples and byte throughput.

Fixture names are content-addressed over the schema, the payload and the file shape, so
editing the generator invalidates cached files automatically. They are cached in
`$TMPDIR/mcapbench` (override with `MCAPBENCH_FIXTURE_DIR`, for instance to keep them off
a RAM-backed `/tmp`) and are not cleaned up on their own — a full benchmark run
materialises every combination at 24 MiB each, so reclaim the space with:

```bash
rm -rf "${MCAPBENCH_FIXTURE_DIR:-${TMPDIR:-/tmp}/mcapbench}"
```

`dev/mcapbench/tests/roundtrip.rs` decodes every generated combination back to the
sample it was produced from; a broken schema or payload fails there rather than inside a
benchmark.

The ignored tests in `dev/mcapbench/tests/reader_alloc_budget.rs` and
`mcapdecode/mcapdecode-ros2-common/tests/alloc_budget.rs` are deliberate future CI gates.
They pin, via `dhat` allocation budgets, that reading a clustered topic does not
decompress the whole file and that `sequence<uint8>` does not allocate per byte. Remove
their `#[ignore]` markers when the corresponding implementation issue is fixed; run them
meanwhile with:

```bash
cargo test -p mcapbench -- --ignored
cargo test -p mcapdecode-ros2-common -- --ignored
```

`dhat` profiles the whole process, so each of those tests is alone in its test binary;
keep it that way when adding more of them.
