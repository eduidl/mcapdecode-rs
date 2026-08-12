# Benchmarking

Benchmarks run against MCAP files that `mcapbench` generates on demand into a cache
directory, so nothing has to be checked in or downloaded first. Generation is
deterministic: a fixed LCG seed feeds CDR encoding for ROS 2 payloads and
`prost-reflect::DynamicMessage` with a generated `FileDescriptorSet` for protobuf.

## Running benchmarks

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

## Coverage

Decoder benchmarks keep payload shape separate from encoding: `flat`, `nested`, `bytes`
and `numeric_array` run for ROS 2 IDL, ROS 2 msg, and protobuf; `strings` runs for ROS 2
IDL and ROS 2 msg. Reader benchmarks vary one axis at a time from a fixed baseline:
selectivity (1%, 50%, 100%) combined with clustered or interleaved layout, none/zstd/lz4
compression, chunk size, and sequential/parallel decoding. Layout matters because only a
clustered topic lets a reader skip chunks; an interleaved one appears in every chunk.
Criterion is configured with ten samples and byte throughput.

## Fixture cache and verification

Fixture names are content-addressed over the schema, payload, and file shape, so editing
the generator invalidates cached files automatically. They are cached in
`$TMPDIR/mcapbench` (override with `MCAPBENCH_FIXTURE_DIR`, for example to keep them off a
RAM-backed `/tmp`) and are not cleaned up automatically. A full benchmark run materializes
every combination at 24 MiB each; reclaim the space with:

```bash
rm -rf "${MCAPBENCH_FIXTURE_DIR:-${TMPDIR:-/tmp}/mcapbench}"
```

`dev/mcapbench/tests/roundtrip.rs` decodes every generated combination back to the sample
it was produced from, so a broken schema or payload fails there rather than inside a
benchmark. These fixture checks are opt-in and do not run as part of a normal
`cargo test --workspace`; run them explicitly with:

```bash
cargo test -p mcapbench --features fixture-tests
```
