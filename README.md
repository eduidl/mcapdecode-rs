# mcapdecode-rs

[![CI](https://github.com/eduidl/mcapdecode-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/eduidl/mcapdecode-rs/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/mcapdecode.svg)](https://crates.io/crates/mcapdecode)

Rust workspace for decoding MCAP data into a normalized schema/value model, with optional Apache Arrow integration.

## Components

| Component | Purpose | Documentation |
| --- | --- | --- |
| [`mcapdecode`](mcapdecode/mcapdecode) | Rust library for decoding MCAP into structured messages, with optional Arrow `RecordBatch` output | [Library usage and feature flags](mcapdecode/mcapdecode/README.md) |
| [`transmcap`](tools/transmcap) | CLI for converting a topic to JSON Lines, CSV, or Parquet | [Usage and options](tools/transmcap/README.md) |
| [`mcaptui`](tools/mcaptui) | Terminal UI for browsing topics, decoded messages, and schemas | [Usage and key bindings](tools/mcaptui/README.md) |
| [`mcapq`](tools/mcapq) | Machine-readable CLI for inspecting topic metadata and schemas | [Usage and output formats](tools/mcapq/README.md) |

`mcapdecode` defaults to the schema/value API plus built-in decoders. Apache Arrow support is opt-in via the `arrow` feature. The `mcapdecode-*` crates are internal support crates used by the library.

## Quick Commands

```bash
cargo build -p transmcap
cargo build -p mcaptui
cargo test --workspace
```

## Performance benchmarks

See [benchmarking](docs/benchmark.md) for fixture generation, benchmark commands,
baselines, and fixture-test details.
