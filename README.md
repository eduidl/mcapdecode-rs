# mcapdecode-rs

[![CI](https://github.com/eduidl/mcapdecode-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/eduidl/mcapdecode-rs/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/mcapdecode.svg)](https://crates.io/crates/mcapdecode)

Rust workspace for decoding MCAP data into a normalized schema/value model, with optional Apache Arrow integration.

## Crates

- [`mcapdecode`](mcapdecode/mcapdecode): library entry point for decoding MCAP into structured messages, with optional Arrow `RecordBatch` output
- [`transmcap`](tools/transmcap): CLI for converting MCAP to `jsonl/csv/parquet`
- [`mcaptui`](tools/mcaptui): terminal UI for browsing topics, decoded messages, and derived schemas interactively
- `mcapdecode-*`: internal/support crates used by `mcapdecode`

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
