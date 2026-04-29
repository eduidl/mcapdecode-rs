# mcapdecode-rs

[![CI](https://github.com/eduidl/mcapdecode-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/eduidl/mcapdecode-rs/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/mcapdecode.svg)](https://crates.io/crates/mcapdecode)

Rust workspace for decoding MCAP data into a normalized schema/value model, with optional Apache Arrow integration.

## Crates

- [`mcapdecode`](mcapdecode/mcapdecode): library entry point for decoding MCAP into structured messages, with optional Arrow `RecordBatch` output
- [`transmcap`](tools/transmcap): CLI for converting MCAP to `jsonl/csv/parquet`
- `mcapdecode-*`: internal/support crates used by `mcapdecode`

## Start Here

- CLI usage and options: [`tools/transmcap/README.md`](tools/transmcap/README.md)
- Library usage and feature flags: [`mcapdecode/mcapdecode/README.md`](mcapdecode/mcapdecode/README.md)

## Quick CLI Usage (`transmcap`)

```bash
cargo run -p transmcap -- convert <input.mcap> --topic <topic> --format jsonl
cargo run -p transmcap -- schema <input.mcap> --topic <topic>
```

Use `-o/--output` to write files (`parquet` requires `-o`).

## Quick Commands

```bash
cargo build -p transmcap
cargo test --workspace
```
