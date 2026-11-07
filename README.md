# mcapdecode

[![CI](https://github.com/eduidl/mcapdecode-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/eduidl/mcapdecode-rs/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/mcapdecode.svg)](https://crates.io/crates/mcapdecode)

`mcapdecode` is a Rust library that decodes MCAP messages into a normalized
schema/value model. Built-in decoders cover protobuf, ROS 2 (`.msg` and IDL),
and raw JSON, and an optional `arrow` feature converts decoded messages into
Apache Arrow `RecordBatch` streams.

## Installation

```toml
[dependencies]
mcapdecode = "0.10.0"
```

The default features enable the protobuf and ROS 2 decoders. See
[Feature Flags](#feature-flags) to trim decoders or enable Arrow output.

## Supported Encodings

| Schema encoding | Message encoding | Feature |
| --- | --- | --- |
| `protobuf` | `protobuf` | `protobuf` (default) |
| `ros2msg` | `cdr` | `ros2msg` (default) |
| `ros2idl` | `cdr` | `ros2idl` (default) |
| `jsonschema` | `json` | `rawjson` |

The `rawjson` decoder does not parse messages or read the schema. Each message
is exposed as a struct with a single `json` string field.

## Decoded Message Usage

Reads require an MCAP summary. When chunk indexes are present, reads use them to
select chunks; otherwise, they scan all messages sequentially. Chunks without
message indexes are scanned to find the requested topics. When chunk indexes are
present, messages outside indexed chunks are not read.

```rust,no_run
use mcapdecode::McapReader;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let reader = McapReader::open("sample.mcap")?;
    reader
        .read()
        .topic("/topic/name")
        .decoded()
        .for_each(|channel, message| {
            println!("{}: publish_time={}", channel.topic, message.publish_time);
            Ok(())
        })?;
    Ok(())
}
```

The opened reader can be reused for multiple reads. Omit `topics` to select all
channels. Decoded and Arrow reads fail if a selected channel cannot be decoded.
Use `topics` or `channels` to select supported channels, for example
`channels([id1, id2])`. Each callback receives `ChannelInfo`, including its ID,
topic, and optional schema name. Custom decoders can be configured with `McapReader::builder()` and opened
with `open_file()`.

Decoded structs carry a shared schema, so fields can be accessed by name without
passing a separate schema:

```rust,no_run
use mcapdecode::McapReader;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let reader = McapReader::open("sample.mcap")?;
    reader
        .read()
        .topic("/topic/name")
        .decoded()
        .for_each(|_, message| {
            let x = message.value
                .get("poses")?
                .get(0)?
                .get("position")?
                .get("x")?
                .try_widen_f64()?;
            println!("x={x}");
            Ok(())
        })?;
    Ok(())
}
```

Name lookup is linear in the number of fields. Lists and arrays also expose
`as_sequence()`, and maps expose `get_key()` and `as_map()`. Integer conversions
only accept source types whose entire range fits the destination; `f32` can
widen to `f64`, and null is reported as
`ValueAccessError::NullValue`. Existing exact-type methods such as `try_str()`
and `try_u64()` retain their null behavior.

For example, `try_widen_i64()` accepts `U32` but rejects `U64`, even for zero;
`try_widen_u64()` accepts unsigned integers but rejects all signed integers.

## Multiple Topics

Read multiple topics in one pass, sharing chunk reads and decompression. The
callback receives channel information, and repeated topic names are read only once:

```rust
reader
    .read()
    .topics(["/localization/pose", "/localization/status"])
    .decoded()
    .for_each(|topic, message| {
        println!("{}: {}", topic.topic, message.publish_time);
        Ok(())
    })?;
```

Use `options` to filter messages by `log_time` and configure parallel reads. The same selection and
options apply to `raw()`, `decoded()`, and, with the `arrow` feature, `arrow()`.

## Arrow Usage

```rust
use mcapdecode::McapReader;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let reader = McapReader::open("sample.mcap")?;

    reader
        .read()
        .topic("/topic/name")
        .arrow()
        .for_each(|_, batch| {
            println!("rows={}, cols={}", batch.num_rows(), batch.num_columns());
            Ok(())
        })?;

    Ok(())
}
```

Enable the `arrow` feature for that API:

```toml
[dependencies]
mcapdecode = { version = "0.10.0", features = ["arrow"] }
```

## Feature Flags

| Feature | Default | Description |
| --- | --- | --- |
| `protobuf` | yes | protobuf decoder |
| `ros2msg` | yes | ROS 2 `.msg` decoder |
| `ros2idl` | yes | ROS 2 IDL decoder |
| `rawjson` | no | raw JSON pass-through decoder |
| `arrow` | no | Arrow `RecordBatch` output |

Disable defaults to trim dependencies, and add only what you need:

```toml
[dependencies]
mcapdecode = { version = "0.10.0", default-features = false, features = ["protobuf", "arrow"] }
```

## Workspace Crates

These are internal support crates used by `mcapdecode`; depend on `mcapdecode`
directly.

- [`mcapdecode-core`](https://github.com/eduidl/mcapdecode-rs/tree/main/mcapdecode/mcapdecode-core): schema/value model and shared errors
- [`mcapdecode-arrow`](https://github.com/eduidl/mcapdecode-rs/tree/main/mcapdecode/mcapdecode-arrow): Arrow conversion
- [`mcapdecode-protobuf`](https://github.com/eduidl/mcapdecode-rs/tree/main/mcapdecode/mcapdecode-protobuf): protobuf decoder
- [`mcapdecode-ros2msg`](https://github.com/eduidl/mcapdecode-rs/tree/main/mcapdecode/mcapdecode-ros2msg): ROS 2 `.msg` decoder
- [`mcapdecode-ros2idl`](https://github.com/eduidl/mcapdecode-rs/tree/main/mcapdecode/mcapdecode-ros2idl): ROS 2 IDL decoder
- [`mcapdecode-rawjson`](https://github.com/eduidl/mcapdecode-rs/tree/main/mcapdecode/mcapdecode-rawjson): raw JSON pass-through decoder

## Tools

Command-line tools built on `mcapdecode`:

- [`transmcap`](https://github.com/eduidl/mcapdecode-rs/tree/main/tools/transmcap): convert a topic to JSON Lines, CSV, or Parquet
- [`mcaptui`](https://github.com/eduidl/mcapdecode-rs/tree/main/tools/mcaptui): terminal UI for browsing topics, decoded messages, and schemas

## Development

See [benchmarking](https://github.com/eduidl/mcapdecode-rs/blob/main/docs/benchmark.md) for fixture generation, benchmark commands, and baselines.
