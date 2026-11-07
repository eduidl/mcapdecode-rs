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

## Usage

Reads require an MCAP summary. When chunk indexes are present, reads use them to
select chunks; otherwise, they scan all messages sequentially. Chunks without
message indexes are scanned to find the requested topics. When chunk indexes are
present, messages outside indexed chunks are not read.

### Decoded Messages

```rust
use std::path::Path;
use mcapdecode::McapReader;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let reader = McapReader::builder().with_default_decoders().build();

    reader.for_each_decoded_message(Path::new("sample.mcap"), "/topic/name", |message| {
        println!("publish_time={}", message.publish_time);
        Ok(())
    })?;

    Ok(())
}
```

### Accessing Fields

Decoded structs carry a shared schema, so fields can be accessed by name without
passing a separate schema:

```rust,no_run
use std::path::Path;
use mcapdecode::{McapReader, ReadOptions};

let reader = McapReader::builder().with_default_decoders().build();
reader.with_prepared_topic(Path::new("sample.mcap"), "/topic/name", |topic| {
    topic.for_each_decoded_message_with_options(&ReadOptions::default(), |message| {
        let x = message.value
            .get("poses")?.get(0)?.get("position")?.get("x")?
            .try_widen_f64()?;
        println!("x={x}");
        Ok(())
    })
})?;
# Ok::<(), Box<dyn std::error::Error>>(())
```

Name lookup is linear in the number of fields. Lists and arrays also expose
`as_sequence()`, and maps expose `get_key()` and `as_map()`. Integer conversions
only accept source types whose entire range fits the destination; `f32` can
widen to `f64`, and null is reported as
`ValueAccessError::NullValue`. Existing exact-type methods such as `try_str()`
and `try_u64()` retain their null behavior.

For example, `try_widen_i64()` accepts `U32` but rejects `U64`, even for zero;
`try_widen_u64()` accepts unsigned integers but rejects all signed integers.

### Multiple Topics

Read multiple topics in one pass, sharing chunk reads and decompression. The
callback receives the topic name, and repeated topic names are read only once:

```rust
reader.for_each_topic_decoded_message(
    Path::new("sample.mcap"),
    &["/localization/pose", "/localization/status"],
    |topic, message| {
        println!("{topic}: {}", message.publish_time);
        Ok(())
    },
)?;
```

Use `for_each_topic_decoded_message_with_options` for log-time filtering and
parallel read options.

### Arrow Output

With the `arrow` feature enabled, the same reads can produce Arrow
`RecordBatch`es:

```rust
use std::path::Path;
use mcapdecode::McapReader;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let reader = McapReader::builder().with_default_decoders().build();

    reader.for_each_record_batch(Path::new("sample.mcap"), "/topic/name", |batch| {
        println!("rows={}, cols={}", batch.num_rows(), batch.num_columns());
        Ok(())
    })?;

    Ok(())
}
```

`for_each_topic_record_batch_with_options` reads multiple topics the same way as
`for_each_topic_decoded_message_with_options`.

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
