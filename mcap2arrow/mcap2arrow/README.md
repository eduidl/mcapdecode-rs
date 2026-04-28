# mcap2arrow

[![crates.io](https://img.shields.io/crates/v/mcap2arrow.svg)](https://crates.io/crates/mcap2arrow)

`mcap2arrow` is a Rust library that decodes MCAP channels/messages and can expose either decoded messages or Apache Arrow `RecordBatch` streams.

## Installation

```toml
[dependencies]
mcap2arrow = "0.3.0"
```

## What It Provides

- Reader API over MCAP files and memory maps
- Conversion from decoded message values to Arrow arrays/schema
- Decoder registration API for different schema/message encodings
- Built-in optional decoders via feature flags

## Feature Flags

Default features:

- `arrow`
- `protobuf`
- `ros2msg`
- `ros2idl`

Disable defaults to trim dependencies:

```toml
[dependencies]
mcap2arrow = { version = "0.3.0", default-features = false, features = ["protobuf"] }
```

Enable `arrow` only when you need `RecordBatch` output:

```toml
[dependencies]
mcap2arrow = { version = "0.3.0", default-features = false, features = ["arrow", "protobuf"] }
```

Encoding pairs supported by built-in decoders:

| Schema encoding | Message encoding | Feature |
| --- | --- | --- |
| `protobuf` | `protobuf` | `protobuf` |
| `ros2msg` | `cdr` | `ros2msg` |
| `ros2idl` | `cdr` | `ros2idl` |

## Minimal Usage

```rust
use std::path::Path;
use mcap2arrow::{McapReader, McapReaderArrowExt};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let reader = McapReader::builder().with_default_decoders().build();

    reader.for_each_record_batch(Path::new("sample.mcap"), "/topic/name", |batch| {
        println!("rows={}, cols={}", batch.num_rows(), batch.num_columns());
        Ok(())
    })?;

    Ok(())
}
```

## Decoded Message Usage

```rust
use std::path::Path;
use mcap2arrow::McapReader;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let reader = McapReader::builder().with_default_decoders().build();

    reader.for_each_decoded_message(Path::new("sample.mcap"), "/topic/name", |message| {
        println!("publish_time={}", message.publish_time);
        Ok(())
    })?;

    Ok(())
}
```

## Related Crates in This Workspace

- `mcap2arrow-core`: schema/value model and shared errors
- `mcap2arrow-arrow`: Arrow conversion implementation
- `mcap2arrow-protobuf`: protobuf decoder
- `mcap2arrow-ros2msg`: ROS 2 `.msg` decoder
- `mcap2arrow-ros2idl`: ROS 2 IDL decoder

## CLI

If you want a command-line interface, see `transmcap`:

- <https://crates.io/crates/transmcap>
- <https://github.com/eduidl/mcap2arrow-rs/tree/main/tools/transmcap>
