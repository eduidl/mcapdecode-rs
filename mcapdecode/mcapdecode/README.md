# mcapdecode

[![crates.io](https://img.shields.io/crates/v/mcapdecode.svg)](https://crates.io/crates/mcapdecode)

`mcapdecode` is a Rust library that decodes MCAP channels/messages into a normalized schema/value model and can optionally expose Apache Arrow `RecordBatch` streams.

## Installation

```toml
[dependencies]
mcapdecode = "0.3.0"
```

## What It Provides

- Reader API over MCAP files and memory maps
- Decoded message API that stays independent from Arrow
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
mcapdecode = { version = "0.3.0", default-features = false, features = ["protobuf"] }
```

Enable `arrow` only when you need `RecordBatch` output:

```toml
[dependencies]
mcapdecode = { version = "0.3.0", default-features = false, features = ["arrow", "protobuf"] }
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
use mcapdecode::{McapReader, McapReaderArrowExt};

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

## Related Crates in This Workspace

- `mcapdecode-core`: schema/value model and shared errors
- `mcapdecode-arrow`: Arrow conversion implementation
- `mcapdecode-protobuf`: protobuf decoder
- `mcapdecode-ros2msg`: ROS 2 `.msg` decoder
- `mcapdecode-ros2idl`: ROS 2 IDL decoder

## CLI

If you want a command-line interface, see `transmcap`:

- <https://crates.io/crates/transmcap>
- <https://github.com/eduidl/mcapdecode-rs/tree/main/tools/transmcap>
