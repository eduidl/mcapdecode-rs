# transmcap

[![crates.io](https://img.shields.io/crates/v/transmcap.svg)](https://crates.io/crates/transmcap)

`transmcap` is a CLI that decodes MCAP messages through `mcap2arrow` and writes results as JSON Lines, CSV, or Parquet.

## Installation

```bash
cargo install transmcap
```

## Usage

```bash
transmcap <command> <input.mcap> [options]
```

Commands:

- `convert`: convert MCAP messages to `jsonl/csv/parquet`
- `schema`: print inferred field schema for a topic

## Supported Schema Encodings

`transmcap` uses `McapReader::with_default_decoders()`. Supported encoding pairs:

| Schema encoding | Message encoding | Required `mcap2arrow` feature | Notes |
| --- | --- | --- | --- |
| `protobuf` | `protobuf` | `protobuf` | Uses `schema.data` as `FileDescriptorSet` |
| `ros2msg` | `cdr` | `ros2msg` | ROS 2 `.msg` schema |
| `ros2idl` | `cdr` | `ros2idl` | ROS 2 IDL schema |

`mcap2arrow` default features are `protobuf`, `ros2msg`, `ros2idl`.

## `convert` Options

- `-f, --format <FORMAT>`: `jsonl | csv | parquet` (default: `jsonl`)
- `-t, --topic <TOPIC>`: topic name to convert (required)
- `-o, --output <PATH>`: output file path (`jsonl/csv` defaults to stdout)
- `--list-policy <POLICY>`: `drop | keep | flatten-fixed`
- `--list-flatten-size <N>`: only valid with `--list-policy flatten-fixed`
- `--array-policy <POLICY>`: `drop | keep | flatten`
- `--map-policy <POLICY>`: `drop | keep`

## `schema` Options

- `-t, --topic <TOPIC>`: topic name (required)
- `-o, --output <PATH>`: output file path (default: stdout)

## Policy Behavior

`convert` flattens Arrow `RecordBatch` columns before writing.

- `list-policy` (`List`): `drop | keep | flatten-fixed`
- `array-policy` (`FixedSizeList`): `drop | keep | flatten`
- `map-policy` (`Map`): `drop | keep`
- `struct-policy` (`Struct`): fixed by format (not a CLI option)

Semantics:

- `drop`: skip the column
- `keep`: keep the column as-is
- `flatten-fixed`: expand list to fixed columns (`name.0`, `name.1`, ...)
- `flatten`: expand child fields into separate columns

If `--list-flatten-size` is set without `--list-policy flatten-fixed`, command returns an error.

## Format Defaults

| Format | list-policy | array-policy | map-policy | struct-policy | list-flatten-size |
| --- | --- | --- | --- | --- | --- |
| `jsonl` | `keep` | `keep` | `keep` | `keep` | `1` |
| `csv` | `drop` | `drop` | `drop` | `flatten` | `1` |
| `parquet` | `keep` | `keep` | `keep` | `flatten` | `1` |

CLI flags override defaults.

## Examples

### JSON Lines

```bash
transmcap convert sample.mcap --format jsonl --topic /imu/data [-o imu.jsonl]
```

### CSV

```bash
transmcap convert sample.mcap --format csv --topic /imu/data [-o imu.csv]
```

### Parquet to file

parquet output requires `-o/--output`:

```bash
transmcap convert sample.mcap --format parquet --topic /imu/data -o imu.parquet
```

### Custom flatten policy

```bash
transmcap convert sample.mcap --format parquet --topic /imu/data \
  --list-policy drop --array-policy flatten --map-policy drop \
  -o out.parquet
```

### Print schema

```bash
transmcap schema sample.mcap --topic /imu/data
```

## Notes

- `--topic` is required for both commands.
- `parquet` requires `-o/--output`.
- Column name collisions during flattening return an error.

## Development

```bash
cargo run -p transmcap -- <command> <input.mcap> [options]
cargo test -p transmcap
```
