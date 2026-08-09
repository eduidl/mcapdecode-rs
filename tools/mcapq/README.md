# mcapq

`mcapq` is a machine-readable MCAP inspection CLI.

## Usage

```bash
mcapq info <input.mcap>
mcapq schema <input.mcap> --topic /imu/data
```

`info` writes one JSON object to standard output. It lists every topic with its
schema name, summary message count, and whether the built-in `mcapdecode`
decoders can derive its schema. When a topic is not decodable, `decode_error`
states why.

Errors are JSON objects on standard error. Exit status is `0` on success, `1`
for runtime errors, and `2` for invalid arguments.

## Schema inspection

`schema` derives a topic's payload schema without reading message payloads. Its
default `jtd` format is [JSON Type Definition](https://jsontypedef.com/): it
preserves integer and floating-point widths, nullability, and enum variants.
Fixed lengths and bounded collections are included as `x-mcap-*` metadata.
Enum `enum` values are the variants declared by the source schema; an unknown
wire value may still decode as its numeric string representation. Numeric enum
values are available in `x-mcap-enum-values` metadata.

```bash
mcapq schema drive.mcap --topic /imu/data
mcapq schema drive.mcap --topic /imu/data --format native
```

`native` emits mcapdecode's FieldDefs text.
