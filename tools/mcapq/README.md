# mcapq

`mcapq` is a machine-readable MCAP inspection CLI.

## Usage

```bash
mcapq info <input.mcap>
```

`info` writes one JSON object to standard output. It lists every topic with its
schema name, summary message count, and whether the built-in `mcapdecode`
decoders can derive its schema. When a topic is not decodable, `decode_error`
states why.

Errors are JSON objects on standard error. Exit status is `0` on success, `1`
for runtime errors, and `2` for invalid arguments.
