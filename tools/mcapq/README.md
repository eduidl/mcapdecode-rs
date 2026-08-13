# mcapq

`mcapq` is a [Model Context Protocol](https://modelcontextprotocol.io/) (MCP)
server for AI-assisted MCAP inspection. It communicates over standard
input/output; do not run it through a shell pipeline or use its standard output
for logs.

## Start the server

Pass every directory from which the server may read MCAP files. A requested
path is canonicalized before use, so symlinks cannot escape an allowed root.

```bash
mcapq --allow-root /data/recordings --allow-root /tmp/investigations
```

The server accepts no single-shot subcommands. It stays alive for its MCP
connection and reads each MCAP file on demand.

## Codex setup

Build or install `mcapq`, then add a local stdio server to your Codex
configuration. Keep the configured roots as narrow as practical.

```toml
[mcp_servers.mcapq]
command = "/absolute/path/to/mcapq"
args = ["--allow-root", "/absolute/path/to/recordings"]
```

Alternatively, use Cargo while developing this repository:

```toml
[mcp_servers.mcapq]
command = "cargo"
args = ["run", "--quiet", "--release", "-p", "mcapq", "--", "--allow-root", "/absolute/path/to/recordings"]
```

The process's current directory must be this workspace for the Cargo form.

## Tools

Every tool accepts a `path` beneath an allowed root and returns structured JSON.

| Tool | Purpose |
| --- | --- |
| `mcap_info(path)` | List topics, schemas, summary counts, and decode errors. |
| `mcap_schema(path, topic)` | Return the topic schema as JTD. |

Pass absolute paths beneath an `--allow-root` to every tool. `mcap_schema`
describes the payload as it would appear in JSON: `log_time` and `publish_time`
lead every message as Unix-epoch nanoseconds encoded as decimal strings, and
all `int64` and `uint64` values are decimal strings. Fields marked nullable in
the source schema accept an explicit `null`. Binary values are hex-encoded
strings, and protobuf maps are JSON objects. Non-string map keys are converted
to protobuf JSON key strings; their source type is available as
`x-mcap-key-type` metadata. The schema carries `x-mcap-original-type` metadata
when a source type is represented differently in JSON; enums additionally
provide `x-mcap-enum-variants` name/value pairs.

All tools currently require an MCAP Summary section. Files without one are not
supported.

## Not yet implemented

This server describes MCAP files; it does not yet return their messages. Reading
decoded rows and querying across topics with SQL are both planned.
