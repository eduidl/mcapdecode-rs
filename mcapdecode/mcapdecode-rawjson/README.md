# mcapdecode-rawjson

[![crates.io](https://img.shields.io/crates/v/mcapdecode-rawjson.svg)](https://crates.io/crates/mcapdecode-rawjson)

internal crate for JSON pass-through used by mcapdecode.

Each message is exposed unparsed as a single `json: string` column. The schema is not read, so it
may be empty. Messages only have to be valid UTF-8; their JSON syntax is not checked.
