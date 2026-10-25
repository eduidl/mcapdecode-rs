# mcapdecode-core

[![crates.io](https://img.shields.io/crates/v/mcapdecode-core.svg)](https://crates.io/crates/mcapdecode-core)

internal crate for core schema/value types used by mcapdecode.

Nullable elements use a `?` suffix in schema display: `uint8?[]` allows null
items, `uint8[]?` allows a null collection, and `uint8?[]?` allows both.
This notation describes nullability rather than whether a field can be omitted.
