# mcapdecode Changelog

All notable changes to the `mcapdecode` library are documented in this file.
Changes to workspace tools, benchmarks, and development infrastructure are
intentionally excluded.

## [Unreleased]

### Breaking Changes

- Removed `McapReaderBuilder::with_batch_size`. Arrow RecordBatch size is now
  set per read through `RecordBatchOptions::batch_size`, and
  `McapReader::for_each_record_batch_with_options` takes `&RecordBatchOptions`
  instead of `&ReadOptions`. [#41]
- Replaced the public `McapReaderArrowExt` trait with inherent Arrow
  record-batch reading methods on `McapReader`. [#32]
- ROS 2 `sequence<uint8>` and `sequence<octet>` fields now decode as `Bytes`
  rather than `List<U8>`. Consumers of decoded values or derived schemas must
  handle these fields as bytes. [#27]
- Added `Enum`, `WString`, and bounded string, bytes, and list variants to the
  public `DataTypeDef` enum. Consumers that match it exhaustively must handle
  the new variants. [#35] [#37]

### Added

- Added `McapReader::with_prepared_topic` and `PreparedTopic`, which resolve a
  topic's summary and decoder once so an output adapter can derive its schema
  and scan messages without reopening the file. [#40]
- Added `ReadOptions` and `TimeRange`, plus option-aware decoded-message and
  Arrow RecordBatch reads. Reads can filter by log time, page with offset and
  limit, and override parallel decoding per operation. [#39]
- Derived schemas now preserve protobuf and ROS 2 IDL enum numeric values, as
  well as ROS 2 bounded collection and string constraints. [#35] [#37]

### Changed

- The sequential reader now uses the MCAP chunk index to skip chunks unrelated
  to the requested topic. [#26]
- Reduced allocations while constructing ROS 2 CDR decoding error paths. [#28]
- Replaced the ROS 2 IDL parser with a lexer-based implementation, improving
  support for valid IDL syntax and unsigned-type diagnostics. [#36]

## [0.5.1] - 2026-05-25

### Fixed

- Correctly consume the CDR placeholder byte for empty or constants-only ROS 2
  structs, preventing subsequent fields from being decoded at the wrong offset.
  [#23]

## [0.5.0] - 2026-05-03

### Added

- Added support for bundled ROS 2 `.msg` schemas containing dependent message
  definitions. [#19]

[Unreleased]: https://github.com/eduidl/mcapdecode-rs/compare/v0.5.1...HEAD
[0.5.1]: https://github.com/eduidl/mcapdecode-rs/compare/v0.5.0...v0.5.1
[0.5.0]: https://github.com/eduidl/mcapdecode-rs/compare/v0.4.1...v0.5.0
[#19]: https://github.com/eduidl/mcapdecode-rs/pull/19
[#23]: https://github.com/eduidl/mcapdecode-rs/pull/23
[#26]: https://github.com/eduidl/mcapdecode-rs/pull/26
[#27]: https://github.com/eduidl/mcapdecode-rs/pull/27
[#28]: https://github.com/eduidl/mcapdecode-rs/pull/28
[#32]: https://github.com/eduidl/mcapdecode-rs/pull/32
[#35]: https://github.com/eduidl/mcapdecode-rs/pull/35
[#36]: https://github.com/eduidl/mcapdecode-rs/pull/36
[#37]: https://github.com/eduidl/mcapdecode-rs/pull/37
[#39]: https://github.com/eduidl/mcapdecode-rs/pull/39
[#40]: https://github.com/eduidl/mcapdecode-rs/pull/40
[#41]: https://github.com/eduidl/mcapdecode-rs/pull/41
