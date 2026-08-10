# Changelog

All notable changes to this project are documented in this file.

## [Unreleased]

### Breaking Changes

- Removed `McapReaderBuilder::with_batch_size`. Arrow RecordBatch size is now
  set per read through `RecordBatchOptions::batch_size`, and
  `McapReader::for_each_record_batch_with_options` takes `&RecordBatchOptions`
  instead of `&ReadOptions`. [#41]
- ROS 2 `sequence<uint8>` and `sequence<octet>` fields now decode as `Bytes`
  rather than `List<U8>`. Consumers of decoded values or derived schemas must
  handle these fields as bytes. [#27]

### Added

- Added `McapReader::with_prepared_topic` and `PreparedTopic`, which resolve a
  topic's summary and decoder once so an output adapter can derive its schema
  and scan messages without reopening the file. [#40]
- Added reproducible generated MCAP fixtures and benchmark suites for decoding,
  reading, Arrow conversion, and Parquet conversion. [#24]

### Changed

- Arrow RecordBatch reading moved from `mcapdecode` into `mcapdecode-arrow`,
  behind that crate's new `reader` feature. `mcapdecode` re-exports
  `McapReaderArrowExt` and `RecordBatchOptions` as before, so its users see no
  change. [#45]
- Extracted the MCAP reader into a new `mcapdecode-reader` crate. `mcapdecode`
  re-exports it, so `mcapdecode::McapReader` and friends keep working
  unchanged. [#43]
- The sequential reader now uses the MCAP chunk index to skip chunks unrelated
  to the requested topic. [#26]
- Reduced allocations while constructing ROS 2 CDR decoding error paths. [#28]

## [0.5.1] - 2026-05-25

### Fixed

- Correctly consume the CDR placeholder byte for empty or constants-only ROS 2
  structs, preventing subsequent fields from being decoded at the wrong offset.
  [#23]

## [0.5.0] - 2026-05-03

### Added

- Added support for bundled ROS 2 `.msg` schemas containing dependent message
  definitions. [#19]

### Changed

- Improved `mcaptui` topic details: the topic list shows both message and
  schema encodings, loading batches are flushed promptly, and large collection
  values are truncated for responsive rendering. [#20] [#21]

[Unreleased]: https://github.com/eduidl/mcapdecode-rs/compare/v0.5.1...HEAD
[0.5.1]: https://github.com/eduidl/mcapdecode-rs/compare/v0.5.0...v0.5.1
[0.5.0]: https://github.com/eduidl/mcapdecode-rs/compare/v0.4.1...v0.5.0
[#19]: https://github.com/eduidl/mcapdecode-rs/pull/19
[#20]: https://github.com/eduidl/mcapdecode-rs/pull/20
[#21]: https://github.com/eduidl/mcapdecode-rs/pull/21
[#23]: https://github.com/eduidl/mcapdecode-rs/pull/23
[#24]: https://github.com/eduidl/mcapdecode-rs/pull/24
[#26]: https://github.com/eduidl/mcapdecode-rs/pull/26
[#27]: https://github.com/eduidl/mcapdecode-rs/pull/27
[#28]: https://github.com/eduidl/mcapdecode-rs/pull/28
[#40]: https://github.com/eduidl/mcapdecode-rs/pull/40
[#41]: https://github.com/eduidl/mcapdecode-rs/pull/41
[#43]: https://github.com/eduidl/mcapdecode-rs/pull/43
[#45]: https://github.com/eduidl/mcapdecode-rs/pull/45
