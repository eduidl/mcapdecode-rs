//! Value types describing topics, messages, and read options.

use std::sync::Arc;

use mcapdecode_core::FieldDefs;

/// Metadata about a topic discovered from the MCAP summary section.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicInfo {
    pub topic: String,
    pub message_count: Option<u64>,
    pub schema_name: Option<String>,
    pub schema_encoding: String,
    pub message_encoding: String,
    pub channel_count: usize,
}

/// Topic metadata and derived schema IR obtained from one MCAP summary read.
#[derive(Debug, Clone, PartialEq)]
pub struct TopicSchema {
    pub info: TopicInfo,
    pub field_defs: FieldDefs,
}

/// Decode support status for a topic discovered from the MCAP summary section.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicDecodeStatus {
    pub topic: TopicInfo,
    pub decodable: bool,
    pub decode_error: Option<String>,
}

/// Raw message payload for topics that cannot be decoded structurally.
#[derive(Debug, Clone)]
pub struct RawMessage {
    pub log_time: u64,
    pub publish_time: u64,
    pub data: Arc<[u8]>,
}

/// Inclusive/exclusive nanosecond range used to filter messages by `log_time`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TimeRange {
    /// Inclusive lower bound.
    pub start: Option<u64>,
    /// Exclusive upper bound.
    pub end: Option<u64>,
}

impl TimeRange {
    pub fn contains(self, time: u64) -> bool {
        self.start.is_none_or(|start| time >= start) && self.end.is_none_or(|end| time < end)
    }

    pub(crate) fn overlaps_chunk(self, chunk_index: &mcap::records::ChunkIndex) -> bool {
        self.start
            .is_none_or(|start| chunk_index.message_end_time >= start)
            && self
                .end
                .is_none_or(|end| chunk_index.message_start_time < end)
    }
}

/// Options shared by filtered decoded-message and RecordBatch reads.
///
/// The fields apply in a fixed order: [`time_range`] selects the messages,
/// then [`offset`] skips from the front of that selection and [`limit`] caps
/// how many of the rest are emitted, so the two page through the filtered
/// messages of a topic.
///
/// [`time_range`]: ReadOptions::time_range
/// [`offset`]: ReadOptions::offset
/// [`limit`]: ReadOptions::limit
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ReadOptions {
    /// Restrict messages to this `log_time` range.
    pub time_range: Option<TimeRange>,
    /// Skip this many messages before emitting any. `0` skips nothing.
    ///
    /// An offset past the last matching message yields no messages rather than
    /// an error. Skipping cannot avoid decompressing the messages it passes
    /// over: MCAP chunk indexes carry no per-chunk message count, so reaching
    /// offset N still requires reading the preceding chunks. Prefer
    /// [`ReadOptions::time_range`] to resume a scan, since a start time does
    /// prune whole chunks through the index.
    ///
    /// Only the sequential path skips without decoding. On the parallel path a
    /// chunk is decoded before its position in the topic is known, so skipped
    /// messages are decoded and discarded — and a decode failure among them
    /// still fails the read.
    pub offset: usize,
    /// Stop after this many emitted messages. `None` has no limit.
    ///
    /// A limit forces the sequential read path so the scan can stop as soon as
    /// it is reached, overriding [`ReadOptions::parallel`] and the reader's own
    /// setting from [`McapReaderBuilder::with_parallel`].
    pub limit: Option<usize>,
    /// Override parallel chunk decompression for this read. `None` keeps the
    /// reader's setting from [`McapReaderBuilder::with_parallel`].
    ///
    /// Ignored when [`ReadOptions::limit`] is set, since stopping early
    /// requires reading chunks in order.
    pub parallel: Option<bool>,
}
