//! Value types describing topics and messages, and internal read options.

use std::{
    collections::BTreeMap,
    ops::{Bound, RangeBounds},
    sync::Arc,
};

use mcapdecode_core::FieldDefs;

/// Metadata about one channel discovered from the MCAP summary section.
///
/// A topic can span several channels, each with its own schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelInfo {
    pub id: u16,
    pub topic: String,
    pub message_count: Option<u64>,
    pub schema_name: Option<String>,
    pub schema_encoding: String,
    pub message_encoding: String,
    pub metadata: BTreeMap<String, String>,
}

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

/// A `log_time` range in nanoseconds used to filter messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TimeRange {
    start: Bound<u64>,
    end: Bound<u64>,
}

impl TimeRange {
    /// Copy the bounds of `range`, or return `None` when it is unbounded on both sides.
    pub(crate) fn new(range: impl RangeBounds<u64>) -> Option<Self> {
        let start = range.start_bound().cloned();
        let end = range.end_bound().cloned();
        (start != Bound::Unbounded || end != Bound::Unbounded).then_some(Self { start, end })
    }

    pub(crate) fn contains(self, time: u64) -> bool {
        (self.start, self.end).contains(&time)
    }

    /// Whether a chunk whose messages span `message_start_time..=message_end_time`
    /// may hold a message in this range.
    pub(crate) fn overlaps_chunk(self, chunk_index: &mcap::records::ChunkIndex) -> bool {
        let after_start = match self.start {
            Bound::Included(start) => chunk_index.message_end_time >= start,
            Bound::Excluded(start) => chunk_index.message_end_time > start,
            Bound::Unbounded => true,
        };
        let before_end = match self.end {
            Bound::Included(end) => chunk_index.message_start_time <= end,
            Bound::Excluded(end) => chunk_index.message_start_time < end,
            Bound::Unbounded => true,
        };
        after_start && before_end
    }
}

/// Options shared by raw, decoded-message and RecordBatch reads.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct ReadOptions {
    pub(crate) time_range: Option<TimeRange>,
    /// `None` keeps the reader's setting.
    pub(crate) parallel: Option<bool>,
}
