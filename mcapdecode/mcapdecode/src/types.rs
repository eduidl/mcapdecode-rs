//! Value types describing topics, messages, and read options.

use std::{collections::BTreeMap, sync::Arc};

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
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ReadOptions {
    /// Restrict messages to this `log_time` range.
    pub time_range: Option<TimeRange>,
    /// Override parallel chunk decompression for this read. `None` keeps the
    /// reader's setting from [`crate::McapReaderBuilder::with_parallel`].
    pub parallel: Option<bool>,
}
