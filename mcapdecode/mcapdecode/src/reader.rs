//! The opened file, its decoder configuration and summary lookups.

use std::{
    collections::{BTreeMap, HashMap},
    fs,
    ops::{ControlFlow, RangeInclusive},
    path::Path,
    sync::Arc,
};

use mcapdecode_core::{
    EncodingKey, FieldDefs, MessageDecoder, MessageEncoding, SchemaEncoding, TopicDecoder,
};
#[cfg(feature = "protobuf")]
use mcapdecode_protobuf::ProtobufDecoder;
#[cfg(feature = "rawjson")]
use mcapdecode_rawjson::RawJsonDecoder;
#[cfg(feature = "ros2idl")]
use mcapdecode_ros2idl::Ros2IdlDecoder;
#[cfg(feature = "ros2msg")]
use mcapdecode_ros2msg::Ros2MsgDecoder;
use memmap2::Mmap;

use crate::{
    ChannelInfo, McapReaderError, TopicInfo, chunk_reader::for_each_selected_message, read::Read,
    types::ReadOptions,
};

type Decoders = HashMap<EncodingKey, Arc<dyn MessageDecoder>>;

/// An opened MCAP file that decodes messages using registered [`MessageDecoder`]s.
///
/// Reuses one memory map and summary for metadata queries and reads.
/// Opening requires an MCAP summary; decoders are resolved only when needed.
pub struct McapReader {
    decoders: Decoders,
    parallel: bool,
    mmap: Mmap,
    summary: mcap::read::Summary,
}

/// Builder for configuring and opening a [`McapReader`].
///
/// One builder can open any number of files with the same configuration.
#[derive(Clone)]
pub struct McapReaderBuilder {
    decoders: Decoders,
    parallel: bool,
}

/// A channel's decoder and derived schema, resolved before reading its messages.
pub(crate) struct TopicDecodeContext {
    pub(crate) channel_id: u16,
    #[cfg(feature = "arrow")]
    pub(crate) schema_name: String,
    pub(crate) decoder: Box<dyn TopicDecoder>,
    pub(crate) field_defs: FieldDefs,
}

impl McapReader {
    /// Open a file with all decoders enabled by cargo features.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, McapReaderError> {
        Self::builder().open(path)
    }

    /// Create a builder with all decoders enabled by cargo features registered.
    ///
    /// Use [`McapReaderBuilder::clear_decoders`] to start from no decoders.
    pub fn builder() -> McapReaderBuilder {
        let builder = McapReaderBuilder {
            decoders: HashMap::new(),
            parallel: true,
        };
        #[cfg(feature = "rawjson")]
        let builder = builder.with_decoder(RawJsonDecoder::new());
        #[cfg(feature = "protobuf")]
        let builder = builder.with_decoder(ProtobufDecoder::new());
        #[cfg(feature = "ros2idl")]
        let builder = builder.with_decoder(Ros2IdlDecoder::new());
        #[cfg(feature = "ros2msg")]
        let builder = builder.with_decoder(Ros2MsgDecoder::new());
        builder
    }

    /// Return the inclusive `log_time` range of all messages in the file.
    ///
    /// Uses summary statistics when present, otherwise the chunk indexes.
    /// Returns `None` when the file has no messages or neither source is available.
    pub fn message_time_range(&self) -> Option<RangeInclusive<u64>> {
        if let Some(stats) = &self.summary.stats {
            return (stats.message_count > 0)
                .then_some(stats.message_start_time..=stats.message_end_time);
        }
        // A chunk without messages, such as one holding only schema and channel
        // records, reports zero for both times. Without message indexes this cannot
        // be told apart from messages logged at time zero, which are then ignored.
        let chunks = self.summary.chunk_indexes.iter().filter(|chunk| {
            !(chunk.message_index_offsets.is_empty()
                && chunk.message_start_time == 0
                && chunk.message_end_time == 0)
        });
        let start = chunks.clone().map(|chunk| chunk.message_start_time).min()?;
        let end = chunks.map(|chunk| chunk.message_end_time).max()?;
        Some(start..=end)
    }

    /// Return the summary section read when the file was opened.
    ///
    /// Chunk indexes are sorted by `chunk_start_offset`.
    pub fn summary(&self) -> &mcap::read::Summary {
        &self.summary
    }

    /// List topics from the file summary.
    pub fn topics(&self) -> Vec<TopicInfo> {
        let mut topics = BTreeMap::<String, TopicInfo>::new();
        for channel in self.summary.channels.values() {
            topics
                .entry(channel.topic.clone())
                .and_modify(|topic_info| {
                    topic_info.channel_count += 1;
                    if let (Some(existing), Some(current)) = (
                        topic_info.message_count.as_mut(),
                        self.channel_message_count(channel.id),
                    ) {
                        *existing += current;
                    }
                })
                .or_insert_with(|| self.topic_info(channel));
        }
        topics.into_values().collect()
    }

    /// List channels from the file summary.
    pub fn channels(&self) -> Vec<ChannelInfo> {
        self.summary_channels()
            .into_iter()
            .map(|channel| self.channel_info(channel))
            .collect()
    }

    /// Select messages to read, or the channels whose schemas to derive.
    /// By default all channels are selected.
    pub fn read(&self) -> Read<'_> {
        Read::new(self)
    }

    pub(crate) fn channel_by_id(
        &self,
        channel_id: u16,
    ) -> Result<&Arc<mcap::Channel<'_>>, McapReaderError> {
        self.summary
            .channels
            .get(&channel_id)
            .ok_or(McapReaderError::ChannelNotFound)
    }

    /// The channels of `topic` in ascending id order.
    pub(crate) fn channels_of_topic(
        &self,
        topic: &str,
    ) -> Result<Vec<&Arc<mcap::Channel<'_>>>, McapReaderError> {
        let channels: Vec<_> = self
            .summary_channels()
            .into_iter()
            .filter(|channel| channel.topic == topic)
            .collect();
        if channels.is_empty() {
            return Err(McapReaderError::TopicNotFound);
        }
        Ok(channels)
    }

    /// All channels in ascending id order.
    pub(crate) fn summary_channels(&self) -> Vec<&Arc<mcap::Channel<'_>>> {
        let mut channels: Vec<_> = self.summary.channels.values().collect();
        channels.sort_unstable_by_key(|channel| channel.id);
        channels
    }

    pub(crate) fn channel_info(&self, channel: &mcap::Channel<'_>) -> ChannelInfo {
        let (schema_name, schema_encoding) = schema_name_and_encoding(channel);
        ChannelInfo {
            id: channel.id,
            topic: channel.topic.clone(),
            message_count: self.channel_message_count(channel.id),
            schema_name,
            schema_encoding,
            message_encoding: channel.message_encoding.clone(),
            metadata: channel.metadata.clone(),
        }
    }

    /// Describe a topic as if `channel` were its only channel.
    fn topic_info(&self, channel: &mcap::Channel<'_>) -> TopicInfo {
        TopicInfo {
            topic: channel.topic.clone(),
            message_count: self.channel_message_count(channel.id),
            channel_count: 1,
        }
    }

    pub(crate) fn mmap(&self) -> &[u8] {
        &self.mmap
    }

    pub(crate) fn decode_context(
        &self,
        channel: &mcap::Channel<'_>,
    ) -> Result<TopicDecodeContext, McapReaderError> {
        let schema = channel
            .schema
            .as_ref()
            .ok_or(McapReaderError::SchemaNotAvailable)?;
        let schema_enc = SchemaEncoding::from(schema.encoding.as_str());
        let message_enc = MessageEncoding::from(channel.message_encoding.as_str());
        let key = EncodingKey::new(schema_enc.clone(), message_enc.clone());
        let decoder = self
            .decoders
            .get(&key)
            .ok_or_else(|| McapReaderError::NoDecoder {
                schema_encoding: schema_enc.to_string(),
                message_encoding: message_enc.to_string(),
            })?;
        let topic_decoder = decoder.build_topic_decoder(&schema.name, &schema.data)?;
        let field_defs = topic_decoder.field_defs().clone();
        Ok(TopicDecodeContext {
            channel_id: channel.id,
            #[cfg(feature = "arrow")]
            schema_name: schema.name.clone(),
            decoder: topic_decoder,
            field_defs,
        })
    }

    /// Process the messages of `channel_ids` selected by `options`.
    /// `process` and `callback` receive the message channel's position in `channel_ids`.
    pub(crate) fn for_each_message<T: Send>(
        &self,
        channel_ids: &[u16],
        options: &ReadOptions,
        process: impl Fn(usize, &mcap::Message<'_>) -> Result<T, McapReaderError> + Sync,
        callback: &mut impl FnMut(usize, T) -> Result<ControlFlow<()>, McapReaderError>,
    ) -> Result<(), McapReaderError> {
        for_each_selected_message(
            self,
            channel_ids,
            options.time_range,
            options.parallel.unwrap_or(self.parallel),
            process,
            callback,
        )
    }

    fn channel_message_count(&self, channel_id: u16) -> Option<u64> {
        self.summary.stats.as_ref().map(|stats| {
            stats
                .channel_message_counts
                .get(&channel_id)
                .copied()
                .unwrap_or_default()
        })
    }
}

/// The schema name, if any, and the schema encoding, empty without a schema.
fn schema_name_and_encoding(channel: &mcap::Channel<'_>) -> (Option<String>, String) {
    match &channel.schema {
        Some(schema) => (Some(schema.name.clone()), schema.encoding.clone()),
        None => (None, String::new()),
    }
}

impl McapReaderBuilder {
    /// Register a message decoder, replacing any decoder registered for the
    /// same encoding pair.
    ///
    /// Built-in decoders with non-default settings, such as `ProtobufDecoder`
    /// with a `PresencePolicy`, are registered here too.
    pub fn with_decoder(self, decoder: impl MessageDecoder + 'static) -> Self {
        self.with_shared_decoder(Arc::new(decoder))
    }

    /// Register a shared message decoder.
    pub fn with_shared_decoder(mut self, decoder: Arc<dyn MessageDecoder>) -> Self {
        self.decoders.insert(decoder.encoding_key(), decoder);
        self
    }

    /// Enable or disable parallel chunk decompression and decoding (default: true).
    ///
    /// This is the reader-wide default; a single read can override it through
    /// [`Read::parallel`](crate::Read::parallel).
    pub fn with_parallel(mut self, parallel: bool) -> Self {
        self.parallel = parallel;
        self
    }

    /// Unregister all decoders, including the built-in ones.
    pub fn clear_decoders(mut self) -> Self {
        self.decoders.clear();
        self
    }

    /// Open a file with this builder's decoders and settings.
    pub fn open(&self, path: impl AsRef<Path>) -> Result<McapReader, McapReaderError> {
        let file = fs::File::open(path.as_ref())?;
        let mmap = unsafe { Mmap::map(&file) }?;
        let mut summary =
            mcap::read::Summary::read(&mmap)?.ok_or(McapReaderError::SummaryNotAvailable)?;
        summary
            .chunk_indexes
            .sort_by_key(|index| index.chunk_start_offset);
        Ok(McapReader {
            decoders: self.decoders.clone(),
            parallel: self.parallel,
            mmap,
            summary,
        })
    }
}
