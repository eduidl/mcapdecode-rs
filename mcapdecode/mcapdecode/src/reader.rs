//! MCAP file reader with pluggable decoder support.

use std::{
    collections::{BTreeMap, HashMap},
    fs, io,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
};

use mcapdecode_core::{
    DecodedMessage, EncodingKey, FieldDefs, MessageDecoder, MessageEncoding, SchemaEncoding,
    TopicDecoder,
};
#[cfg(feature = "protobuf")]
use mcapdecode_protobuf::ProtobufDecoder;
#[cfg(feature = "ros2idl")]
use mcapdecode_ros2idl::Ros2IdlDecoder;
#[cfg(feature = "ros2msg")]
use mcapdecode_ros2msg::Ros2MsgDecoder;
use memmap2::Mmap;

use crate::error::McapReaderError;

/// Reads an MCAP file and decodes messages using registered [`MessageDecoder`]s.
pub struct McapReader {
    decoders: HashMap<EncodingKey, Arc<dyn MessageDecoder>>,
    parallel: bool,
}

/// Builder for configuring [`McapReader`].
pub struct McapReaderBuilder {
    decoders: Vec<Arc<dyn MessageDecoder>>,
    parallel: bool,
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

    fn overlaps_chunk(self, chunk_index: &mcap::records::ChunkIndex) -> bool {
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
    /// Skip this many messages before emitting any. `0` skips nothing, and an
    /// offset past the last match yields no messages rather than an error.
    ///
    /// Skipping still reads the chunks it passes over, and on the parallel
    /// path those messages are decoded and discarded, so a decode failure
    /// among them still fails the read. Prefer [`ReadOptions::time_range`] to
    /// resume a scan: a start time prunes whole chunks through the index.
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

/// Chunk- and message-level predicates shared by the sequential and parallel
/// read paths.
///
/// The two paths differ in how they schedule decoding, but they must agree on
/// which chunks and messages belong to a read, so both select through this.
#[derive(Debug, Clone, Copy)]
struct MessageFilter {
    channel_id: u16,
    time_range: Option<TimeRange>,
}

impl MessageFilter {
    fn new(context: &TopicDecodeContext, options: &ReadOptions) -> Self {
        Self {
            channel_id: context.channel_id,
            time_range: options.time_range,
        }
    }

    /// Rejected chunks are never decompressed, so this is the read's cheapest
    /// filter and the only one that saves I/O.
    fn accepts_chunk(self, chunk_index: &mcap::records::ChunkIndex) -> bool {
        chunk_index
            .message_index_offsets
            .contains_key(&self.channel_id)
            && self
                .time_range
                .is_none_or(|range| range.overlaps_chunk(chunk_index))
    }

    /// A chunk can hold messages from other channels and from outside the time
    /// range, so surviving [`MessageFilter::accepts_chunk`] is not enough.
    fn accepts_message(self, message: &mcap::Message<'_>) -> bool {
        message.channel.id == self.channel_id
            && self
                .time_range
                .is_none_or(|range| range.contains(message.log_time))
    }
}

impl McapReader {
    /// Create a builder for [`McapReader`].
    pub fn builder() -> McapReaderBuilder {
        McapReaderBuilder {
            decoders: Vec::new(),
            parallel: true,
        }
    }

    pub fn new() -> Self {
        Self {
            decoders: HashMap::new(),
            parallel: true,
        }
    }

    /// Register a decoder for a specific encoding pair.
    pub fn register_decoder(&mut self, decoder: Box<dyn MessageDecoder>) {
        self.register_shared_decoder(Arc::from(decoder));
    }

    /// Register a shared decoder for a specific encoding pair.
    pub fn register_shared_decoder(&mut self, decoder: Arc<dyn MessageDecoder>) {
        self.decoders.insert(decoder.encoding_key(), decoder);
    }

    pub(crate) fn mmap_file(&self, path: &Path) -> Result<Mmap, McapReaderError> {
        let file = fs::File::open(path)?;
        Ok(unsafe { Mmap::map(&file) }?)
    }

    pub(crate) fn read_summary(
        &self,
        path: &Path,
        mmap: &Mmap,
    ) -> Result<mcap::read::Summary, McapReaderError> {
        mcap::read::Summary::read(mmap)?.ok_or_else(|| McapReaderError::SummaryNotAvailable {
            path: path.display().to_string(),
        })
    }

    fn find_decoder(
        &self,
        topic: &str,
        schema_enc: &SchemaEncoding,
        message_enc: &MessageEncoding,
    ) -> Result<&Arc<dyn MessageDecoder>, McapReaderError> {
        let key = EncodingKey::new(schema_enc.clone(), message_enc.clone());
        self.decoders
            .get(&key)
            .ok_or_else(|| McapReaderError::NoDecoder {
                schema_encoding: schema_enc.to_string(),
                message_encoding: message_enc.to_string(),
                topic: topic.to_string(),
            })
    }

    pub(crate) fn resolve_topic_decode_context(
        &self,
        summary: &mcap::read::Summary,
        topic: &str,
    ) -> Result<TopicDecodeContext, McapReaderError> {
        let channel = get_channel_from_summary(summary, topic)?;
        let schema = Arc::clone(get_schema_from_channel(channel)?);
        let schema_enc = SchemaEncoding::from(schema.encoding.as_str());
        let message_enc = MessageEncoding::from(channel.message_encoding.as_str());
        let decoder = Arc::clone(self.find_decoder(&channel.topic, &schema_enc, &message_enc)?);
        let topic_decoder = decoder
            .build_topic_decoder(&schema.name, &schema.data)
            .map_err(|e| McapReaderError::SchemaDerivationFailed {
                topic: topic.to_string(),
                source: e,
            })?;
        let field_defs = topic_decoder.field_defs().clone();

        Ok(TopicDecodeContext {
            channel_id: channel.id,
            schema_name: schema.name.clone(),
            decoder: topic_decoder,
            field_defs,
        })
    }

    /// Prepare one topic for a decoder-backed output adapter.
    ///
    /// The callback receives a handle that reuses this operation's mapped file,
    /// summary, and decoder context. It cannot escape the callback.
    pub fn with_prepared_topic<T>(
        &self,
        path: &Path,
        topic: &str,
        callback: impl FnOnce(&PreparedTopic<'_>) -> Result<T, McapReaderError>,
    ) -> Result<T, McapReaderError> {
        let mmap = self.mmap_file(path)?;
        let summary = self.read_summary(path, &mmap)?;
        let context = self.resolve_topic_decode_context(&summary, topic)?;
        callback(&PreparedTopic {
            reader: self,
            mmap,
            summary,
            context,
            topic: topic.to_string(),
        })
    }

    /// List topics present in the MCAP summary section.
    pub fn list_topics(&self, path: &Path) -> Result<Vec<TopicInfo>, McapReaderError> {
        let mmap = self.mmap_file(path)?;
        let summary = self.read_summary(path, &mmap)?;
        Ok(topic_infos_from_summary(&summary))
    }

    /// List topics and report whether each topic's schema can be derived with the
    /// registered decoders. The MCAP summary is read once for the whole operation.
    pub fn list_topics_with_decode_status(
        &self,
        path: &Path,
    ) -> Result<Vec<TopicDecodeStatus>, McapReaderError> {
        let mmap = self.mmap_file(path)?;
        let summary = self.read_summary(path, &mmap)?;

        Ok(topic_infos_from_summary(&summary)
            .into_iter()
            .map(|topic| {
                let decode_error = self
                    .resolve_topic_decode_context(&summary, &topic.topic)
                    .err()
                    .map(|error| error.to_string());
                TopicDecodeStatus {
                    topic,
                    decodable: decode_error.is_none(),
                    decode_error,
                }
            })
            .collect())
    }

    /// Read decoded messages for a topic and emit them one-by-one to callback.
    ///
    /// Chunks in the MCAP file are decompressed in parallel using rayon.
    /// The callback is still invoked sequentially in file order.
    pub fn for_each_decoded_message(
        &self,
        path: &Path,
        topic: &str,
        callback: impl FnMut(DecodedMessage) -> Result<(), Box<dyn std::error::Error + Send + Sync>>,
    ) -> Result<(), McapReaderError> {
        self.for_each_decoded_message_with_options(path, topic, &ReadOptions::default(), callback)
    }

    /// Read decoded messages subject to `options` and emit them one-by-one to callback.
    pub fn for_each_decoded_message_with_options(
        &self,
        path: &Path,
        topic: &str,
        options: &ReadOptions,
        callback: impl FnMut(DecodedMessage) -> Result<(), Box<dyn std::error::Error + Send + Sync>>,
    ) -> Result<(), McapReaderError> {
        self.with_prepared_topic(path, topic, |prepared| {
            prepared.for_each_decoded_message_with_options(options, callback)
        })
    }

    /// Read raw message payloads for a topic and emit them one-by-one to callback.
    pub fn for_each_raw_message(
        &self,
        path: &Path,
        topic: &str,
        mut callback: impl FnMut(RawMessage) -> Result<(), Box<dyn std::error::Error + Send + Sync>>,
    ) -> Result<(), McapReaderError> {
        let mmap = self.mmap_file(path)?;
        let summary = self.read_summary(path, &mmap)?;
        let channel = get_channel_from_summary(&summary, topic)?;

        for message in mcap::MessageStream::new(&mmap)? {
            let message = message?;
            if message.channel.id != channel.id {
                continue;
            }

            callback(RawMessage {
                log_time: message.log_time,
                publish_time: message.publish_time,
                data: Arc::from(message.data),
            })
            .map_err(McapReaderError::Callback)?;
        }

        Ok(())
    }

    pub(crate) fn for_each_decoded_message_impl<F>(
        &self,
        mmap: &Mmap,
        summary: &mcap::read::Summary,
        context: &TopicDecodeContext,
        request: DecodeRequest<'_>,
        callback: &mut F,
    ) -> Result<(), McapReaderError>
    where
        F: FnMut(DecodedMessage) -> Result<(), McapReaderError>,
    {
        // TODO(P2a): When a limit is set, use per-channel MessageIndex entries to
        // identify the minimal set of chunks that can satisfy it, then decode that
        // bounded set in parallel. Message indexes are optional in MCAP, so files
        // without them must retain this sequential, immediate-stop fallback.
        let parallel = request.options.parallel.unwrap_or(self.parallel);
        if parallel && request.options.limit.is_none() {
            self.for_each_decoded_message_parallel(mmap, summary, context, request, callback)
        } else {
            self.for_each_decoded_message_sequential(mmap, summary, context, request, callback)
        }
    }

    fn for_each_decoded_message_parallel<F>(
        &self,
        mmap: &Mmap,
        summary: &mcap::read::Summary,
        context: &TopicDecodeContext,
        request: DecodeRequest<'_>,
        callback: &mut F,
    ) -> Result<(), McapReaderError>
    where
        F: FnMut(DecodedMessage) -> Result<(), McapReaderError>,
    {
        use rayon::prelude::*;

        let filter = MessageFilter::new(context, request.options);
        let chunk_indexes: Vec<_> = Self::matching_chunk_indexes(summary, filter).collect();
        let chunk_count = chunk_indexes.len();
        let cancelled = Arc::new(AtomicBool::new(false));
        let (sender, receiver) = mpsc::channel();

        std::thread::scope(|scope| -> Result<(), McapReaderError> {
            let worker_sender = sender.clone();
            let worker = scope.spawn(|| {
                chunk_indexes.into_par_iter().enumerate().for_each_with(
                    worker_sender,
                    |sender, (position, chunk_index)| {
                        let result = self.decode_chunk_messages(
                            mmap,
                            summary,
                            context,
                            chunk_index,
                            request,
                            &cancelled,
                        );
                        let _ = sender.send((position, result));
                    },
                );
            });
            drop(sender);

            let mut next_position = 0usize;
            let mut skipped = 0usize;
            let mut pending = BTreeMap::new();
            while next_position < chunk_count {
                let (position, result) = receiver.recv().map_err(|_| {
                    McapReaderError::Io(io::Error::other(
                        "parallel decode worker disconnected unexpectedly",
                    ))
                })?;
                pending.insert(position, result);

                while let Some(result) = pending.remove(&next_position) {
                    let chunk_messages = match result {
                        Ok(messages) => messages,
                        Err(error) => {
                            cancelled.store(true, Ordering::Relaxed);
                            return Err(error);
                        }
                    };
                    for decoded in chunk_messages {
                        // Chunks are decoded before their position in the topic
                        // is known, so the offset can only be applied here.
                        if skipped < request.options.offset {
                            skipped += 1;
                            continue;
                        }
                        if let Err(error) = callback(decoded) {
                            cancelled.store(true, Ordering::Relaxed);
                            return Err(error);
                        }
                    }
                    next_position += 1;
                }
            }
            let _ = worker.join();
            Ok(())
        })?;

        Ok(())
    }

    fn matching_chunk_indexes(
        summary: &mcap::read::Summary,
        filter: MessageFilter,
    ) -> impl Iterator<Item = &mcap::records::ChunkIndex> {
        summary
            .chunk_indexes
            .iter()
            .filter(move |chunk_index| filter.accepts_chunk(chunk_index))
    }

    fn decode_chunk_messages(
        &self,
        mmap: &Mmap,
        summary: &mcap::read::Summary,
        context: &TopicDecodeContext,
        chunk_index: &mcap::records::ChunkIndex,
        request: DecodeRequest<'_>,
        cancelled: &AtomicBool,
    ) -> Result<Vec<DecodedMessage>, McapReaderError> {
        if cancelled.load(Ordering::Relaxed) {
            return Ok(Vec::new());
        }

        let filter = MessageFilter::new(context, request.options);
        let mut decoded_messages = Vec::new();
        for msg_result in summary.stream_chunk(mmap, chunk_index)? {
            if cancelled.load(Ordering::Relaxed) {
                return Ok(decoded_messages);
            }

            let msg = msg_result?;
            if !filter.accepts_message(&msg) {
                continue;
            }
            decoded_messages.push(self.decode_message(
                context,
                request.topic,
                msg.log_time,
                msg.publish_time,
                &msg.data,
            )?);
        }

        Ok(decoded_messages)
    }

    fn for_each_decoded_message_sequential<F>(
        &self,
        mmap: &Mmap,
        summary: &mcap::read::Summary,
        context: &TopicDecodeContext,
        request: DecodeRequest<'_>,
        callback: &mut F,
    ) -> Result<(), McapReaderError>
    where
        F: FnMut(DecodedMessage) -> Result<(), McapReaderError>,
    {
        if request.options.limit == Some(0) {
            return Ok(());
        }
        let filter = MessageFilter::new(context, request.options);
        let mut skipped = 0usize;
        let mut emitted = 0usize;
        for chunk_index in Self::matching_chunk_indexes(summary, filter) {
            for message in summary.stream_chunk(mmap, chunk_index)? {
                let message = message?;
                if !filter.accepts_message(&message) {
                    continue;
                }
                // Skipping here keeps the offset messages out of the decoder.
                if skipped < request.options.offset {
                    skipped += 1;
                    continue;
                }

                let decoded = self.decode_message(
                    context,
                    request.topic,
                    message.log_time,
                    message.publish_time,
                    &message.data,
                )?;
                callback(decoded)?;
                emitted += 1;
                if request.options.limit.is_some_and(|limit| emitted >= limit) {
                    return Ok(());
                }
            }
        }

        Ok(())
    }

    fn decode_message(
        &self,
        context: &TopicDecodeContext,
        topic: &str,
        log_time: u64,
        publish_time: u64,
        data: &[u8],
    ) -> Result<DecodedMessage, McapReaderError> {
        let value =
            context
                .decoder
                .decode(data)
                .map_err(|e| McapReaderError::MessageDecodeFailed {
                    topic: topic.to_string(),
                    source: e,
                })?;

        Ok(DecodedMessage {
            log_time,
            publish_time,
            value,
        })
    }

    /// Return the total message count from the MCAP summary section.
    ///
    /// MCAP summary and summary stats are required.
    pub fn message_count(&self, path: &Path, topic: &str) -> Result<u64, McapReaderError> {
        let mmap = self.mmap_file(path)?;
        let summary = self.read_summary(path, &mmap)?;
        let channel = get_channel_from_summary(&summary, topic)?;

        let stats = summary
            .stats
            .as_ref()
            .ok_or_else(|| McapReaderError::StatsNotAvailable {
                path: path.display().to_string(),
            })?;

        Ok(stats
            .channel_message_counts
            .get(&channel.id)
            .copied()
            .unwrap_or_default())
    }

    /// Derive and return schema IR (`FieldDef`) for a topic without reading message payloads.
    pub fn topic_field_defs(&self, path: &Path, topic: &str) -> Result<FieldDefs, McapReaderError> {
        let mmap = self.mmap_file(path)?;
        let summary = self.read_summary(path, &mmap)?;
        let context = self.resolve_topic_decode_context(&summary, topic)?;
        Ok(context.field_defs)
    }

    /// Return topic metadata and schema IR without reading message payloads.
    ///
    /// The MCAP file and its summary are each read once for this operation.
    pub fn topic_schema(&self, path: &Path, topic: &str) -> Result<TopicSchema, McapReaderError> {
        let mmap = self.mmap_file(path)?;
        let summary = self.read_summary(path, &mmap)?;
        let context = self.resolve_topic_decode_context(&summary, topic)?;
        let info = topic_infos_from_summary(&summary)
            .into_iter()
            .find(|info| info.topic == topic)
            .expect("resolved topic must be present in the summary");

        Ok(TopicSchema {
            info,
            field_defs: context.field_defs,
        })
    }
}

impl Default for McapReader {
    fn default() -> Self {
        Self::new()
    }
}

impl McapReaderBuilder {
    /// Register a message decoder.
    pub fn with_decoder(mut self, decoder: Box<dyn MessageDecoder>) -> Self {
        self.decoders.push(Arc::from(decoder));
        self
    }

    /// Enable or disable parallel chunk decompression and decoding (default: true).
    ///
    /// This is the reader-wide default; a single read can override it through
    /// [`ReadOptions::parallel`].
    pub fn with_parallel(mut self, parallel: bool) -> Self {
        self.parallel = parallel;
        self
    }

    /// Register all built-in decoders (Protobuf).
    pub fn with_default_decoders(self) -> Self {
        let s = self;
        #[cfg(feature = "protobuf")]
        let s = s.with_decoder(Box::new(ProtobufDecoder::new()));
        #[cfg(feature = "ros2idl")]
        let s = s.with_decoder(Box::new(Ros2IdlDecoder::new()));
        #[cfg(feature = "ros2msg")]
        let s = s.with_decoder(Box::new(Ros2MsgDecoder::new()));
        s
    }

    /// Build the reader.
    pub fn build(self) -> McapReader {
        let mut reader = McapReader::new();
        reader.parallel = self.parallel;
        for decoder in self.decoders {
            reader.register_shared_decoder(decoder);
        }
        reader
    }
}

fn get_channel_from_summary<'a>(
    summary: &'a mcap::read::Summary,
    topic: &str,
) -> Result<&'a Arc<mcap::Channel<'a>>, McapReaderError> {
    let mut channels = summary.channels.values().filter(|ch| ch.topic == topic);
    let first = channels
        .next()
        .ok_or_else(|| McapReaderError::TopicNotFound {
            topic: topic.to_string(),
        })?;
    if channels.next().is_some() {
        return Err(McapReaderError::MultipleChannels {
            topic: topic.to_string(),
        });
    }
    Ok(first)
}

fn topic_infos_from_summary(summary: &mcap::read::Summary) -> Vec<TopicInfo> {
    let stats = summary.stats.as_ref();
    let mut topics = BTreeMap::<String, TopicInfo>::new();

    for channel in summary.channels.values() {
        let message_count = stats.map(|summary_stats| {
            summary_stats
                .channel_message_counts
                .get(&channel.id)
                .copied()
                .unwrap_or_default()
        });
        let schema = channel.schema.as_ref();

        topics
            .entry(channel.topic.clone())
            .and_modify(|topic_info| {
                topic_info.channel_count += 1;
                if let (Some(existing), Some(current)) =
                    (topic_info.message_count.as_mut(), message_count)
                {
                    *existing += current;
                }
            })
            .or_insert_with(|| TopicInfo {
                topic: channel.topic.clone(),
                message_count,
                schema_name: schema.map(|schema| schema.name.clone()),
                schema_encoding: schema
                    .map(|schema| schema.encoding.clone())
                    .unwrap_or_default(),
                message_encoding: channel.message_encoding.clone(),
                channel_count: 1,
            });
    }

    topics.into_values().collect()
}

fn get_schema_from_channel<'a>(
    channel: &'a Arc<mcap::Channel>,
) -> Result<&'a Arc<mcap::Schema<'a>>, McapReaderError> {
    channel
        .schema
        .as_ref()
        .ok_or_else(|| McapReaderError::SchemaNotAvailable {
            topic: channel.topic.clone(),
            channel_id: channel.id,
        })
}

/// A topic whose MCAP summary and decoder have already been resolved.
///
/// Values are created only through [`McapReader::with_prepared_topic`]. This
/// keeps the mapped MCAP file, its summary, and decoder context together so an
/// output adapter can derive a schema and scan messages without reopening the
/// file or rereading the summary.
pub struct PreparedTopic<'reader> {
    reader: &'reader McapReader,
    mmap: Mmap,
    summary: mcap::read::Summary,
    context: TopicDecodeContext,
    topic: String,
}

impl PreparedTopic<'_> {
    /// The topic name requested when this value was prepared.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// The MCAP schema name associated with this topic.
    pub fn schema_name(&self) -> &str {
        &self.context.schema_name
    }

    /// The schema IR derived by the registered decoder for this topic.
    pub fn field_defs(&self) -> &FieldDefs {
        &self.context.field_defs
    }

    /// Read decoded messages subject to `options` and emit them one-by-one to
    /// callback.
    pub fn for_each_decoded_message_with_options(
        &self,
        options: &ReadOptions,
        mut callback: impl FnMut(DecodedMessage) -> Result<(), Box<dyn std::error::Error + Send + Sync>>,
    ) -> Result<(), McapReaderError> {
        self.reader.for_each_decoded_message_impl(
            &self.mmap,
            &self.summary,
            &self.context,
            DecodeRequest {
                topic: &self.topic,
                options,
            },
            &mut |decoded| callback(decoded).map_err(McapReaderError::Callback),
        )
    }
}

pub(crate) struct TopicDecodeContext {
    pub(crate) channel_id: u16,
    pub(crate) schema_name: String,
    pub(crate) decoder: Box<dyn TopicDecoder>,
    pub(crate) field_defs: FieldDefs,
}

#[derive(Clone, Copy)]
pub(crate) struct DecodeRequest<'a> {
    pub(crate) topic: &'a str,
    pub(crate) options: &'a ReadOptions,
}
