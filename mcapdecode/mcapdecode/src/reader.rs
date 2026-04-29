//! MCAP file reader with pluggable decoder support.

use std::{
    collections::{BTreeMap, HashMap},
    fs,
    path::Path,
    sync::Arc,
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
    batch_size: usize,
    parallel: bool,
}

/// Builder for configuring [`McapReader`].
pub struct McapReaderBuilder {
    decoders: Vec<Arc<dyn MessageDecoder>>,
    batch_size: usize,
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

/// Raw message payload for topics that cannot be decoded structurally.
#[derive(Debug, Clone)]
pub struct RawMessage {
    pub log_time: u64,
    pub publish_time: u64,
    pub data: Arc<[u8]>,
}

impl McapReader {
    /// Create a builder for [`McapReader`].
    pub fn builder() -> McapReaderBuilder {
        McapReaderBuilder {
            decoders: Vec::new(),
            batch_size: 1024,
            parallel: true,
        }
    }

    pub fn new() -> Self {
        Self {
            decoders: HashMap::new(),
            batch_size: 1024,
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

    #[cfg(feature = "arrow")]
    pub(crate) fn batch_size(&self) -> usize {
        self.batch_size
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
            decoder: topic_decoder,
            field_defs,
        })
    }

    /// List topics present in the MCAP summary section.
    pub fn list_topics(&self, path: &Path) -> Result<Vec<TopicInfo>, McapReaderError> {
        let mmap = self.mmap_file(path)?;
        let summary = self.read_summary(path, &mmap)?;
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

        Ok(topics.into_values().collect())
    }

    /// Read decoded messages for a topic and emit them one-by-one to callback.
    ///
    /// Chunks in the MCAP file are decompressed in parallel using rayon.
    /// The callback is still invoked sequentially in file order.
    pub fn for_each_decoded_message(
        &self,
        path: &Path,
        topic: &str,
        mut callback: impl FnMut(DecodedMessage) -> Result<(), Box<dyn std::error::Error + Send + Sync>>,
    ) -> Result<(), McapReaderError> {
        let mmap = self.mmap_file(path)?;
        let summary = self.read_summary(path, &mmap)?;
        let context = self.resolve_topic_decode_context(&summary, topic)?;
        self.for_each_decoded_message_impl(&mmap, &summary, &context, topic, &mut |decoded| {
            callback(decoded).map_err(McapReaderError::Callback)
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
        topic: &str,
        callback: &mut F,
    ) -> Result<(), McapReaderError>
    where
        F: FnMut(DecodedMessage) -> Result<(), McapReaderError>,
    {
        if self.parallel {
            self.for_each_decoded_message_parallel(mmap, summary, context, topic, callback)
        } else {
            self.for_each_decoded_message_sequential(mmap, context, topic, callback)
        }
    }

    fn for_each_decoded_message_parallel<F>(
        &self,
        mmap: &Mmap,
        summary: &mcap::read::Summary,
        context: &TopicDecodeContext,
        topic: &str,
        callback: &mut F,
    ) -> Result<(), McapReaderError>
    where
        F: FnMut(DecodedMessage) -> Result<(), McapReaderError>,
    {
        use rayon::prelude::*;

        // chunk_indexes preserves file order; par_iter preserves that order in results.
        let chunk_decoded: Vec<Vec<DecodedMessage>> = summary
            .chunk_indexes
            .par_iter()
            .filter(|ci| ci.message_index_offsets.contains_key(&context.channel_id))
            .map(
                |chunk_index| -> Result<Vec<DecodedMessage>, McapReaderError> {
                    summary
                        .stream_chunk(mmap, chunk_index)?
                        .filter_map(|msg_result| match msg_result {
                            Ok(msg) if msg.channel.id == context.channel_id => {
                                Some(self.decode_message(
                                    context,
                                    topic,
                                    msg.log_time,
                                    msg.publish_time,
                                    &msg.data,
                                ))
                            }
                            Ok(_) => None,
                            Err(e) => Some(Err(e.into())),
                        })
                        .collect()
                },
            )
            .collect::<Result<Vec<_>, McapReaderError>>()?;

        for chunk_messages in chunk_decoded {
            for decoded in chunk_messages {
                callback(decoded)?;
            }
        }

        Ok(())
    }

    fn for_each_decoded_message_sequential<F>(
        &self,
        mmap: &Mmap,
        context: &TopicDecodeContext,
        topic: &str,
        callback: &mut F,
    ) -> Result<(), McapReaderError>
    where
        F: FnMut(DecodedMessage) -> Result<(), McapReaderError>,
    {
        for message in mcap::MessageStream::new(mmap)? {
            let message = message?;
            if message.channel.id != context.channel_id {
                continue;
            }

            let decoded = self.decode_message(
                context,
                topic,
                message.log_time,
                message.publish_time,
                &message.data,
            )?;
            callback(decoded)?;
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

    /// Set the number of messages per RecordBatch (default: 1024).
    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }

    /// Enable or disable parallel chunk decompression and decoding (default: true).
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
        reader.batch_size = self.batch_size;
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

pub(crate) struct TopicDecodeContext {
    pub(crate) channel_id: u16,
    pub(crate) decoder: Box<dyn TopicDecoder>,
    pub(crate) field_defs: FieldDefs,
}
