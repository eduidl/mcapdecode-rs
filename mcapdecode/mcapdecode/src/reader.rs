//! MCAP file reader with pluggable decoder support.

use std::{collections::HashMap, fs, path::Path, sync::Arc};

use mcapdecode_core::{
    DecodedMessage, EncodingKey, FieldDefs, MessageDecoder, MessageEncoding, SchemaEncoding,
};
#[cfg(feature = "protobuf")]
use mcapdecode_protobuf::ProtobufDecoder;
#[cfg(feature = "ros2idl")]
use mcapdecode_ros2idl::Ros2IdlDecoder;
#[cfg(feature = "ros2msg")]
use mcapdecode_ros2msg::Ros2MsgDecoder;
use memmap2::Mmap;

use crate::{
    PreparedTopic, RawMessage, ReadOptions, TopicDecodeStatus, TopicInfo, TopicSchema,
    decode::TopicDecodeContext,
    error::McapReaderError,
    summary::{get_channel_from_summary, get_schema_from_channel, topic_infos_from_summary},
};

/// Reads an MCAP file and decodes messages using registered [`MessageDecoder`]s.
pub struct McapReader {
    decoders: HashMap<EncodingKey, Arc<dyn MessageDecoder>>,
    pub(crate) parallel: bool,
}

/// Builder for configuring [`McapReader`].
pub struct McapReaderBuilder {
    decoders: Vec<Arc<dyn MessageDecoder>>,
    parallel: bool,
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

    pub(crate) fn read_summary(&self, mmap: &Mmap) -> Result<mcap::read::Summary, McapReaderError> {
        mcap::read::Summary::read(mmap)?.ok_or(McapReaderError::SummaryNotAvailable)
    }

    fn find_decoder(
        &self,
        schema_enc: &SchemaEncoding,
        message_enc: &MessageEncoding,
    ) -> Result<&Arc<dyn MessageDecoder>, McapReaderError> {
        let key = EncodingKey::new(schema_enc.clone(), message_enc.clone());
        self.decoders
            .get(&key)
            .ok_or_else(|| McapReaderError::NoDecoder {
                schema_encoding: schema_enc.to_string(),
                message_encoding: message_enc.to_string(),
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
        let decoder = Arc::clone(self.find_decoder(&schema_enc, &message_enc)?);
        let topic_decoder = decoder.build_topic_decoder(&schema.name, &schema.data)?;
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
        let summary = self.read_summary(&mmap)?;
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
        let summary = self.read_summary(&mmap)?;
        Ok(topic_infos_from_summary(&summary))
    }

    /// List topics and report whether each topic's schema can be derived with the
    /// registered decoders. The MCAP summary is read once for the whole operation.
    pub fn list_topics_with_decode_status(
        &self,
        path: &Path,
    ) -> Result<Vec<TopicDecodeStatus>, McapReaderError> {
        let mmap = self.mmap_file(path)?;
        let summary = self.read_summary(&mmap)?;

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
        let summary = self.read_summary(&mmap)?;
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

    /// Return the total message count from the MCAP summary section.
    ///
    /// MCAP summary and summary stats are required.
    pub fn message_count(&self, path: &Path, topic: &str) -> Result<u64, McapReaderError> {
        let mmap = self.mmap_file(path)?;
        let summary = self.read_summary(&mmap)?;
        let channel = get_channel_from_summary(&summary, topic)?;

        let stats = summary
            .stats
            .as_ref()
            .ok_or(McapReaderError::StatsNotAvailable)?;

        Ok(stats
            .channel_message_counts
            .get(&channel.id)
            .copied()
            .unwrap_or_default())
    }

    /// Derive and return schema IR (`FieldDef`) for a topic without reading message payloads.
    pub fn topic_field_defs(&self, path: &Path, topic: &str) -> Result<FieldDefs, McapReaderError> {
        let mmap = self.mmap_file(path)?;
        let summary = self.read_summary(&mmap)?;
        let context = self.resolve_topic_decode_context(&summary, topic)?;
        Ok(context.field_defs)
    }

    /// Return topic metadata and schema IR without reading message payloads.
    ///
    /// The MCAP file and its summary are each read once for this operation.
    pub fn topic_schema(&self, path: &Path, topic: &str) -> Result<TopicSchema, McapReaderError> {
        let mmap = self.mmap_file(path)?;
        let summary = self.read_summary(&mmap)?;
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
