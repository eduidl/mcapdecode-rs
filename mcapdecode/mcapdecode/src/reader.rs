//! MCAP file reader with pluggable decoder support.

use std::{path::Path, sync::Arc};

use mcapdecode_core::{DecodedMessage, FieldDefs, MessageDecoder};
#[cfg(feature = "protobuf")]
use mcapdecode_protobuf::ProtobufDecoder;
#[cfg(feature = "rawjson")]
use mcapdecode_rawjson::RawJsonDecoder;
#[cfg(feature = "ros2idl")]
use mcapdecode_ros2idl::Ros2IdlDecoder;
#[cfg(feature = "ros2msg")]
use mcapdecode_ros2msg::Ros2MsgDecoder;

use crate::{
    ChannelInfo, PreparedTopic, RawMessage, ReadOptions, TopicDecodeStatus, TopicInfo, TopicSchema,
    error::McapReaderError,
    internal_reader::{InternalReader, for_each_raw_message_impl},
    mcap_file::McapFile,
    summary::{
        channel_infos_from_summary, get_channel_by_id, get_channel_from_summary,
        topic_infos_from_summary,
    },
};

/// Reads an MCAP file and decodes messages using registered [`MessageDecoder`]s.
pub struct McapReader {
    pub(crate) inner: InternalReader,
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
            inner: InternalReader::new(),
        }
    }

    /// Register a decoder for a specific encoding pair.
    pub fn register_decoder(&mut self, decoder: Box<dyn MessageDecoder>) {
        self.register_shared_decoder(Arc::from(decoder));
    }

    /// Register a shared decoder for a specific encoding pair.
    pub fn register_shared_decoder(&mut self, decoder: Arc<dyn MessageDecoder>) {
        self.inner.register_shared_decoder(decoder);
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
        let file = McapFile::open(path)?;
        let context = self
            .inner
            .resolve_topic_decode_context(&file.summary, topic)?;
        callback(&PreparedTopic {
            reader: &self.inner,
            file,
            context,
            topic: topic.to_string(),
        })
    }

    /// List topics present in the MCAP summary section.
    pub fn list_topics(&self, path: &Path) -> Result<Vec<TopicInfo>, McapReaderError> {
        let file = McapFile::open(path)?;
        Ok(topic_infos_from_summary(&file.summary))
    }

    /// List every channel in the MCAP summary section, in ascending id order.
    pub fn list_channels(&self, path: &Path) -> Result<Vec<ChannelInfo>, McapReaderError> {
        let file = McapFile::open(path)?;
        Ok(channel_infos_from_summary(&file.summary))
    }

    /// List topics and report whether each topic's schema can be derived with the
    /// registered decoders. The MCAP summary is read once for the whole operation.
    pub fn list_topics_with_decode_status(
        &self,
        path: &Path,
    ) -> Result<Vec<TopicDecodeStatus>, McapReaderError> {
        let file = McapFile::open(path)?;
        self.inner.list_topics_with_decode_status(&file.summary)
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
        mut callback: impl FnMut(DecodedMessage) -> Result<(), Box<dyn std::error::Error + Send + Sync>>,
    ) -> Result<(), McapReaderError> {
        self.for_each_topic_decoded_message_with_options(path, &[topic], options, |_, message| {
            callback(message)
        })
    }

    /// Read decoded messages of one channel and emit them one-by-one to callback.
    pub fn for_each_decoded_message_by_channel_id(
        &self,
        path: &Path,
        channel_id: u16,
        callback: impl FnMut(DecodedMessage) -> Result<(), Box<dyn std::error::Error + Send + Sync>>,
    ) -> Result<(), McapReaderError> {
        self.for_each_decoded_message_by_channel_id_with_options(
            path,
            channel_id,
            &ReadOptions::default(),
            callback,
        )
    }

    /// Read decoded messages of one channel subject to `options` and emit them
    /// one-by-one to callback.
    pub fn for_each_decoded_message_by_channel_id_with_options(
        &self,
        path: &Path,
        channel_id: u16,
        options: &ReadOptions,
        mut callback: impl FnMut(DecodedMessage) -> Result<(), Box<dyn std::error::Error + Send + Sync>>,
    ) -> Result<(), McapReaderError> {
        let file = McapFile::open(path)?;
        let context = self
            .inner
            .resolve_channel_decode_context(get_channel_by_id(&file.summary, channel_id)?)?;
        self.inner
            .for_each_decoded_message_impl(&file, &context, options, &mut |decoded| {
                callback(decoded).map_err(McapReaderError::Callback)
            })
    }

    /// Read raw message payloads for a topic and emit them one-by-one to callback.
    pub fn for_each_raw_message(
        &self,
        path: &Path,
        topic: &str,
        callback: impl FnMut(RawMessage) -> Result<(), Box<dyn std::error::Error + Send + Sync>>,
    ) -> Result<(), McapReaderError> {
        let file = McapFile::open(path)?;
        let channel = get_channel_from_summary(&file.summary, topic)?;
        for_each_raw_message_impl(&file.mmap, channel.id, callback)
    }

    /// Read raw message payloads of one channel and emit them one-by-one to callback.
    pub fn for_each_raw_message_by_channel_id(
        &self,
        path: &Path,
        channel_id: u16,
        callback: impl FnMut(RawMessage) -> Result<(), Box<dyn std::error::Error + Send + Sync>>,
    ) -> Result<(), McapReaderError> {
        let file = McapFile::open(path)?;
        get_channel_by_id(&file.summary, channel_id)?;
        for_each_raw_message_impl(&file.mmap, channel_id, callback)
    }

    /// Return the total message count from the MCAP summary section.
    ///
    /// MCAP summary and summary stats are required.
    pub fn message_count(&self, path: &Path, topic: &str) -> Result<u64, McapReaderError> {
        let file = McapFile::open(path)?;
        crate::summary::message_count(&file.summary, topic)
    }

    /// Derive and return schema IR (`FieldDef`) for a topic without reading message payloads.
    pub fn topic_field_defs(&self, path: &Path, topic: &str) -> Result<FieldDefs, McapReaderError> {
        let file = McapFile::open(path)?;
        let context = self
            .inner
            .resolve_topic_decode_context(&file.summary, topic)?;
        Ok(context.field_defs)
    }

    /// Derive and return schema IR (`FieldDef`) for one channel without reading
    /// message payloads.
    pub fn field_defs_by_channel_id(
        &self,
        path: &Path,
        channel_id: u16,
    ) -> Result<FieldDefs, McapReaderError> {
        let file = McapFile::open(path)?;
        let context = self
            .inner
            .resolve_channel_decode_context(get_channel_by_id(&file.summary, channel_id)?)?;
        Ok(context.field_defs)
    }

    /// Return topic metadata and schema IR without reading message payloads.
    ///
    /// The MCAP file and its summary are each read once for this operation.
    pub fn topic_schema(&self, path: &Path, topic: &str) -> Result<TopicSchema, McapReaderError> {
        let file = McapFile::open(path)?;
        self.inner.topic_schema(&file.summary, topic)
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

    /// Register all built-in decoders enabled by cargo features.
    pub fn with_default_decoders(self) -> Self {
        let s = self;
        #[cfg(feature = "rawjson")]
        let s = s.with_decoder(Box::new(RawJsonDecoder::new()));
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
        reader.inner.parallel = self.parallel;
        for decoder in self.decoders {
            reader.register_shared_decoder(decoder);
        }
        reader
    }
}
