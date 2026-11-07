//! File-backed reader and message selection.

use std::{
    collections::HashSet,
    ops::{ControlFlow, RangeInclusive},
    path::Path,
    sync::Arc,
};

use mcapdecode_core::{DecodedMessage, FieldDefs};

use crate::{
    ChannelInfo, McapReaderBuilder, McapReaderError, RawMessage, ReadOptions, TopicDecodeStatus,
    TopicInfo, TopicSchema,
    chunk_reader::for_each_selected_message,
    internal_reader::InternalReader,
    mcap_file::McapFile,
    summary::{
        channel_info, channel_infos_from_summary, get_channel_by_id, get_channel_from_summary,
        topic_infos_from_summary,
    },
};

/// An opened MCAP file that decodes messages using registered decoders.
///
/// Reuses one memory map and summary for metadata queries and reads.
/// Opening requires an MCAP summary; decoders are resolved only when needed.
pub struct McapReader {
    reader: InternalReader,
    file: McapFile,
}

impl McapReader {
    /// Open a file with all decoders enabled by cargo features.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, McapReaderError> {
        Self::builder().with_default_decoders().open(path)
    }

    /// Create a builder with no decoders registered.
    pub fn builder() -> McapReaderBuilder {
        McapReaderBuilder::new()
    }

    pub(crate) fn from_parts(reader: InternalReader, path: &Path) -> Result<Self, McapReaderError> {
        Ok(Self {
            reader,
            file: McapFile::open(path)?,
        })
    }
    /// Return topic metadata and schema IR without reading message payloads.
    pub fn topic_schema(&self, topic: &str) -> Result<TopicSchema, McapReaderError> {
        self.reader.topic_schema(&self.file.summary, topic)
    }

    /// List topics and report whether their schemas can be derived by the configured decoders.
    pub fn list_topics_with_decode_status(&self) -> Vec<TopicDecodeStatus> {
        self.reader
            .list_topics_with_decode_status(&self.file.summary)
    }

    /// Derive the schema of a topic without reading messages.
    pub fn topic_field_defs(&self, topic: &str) -> Result<FieldDefs, McapReaderError> {
        Ok(self
            .reader
            .resolve_topic_decode_context(&self.file.summary, topic)?
            .field_defs)
    }

    /// Derive the schema of a channel without reading messages.
    pub fn field_defs_by_channel_id(&self, id: u16) -> Result<FieldDefs, McapReaderError> {
        let channel = get_channel_by_id(&self.file.summary, id)?;
        Ok(self
            .reader
            .resolve_channel_decode_context(channel)?
            .field_defs)
    }

    /// Return a topic's message count from the summary statistics.
    pub fn message_count(&self, topic: &str) -> Result<u64, McapReaderError> {
        crate::summary::message_count(&self.file.summary, topic)
    }

    /// Return the inclusive `log_time` range of all messages in the file.
    ///
    /// Uses summary statistics when present, otherwise the chunk indexes.
    /// Returns `None` when the file has no messages or neither source is available.
    pub fn message_time_range(&self) -> Option<RangeInclusive<u64>> {
        crate::summary::message_time_range(&self.file.summary)
    }

    /// Return the summary section read when the file was opened.
    ///
    /// Chunk indexes are sorted by `chunk_start_offset`.
    pub fn summary(&self) -> &mcap::read::Summary {
        &self.file.summary
    }

    /// Derive the Arrow batch schema for a topic before reading messages.
    ///
    /// This is the schema that [`ArrowRead`] emits for `topic` with the same `metadata`.
    /// Callers that must state the schema up front, such as a Parquet writer,
    /// should take it from here so the declared schema and the emitted batches cannot disagree.
    #[cfg(feature = "arrow")]
    pub fn topic_batch_schema(
        &self,
        topic: &str,
        metadata: &crate::MetadataColumns,
    ) -> Result<crate::MessageBatchSchema, McapReaderError> {
        let context = self
            .reader
            .resolve_topic_decode_context(&self.file.summary, topic)?;
        crate::arrow_ext::batch_schema_for_context(&context, metadata)
    }

    /// List topics from the file summary.
    pub fn list_topics(&self) -> Vec<TopicInfo> {
        topic_infos_from_summary(&self.file.summary)
    }

    /// List channels from the file summary.
    pub fn list_channels(&self) -> Vec<ChannelInfo> {
        channel_infos_from_summary(&self.file.summary)
    }

    /// Select messages to read. By default all topics are selected.
    pub fn read(&self) -> Read<'_> {
        Read {
            file: self,
            topics: None,
            channel_ids: None,
            options: ReadOptions::default(),
        }
    }
}

/// Message selection and traversal options shared by all output formats.
pub struct Read<'a> {
    file: &'a McapReader,
    topics: Option<Vec<String>>,
    channel_ids: Option<Vec<u16>>,
    options: ReadOptions,
}

impl<'a> Read<'a> {
    /// Select one topic instead of channels.
    pub fn topic(self, topic: impl AsRef<str>) -> Self {
        self.topics([topic])
    }

    /// Select topics. Repeated names are read once.
    pub fn topics(mut self, topics: impl IntoIterator<Item = impl AsRef<str>>) -> Self {
        self.topics = Some(topics.into_iter().map(|s| s.as_ref().to_owned()).collect());
        self.channel_ids = None;
        self
    }

    /// Select one channel instead of topics.
    pub fn channel(self, channel_id: u16) -> Self {
        self.channels([channel_id])
    }

    /// Select channels instead of topics. Repeated IDs are read once.
    pub fn channels(mut self, channel_ids: impl IntoIterator<Item = u16>) -> Self {
        self.channel_ids = Some(channel_ids.into_iter().collect());
        self.topics = None;
        self
    }

    /// Set time filtering and parallel traversal options.
    pub fn options(mut self, options: ReadOptions) -> Self {
        self.options = options;
        self
    }

    /// Read raw payloads.
    pub fn raw(self) -> RawRead<'a> {
        RawRead(self)
    }

    /// Read decoded values.
    pub fn decoded(self) -> DecodedRead<'a> {
        DecodedRead(self)
    }

    /// Read Arrow record batches.
    #[cfg(feature = "arrow")]
    pub fn arrow(self) -> ArrowRead<'a> {
        ArrowRead {
            read: self,
            batch_size: 1024,
            metadata: Default::default(),
        }
    }

    fn decoded_contexts(
        &self,
    ) -> Result<(Vec<ChannelInfo>, Vec<crate::decode::TopicDecodeContext>), McapReaderError> {
        let channels = self.resolve_channels()?;
        let infos = self.channel_infos(&channels);
        let contexts = channels
            .iter()
            .map(|channel| self.file.reader.resolve_channel_decode_context(channel))
            .collect::<Result<Vec<_>, _>>()?;
        Ok((infos, contexts))
    }

    fn channel_infos(&self, channels: &[&Arc<mcap::Channel<'_>>]) -> Vec<ChannelInfo> {
        channels
            .iter()
            .map(|channel| channel_info(&self.file.file.summary, channel))
            .collect()
    }

    fn resolve_channels(&self) -> Result<Vec<&Arc<mcap::Channel<'_>>>, McapReaderError> {
        let summary = &self.file.file.summary;
        if let Some(ids) = &self.channel_ids {
            let mut seen = HashSet::new();
            return ids
                .iter()
                .copied()
                .filter(|id| seen.insert(*id))
                .map(|id| get_channel_by_id(summary, id))
                .collect();
        }
        let Some(topics) = &self.topics else {
            let mut channels: Vec<_> = summary.channels.values().collect();
            channels.sort_unstable_by_key(|channel| channel.id);
            return Ok(channels);
        };
        let mut seen = HashSet::new();
        topics
            .iter()
            .filter(|topic| seen.insert(topic.as_str()))
            .map(|topic| get_channel_from_summary(summary, topic))
            .collect()
    }
}

type CallbackError = Box<dyn std::error::Error + Send + Sync>;

/// Return types accepted by `try_for_each` callbacks.
///
/// An `Err` stops the read and is reported as [`McapReaderError::Callback`];
/// [`ControlFlow::Break`] stops the read without error.
pub trait CallbackOutcome: sealed::Sealed {
    #[doc(hidden)]
    fn into_flow(self) -> Result<ControlFlow<()>, CallbackError>;
}

impl CallbackOutcome for Result<(), CallbackError> {
    fn into_flow(self) -> Result<ControlFlow<()>, CallbackError> {
        self.map(ControlFlow::Continue)
    }
}

impl CallbackOutcome for ControlFlow<()> {
    fn into_flow(self) -> Result<ControlFlow<()>, CallbackError> {
        Ok(self)
    }
}

impl CallbackOutcome for Result<ControlFlow<()>, CallbackError> {
    fn into_flow(self) -> Result<ControlFlow<()>, CallbackError> {
        self
    }
}

mod sealed {
    use std::ops::ControlFlow;

    pub trait Sealed {}
    impl Sealed for Result<(), super::CallbackError> {}
    impl Sealed for ControlFlow<()> {}
    impl Sealed for Result<ControlFlow<()>, super::CallbackError> {}
}

/// Raw message output.
pub struct RawRead<'a>(Read<'a>);

impl RawRead<'_> {
    /// Visit selected messages, receiving the originating channel information.
    pub fn for_each(
        self,
        mut callback: impl FnMut(&ChannelInfo, RawMessage),
    ) -> Result<(), McapReaderError> {
        self.try_for_each(|info, message| {
            callback(info, message);
            ControlFlow::Continue(())
        })
    }

    /// Visit selected messages until the callback fails or breaks.
    /// See [`CallbackOutcome`] for the accepted return types.
    pub fn try_for_each<R: CallbackOutcome>(
        self,
        mut callback: impl FnMut(&ChannelInfo, RawMessage) -> R,
    ) -> Result<(), McapReaderError> {
        let read = self.0;
        let channels = read.resolve_channels()?;
        let infos = read.channel_infos(&channels);
        let channel_ids: Vec<_> = channels.iter().map(|channel| channel.id).collect();
        for_each_selected_message(
            &read.file.file,
            &channel_ids,
            read.options.time_range,
            read.options.parallel.unwrap_or(read.file.reader.parallel),
            |_, message| {
                Ok(RawMessage {
                    log_time: message.log_time,
                    publish_time: message.publish_time,
                    data: Arc::from(message.data.as_ref()),
                })
            },
            &mut |i, message| {
                callback(&infos[i], message)
                    .into_flow()
                    .map_err(McapReaderError::Callback)
            },
        )
    }
}

/// Decoded message output.
pub struct DecodedRead<'a>(Read<'a>);

impl DecodedRead<'_> {
    /// Visit selected messages, receiving the originating channel information.
    pub fn for_each(
        self,
        mut callback: impl FnMut(&ChannelInfo, DecodedMessage),
    ) -> Result<(), McapReaderError> {
        self.try_for_each(|info, message| {
            callback(info, message);
            ControlFlow::Continue(())
        })
    }

    /// Visit selected messages until the callback fails or breaks.
    /// See [`CallbackOutcome`] for the accepted return types.
    pub fn try_for_each<R: CallbackOutcome>(
        self,
        mut callback: impl FnMut(&ChannelInfo, DecodedMessage) -> R,
    ) -> Result<(), McapReaderError> {
        let read = self.0;
        let (infos, contexts) = read.decoded_contexts()?;
        let reader = &read.file.reader;
        reader.for_each_topic_decoded_message_impl(
            &read.file.file,
            &contexts,
            &read.options,
            &mut |index, message| {
                callback(&infos[index], message)
                    .into_flow()
                    .map_err(McapReaderError::Callback)
            },
        )
    }
}

/// Arrow record batch output.
#[cfg(feature = "arrow")]
pub struct ArrowRead<'a> {
    read: Read<'a>,
    batch_size: usize,
    metadata: crate::MetadataColumns,
}

#[cfg(feature = "arrow")]
impl ArrowRead<'_> {
    /// Set the maximum number of rows per batch; zero is treated as one.
    pub fn batch_size(mut self, size: usize) -> Self {
        self.batch_size = size.max(1);
        self
    }

    /// Set metadata column names and timestamp format.
    pub fn metadata(mut self, metadata: crate::MetadataColumns) -> Self {
        self.metadata = metadata;
        self
    }

    /// Visit selected batches, receiving the originating channel information.
    pub fn for_each(
        self,
        mut callback: impl FnMut(&ChannelInfo, arrow::record_batch::RecordBatch),
    ) -> Result<(), McapReaderError> {
        self.try_for_each(|info, batch| {
            callback(info, batch);
            ControlFlow::Continue(())
        })
    }

    /// Visit selected batches until the callback fails or breaks.
    /// See [`CallbackOutcome`] for the accepted return types.
    /// After a break, rows buffered for batches not yet delivered are discarded.
    pub fn try_for_each<R: CallbackOutcome>(
        self,
        mut callback: impl FnMut(&ChannelInfo, arrow::record_batch::RecordBatch) -> R,
    ) -> Result<(), McapReaderError> {
        let read = self.read;
        let (infos, contexts) = read.decoded_contexts()?;
        let reader = &read.file.reader;
        reader.for_each_record_batch_impl(
            &read.file.file,
            &contexts,
            &read.options,
            self.batch_size,
            &self.metadata,
            &mut |i, batch| callback(&infos[i], batch).into_flow(),
        )
    }
}
