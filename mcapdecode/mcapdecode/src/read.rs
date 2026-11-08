//! Message selection and output formats.

use std::{
    collections::HashSet,
    ops::{ControlFlow, RangeBounds},
    sync::Arc,
};

#[cfg(feature = "arrow")]
use arrow::record_batch::RecordBatch;
#[cfg(feature = "arrow")]
use mcapdecode_arrow::{MessageBatchSchema, MetadataColumns};
use mcapdecode_core::{DecodedMessage, FieldDefs};

use crate::{
    ChannelInfo, McapReader, McapReaderError, RawMessage,
    reader::TopicDecodeContext,
    types::{ReadOptions, TimeRange},
};

/// Message selection and traversal options shared by all output formats.
pub struct Read<'a> {
    reader: &'a McapReader,
    topics: Option<Vec<String>>,
    channel_ids: Option<Vec<u16>>,
    options: ReadOptions,
}

impl<'a> Read<'a> {
    pub(crate) fn new(reader: &'a McapReader) -> Self {
        Self {
            reader,
            topics: None,
            channel_ids: None,
            options: ReadOptions::default(),
        }
    }

    /// Select all channels of one topic instead of channel ids.
    pub fn topic(self, topic: impl AsRef<str>) -> Self {
        self.topics([topic])
    }

    /// Select all channels of these topics instead of channel ids.
    /// Repeated names are read once.
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

    /// Select messages whose `log_time` falls in `range`, in nanoseconds.
    ///
    /// Accepts any range, such as `start..end`, `start..` or the inclusive range
    /// from [`McapReader::message_time_range`]. Later calls replace earlier ones.
    pub fn time_range(mut self, range: impl RangeBounds<u64>) -> Self {
        self.options.time_range = TimeRange::new(range);
        self
    }

    /// Enable or disable parallel chunk decompression and decoding for this read,
    /// overriding [`McapReaderBuilder::with_parallel`](crate::McapReaderBuilder::with_parallel).
    pub fn parallel(mut self, parallel: bool) -> Self {
        self.options.parallel = Some(parallel);
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

    /// List the selected channels in selection order, the channels of one topic
    /// by ascending id.
    ///
    /// Repeated selections appear once. An unknown topic or channel id is an error.
    pub fn selected_channels(&self) -> Result<Vec<ChannelInfo>, McapReaderError> {
        Ok(self.channel_infos(&self.resolve_channels()?))
    }

    /// Derive the schema of each selected channel without reading messages,
    /// in the order of [`Read::selected_channels`].
    pub fn field_defs(&self) -> Result<Vec<(ChannelInfo, FieldDefs)>, McapReaderError> {
        let (infos, contexts) = self.decoded_contexts()?;
        Ok(infos
            .into_iter()
            .zip(contexts.into_iter().map(|context| context.field_defs))
            .collect())
    }

    fn decoded_contexts(
        &self,
    ) -> Result<(Vec<ChannelInfo>, Vec<TopicDecodeContext>), McapReaderError> {
        let channels = self.resolve_channels()?;
        let infos = self.channel_infos(&channels);
        let contexts = channels
            .iter()
            .map(|channel| self.reader.decode_context(channel))
            .collect::<Result<Vec<_>, _>>()?;
        Ok((infos, contexts))
    }

    fn channel_infos(&self, channels: &[&Arc<mcap::Channel<'_>>]) -> Vec<ChannelInfo> {
        channels
            .iter()
            .map(|channel| self.reader.channel_info(channel))
            .collect()
    }

    fn resolve_channels(&self) -> Result<Vec<&'a Arc<mcap::Channel<'a>>>, McapReaderError> {
        let reader = self.reader;
        if let Some(ids) = &self.channel_ids {
            let mut seen = HashSet::new();
            return ids
                .iter()
                .copied()
                .filter(|id| seen.insert(*id))
                .map(|id| reader.channel_by_id(id))
                .collect();
        }
        let Some(topics) = &self.topics else {
            return Ok(reader.summary_channels());
        };
        let mut seen = HashSet::new();
        let mut channels = Vec::new();
        for topic in topics {
            for channel in reader.channels_of_topic(topic)? {
                if seen.insert(channel.id) {
                    channels.push(channel);
                }
            }
        }
        Ok(channels)
    }

    /// Decode the messages of the channels in `contexts`.
    fn for_each_decoded(
        &self,
        contexts: &[TopicDecodeContext],
        callback: &mut impl FnMut(usize, DecodedMessage) -> Result<ControlFlow<()>, McapReaderError>,
    ) -> Result<(), McapReaderError> {
        let channel_ids: Vec<_> = contexts.iter().map(|context| context.channel_id).collect();
        self.reader.for_each_message(
            &channel_ids,
            &self.options,
            |i, message| {
                Ok(DecodedMessage {
                    log_time: message.log_time,
                    publish_time: message.publish_time,
                    value: contexts[i].decoder.decode(&message.data)?,
                })
            },
            callback,
        )
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
        read.reader.for_each_message(
            &channel_ids,
            &read.options,
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
        read.for_each_decoded(&contexts, &mut |index, message| {
            callback(&infos[index], message)
                .into_flow()
                .map_err(McapReaderError::Callback)
        })
    }
}

/// Arrow record batch output.
#[cfg(feature = "arrow")]
pub struct ArrowRead<'a> {
    read: Read<'a>,
    batch_size: usize,
    metadata: MetadataColumns,
}

#[cfg(feature = "arrow")]
impl ArrowRead<'_> {
    /// Set the maximum number of rows per batch; zero is treated as one.
    pub fn batch_size(mut self, size: usize) -> Self {
        self.batch_size = size.max(1);
        self
    }

    /// Set metadata column names and timestamp format.
    pub fn metadata(mut self, metadata: MetadataColumns) -> Self {
        self.metadata = metadata;
        self
    }

    /// Derive the batch schema of each selected channel without reading messages,
    /// in the order of [`Read::selected_channels`].
    ///
    /// These are the schemas of the batches this read emits. Callers that must
    /// state the schema up front, such as a Parquet writer, should take it from
    /// here so the declared schema and the emitted batches cannot disagree.
    pub fn schemas(&self) -> Result<Vec<(ChannelInfo, MessageBatchSchema)>, McapReaderError> {
        let (infos, contexts) = self.read.decoded_contexts()?;
        Ok(infos
            .into_iter()
            .zip(self.batch_schemas(&contexts)?)
            .collect())
    }

    fn batch_schemas(
        &self,
        contexts: &[TopicDecodeContext],
    ) -> Result<Vec<MessageBatchSchema>, McapReaderError> {
        contexts
            .iter()
            .map(|context| batch_schema_for_context(context, &self.metadata))
            .collect()
    }

    /// Visit selected batches, receiving the originating channel information.
    pub fn for_each(
        self,
        mut callback: impl FnMut(&ChannelInfo, RecordBatch),
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
        mut callback: impl FnMut(&ChannelInfo, RecordBatch) -> R,
    ) -> Result<(), McapReaderError> {
        let (infos, contexts) = self.read.decoded_contexts()?;
        let schemas = self.batch_schemas(&contexts)?;
        let read = self.read;
        let batch_size = self.batch_size;
        let mut rows: Vec<_> = contexts
            .iter()
            .map(|_| Vec::with_capacity(batch_size))
            .collect();
        let mut emit = |i: usize, batch| callback(&infos[i], batch).into_flow();
        let mut stopped = false;
        read.for_each_decoded(&contexts, &mut |i, message| {
            rows[i].push(message);
            if rows[i].len() < batch_size {
                return Ok(ControlFlow::Continue(()));
            }
            let flow = flush_batch(&schemas[i], &mut rows[i], &mut |batch| emit(i, batch))?;
            stopped = flow.is_break();
            Ok(flow)
        })?;
        if stopped {
            return Ok(());
        }
        for (i, rows) in rows.iter_mut().enumerate() {
            if flush_batch(&schemas[i], rows, &mut |batch| emit(i, batch))?.is_break() {
                break;
            }
        }
        Ok(())
    }
}

#[cfg(feature = "arrow")]
fn batch_schema_for_context(
    context: &TopicDecodeContext,
    metadata: &MetadataColumns,
) -> Result<MessageBatchSchema, McapReaderError> {
    if context.field_defs.is_empty() {
        return Err(McapReaderError::EmptyDerivedSchema {
            schema_name: context.schema_name.clone(),
        });
    }
    Ok(MessageBatchSchema::from_field_defs(
        &context.field_defs,
        metadata.clone(),
    )?)
}

#[cfg(feature = "arrow")]
fn flush_batch(
    batch_schema: &MessageBatchSchema,
    rows: &mut Vec<DecodedMessage>,
    callback: &mut impl FnMut(RecordBatch) -> Result<ControlFlow<()>, CallbackError>,
) -> Result<ControlFlow<()>, McapReaderError> {
    if rows.is_empty() {
        return Ok(ControlFlow::Continue(()));
    }
    let batch = batch_schema.to_record_batch(rows.as_slice())?;
    rows.clear();
    callback(batch).map_err(McapReaderError::Callback)
}
