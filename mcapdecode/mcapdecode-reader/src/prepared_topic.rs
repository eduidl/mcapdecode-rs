//! A prepared topic handed to decoder-backed output adapters.

use mcapdecode_core::{DecodedMessage, FieldDefs};
use memmap2::Mmap;

use crate::{
    McapReader, ReadOptions,
    decode::{DecodeRequest, TopicDecodeContext},
    error::McapReaderError,
};

/// A topic whose MCAP summary and decoder have already been resolved.
///
/// Values are created only through [`McapReader::with_prepared_topic`]. This
/// keeps the mapped MCAP file, its summary, and decoder context together so an
/// output adapter can derive a schema and scan messages without reopening the
/// file or rereading the summary.
pub struct PreparedTopic<'reader> {
    pub(crate) reader: &'reader McapReader,
    pub(crate) mmap: Mmap,
    pub(crate) summary: mcap::read::Summary,
    pub(crate) context: TopicDecodeContext,
    pub(crate) topic: String,
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
