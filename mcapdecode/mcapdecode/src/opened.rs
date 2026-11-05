//! An opened MCAP file with configured decoders.

use std::path::Path;

use mcapdecode_core::FieldDefs;

use crate::{
    ChannelInfo, McapReader, McapReaderError, TopicDecodeStatus, TopicInfo, TopicSchema,
    internal_reader::InternalReader,
    mcap_file::McapFile,
    summary::{channel_infos_from_summary, get_channel_by_id, topic_infos_from_summary},
};

/// An opened MCAP file and its configured decoders.
///
/// Reuses one memory map and summary for metadata and schema queries.
/// Opening requires an MCAP summary; decoders are resolved only when querying schemas.
pub struct OpenedMcap {
    reader: InternalReader,
    file: McapFile,
}

impl McapReader {
    /// Open a file with all decoders enabled by cargo features.
    pub fn open(path: impl AsRef<Path>) -> Result<OpenedMcap, McapReaderError> {
        Self::builder()
            .with_default_decoders()
            .build()
            .open_file(path)
    }

    /// Open a file with a snapshot of this reader's decoder registrations and settings.
    /// Later registration changes do not affect files that are already open.
    pub fn open_file(&self, path: impl AsRef<Path>) -> Result<OpenedMcap, McapReaderError> {
        Ok(OpenedMcap {
            reader: self.inner.clone(),
            file: McapFile::open(path.as_ref())?,
        })
    }
}

impl OpenedMcap {
    /// Return topic metadata and schema IR without reading message payloads.
    pub fn topic_schema(&self, topic: &str) -> Result<TopicSchema, McapReaderError> {
        self.reader.topic_schema(&self.file.summary, topic)
    }

    /// List topics and report whether their schemas can be derived by the configured decoders.
    pub fn list_topics_with_decode_status(
        &self,
    ) -> Result<Vec<TopicDecodeStatus>, McapReaderError> {
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

    /// Derive the Arrow batch schema for a topic before reading messages.
    #[cfg(feature = "arrow")]
    pub fn topic_batch_schema(
        &self,
        topic: &str,
        options: &crate::RecordBatchOptions,
    ) -> Result<crate::MessageBatchSchema, McapReaderError> {
        let context = self
            .reader
            .resolve_topic_decode_context(&self.file.summary, topic)?;
        crate::arrow_ext::batch_schema_for_context(&context, options)
    }

    /// List topics from the file summary.
    pub fn list_topics(&self) -> Vec<TopicInfo> {
        topic_infos_from_summary(&self.file.summary)
    }

    /// List channels from the file summary.
    pub fn list_channels(&self) -> Vec<ChannelInfo> {
        channel_infos_from_summary(&self.file.summary)
    }
}
