//! Message decoding over the selected channels of a file.

use std::{collections::HashSet, path::Path};

use mcapdecode_core::{DecodedMessage, FieldDefs, TopicDecoder};

use crate::{
    McapReader, ReadOptions, chunk_reader::for_each_selected_message, error::McapReaderError,
    internal_reader::InternalReader, mcap_file::McapFile,
};

impl InternalReader {
    pub(crate) fn for_each_decoded_message_impl(
        &self,
        file: &McapFile,
        context: &TopicDecodeContext,
        options: &ReadOptions,
        callback: &mut impl FnMut(DecodedMessage) -> Result<(), McapReaderError>,
    ) -> Result<(), McapReaderError> {
        self.for_each_topic_decoded_message_impl(
            file,
            std::slice::from_ref(context),
            options,
            &mut |_, message| callback(message),
        )
    }

    pub(crate) fn for_each_topic_decoded_message_impl(
        &self,
        file: &McapFile,
        contexts: &[TopicDecodeContext],
        options: &ReadOptions,
        callback: &mut impl FnMut(usize, DecodedMessage) -> Result<(), McapReaderError>,
    ) -> Result<(), McapReaderError> {
        let channel_ids: Vec<_> = contexts.iter().map(|context| context.channel_id).collect();
        for_each_selected_message(
            file,
            &channel_ids,
            options.time_range,
            options.parallel.unwrap_or(self.parallel),
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

impl McapReader {
    /// Read several topics while decompressing each selected chunk only once.
    /// The callback runs on the calling thread, in file order within each topic.
    /// Order between topics is unspecified. Duplicate topics are read once.
    pub fn for_each_topic_decoded_message(
        &self,
        path: &Path,
        topics: &[&str],
        callback: impl FnMut(
            &str,
            DecodedMessage,
        ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>,
    ) -> Result<(), McapReaderError> {
        self.for_each_topic_decoded_message_with_options(
            path,
            topics,
            &ReadOptions::default(),
            callback,
        )
    }

    /// Read selected topics with log-time filtering and parallel read options.
    /// Duplicate topics are read once.
    pub fn for_each_topic_decoded_message_with_options(
        &self,
        path: &Path,
        topics: &[&str],
        options: &ReadOptions,
        mut callback: impl FnMut(
            &str,
            DecodedMessage,
        ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>,
    ) -> Result<(), McapReaderError> {
        if topics.is_empty() {
            return Ok(());
        }
        self.with_topic_contexts(path, topics, |file, topics, contexts| {
            self.inner.for_each_topic_decoded_message_impl(
                file,
                contexts,
                options,
                &mut |i, message| callback(topics[i], message).map_err(McapReaderError::Callback),
            )
        })
    }

    pub(crate) fn with_topic_contexts<T>(
        &self,
        path: &Path,
        topics: &[&str],
        callback: impl FnOnce(&McapFile, &[&str], &[TopicDecodeContext]) -> Result<T, McapReaderError>,
    ) -> Result<T, McapReaderError> {
        let file = McapFile::open(path)?;
        let mut seen = HashSet::new();
        let topics: Vec<_> = topics
            .iter()
            .copied()
            .filter(|topic| seen.insert(*topic))
            .collect();
        let contexts: Vec<_> = topics
            .iter()
            .map(|topic| {
                self.inner
                    .resolve_topic_decode_context(&file.summary, topic)
            })
            .collect::<Result<_, _>>()?;
        callback(&file, &topics, &contexts)
    }
}

pub(crate) struct TopicDecodeContext {
    pub(crate) channel_id: u16,
    pub(crate) schema_name: String,
    pub(crate) decoder: Box<dyn TopicDecoder>,
    pub(crate) field_defs: FieldDefs,
}
