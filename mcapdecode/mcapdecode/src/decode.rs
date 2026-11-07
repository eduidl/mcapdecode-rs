//! Message decoding over the selected channels of a file.

use std::ops::ControlFlow;

use mcapdecode_core::{DecodedMessage, FieldDefs, TopicDecoder};

use crate::{
    ReadOptions, chunk_reader::for_each_selected_message, error::McapReaderError,
    internal_reader::InternalReader, mcap_file::McapFile,
};

impl InternalReader {
    pub(crate) fn for_each_topic_decoded_message_impl(
        &self,
        file: &McapFile,
        contexts: &[TopicDecodeContext],
        options: &ReadOptions,
        callback: &mut impl FnMut(usize, DecodedMessage) -> Result<ControlFlow<()>, McapReaderError>,
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

pub(crate) struct TopicDecodeContext {
    pub(crate) channel_id: u16,
    #[cfg(feature = "arrow")]
    pub(crate) schema_name: String,
    pub(crate) decoder: Box<dyn TopicDecoder>,
    pub(crate) field_defs: FieldDefs,
}
