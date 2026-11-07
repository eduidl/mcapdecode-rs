//! Shared decoder configuration and file-independent reading implementation.

use std::{collections::HashMap, sync::Arc};

use mcapdecode_core::{EncodingKey, MessageDecoder, MessageEncoding, SchemaEncoding};

use crate::{
    RawMessage, TopicDecodeStatus, TopicSchema,
    chunk_reader::for_each_selected_message,
    decode::TopicDecodeContext,
    error::McapReaderError,
    mcap_file::McapFile,
    summary::{get_channel_from_summary, get_schema_from_channel, topic_infos_from_summary},
};

#[derive(Clone)]
pub(crate) struct InternalReader {
    decoders: HashMap<EncodingKey, Arc<dyn MessageDecoder>>,
    pub(crate) parallel: bool,
}

impl InternalReader {
    pub(crate) fn new() -> Self {
        Self {
            decoders: HashMap::new(),
            parallel: true,
        }
    }

    pub(crate) fn register_shared_decoder(&mut self, decoder: Arc<dyn MessageDecoder>) {
        self.decoders.insert(decoder.encoding_key(), decoder);
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
        self.resolve_channel_decode_context(get_channel_from_summary(summary, topic)?)
    }

    pub(crate) fn resolve_channel_decode_context(
        &self,
        channel: &Arc<mcap::Channel<'_>>,
    ) -> Result<TopicDecodeContext, McapReaderError> {
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

    pub(crate) fn list_topics_with_decode_status(
        &self,
        summary: &mcap::read::Summary,
    ) -> Result<Vec<TopicDecodeStatus>, McapReaderError> {
        Ok(topic_infos_from_summary(summary)
            .into_iter()
            .map(|topic| {
                let decode_error = self
                    .resolve_topic_decode_context(summary, &topic.topic)
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

    pub(crate) fn topic_schema(
        &self,
        summary: &mcap::read::Summary,
        topic: &str,
    ) -> Result<TopicSchema, McapReaderError> {
        let context = self.resolve_topic_decode_context(summary, topic)?;
        let info = topic_infos_from_summary(summary)
            .into_iter()
            .find(|info| info.topic == topic)
            .expect("resolved topic must be present in the summary");

        Ok(TopicSchema {
            info,
            field_defs: context.field_defs,
        })
    }
}

pub(crate) fn for_each_raw_message_impl(
    file: &McapFile,
    channel_id: u16,
    parallel: bool,
    mut callback: impl FnMut(RawMessage) -> Result<(), Box<dyn std::error::Error + Send + Sync>>,
) -> Result<(), McapReaderError> {
    for_each_selected_message(
        file,
        &[channel_id],
        None,
        parallel,
        |_, message| {
            Ok(RawMessage {
                log_time: message.log_time,
                publish_time: message.publish_time,
                data: Arc::from(message.data.as_ref()),
            })
        },
        &mut |_, message| callback(message).map_err(McapReaderError::Callback),
    )
}
