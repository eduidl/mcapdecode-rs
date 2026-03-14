//! Decoder trait and encoding key used to register pluggable message decoders.

use crate::{
    error::DecoderError, message_encoding::MessageEncoding, schema::FieldDefs,
    schema_encoding::SchemaEncoding, value::Value,
};

/// Key identifying a (schema_encoding, message_encoding) pair.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EncodingKey {
    pub schema_encoding: SchemaEncoding,
    pub message_encoding: MessageEncoding,
}

impl EncodingKey {
    pub fn new(schema_encoding: SchemaEncoding, message_encoding: MessageEncoding) -> Self {
        Self {
            schema_encoding,
            message_encoding,
        }
    }
}

/// Topic-local decoder built from MCAP schema metadata.
///
/// Implementations are created by [`MessageDecoder`] once per schema/topic and
/// reused for all messages in that topic.
pub trait TopicDecoder: Send + Sync {
    /// Decode a single message payload into a [`Value`].
    fn decode(&self, message_data: &[u8]) -> Result<Value, DecoderError>;

    /// Return the Arrow-independent schema for decoded values.
    fn field_defs(&self) -> &FieldDefs;
}

/// Factory trait that builds topic-local decoders from MCAP schema metadata.
///
/// Implementations are registered with `mcap2arrow::McapReader` and
/// dispatched based on [`EncodingKey`].
pub trait MessageDecoder: Send + Sync {
    /// Returns the encoding pair this decoder handles.
    fn encoding_key(&self) -> EncodingKey;

    /// Build a topic-local decoder for the given MCAP schema.
    ///
    /// Returns `Err` if the schema cannot be parsed or is structurally invalid.
    fn build_topic_decoder(
        &self,
        schema_name: &str,
        schema_data: &[u8],
    ) -> Result<Box<dyn TopicDecoder>, DecoderError>;
}
