//! Protobuf [`MessageDecoder`] implementation for the mcapdecode pipeline.
//!
//! This crate provides [`ProtobufDecoder`], which decodes protobuf-encoded
//! MCAP messages into the intermediate [`Value`] representation used by
//! mcapdecode-core.  It also re-exports the lower-level helpers
//! [`decode_protobuf_to_value`], [`decode_protobuf_to_value_with_policy`],
//! [`parse_message_descriptor`], and [`message_fields_to_field_defs`]
//! for direct use.

mod policy;
mod proto_to_arrow;
mod schema;

use mcapdecode_core::{
    DecoderError, EncodingKey, FieldDefs, MessageDecoder, MessageEncoding, SchemaEncoding,
    TopicDecoder, Value,
};
pub use policy::PresencePolicy;
use prost_reflect::MessageDescriptor;
pub use proto_to_arrow::{decode_protobuf_to_value, decode_protobuf_to_value_with_policy};
pub use schema::{message_fields_to_field_defs, parse_message_descriptor};

/// Decoder that converts protobuf-encoded MCAP messages into
/// [`Value`] / [`FieldDefs`] via the [`MessageDecoder`] factory trait.
pub struct ProtobufDecoder {
    presence_policy: PresencePolicy,
}

impl ProtobufDecoder {
    pub fn new() -> Self {
        Self::new_with_presence_policy(PresencePolicy::PresenceAware)
    }

    pub fn new_with_presence_policy(presence_policy: PresencePolicy) -> Self {
        Self { presence_policy }
    }
}

impl Default for ProtobufDecoder {
    fn default() -> Self {
        Self::new()
    }
}

impl MessageDecoder for ProtobufDecoder {
    fn encoding_key(&self) -> EncodingKey {
        EncodingKey::new(SchemaEncoding::Protobuf, MessageEncoding::Protobuf)
    }

    fn build_topic_decoder(
        &self,
        schema_name: &str,
        schema_data: &[u8],
    ) -> Result<Box<dyn TopicDecoder>, DecoderError> {
        let desc = schema::parse_message_descriptor(schema_name, schema_data)?;
        let field_defs = schema::message_fields_to_field_defs(&desc, self.presence_policy)?;
        Ok(Box::new(ProtobufTopicDecoder {
            desc,
            field_defs,
            presence_policy: self.presence_policy,
        }))
    }
}

struct ProtobufTopicDecoder {
    desc: MessageDescriptor,
    field_defs: FieldDefs,
    presence_policy: PresencePolicy,
}

impl TopicDecoder for ProtobufTopicDecoder {
    fn decode(&self, message_data: &[u8]) -> Result<Value, DecoderError> {
        proto_to_arrow::decode_from_descriptor(&self.desc, message_data, self.presence_policy)
    }

    fn field_defs(&self) -> &FieldDefs {
        &self.field_defs
    }
}
