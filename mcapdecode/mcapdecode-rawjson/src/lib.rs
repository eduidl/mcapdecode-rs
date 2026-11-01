//! Pass-through [`MessageDecoder`] for `jsonschema` + `json` MCAP channels.
//!
//! JSON Schema validates documents rather than defining column types, and real
//! files may carry an empty schema, so [`RawJsonDecoder`] reads neither the
//! schema nor the JSON syntax. Each message becomes one [`COLUMN_NAME`] string.

use std::{str, sync::Arc};

use mcapdecode_core::{
    DataTypeDef, DecoderError, EncodingKey, FieldDef, FieldDefs, MessageDecoder, MessageEncoding,
    SchemaEncoding, TopicDecoder, Value,
};

/// Name of the column holding the unparsed message text.
pub const COLUMN_NAME: &str = "json";

/// Decoder that exposes JSON messages unparsed via the [`MessageDecoder`]
/// factory trait.
#[derive(Debug, Clone, Copy, Default)]
pub struct RawJsonDecoder;

impl RawJsonDecoder {
    pub fn new() -> Self {
        Self
    }
}

impl MessageDecoder for RawJsonDecoder {
    fn encoding_key(&self) -> EncodingKey {
        EncodingKey::new(SchemaEncoding::JsonSchema, MessageEncoding::Json)
    }

    fn build_topic_decoder(
        &self,
        _schema_name: &str,
        _schema_data: &[u8],
    ) -> Result<Box<dyn TopicDecoder>, DecoderError> {
        let field_defs = vec![FieldDef::new(COLUMN_NAME, DataTypeDef::String, false)].into();
        Ok(Box::new(RawJsonTopicDecoder { field_defs }))
    }
}

struct RawJsonTopicDecoder {
    field_defs: FieldDefs,
}

impl TopicDecoder for RawJsonTopicDecoder {
    fn decode(&self, message_data: &[u8]) -> Result<Value, DecoderError> {
        let text =
            str::from_utf8(message_data).map_err(|e| DecoderError::MessageDecode(Box::new(e)))?;
        Ok(Value::Struct(vec![Value::String(Arc::from(text))]))
    }

    fn field_defs(&self) -> &FieldDefs {
        &self.field_defs
    }
}
