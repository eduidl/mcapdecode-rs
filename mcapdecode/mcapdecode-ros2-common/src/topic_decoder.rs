use std::sync::Arc;

use mcapdecode_core::{DecoderError, FieldDefs, StructDef, TopicDecoder, Value};

use crate::{
    ResolvedSchema, cdr::decode_with_schema, resolved_schema_to_field_defs,
    schema::try_resolved_schema_to_field_defs,
};

/// Shared ROS 2 CDR topic decoder used by both `ros2msg` and `ros2idl`.
pub struct Ros2CdrTopicDecoder {
    resolved: ResolvedSchema,
    schema: Arc<StructDef>,
}

impl Ros2CdrTopicDecoder {
    /// # Panics
    ///
    /// Panics under the same conditions as [`resolved_schema_to_field_defs`].
    pub fn new(resolved: ResolvedSchema) -> Self {
        let schema = Arc::new(StructDef::from(resolved_schema_to_field_defs(&resolved)));
        Self { resolved, schema }
    }

    /// Like [`Self::new`], but reports a schema that cannot be derived as
    /// [`DecoderError::SchemaDerivation`].
    pub(crate) fn try_new(resolved: ResolvedSchema) -> Result<Self, DecoderError> {
        let field_defs =
            try_resolved_schema_to_field_defs(&resolved).map_err(DecoderError::SchemaDerivation)?;
        Ok(Self {
            resolved,
            schema: Arc::new(StructDef::from(field_defs)),
        })
    }
}

impl TopicDecoder for Ros2CdrTopicDecoder {
    fn decode(&self, message_data: &[u8]) -> Result<Value, DecoderError> {
        decode_with_schema(&self.resolved, message_data, &self.schema)
    }

    fn field_defs(&self) -> &FieldDefs {
        &self.schema.fields
    }
}
