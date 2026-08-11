use mcapdecode_core::{DecoderError, TopicDecoder};

use crate::{ResolvedSchema, Ros2CdrTopicDecoder, Ros2Error};

/// Function that parses and resolves a ROS 2 schema text.
pub type ResolveSchemaFn = fn(&str, &str) -> Result<ResolvedSchema, Ros2Error>;

/// Decode schema bytes as UTF-8 and resolve them for CDR decoding.
///
/// The schema-format crates provide the format-specific resolver; this helper
/// keeps the shared UTF-8 and [`DecoderError::SchemaParse`] handling identical.
pub fn resolve_for_cdr(
    schema_name: &str,
    schema_data: &[u8],
    resolve_schema: ResolveSchemaFn,
) -> Result<ResolvedSchema, DecoderError> {
    let schema_text = std::str::from_utf8(schema_data)
        .map_err(|source| DecoderError::SchemaParse(Box::new(source)))?;

    resolve_schema(schema_name, schema_text)
        .map_err(|source| DecoderError::SchemaParse(Box::new(source)))
}

/// Build the shared ROS 2 CDR topic decoder from schema bytes.
pub fn build_cdr_topic_decoder(
    schema_name: &str,
    schema_data: &[u8],
    resolve_schema: ResolveSchemaFn,
) -> Result<Box<dyn TopicDecoder>, DecoderError> {
    let resolved = resolve_for_cdr(schema_name, schema_data, resolve_schema)?;
    Ok(Box::new(Ros2CdrTopicDecoder::new(resolved)))
}
