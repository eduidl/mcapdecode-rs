//! ROS 2 IDL → CDR decoder for `mcap2arrow`.
//!
//! Implements [`MessageDecoder`] for the
//! `(schema_encoding = ros2idl, message_encoding = cdr)` key.
//!
//! # Pipeline
//!
//! ```text
//! schema bytes (UTF-8 IDL bundle)
//!   └─ SchemaBundle::parse       – split sections at `====` separators
//!       └─ parse_idl_section     – nom-based IDL parser → ParsedSection
//!           └─ resolve_schema    – type-name resolution → ResolvedSchema
//!               └─ decode_cdr_to_value  – CDR bytes → Value
//! ```

mod lex;
mod parser;
mod resolver;
mod schema_bundle;

use mcap2arrow_core::{
    DecoderError, EncodingKey, MessageDecoder, MessageEncoding, SchemaEncoding, TopicDecoder,
};
use mcap2arrow_ros2_common::{ResolvedSchema, Ros2CdrTopicDecoder};
pub use parser::parse_idl_section;
pub use resolver::resolve_schema;
pub use schema_bundle::{IdlSection, SchemaBundle};

/// [`MessageDecoder`] for ROS 2 IDL schemas with CDR-encoded messages.
pub struct Ros2IdlDecoder;

impl Ros2IdlDecoder {
    pub fn new() -> Self {
        Self
    }
}

impl Default for Ros2IdlDecoder {
    fn default() -> Self {
        Self::new()
    }
}

impl MessageDecoder for Ros2IdlDecoder {
    fn encoding_key(&self) -> EncodingKey {
        EncodingKey::new(SchemaEncoding::Ros2Idl, MessageEncoding::Cdr)
    }

    fn build_topic_decoder(
        &self,
        schema_name: &str,
        schema_data: &[u8],
    ) -> Result<Box<dyn TopicDecoder>, DecoderError> {
        let resolved = resolve_for_cdr(schema_name, schema_data)?;
        Ok(Box::new(Ros2CdrTopicDecoder::new(resolved)))
    }
}

/// Parse and resolve an IDL schema bundle into a [`ResolvedSchema`] ready for CDR decoding.
pub fn resolve_for_cdr(
    schema_name: &str,
    schema_data: &[u8],
) -> Result<ResolvedSchema, DecoderError> {
    let schema_str = std::str::from_utf8(schema_data).map_err(|e| DecoderError::SchemaParse {
        schema_name: schema_name.to_string(),
        source: Box::new(e),
    })?;
    resolve_schema(schema_name, schema_str).map_err(|e| DecoderError::SchemaParse {
        schema_name: schema_name.to_string(),
        source: e.into(),
    })
}
