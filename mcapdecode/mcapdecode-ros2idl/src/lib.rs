//! ROS 2 IDL → CDR decoder for `mcapdecode`.
//!
//! Implements [`MessageDecoder`] for the
//! `(schema_encoding = ros2idl, message_encoding = cdr)` key.
//!
//! # Pipeline
//!
//! ```text
//! schema bytes (UTF-8 IDL bundle)
//!   └─ parse_schema_bundle       – split sections at `====` separators
//!       └─ parse_idl_section     – recursive IDL parser → ParsedSection
//!           └─ resolve_schema    – type-name resolution → ResolvedSchema
//!               └─ decode_cdr_to_value  – CDR bytes → Value
//! ```

mod idl_lexer;
mod parser;
mod resolver;
mod schema_bundle;

use mcapdecode_core::{
    DecoderError, EncodingKey, MessageDecoder, MessageEncoding, SchemaEncoding, TopicDecoder,
};
use mcapdecode_ros2_common::build_cdr_topic_decoder;
pub use mcapdecode_ros2_common::{SchemaBundle, SchemaSection};
pub use parser::parse_idl_section;
pub use resolver::resolve_schema;
pub use schema_bundle::parse_schema_bundle;

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
        build_cdr_topic_decoder(schema_name, schema_data, resolve_schema)
    }
}
