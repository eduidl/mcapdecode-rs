//! ROS 2 .msg → CDR decoder for `mcapdecode`.
//!
//! Implements [`MessageDecoder`] for the
//! `(schema_encoding = ros2msg, message_encoding = cdr)` key.
//!
//! ROS 2 MCAP writers often embed dependent `.msg` definitions in the schema
//! blob, separated by `====` lines and `MSG:` headers. `ros2msg` therefore
//! supports both a single `.msg` file and bundled dependency sections.
//! `builtin_interfaces` types (`Time`, `Duration`) are still injected
//! automatically when not explicitly included.
//!
//! # Pipeline
//!
//! ```text
//! schema bytes (UTF-8 .msg or bundled .msg sections)
//!   └─ SchemaBundle::parse      – optional split at `====` / `MSG:`
//!       └─ parse_msg            – re_ros_msg parser → StructDef
//!           └─ resolve_schema   – type resolution → ResolvedSchema
//!               └─ decode_cdr_to_value – CDR bytes → Value
//! ```

mod parser;
mod resolver;
mod schema_bundle;

use mcapdecode_core::{
    DecoderError, EncodingKey, MessageDecoder, MessageEncoding, SchemaEncoding, TopicDecoder,
};
use mcapdecode_ros2_common::{ResolvedSchema, Ros2CdrTopicDecoder};
pub use parser::parse_msg;
pub use resolver::resolve_schema;
pub use schema_bundle::{MsgSection, SchemaBundle};

/// [`MessageDecoder`] for ROS 2 .msg schemas with CDR-encoded messages.
pub struct Ros2MsgDecoder;

impl Ros2MsgDecoder {
    pub fn new() -> Self {
        Self
    }
}

impl Default for Ros2MsgDecoder {
    fn default() -> Self {
        Self::new()
    }
}

impl MessageDecoder for Ros2MsgDecoder {
    fn encoding_key(&self) -> EncodingKey {
        EncodingKey::new(SchemaEncoding::Ros2Msg, MessageEncoding::Cdr)
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

/// Parse and resolve a .msg schema into a [`ResolvedSchema`] ready for CDR decoding.
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
