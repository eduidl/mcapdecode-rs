//! Encoding-agnostic core types and decoder contracts for `mcapdecode`.
//!
//! This crate provides Arrow-independent intermediate representations
//! ([`Value`] / [`DataTypeDef`]) and the [`MessageDecoder`] trait.

mod decoder;
mod error;
mod message;
mod message_encoding;
mod schema;
mod schema_encoding;
mod value;

pub use decoder::{EncodingKey, MessageDecoder, TopicDecoder};
pub use error::{DecoderError, ValueTypeError};
pub use message::DecodedMessage;
pub use message_encoding::MessageEncoding;
pub use schema::{DataTypeDef, ElementDef, EnumVariant, FieldDef, FieldDefs, format_field_defs};
pub use schema_encoding::SchemaEncoding;
pub use value::Value;
