//! Error types for the decoder layer.
use thiserror::Error;

/// Error returned by [`MessageDecoder`](crate::MessageDecoder) implementations.
#[derive(Debug, Error)]
pub enum DecoderError {
    /// Schema data (e.g., a serialized `FileDescriptorSet`) could not be parsed.
    #[error("failed to parse schema: {0}")]
    SchemaParse(#[source] Box<dyn std::error::Error + Send + Sync>),

    /// Schema data was parsed but could not be derived into a decoder schema.
    #[error("schema derivation failed: {0}")]
    SchemaDerivation(String),

    /// Message payload bytes could not be decoded.
    #[error("failed to decode message: {0}")]
    MessageDecode(#[source] Box<dyn std::error::Error + Send + Sync>),
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("expected {expected}, got {actual}")]
pub struct ValueTypeError {
    expected: String,
    actual: String,
}

impl ValueTypeError {
    pub fn new(expected: impl Into<String>, actual: impl Into<String>) -> Self {
        Self {
            expected: expected.into(),
            actual: actual.into(),
        }
    }
}
