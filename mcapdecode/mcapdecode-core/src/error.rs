//! Error types for the decoder layer.

use thiserror::Error;

use crate::value::ValueType;

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

// Box arbitrary descriptions so errors containing only type names stay small.
#[derive(Clone, PartialEq, Eq)]
enum TypeMismatch {
    Types(ValueType, ValueType),
    Descriptions(Box<(String, String)>),
}

impl TypeMismatch {
    fn descriptions(&self) -> (&str, &str) {
        match self {
            Self::Types(expected, actual) => (expected.as_str(), actual.as_str()),
            Self::Descriptions(pair) => (&pair.0, &pair.1),
        }
    }
}

impl std::fmt::Display for TypeMismatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (expected, actual) = self.descriptions();
        write!(f, "expected {expected}, got {actual}")
    }
}

#[derive(Clone, PartialEq, Eq, Error)]
#[error("{inner}")]
pub struct ValueTypeError {
    inner: TypeMismatch,
}

impl std::fmt::Debug for ValueTypeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (expected, actual) = self.inner.descriptions();
        f.debug_struct("ValueTypeError")
            .field("expected", &expected)
            .field("actual", &actual)
            .finish()
    }
}

impl ValueTypeError {
    pub fn new(expected: impl Into<String>, actual: impl Into<String>) -> Self {
        let expected = expected.into();
        let actual = actual.into();
        match (ValueType::parse(&expected), ValueType::parse(&actual)) {
            (Some(expected), Some(actual)) => Self::for_types(expected, actual),
            _ => Self {
                inner: TypeMismatch::Descriptions(Box::new((expected, actual))),
            },
        }
    }

    pub(crate) fn for_types(expected: ValueType, actual: ValueType) -> Self {
        Self {
            inner: TypeMismatch::Types(expected, actual),
        }
    }

    pub(crate) fn for_value(expected: impl Into<String>, actual: ValueType) -> Self {
        let expected = expected.into();
        match ValueType::parse(&expected) {
            Some(expected) => Self::for_types(expected, actual),
            None => Self::new(expected, actual.as_str()),
        }
    }
}

/// An error encountered while accessing or converting a decoded value.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ValueAccessError {
    #[error("field or element not found")]
    NotFound,
    #[error("value is null")]
    NullValue,
    #[error(transparent)]
    TypeMismatch(#[from] ValueTypeError),
    #[error("value does not match its schema")]
    SchemaMismatch,
}
