//! Error types for the MCAP reader.

use mcapdecode_core::{DecoderError, MessageEncoding, SchemaEncoding};

/// Errors produced by [`McapReader`](crate::McapReader).
#[derive(Debug, thiserror::Error)]
pub enum McapReaderError {
    /// I/O error while opening or memory-mapping a file.
    #[error(transparent)]
    Io(#[from] std::io::Error),

    /// Error from the underlying `mcap` crate (bad magic, CRC mismatch, ...).
    #[error(transparent)]
    Mcap(#[from] mcap::McapError),

    /// The MCAP file has no summary section.
    #[error("MCAP summary not available")]
    SummaryNotAvailable,

    /// A channel that was about to be decoded has no schema attached.
    #[error("schema not available")]
    SchemaNotAvailable,

    /// The requested topic was not found in the MCAP file.
    #[error("topic not found")]
    TopicNotFound,

    /// The requested channel id was not found in the MCAP file.
    #[error("channel not found")]
    ChannelNotFound,

    /// No [`MessageDecoder`](mcapdecode_core::MessageDecoder) was registered for
    /// the encoding pair found on a channel.
    #[error(
        "no decoder registered for schema_encoding='{schema_encoding}', message_encoding='{message_encoding}'"
    )]
    NoDecoder {
        schema_encoding: SchemaEncoding,
        message_encoding: MessageEncoding,
    },

    /// A decoder-derived schema had no fields and cannot be converted to Arrow.
    #[error("failed to derive schema (schema: '{schema_name}')")]
    EmptyDerivedSchema { schema_name: String },

    /// Arrow conversion failed while producing a record batch.
    #[cfg(feature = "arrow")]
    #[error(transparent)]
    ArrowConvert(#[from] mcapdecode_arrow::ArrowConvertError),

    /// Error reported by a registered message decoder.
    #[error(transparent)]
    Decoder(#[from] DecoderError),

    /// An error returned by the user-supplied callback in reader iteration APIs.
    #[error(transparent)]
    Callback(Box<dyn std::error::Error + Send + Sync>),
}

impl McapReaderError {
    /// Whether the error means that a channel cannot be decoded with the
    /// registered decoders: it has no schema, no decoder handles its encodings,
    /// or the decoder cannot parse or derive its schema.
    ///
    /// Such channels can still be read with [`Read::raw`](crate::Read::raw).
    /// Failures to decode individual messages, I/O errors and other errors
    /// return `false`.
    pub fn is_unsupported_channel(&self) -> bool {
        matches!(
            self,
            Self::NoDecoder { .. }
                | Self::SchemaNotAvailable
                | Self::Decoder(DecoderError::SchemaParse(_) | DecoderError::SchemaDerivation(_))
        )
    }
}
