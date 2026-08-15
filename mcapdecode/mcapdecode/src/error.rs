//! Error types for the MCAP reader.

use mcapdecode_core::DecoderError;

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

    /// The MCAP summary section has no statistics record.
    #[error("MCAP summary stats not available")]
    StatsNotAvailable,

    /// A channel that was about to be decoded has no schema attached.
    #[error("schema not available")]
    SchemaNotAvailable,

    /// The requested topic was not found in the MCAP file.
    #[error("topic not found")]
    TopicNotFound,

    /// No [`MessageDecoder`](mcapdecode_core::MessageDecoder) was registered for
    /// the encoding pair found on a channel.
    #[error(
        "no decoder registered for schema_encoding='{schema_encoding}', message_encoding='{message_encoding}'"
    )]
    NoDecoder {
        schema_encoding: String,
        message_encoding: String,
    },

    /// A decoder-derived schema had no fields and cannot be converted to Arrow.
    #[error("failed to derive schema (schema: '{schema_name}')")]
    EmptyDerivedSchema { schema_name: String },

    /// Arrow conversion failed while producing a record batch.
    #[cfg(feature = "arrow")]
    #[error(transparent)]
    ArrowConvert(#[from] mcapdecode_arrow::ArrowConvertError),

    /// Multiple channels found for the same topic in the MCAP file.
    #[error("multiple channels found for requested topic")]
    MultipleChannels,

    /// Error reported by a registered message decoder.
    #[error(transparent)]
    Decoder(#[from] DecoderError),

    /// An error returned by the user-supplied callback in reader iteration APIs.
    #[error(transparent)]
    Callback(Box<dyn std::error::Error + Send + Sync>),
}
