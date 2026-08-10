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
    #[error("MCAP summary not available in {path}")]
    SummaryNotAvailable { path: String },

    /// The MCAP summary section has no statistics record.
    #[error("MCAP summary stats not available in {path}")]
    StatsNotAvailable { path: String },

    /// A channel that was about to be decoded has no schema attached.
    #[error("schema not available for topic '{topic}' (channel id {channel_id})")]
    SchemaNotAvailable { topic: String, channel_id: u16 },

    /// The requested topic was not found in the MCAP file.
    #[error("topic '{topic}' not found")]
    TopicNotFound { topic: String },

    /// No [`MessageDecoder`](mcapdecode_core::MessageDecoder) was registered for
    /// the encoding pair found on a channel.
    #[error(
        "no decoder registered for schema_encoding='{schema_encoding}', message_encoding='{message_encoding}' on topic '{topic}'"
    )]
    NoDecoder {
        schema_encoding: String,
        message_encoding: String,
        topic: String,
    },

    /// A decoder-derived schema had no fields and cannot be converted to Arrow.
    #[error("failed to derive schema for topic '{topic}' (schema: '{schema_name}')")]
    EmptyDerivedSchema { topic: String, schema_name: String },

    /// Multiple channels found for the same topic in the MCAP file.
    #[error("multiple channels found for topic '{topic}'")]
    MultipleChannels { topic: String },

    /// Decoder failed to derive schema from MCAP schema data.
    #[error("schema derivation failed for topic '{topic}': {source}")]
    SchemaDerivationFailed {
        topic: String,
        #[source]
        source: DecoderError,
    },

    /// Decoder failed to decode a message payload.
    #[error("message decode failed for topic '{topic}': {source}")]
    MessageDecodeFailed {
        topic: String,
        #[source]
        source: DecoderError,
    },

    /// An error returned by the user-supplied callback in reader iteration APIs.
    #[error(transparent)]
    Callback(Box<dyn std::error::Error + Send + Sync>),
}
