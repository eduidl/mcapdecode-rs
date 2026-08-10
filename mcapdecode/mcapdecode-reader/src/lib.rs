//! MCAP reading and decoding with pluggable payload decoders.

mod error;
mod reader;

pub use error::McapReaderError;
/// Re-export of the crate whose types appear in this crate's public API, so a
/// dependent does not have to match `mcapdecode-core` versions by hand.
pub use mcapdecode_core as core;
pub use reader::{
    McapReader, McapReaderBuilder, PreparedTopic, RawMessage, ReadOptions, TimeRange,
    TopicDecodeStatus, TopicInfo, TopicSchema,
};
