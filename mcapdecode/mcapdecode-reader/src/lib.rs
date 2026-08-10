//! MCAP reading and decoding with pluggable payload decoders.

mod error;
mod reader;

pub use error::McapReaderError;
pub use reader::{
    McapReader, McapReaderBuilder, PreparedTopic, RawMessage, ReadOptions, TimeRange,
    TopicDecodeStatus, TopicInfo, TopicSchema,
};
