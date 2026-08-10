//! MCAP reading and decoding with pluggable payload decoders.

mod decode;
mod error;
mod prepared_topic;
mod reader;
mod summary;
mod types;

pub use error::McapReaderError;
/// Re-export of the crate whose types appear in this crate's public API, so a
/// dependent does not have to match `mcapdecode-core` versions by hand.
pub use mcapdecode_core as core;
pub use prepared_topic::PreparedTopic;
pub use reader::{McapReader, McapReaderBuilder};
pub use types::{RawMessage, ReadOptions, TimeRange, TopicDecodeStatus, TopicInfo, TopicSchema};
