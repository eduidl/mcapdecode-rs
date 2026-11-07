#[cfg(feature = "arrow")]
pub use mcapdecode_arrow::{
    Integer64Encoding, JsonlWriter, JsonlWriterBuilder, MessageBatchSchema, MetadataColumns,
    MetadataTimestampFormat, NonFiniteFloats,
};
mod chunk_reader;
mod error;
mod read;
mod reader;
mod types;

pub use error::McapReaderError;
/// The `mcap` crate whose types [`McapReader::summary`] exposes.
pub use mcap;
#[cfg(feature = "arrow")]
pub use mcapdecode_arrow as arrow;
pub use mcapdecode_core as core;
#[cfg(feature = "arrow")]
pub use read::ArrowRead;
pub use read::{CallbackOutcome, DecodedRead, RawRead, Read};
pub use reader::{McapReader, McapReaderBuilder};
pub use types::{
    ChannelInfo, RawMessage, ReadOptions, TimeRange, TopicDecodeStatus, TopicInfo, TopicSchema,
};
