#[cfg(feature = "arrow")]
mod arrow_ext;
#[cfg(feature = "arrow")]
pub use arrow_ext::RecordBatchOptions;
#[cfg(feature = "arrow")]
pub use mcapdecode_arrow::{
    Integer64Encoding, JsonlWriter, JsonlWriterBuilder, MessageBatchSchema, MetadataColumns,
    MetadataTimestampFormat, NonFiniteFloats,
};
mod decode;
mod error;
mod internal_reader;
mod mcap_file;
mod opened;
mod prepared_topic;
mod reader;
mod summary;
mod types;

pub use error::McapReaderError;
#[cfg(feature = "arrow")]
pub use mcapdecode_arrow as arrow;
pub use mcapdecode_core as core;
pub use opened::OpenedMcap;
pub use prepared_topic::PreparedTopic;
pub use reader::{McapReader, McapReaderBuilder};
pub use types::{
    ChannelInfo, RawMessage, ReadOptions, TimeRange, TopicDecodeStatus, TopicInfo, TopicSchema,
};
