#[cfg(feature = "arrow")]
mod arrow_ext;
#[cfg(feature = "arrow")]
pub use arrow_ext::RecordBatchOptions;
mod decode;
mod error;
mod prepared_topic;
mod reader;
mod summary;
mod types;

pub use error::McapReaderError;
#[cfg(feature = "arrow")]
pub use mcapdecode_arrow as arrow;
pub use mcapdecode_core as core;
pub use prepared_topic::PreparedTopic;
pub use reader::{McapReader, McapReaderBuilder};
pub use types::{RawMessage, ReadOptions, TimeRange, TopicDecodeStatus, TopicInfo, TopicSchema};
