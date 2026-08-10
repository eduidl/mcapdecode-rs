#[cfg(feature = "arrow")]
mod arrow_ext;
#[cfg(feature = "arrow")]
pub use arrow_ext::RecordBatchOptions;
mod error;
mod reader;

pub use error::McapReaderError;
#[cfg(feature = "arrow")]
pub use mcapdecode_arrow as arrow;
pub use mcapdecode_core as core;
pub use reader::{
    McapReader, McapReaderBuilder, PreparedTopic, RawMessage, ReadOptions, TimeRange,
    TopicDecodeStatus, TopicInfo, TopicSchema,
};
