#[cfg(feature = "arrow")]
mod arrow_ext;
#[cfg(feature = "arrow")]
pub use arrow_ext::{McapReaderArrowExt, RecordBatchOptions};
#[cfg(feature = "arrow")]
pub use mcapdecode_arrow as arrow;
pub use mcapdecode_core as core;
pub use mcapdecode_reader::{
    McapReader, McapReaderBuilder, McapReaderError, PreparedTopic, RawMessage, ReadOptions,
    TimeRange, TopicDecodeStatus, TopicInfo, TopicSchema,
};
