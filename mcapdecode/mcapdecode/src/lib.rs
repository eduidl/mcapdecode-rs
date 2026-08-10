#[cfg(feature = "arrow")]
pub use mcapdecode_arrow as arrow;
#[cfg(feature = "arrow")]
pub use mcapdecode_arrow::{McapReaderArrowExt, RecordBatchOptions};
pub use mcapdecode_core as core;
pub use mcapdecode_reader::{
    McapReader, McapReaderBuilder, McapReaderError, PreparedTopic, RawMessage, ReadOptions,
    TimeRange, TopicDecodeStatus, TopicInfo, TopicSchema,
};
