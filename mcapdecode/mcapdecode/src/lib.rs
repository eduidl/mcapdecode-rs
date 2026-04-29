#[cfg(feature = "arrow")]
mod arrow_ext;
mod error;
mod reader;

#[cfg(feature = "arrow")]
pub use arrow_ext::McapReaderArrowExt;
pub use error::McapReaderError;
#[cfg(feature = "arrow")]
pub use mcapdecode_arrow as arrow;
pub use mcapdecode_core as core;
pub use reader::{McapReader, RawMessage, TopicInfo};
