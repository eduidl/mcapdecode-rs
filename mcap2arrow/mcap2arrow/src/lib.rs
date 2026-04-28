mod error;
mod reader;

pub use error::McapReaderError;
pub use mcap2arrow_arrow as arrow;
pub use mcap2arrow_core as core;
pub use reader::{McapReader, TopicInfo};
