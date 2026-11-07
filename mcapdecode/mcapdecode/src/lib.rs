/// The `arrow` crate whose `RecordBatch`es [`ArrowRead`] produces.
#[cfg(feature = "arrow")]
pub use arrow;
#[cfg(feature = "arrow")]
pub use mcapdecode_arrow::{
    ArrowConvertError, MessageBatchSchema, MetadataColumns, MetadataTimestampFormat,
};
/// Utilities for post-processing and writing `RecordBatch`es produced by [`ArrowRead`].
#[cfg(feature = "arrow")]
pub mod batch {
    pub use mcapdecode_arrow::{
        ArrayPolicy, FlattenPolicy, Integer64Encoding, JsonlWriter, JsonlWriterBuilder, ListPolicy,
        MapPolicy, NonFiniteFloats, StructPolicy, flatten_record_batch, project_record_batch,
    };
}
/// Types for implementing and registering custom message decoders.
pub mod decoder {
    pub use mcapdecode_core::{
        DecoderError, EncodingKey, MessageDecoder, MessageEncoding, SchemaEncoding, TopicDecoder,
    };
}

mod chunk_reader;
mod error;
mod read;
mod reader;
mod types;

pub use error::McapReaderError;
/// The `mcap` crate whose types [`McapReader::summary`] exposes.
pub use mcap;
pub use mcapdecode_core::{
    DataTypeDef, DecodedMessage, ElementDef, EnumDef, EnumVariant, FieldDef, FieldDefs, FieldPath,
    MapKey, StructDef, StructValue, Value, ValueAccessError, ValueIndex, ValueTypeError,
};
#[cfg(feature = "arrow")]
pub use read::ArrowRead;
pub use read::{CallbackOutcome, DecodedRead, RawRead, Read};
pub use reader::{McapReader, McapReaderBuilder};
pub use types::{ChannelInfo, RawMessage, TopicDecodeStatus, TopicInfo, TopicSchema};
