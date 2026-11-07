#[cfg(feature = "arrow")]
pub use mcapdecode_arrow::{
    Integer64Encoding, JsonlWriter, JsonlWriterBuilder, MessageBatchSchema, MetadataColumns,
    MetadataTimestampFormat, NonFiniteFloats,
};
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
#[cfg(feature = "arrow")]
pub use mcapdecode_arrow as arrow;
pub use mcapdecode_core::{
    DataTypeDef, DecodedMessage, ElementDef, EnumDef, EnumVariant, FieldDef, FieldDefs, FieldPath,
    MapKey, StructDef, StructValue, Value, ValueAccessError, ValueIndex, ValueTypeError,
};
#[cfg(feature = "arrow")]
pub use read::ArrowRead;
pub use read::{CallbackOutcome, DecodedRead, RawRead, Read};
pub use reader::{McapReader, McapReaderBuilder};
pub use types::{ChannelInfo, RawMessage, TopicDecodeStatus, TopicInfo, TopicSchema};
