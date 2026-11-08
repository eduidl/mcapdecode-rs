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
/// Built-in message decoders, and types for implementing custom ones.
///
/// [`McapReader::builder`] registers every built-in decoder enabled by cargo
/// features. Register one again with
/// [`McapReaderBuilder::with_decoder`] to change its settings.
pub mod decoder {
    pub use mcapdecode_core::{DecoderError, EncodingKey, MessageDecoder, TopicDecoder};
    #[cfg(feature = "protobuf")]
    pub use mcapdecode_protobuf::{PresencePolicy, ProtobufDecoder};
    #[cfg(feature = "rawjson")]
    pub use mcapdecode_rawjson::RawJsonDecoder;
    #[cfg(feature = "ros2idl")]
    pub use mcapdecode_ros2idl::Ros2IdlDecoder;
    #[cfg(feature = "ros2msg")]
    pub use mcapdecode_ros2msg::Ros2MsgDecoder;
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
    MapKey, MessageEncoding, SchemaEncoding, StructDef, StructValue, Value, ValueAccessError,
    ValueIndex, ValueType, ValueTypeError,
};
#[cfg(feature = "arrow")]
pub use read::ArrowRead;
pub use read::{CallbackOutcome, DecodedRead, RawRead, Read};
pub use reader::{McapReader, McapReaderBuilder};
pub use types::{ChannelInfo, ChannelSchema, RawMessage, TopicInfo};
