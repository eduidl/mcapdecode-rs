//! Decoder configuration for opening MCAP files.

use std::{path::Path, sync::Arc};

use mcapdecode_core::MessageDecoder;
#[cfg(feature = "protobuf")]
use mcapdecode_protobuf::ProtobufDecoder;
#[cfg(feature = "rawjson")]
use mcapdecode_rawjson::RawJsonDecoder;
#[cfg(feature = "ros2idl")]
use mcapdecode_ros2idl::Ros2IdlDecoder;
#[cfg(feature = "ros2msg")]
use mcapdecode_ros2msg::Ros2MsgDecoder;

use crate::{McapReader, error::McapReaderError, internal_reader::InternalReader};

/// Builder for configuring and opening a [`McapReader`].
///
/// One builder can open any number of files with the same configuration.
#[derive(Clone)]
pub struct McapReaderBuilder {
    inner: InternalReader,
}

impl McapReaderBuilder {
    pub(crate) fn new() -> Self {
        Self {
            inner: InternalReader::new(),
        }
    }

    /// Register a message decoder.
    pub fn with_decoder(self, decoder: Box<dyn MessageDecoder>) -> Self {
        self.with_shared_decoder(Arc::from(decoder))
    }

    /// Register a shared message decoder.
    pub fn with_shared_decoder(mut self, decoder: Arc<dyn MessageDecoder>) -> Self {
        self.inner.register_shared_decoder(decoder);
        self
    }

    /// Enable or disable parallel chunk decompression and decoding (default: true).
    ///
    /// This is the reader-wide default; a single read can override it through
    /// [`crate::ReadOptions::parallel`].
    pub fn with_parallel(mut self, parallel: bool) -> Self {
        self.inner.parallel = parallel;
        self
    }

    /// Register all built-in decoders enabled by cargo features.
    pub fn with_default_decoders(self) -> Self {
        let s = self;
        #[cfg(feature = "rawjson")]
        let s = s.with_decoder(Box::new(RawJsonDecoder::new()));
        #[cfg(feature = "protobuf")]
        let s = s.with_decoder(Box::new(ProtobufDecoder::new()));
        #[cfg(feature = "ros2idl")]
        let s = s.with_decoder(Box::new(Ros2IdlDecoder::new()));
        #[cfg(feature = "ros2msg")]
        let s = s.with_decoder(Box::new(Ros2MsgDecoder::new()));
        s
    }

    /// Open a file with this builder's decoders and settings.
    pub fn open(&self, path: impl AsRef<Path>) -> Result<McapReader, McapReaderError> {
        McapReader::from_parts(self.inner.clone(), path.as_ref())
    }
}
