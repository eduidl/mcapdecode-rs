//! Deterministic, on-demand MCAP fixtures used by benchmarks and reproducibility tools.
//!
//! No fixture is checked into the repository: callers create it in a temporary directory
//! (or use the CLI) and may cache the result for the lifetime of their process.
//!
//! One [`model`] describes each payload case, and the schema text, the CDR/protobuf
//! payload and the expected decoded values are all derived from it. Schema and payload
//! therefore cannot drift apart.

mod fixture;
mod model;
mod protobuf;
mod ros2;

pub use fixture::{Fixture, ensure_generated, fixture, fixture_dir, generate, generated_path};
pub use model::{Lcg, Sample, sample};

/// Topic the benchmarks read.
pub const TOPIC: &str = "/bench";

/// Topic carrying everything the benchmarks are not reading.
pub(crate) const OTHER_TOPIC: &str = "/other";

pub type BenchResult<T> = Result<T, Box<dyn std::error::Error>>;

/// Shape of one message, chosen to isolate a specific cost in the decoder.
#[derive(Clone, Copy, Debug, clap::ValueEnum, PartialEq, Eq)]
pub enum PayloadCase {
    Flat,
    Nested,
    Bytes,
    NumericArray,
    Strings,
}

#[derive(Clone, Copy, Debug, clap::ValueEnum, PartialEq, Eq)]
pub enum Encoding {
    Ros2idl,
    Ros2msg,
    Protobuf,
}

#[derive(Clone, Copy, Debug, clap::ValueEnum, PartialEq, Eq)]
pub enum CompressionKind {
    None,
    Zstd,
    Lz4,
}

/// How the benchmarked topic is distributed over the file.
///
/// [`Layout::Interleaved`] mixes the topic evenly with the rest, so every chunk holds
/// some of it. [`Layout::Clustered`] writes it as one contiguous run, which is what a
/// low-rate topic in a real recording looks like and the only layout where skipping
/// chunks via the chunk index can pay off.
#[derive(Clone, Copy, Debug, clap::ValueEnum, PartialEq, Eq)]
pub enum Layout {
    Interleaved,
    Clustered,
}

#[derive(Clone, Copy, Debug)]
pub struct FileShape {
    pub select_percent: u8,
    pub compression: CompressionKind,
    pub chunk_bytes: usize,
    pub layout: Layout,
}

impl Default for FileShape {
    fn default() -> Self {
        Self {
            select_percent: 100,
            compression: CompressionKind::None,
            chunk_bytes: 1024 * 1024,
            layout: Layout::Interleaved,
        }
    }
}
