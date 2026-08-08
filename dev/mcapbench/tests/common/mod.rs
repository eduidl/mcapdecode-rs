//! Shared fixture and reader helpers for the reader regression contracts.

use mcapbench::{CompressionKind, Encoding, FileShape, Layout, PayloadCase, ensure_generated};
use mcapdecode::McapReader;

/// 24 MiB of messages in 1 MiB chunks, with the benchmarked topic clustered into the
/// first one percent of the file: a reader that consults the chunk index touches
/// roughly a single chunk, one that scans linearly decompresses all 24 MiB.
pub fn clustered_fixture() -> std::path::PathBuf {
    ensure_generated(
        PayloadCase::Flat,
        Encoding::Ros2idl,
        FileShape {
            select_percent: 1,
            compression: CompressionKind::Zstd,
            chunk_bytes: 1024 * 1024,
            layout: Layout::Clustered,
        },
    )
    .unwrap()
}

pub fn read_topic(path: &std::path::Path, parallel: bool) -> usize {
    let mut count = 0;
    McapReader::builder()
        .with_default_decoders()
        .with_parallel(parallel)
        .build()
        .for_each_decoded_message(path, mcapbench::TOPIC, |_| {
            count += 1;
            Ok(())
        })
        .unwrap();
    count
}
