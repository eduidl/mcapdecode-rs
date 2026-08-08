//! Deterministic regression contract for how much of a file the reader touches.
//!
//! Timing is too noisy to gate in CI, but allocation volume is not: decompressing a
//! chunk allocates, so the number of bytes allocated while reading one low-rate topic
//! tells us whether the reader used the chunk index or scanned the whole file.
//!
//! `dhat::HeapStats` counts the whole process, so this is the only test in the binary —
//! see `reader_contract.rs` for the correctness half of the contract.

mod common;

use common::{clustered_fixture, read_topic};

#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

/// Contract for the sequential read path: chunks outside the requested topic must be skipped.
#[test]
fn reading_a_clustered_topic_does_not_touch_the_whole_file() {
    let path = clustered_fixture();
    let _profiler = dhat::Profiler::builder().testing().build();
    read_topic(&path, false);
    let stats = dhat::HeapStats::get();
    // The topic occupies about one 1 MiB chunk; 8 MiB leaves generous headroom for
    // decoding while still failing loudly if all 24 MiB get decompressed.
    dhat::assert!(
        stats.total_bytes <= 8 * 1024 * 1024,
        "allocated {} bytes, which means chunks outside the topic were decompressed",
        stats.total_bytes
    );
}
