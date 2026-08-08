//! `dhat::HeapStats` counts the whole process, so a test asserting on it has to be the
//! only test in its binary: cargo runs test targets one at a time, but the threads
//! within one target run concurrently and their allocations would land in this window.

mod common;

use common::{bytes_schema, one_megabyte_cdr};
use mcapdecode_ros2_common::decode_cdr_to_value;

#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

/// Contract for issue #2. The dhat heap profiler makes this deterministic enough for CI.
#[test]
fn byte_sequence_allocates_at_most_two_megabytes() {
    let cdr = one_megabyte_cdr();
    let _profiler = dhat::Profiler::builder().testing().build();
    let _ = decode_cdr_to_value(&bytes_schema(), &cdr).unwrap();
    let stats = dhat::HeapStats::get();
    dhat::assert!(
        stats.total_bytes <= 2 * 1024 * 1024,
        "allocated {} bytes",
        stats.total_bytes
    );
}
