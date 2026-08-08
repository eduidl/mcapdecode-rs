//! Reader-layer benchmarks: how much of the file the reader has to touch.
//!
//! One axis is varied at a time from a fixed baseline instead of running the full
//! cross product, which keeps the suite small enough to run routinely while still
//! covering selectivity, layout, compression, chunk size and parallelism.

use std::sync::LazyLock;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use mcapbench::{CompressionKind, Encoding, FileShape, Layout, PayloadCase, ensure_generated};
use mcapdecode::McapReader;

const BASELINE: FileShape = FileShape {
    select_percent: 100,
    compression: CompressionKind::Zstd,
    chunk_bytes: 1024 * 1024,
    layout: Layout::Interleaved,
};

struct Case {
    name: String,
    shape: FileShape,
    parallel: bool,
}

static CASES: LazyLock<Vec<Case>> = LazyLock::new(|| {
    let mut cases = Vec::new();

    // Selectivity and layout. Clustered layout is the one where chunk-index based
    // skipping can pay off; interleaved spreads the topic over every chunk.
    for (select_percent, layout) in [
        (1, Layout::Clustered),
        (1, Layout::Interleaved),
        (50, Layout::Clustered),
        (50, Layout::Interleaved),
        (100, Layout::Interleaved),
    ] {
        for parallel in [false, true] {
            cases.push(Case {
                name: format!("select{select_percent}/{layout:?}/parallel{parallel}"),
                shape: FileShape {
                    select_percent,
                    layout,
                    ..BASELINE
                },
                parallel,
            });
        }
    }

    // Compression.
    for compression in [
        CompressionKind::None,
        CompressionKind::Zstd,
        CompressionKind::Lz4,
    ] {
        cases.push(Case {
            name: format!("compression/{compression:?}"),
            shape: FileShape {
                compression,
                ..BASELINE
            },
            parallel: false,
        });
    }

    // Chunk size.
    for (label, chunk_bytes) in [
        ("small", 64 * 1024),
        ("medium", 1024 * 1024),
        ("large", 4 * 1024 * 1024),
    ] {
        cases.push(Case {
            name: format!("chunk/{label}"),
            shape: FileShape {
                chunk_bytes,
                ..BASELINE
            },
            parallel: false,
        });
    }

    cases
});

fn bench_reader(c: &mut Criterion) {
    for case in CASES.iter() {
        let path = ensure_generated(PayloadCase::Flat, Encoding::Ros2idl, case.shape).unwrap();
        let mut group = c.benchmark_group(format!("reader/{}", case.name));
        group
            .sample_size(10)
            .throughput(Throughput::Bytes(std::fs::metadata(&path).unwrap().len()));
        group.bench_function("decoded", |b| {
            b.iter(|| {
                McapReader::builder()
                    .with_default_decoders()
                    .with_parallel(case.parallel)
                    .build()
                    .for_each_decoded_message(&path, mcapbench::TOPIC, |_| Ok(()))
                    .unwrap();
            })
        });
        group.finish();
    }
}

criterion_group!(benches, bench_reader);
criterion_main!(benches);
