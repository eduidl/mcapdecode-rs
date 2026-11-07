//! Reader path over a generated file: chunk decompression, chunk selection and
//! parallel scheduling on top of payload decoding.
//!
//! Payload decoding on its own is covered by the `decode` suite; these cases only
//! exist for what the reader adds around it.

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use mcapbench::{CompressionKind, Encoding, FileShape, Layout, PayloadCase, ensure_generated};
use mcapdecode::McapReader;

/// Every chunk holds the read topic, so the whole file is decompressed.
const FULL: FileShape = FileShape {
    select_percent: 100,
    compression: CompressionKind::Zstd,
    chunk_bytes: 1024 * 1024,
    layout: Layout::Interleaved,
};

/// The read topic sits in a few contiguous chunks; the rest is skipped via the
/// chunk index.
const SPARSE: FileShape = FileShape {
    select_percent: 1,
    layout: Layout::Clustered,
    ..FULL
};

fn bench_reader(c: &mut Criterion) {
    for (name, shape, parallel) in [
        ("sequential", FULL, false),
        ("parallel", FULL, true),
        ("skip_chunks", SPARSE, false),
    ] {
        let path = ensure_generated(PayloadCase::Flat, Encoding::Ros2idl, shape).unwrap();
        let mut group = c.benchmark_group("reader/decoded");
        group.throughput(Throughput::Bytes(std::fs::metadata(&path).unwrap().len()));
        group.bench_function(name, |b| {
            b.iter(|| {
                McapReader::builder()
                    .with_default_decoders()
                    .with_parallel(parallel)
                    .build()
                    .open_file(&path)
                    .unwrap()
                    .read()
                    .topic(mcapbench::TOPIC)
                    .decoded()
                    .for_each(|_, _| {})
                    .unwrap();
            })
        });
        group.finish();
        let mut group = c.benchmark_group("reader/raw");
        group.throughput(Throughput::Bytes(std::fs::metadata(&path).unwrap().len()));
        group.bench_function(name, |b| {
            b.iter(|| {
                McapReader::builder()
                    .with_parallel(parallel)
                    .build()
                    .open_file(&path)
                    .unwrap()
                    .read()
                    .topic(mcapbench::TOPIC)
                    .raw()
                    .for_each(|_, _| {})
                    .unwrap();
            })
        });
        group.finish();
    }
}

criterion_group! {
    name = benches;
    config = mcapbench::criterion();
    targets = bench_reader
}
criterion_main!(benches);
