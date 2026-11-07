//! Multi-topic reader throughput with two interleaved channels.

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use mcapbench::{CompressionKind, Encoding, FileShape, Layout, PayloadCase, ensure_generated};
use mcapdecode::McapReader;

fn bench_reader_multi(c: &mut Criterion) {
    let shape = FileShape {
        select_percent: 50,
        compression: CompressionKind::Zstd,
        chunk_bytes: 1024 * 1024,
        layout: Layout::Interleaved,
    };
    let path = ensure_generated(PayloadCase::Flat, Encoding::Ros2idl, shape).unwrap();
    let mut group = c.benchmark_group("reader/multi_topic");
    group.throughput(Throughput::Bytes(std::fs::metadata(&path).unwrap().len()));
    for parallel in [false, true] {
        let name = if parallel { "parallel" } else { "sequential" };
        group.bench_function(name, |b| {
            b.iter(|| {
                McapReader::builder()
                    .with_parallel(parallel)
                    .open(&path)
                    .unwrap()
                    .read()
                    .topics([mcapbench::TOPIC, "/other"])
                    .decoded()
                    .for_each(|_, _| {})
                    .unwrap();
            })
        });
    }
    group.finish();
}

criterion_group! {
    name = benches;
    config = mcapbench::criterion();
    targets = bench_reader_multi
}
criterion_main!(benches);
