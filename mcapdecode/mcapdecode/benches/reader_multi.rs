//! Multi-topic reader throughput with two interleaved channels.

use criterion::{Criterion, criterion_group, criterion_main};
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
    for parallel in [false, true] {
        let name = if parallel { "parallel" } else { "sequential" };
        c.bench_function(&format!("reader/multi_topic/{name}"), |b| {
            b.iter(|| {
                McapReader::builder()
                    .with_default_decoders()
                    .with_parallel(parallel)
                    .build()
                    .for_each_topic_decoded_message(&path, &[mcapbench::TOPIC, "/other"], |_, _| {
                        Ok(())
                    })
                    .unwrap();
            })
        });
    }
}

criterion_group! {
    name = benches;
    config = mcapbench::criterion();
    targets = bench_reader_multi
}
criterion_main!(benches);
