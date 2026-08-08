//! Decoder-layer benchmarks: payload shape kept separate from schema encoding.

use std::{path::PathBuf, sync::LazyLock};

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use mcapbench::{Encoding, FileShape, PayloadCase, ensure_generated};
use mcapdecode::McapReader;

static CASES: LazyLock<Vec<(PayloadCase, Encoding, PathBuf)>> = LazyLock::new(|| {
    let mut combinations = Vec::new();
    for case in [
        PayloadCase::Flat,
        PayloadCase::Nested,
        PayloadCase::Bytes,
        PayloadCase::NumericArray,
    ] {
        for encoding in [Encoding::Ros2idl, Encoding::Ros2msg, Encoding::Protobuf] {
            combinations.push((case, encoding));
        }
    }
    // `strings` has no protobuf fixture.
    for encoding in [Encoding::Ros2idl, Encoding::Ros2msg] {
        combinations.push((PayloadCase::Strings, encoding));
    }

    combinations
        .into_iter()
        .map(|(case, encoding)| {
            let path = ensure_generated(case, encoding, FileShape::default()).unwrap();
            (case, encoding, path)
        })
        .collect()
});

fn bench_decoder(c: &mut Criterion) {
    for (case, encoding, path) in CASES.iter() {
        let bytes = std::fs::metadata(path).unwrap().len();
        let mut group = c.benchmark_group(format!("decode_value/{case:?}/{encoding:?}"));
        group.sample_size(10).throughput(Throughput::Bytes(bytes));
        group.bench_function("mcap", |b| {
            b.iter(|| {
                McapReader::builder()
                    .with_default_decoders()
                    .with_parallel(false)
                    .build()
                    .for_each_decoded_message(path, mcapbench::TOPIC, |_| Ok(()))
                    .unwrap();
            })
        });
        group.finish();
    }
}

criterion_group!(benches, bench_decoder);
criterion_main!(benches);
