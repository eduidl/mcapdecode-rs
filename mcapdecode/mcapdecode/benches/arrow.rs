//! Arrow stage: decoded values converted into `RecordBatch`es.

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use mcapbench::{Encoding, FileShape, PayloadCase, ensure_generated};
use mcapdecode::McapReader;

fn bench_arrow(c: &mut Criterion) {
    // `bytes` is included because byte sequences are the case where the value
    // representation drives the Arrow conversion cost.
    for case in [PayloadCase::Flat, PayloadCase::Bytes] {
        let path = ensure_generated(case, Encoding::Ros2idl, FileShape::default()).unwrap();
        let bytes = std::fs::metadata(&path).unwrap().len();
        let mut group = c.benchmark_group(format!("value_to_arrow/{case:?}/Ros2idl"));
        group.sample_size(10).throughput(Throughput::Bytes(bytes));
        group.bench_function("record_batch", |b| {
            b.iter(|| {
                McapReader::builder()
                    .with_default_decoders()
                    .with_parallel(false)
                    .build()
                    .for_each_record_batch(&path, mcapbench::TOPIC, |_| Ok(()))
                    .unwrap()
            })
        });
        group.finish();
    }
}

criterion_group!(benches, bench_arrow);
criterion_main!(benches);
