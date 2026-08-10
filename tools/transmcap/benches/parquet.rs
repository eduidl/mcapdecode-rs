//! End-to-end stage: MCAP through the Arrow conversion and out as parquet.
//!
//! The writer module is pulled in by path because `transmcap` is a binary crate and
//! does not expose it as a library; only `ParquetWriter` is used here.
#[allow(dead_code)]
#[path = "../src/writer.rs"]
mod writer;

use std::sync::LazyLock;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use mcapbench::{Encoding, FileShape, PayloadCase, ensure_generated};
use mcapdecode::{McapReader, McapReaderArrowExt as _};
use writer::{ParquetWriter, RecordBatchWriter};

static FILE: LazyLock<std::path::PathBuf> = LazyLock::new(|| {
    ensure_generated(PayloadCase::Flat, Encoding::Ros2idl, FileShape::default()).unwrap()
});

fn bench_parquet(c: &mut Criterion) {
    let path = &*FILE;
    let bytes = std::fs::metadata(path).unwrap().len();
    // Process-unique: a fixed name in a world-writable directory fails with EACCES when
    // another user owns it, and two concurrent runs would clobber each other.
    let output = mcapbench::fixture_dir().join(format!("bench-{}.parquet", std::process::id()));
    std::fs::create_dir_all(output.parent().unwrap()).unwrap();
    let mut group = c.benchmark_group("parquet/flat/ros2idl");
    group.sample_size(10).throughput(Throughput::Bytes(bytes));
    group.bench_function("write", |b| {
        b.iter(|| {
            let mut writer = ParquetWriter::new(&output).unwrap();
            McapReader::builder()
                .with_default_decoders()
                .with_parallel(false)
                .build()
                .for_each_record_batch(path, mcapbench::TOPIC, |batch| {
                    writer
                        .write_batch(batch)
                        .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { e.into() })
                })
                .unwrap();
            writer.finish().unwrap();
        })
    });
    group.finish();
    // Each iteration overwrites the file, so it is unlinked once here instead of inside
    // the measured closure.
    let _ = std::fs::remove_file(&output);
}

criterion_group!(benches, bench_parquet);
criterion_main!(benches);
