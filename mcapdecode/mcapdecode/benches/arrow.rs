//! Arrow conversion in isolation: decoded rows to one `RecordBatch`.
//!
//! Rows are decoded outside the measured loop, so only
//! `MessageBatchSchema::to_record_batch` is timed.

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use mcapbench::{Encoding, PayloadCase, fixture};
use mcapdecode::{DecodedMessage, MessageBatchSchema, MetadataColumns, decoder::MessageDecoder};
use mcapdecode_ros2idl::Ros2IdlDecoder;

/// Payload volume converted per iteration, so small and large payloads do
/// comparable amounts of work.
const BATCH_PAYLOAD_BYTES: usize = 4 * 1024 * 1024;

fn bench_arrow(c: &mut Criterion) {
    for case in [
        PayloadCase::Flat,
        PayloadCase::Nested,
        PayloadCase::Bytes,
        PayloadCase::NumericArray,
        PayloadCase::Strings,
    ] {
        let fixture = fixture(case, Encoding::Ros2idl).unwrap();
        let decoder = Ros2IdlDecoder::new()
            .build_topic_decoder(&fixture.schema_name, &fixture.schema)
            .unwrap();
        let value = decoder.decode(&fixture.payload).unwrap();
        let count = (BATCH_PAYLOAD_BYTES / fixture.payload.len()).max(1);
        let rows: Vec<DecodedMessage> = (0..count as u64)
            .map(|i| DecodedMessage {
                log_time: i,
                publish_time: i,
                value: value.clone(),
            })
            .collect();
        let schema =
            MessageBatchSchema::from_field_defs(decoder.field_defs(), MetadataColumns::default())
                .unwrap();

        let mut group = c.benchmark_group(format!("to_record_batch/{case:?}"));
        group.throughput(Throughput::Bytes((count * fixture.payload.len()) as u64));
        group.bench_function("Ros2idl", |b| {
            b.iter(|| schema.to_record_batch(&rows).unwrap())
        });
        group.finish();
    }
}

criterion_group! {
    name = benches;
    config = mcapbench::criterion();
    targets = bench_arrow
}
criterion_main!(benches);
