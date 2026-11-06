//! Decoder functions in isolation: no file, no chunk decompression.
//!
//! `decode` measures one `TopicDecoder::decode` call on an in-memory payload, with the
//! decoder built outside the measured loop. `build_topic_decoder` measures schema
//! parsing and resolution on its own.

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use mcapbench::{Encoding, PayloadCase, fixture};
use mcapdecode::core::MessageDecoder;
use mcapdecode_protobuf::ProtobufDecoder;
use mcapdecode_ros2idl::Ros2IdlDecoder;
use mcapdecode_ros2msg::Ros2MsgDecoder;

fn message_decoder(encoding: Encoding) -> Box<dyn MessageDecoder> {
    match encoding {
        Encoding::Ros2idl => Box::new(Ros2IdlDecoder::new()),
        Encoding::Ros2msg => Box::new(Ros2MsgDecoder::new()),
        Encoding::Protobuf => Box::new(ProtobufDecoder::new()),
    }
}

fn bench_decode(c: &mut Criterion) {
    // ros2msg is left out: once built, its topic decoder is the same CDR decoder as
    // ros2idl, so only `build_topic_decoder` differs between the two.
    let mut cases = Vec::new();
    for case in [
        PayloadCase::Flat,
        PayloadCase::Nested,
        PayloadCase::Bytes,
        PayloadCase::NumericArray,
    ] {
        cases.push((case, Encoding::Ros2idl));
        cases.push((case, Encoding::Protobuf));
    }
    // `strings` has no protobuf fixture.
    cases.push((PayloadCase::Strings, Encoding::Ros2idl));

    for (case, encoding) in cases {
        let fixture = fixture(case, encoding).unwrap();
        let decoder = message_decoder(encoding)
            .build_topic_decoder(&fixture.schema_name, &fixture.schema)
            .unwrap();
        let mut group = c.benchmark_group(format!("decode/{case:?}"));
        group.throughput(Throughput::Bytes(fixture.payload.len() as u64));
        group.bench_function(format!("{encoding:?}"), |b| {
            b.iter(|| decoder.decode(&fixture.payload).unwrap())
        });
        group.finish();
    }
}

fn bench_build_topic_decoder(c: &mut Criterion) {
    // `nested` has the most struct definitions to resolve.
    for encoding in [Encoding::Ros2idl, Encoding::Ros2msg, Encoding::Protobuf] {
        let fixture = fixture(PayloadCase::Nested, encoding).unwrap();
        let decoder = message_decoder(encoding);
        let mut group = c.benchmark_group(format!("build_topic_decoder/{encoding:?}"));
        group.bench_function("nested", |b| {
            b.iter(|| {
                decoder
                    .build_topic_decoder(&fixture.schema_name, &fixture.schema)
                    .unwrap()
            })
        });
        group.finish();
    }
}

criterion_group! {
    name = benches;
    config = mcapbench::criterion();
    targets = bench_decode, bench_build_topic_decoder
}
criterion_main!(benches);
