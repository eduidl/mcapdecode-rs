//! Round-trip contract for the generated fixtures.
//!
//! Every payload case is encoded from the shared [`Sample`] tree and decoded again with
//! the real decoders; the decoded values must match the sample that produced them. This
//! is what keeps the schema text and the encoded payload from drifting apart, and it is
//! also why a broken combination cannot reach the benchmarks unnoticed.

use mcapbench::{Encoding, PayloadCase, Sample, fixture, sample};
use mcapdecode_core::{MessageDecoder, Value};

fn combinations() -> Vec<(PayloadCase, Encoding)> {
    let mut out = Vec::new();
    for case in [
        PayloadCase::Flat,
        PayloadCase::Nested,
        PayloadCase::Bytes,
        PayloadCase::NumericArray,
    ] {
        for encoding in [Encoding::Ros2idl, Encoding::Ros2msg, Encoding::Protobuf] {
            out.push((case, encoding));
        }
    }
    for encoding in [Encoding::Ros2idl, Encoding::Ros2msg] {
        out.push((PayloadCase::Strings, encoding));
    }
    out
}

fn decode(case: PayloadCase, encoding: Encoding) -> Value {
    let fixture = fixture(case, encoding).unwrap();
    let decoder: Box<dyn MessageDecoder> = match encoding {
        Encoding::Ros2idl => Box::new(mcapdecode_ros2idl::Ros2IdlDecoder::new()),
        Encoding::Ros2msg => Box::new(mcapdecode_ros2msg::Ros2MsgDecoder::new()),
        Encoding::Protobuf => Box::new(mcapdecode_protobuf::ProtobufDecoder::new()),
    };
    decoder
        .build_topic_decoder(&fixture.schema_name, &fixture.schema)
        .unwrap_or_else(|e| panic!("{case:?}/{encoding:?}: schema rejected: {e}"))
        .decode(&fixture.payload)
        .unwrap_or_else(|e| panic!("{case:?}/{encoding:?}: payload rejected: {e}"))
}

/// Compare a decoded value against the sample it was generated from.
///
/// Byte sequences decode as `Bytes`; fixed-size arrays arrive as `Array` in CDR but as
/// `List` in protobuf, so both array spellings are accepted.
fn matches(value: &Value, expected: &Sample) -> bool {
    match (value, expected) {
        (Value::U64(actual), Sample::U64(want)) => actual == want,
        (Value::String(actual), Sample::Str(want)) => actual.as_ref() == want.as_str(),
        (Value::Bytes(actual), Sample::Bytes(want)) => actual.as_ref() == want.as_slice(),
        (Value::Array(items) | Value::List(items), Sample::F64List(want)) => {
            items.len() == want.len()
                && items
                    .iter()
                    .zip(want)
                    .all(|(item, v)| matches!(item, Value::F64(actual) if actual == v))
        }
        (Value::Struct(fields), Sample::Struct(want)) => {
            fields.len() == want.len()
                && fields
                    .iter()
                    .zip(want)
                    .all(|(field, expected)| matches(field, expected))
        }
        _ => false,
    }
}

#[test]
fn every_generated_combination_decodes_back_to_its_sample() {
    for (case, encoding) in combinations() {
        let value = decode(case, encoding);
        let expected = sample(case);
        assert!(
            matches(&value, &expected),
            "{case:?}/{encoding:?}: decoded value does not match the generated sample"
        );
    }
}

#[test]
fn nested_case_is_actually_nested() {
    // Guards the benchmark's intent: `nested` exists to exercise per-field work, so a
    // flattened definition would silently make it a duplicate of `flat`.
    fn depth(value: &Value) -> usize {
        match value {
            Value::Struct(fields) => 1 + fields.iter().map(depth).max().unwrap_or(0),
            _ => 0,
        }
    }
    assert!(depth(&decode(PayloadCase::Nested, Encoding::Ros2idl)) >= 4);
    assert!(depth(&decode(PayloadCase::Nested, Encoding::Ros2msg)) >= 4);
    assert!(depth(&decode(PayloadCase::Nested, Encoding::Protobuf)) >= 4);
}

#[test]
fn strings_are_not_offered_for_protobuf() {
    assert!(fixture(PayloadCase::Strings, Encoding::Protobuf).is_err());
}
