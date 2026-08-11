mod test_helpers;

use mcapdecode_core::{DecoderError, Value};
use mcapdecode_protobuf::{
    PresencePolicy, decode_protobuf_to_value, decode_protobuf_to_value_with_policy,
};
use prost::Message;
use prost_reflect::{DescriptorPool, DynamicMessage};
use prost_types::{
    DescriptorProto,
    field_descriptor_proto::{Label, Type},
};
use test_helpers::*;

/// Encode a `DynamicMessage` to wire-format bytes.
fn encode_dynamic(msg: &DynamicMessage) -> Vec<u8> {
    msg.encode_to_vec()
}

/// Build a `DescriptorPool` from FDS bytes and get a message descriptor.
fn pool_and_desc(fds: &[u8], name: &str) -> (DescriptorPool, prost_reflect::MessageDescriptor) {
    let pool = DescriptorPool::decode(fds).unwrap();
    let desc = pool.get_message_by_name(name).unwrap();
    (pool, desc)
}

#[test]
fn decode_scalar_fields() {
    let msg = DescriptorProto {
        name: Some("Scalars".to_string()),
        field: vec![
            scalar_field("f_double", 1, Type::Double),
            scalar_field("f_float", 2, Type::Float),
            scalar_field("f_int32", 3, Type::Int32),
            scalar_field("f_int64", 4, Type::Int64),
            scalar_field("f_uint32", 5, Type::Uint32),
            scalar_field("f_uint64", 6, Type::Uint64),
            scalar_field("f_bool", 7, Type::Bool),
            scalar_field("f_string", 8, Type::String),
            scalar_field("f_bytes", 9, Type::Bytes),
        ],
        ..Default::default()
    };
    let fds = build_fds("scalars.proto", vec![msg]);
    let (_pool, desc) = pool_and_desc(&fds, "Scalars");

    let mut dm = DynamicMessage::new(desc);
    dm.set_field_by_name("f_double", prost_reflect::Value::F64(3.2));
    dm.set_field_by_name("f_float", prost_reflect::Value::F32(2.5));
    dm.set_field_by_name("f_int32", prost_reflect::Value::I32(-42));
    dm.set_field_by_name("f_int64", prost_reflect::Value::I64(-100));
    dm.set_field_by_name("f_uint32", prost_reflect::Value::U32(42));
    dm.set_field_by_name("f_uint64", prost_reflect::Value::U64(100));
    dm.set_field_by_name("f_bool", prost_reflect::Value::Bool(true));
    dm.set_field_by_name(
        "f_string",
        prost_reflect::Value::String("hello".to_string()),
    );
    dm.set_field_by_name(
        "f_bytes",
        prost_reflect::Value::Bytes(bytes::Bytes::from_static(b"\x01\x02\x03")),
    );

    let wire = encode_dynamic(&dm);
    let value = decode_protobuf_to_value("Scalars", &fds, &wire).unwrap();

    let Value::Struct(fields) = value else {
        panic!("expected Struct, got {value:?}");
    };
    assert_eq!(fields.len(), 9);

    assert!(matches!(fields[0], Value::F64(v) if (v - 3.2).abs() < 1e-10));
    assert!(matches!(fields[1], Value::F32(v) if (v - 2.5).abs() < 1e-6));
    assert!(matches!(fields[2], Value::I32(-42)));
    assert!(matches!(fields[3], Value::I64(-100)));
    assert!(matches!(fields[4], Value::U32(42)));
    assert!(matches!(fields[5], Value::U64(100)));
    assert!(matches!(fields[6], Value::Bool(true)));
    match &fields[7] {
        Value::String(s) => assert_eq!(&**s, "hello"),
        other => panic!("expected String, got {other:?}"),
    }
    match &fields[8] {
        Value::Bytes(b) => assert_eq!(&**b, &[1, 2, 3]),
        other => panic!("expected Bytes, got {other:?}"),
    }
}

#[test]
fn decode_default_values() {
    // Proto3 default values: all fields default to zero/empty.
    let msg = DescriptorProto {
        name: Some("Defaults".to_string()),
        field: vec![
            scalar_field("n", 1, Type::Int32),
            scalar_field("s", 2, Type::String),
            scalar_field("b", 3, Type::Bool),
        ],
        ..Default::default()
    };
    let fds = build_fds("defaults.proto", vec![msg]);

    // Empty message (no fields set) → all defaults.
    let value = decode_protobuf_to_value("Defaults", &fds, &[]).unwrap();
    let Value::Struct(fields) = value else {
        panic!("expected Struct");
    };
    assert!(matches!(fields[0], Value::I32(0)));
    match &fields[1] {
        Value::String(s) => assert_eq!(&**s, ""),
        other => panic!("expected String, got {other:?}"),
    }
    assert!(matches!(fields[2], Value::Bool(false)));
}

#[test]
fn decode_nested_message() {
    let inner = DescriptorProto {
        name: Some("Inner".to_string()),
        field: vec![scalar_field("x", 1, Type::Int32)],
        ..Default::default()
    };
    let outer = DescriptorProto {
        name: Some("Outer".to_string()),
        field: vec![message_field("inner", 1, ".Inner", Label::Optional)],
        ..Default::default()
    };
    let fds = build_fds("nested.proto", vec![inner, outer]);
    let (_pool, outer_desc) = pool_and_desc(&fds, "Outer");
    let inner_desc = _pool.get_message_by_name("Inner").unwrap();

    let mut inner_dm = DynamicMessage::new(inner_desc);
    inner_dm.set_field_by_name("x", prost_reflect::Value::I32(99));

    let mut outer_dm = DynamicMessage::new(outer_desc);
    outer_dm.set_field_by_name("inner", prost_reflect::Value::Message(inner_dm));

    let wire = encode_dynamic(&outer_dm);
    let value = decode_protobuf_to_value("Outer", &fds, &wire).unwrap();

    let Value::Struct(outer_fields) = value else {
        panic!("expected Struct");
    };
    let Value::Struct(inner_fields) = &outer_fields[0] else {
        panic!("expected nested Struct");
    };
    assert!(matches!(inner_fields[0], Value::I32(99)));
}

#[test]
fn decode_repeated_field() {
    let msg = DescriptorProto {
        name: Some("WithList".to_string()),
        field: vec![repeated_field("values", 1, Type::Int32)],
        ..Default::default()
    };
    let fds = build_fds("list.proto", vec![msg]);
    let (_pool, desc) = pool_and_desc(&fds, "WithList");

    let mut dm = DynamicMessage::new(desc);
    dm.set_field_by_name(
        "values",
        prost_reflect::Value::List(vec![
            prost_reflect::Value::I32(10),
            prost_reflect::Value::I32(20),
            prost_reflect::Value::I32(30),
        ]),
    );

    let wire = encode_dynamic(&dm);
    let value = decode_protobuf_to_value("WithList", &fds, &wire).unwrap();

    let Value::Struct(fields) = value else {
        panic!("expected Struct");
    };
    let Value::List(items) = &fields[0] else {
        panic!("expected List");
    };
    assert_eq!(items.len(), 3);
    assert!(matches!(items[0], Value::I32(10)));
    assert!(matches!(items[1], Value::I32(20)));
    assert!(matches!(items[2], Value::I32(30)));
}

#[test]
fn decode_enum_field() {
    let color_enum = simple_enum("Color", &[("RED", 0), ("GREEN", 7), ("BLUE", 42)]);
    let msg = DescriptorProto {
        name: Some("WithEnum".to_string()),
        field: vec![enum_field("color", 1, ".Color")],
        ..Default::default()
    };
    let fds = build_fds_with_enums("enum.proto", vec![msg], vec![color_enum]);
    let (_pool, desc) = pool_and_desc(&fds, "WithEnum");

    let mut dm = DynamicMessage::new(desc);
    dm.set_field_by_name("color", prost_reflect::Value::EnumNumber(42));

    let wire = encode_dynamic(&dm);
    let value = decode_protobuf_to_value("WithEnum", &fds, &wire).unwrap();

    let Value::Struct(fields) = value else {
        panic!("expected Struct");
    };
    match &fields[0] {
        Value::String(s) => assert_eq!(&**s, "BLUE"),
        other => panic!("expected String, got {other:?}"),
    }
}

#[test]
fn decode_unknown_enum_value_falls_back_to_number() {
    let color_enum = simple_enum("Color", &[("RED", 0), ("GREEN", 1)]);
    let msg = DescriptorProto {
        name: Some("WithEnum".to_string()),
        field: vec![enum_field("color", 1, ".Color")],
        ..Default::default()
    };
    let fds = build_fds_with_enums("enum.proto", vec![msg], vec![color_enum]);
    let (_pool, desc) = pool_and_desc(&fds, "WithEnum");

    let mut dm = DynamicMessage::new(desc);
    // Use an enum number that has no defined name.
    dm.set_field_by_name("color", prost_reflect::Value::EnumNumber(999));

    let wire = encode_dynamic(&dm);
    let value = decode_protobuf_to_value("WithEnum", &fds, &wire).unwrap();

    let Value::Struct(fields) = value else {
        panic!("expected Struct");
    };
    match &fields[0] {
        Value::String(s) => assert_eq!(&**s, "999"),
        other => panic!("expected String, got {other:?}"),
    }
}

#[test]
fn decode_map_field() {
    let entry = map_entry_message("LabelsEntry", Type::String, Type::Int32);
    let msg = DescriptorProto {
        name: Some("WithMap".to_string()),
        field: vec![message_field(
            "labels",
            1,
            ".WithMap.LabelsEntry",
            Label::Repeated,
        )],
        nested_type: vec![entry],
        ..Default::default()
    };
    let fds = build_fds("map.proto", vec![msg]);
    let (_pool, desc) = pool_and_desc(&fds, "WithMap");

    let mut dm = DynamicMessage::new(desc);
    let map_val = prost_reflect::Value::Map(
        vec![
            (
                prost_reflect::MapKey::String("a".to_string()),
                prost_reflect::Value::I32(1),
            ),
            (
                prost_reflect::MapKey::String("b".to_string()),
                prost_reflect::Value::I32(2),
            ),
        ]
        .into_iter()
        .collect(),
    );
    dm.set_field_by_name("labels", map_val);

    let wire = encode_dynamic(&dm);
    let value = decode_protobuf_to_value("WithMap", &fds, &wire).unwrap();

    let Value::Struct(fields) = value else {
        panic!("expected Struct");
    };
    let Value::Map(entries) = &fields[0] else {
        panic!("expected Map, got {:?}", fields[0]);
    };
    assert_eq!(entries.len(), 2);

    // Map ordering is not guaranteed, so collect into a sortable form.
    let mut kv: Vec<(String, i32)> = entries
        .iter()
        .map(|(k, v)| {
            let key = match k {
                Value::String(s) => s.to_string(),
                _ => panic!("expected string key"),
            };
            let val = match v {
                Value::I32(n) => *n,
                _ => panic!("expected i32 value"),
            };
            (key, val)
        })
        .collect();
    kv.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(kv, vec![("a".to_string(), 1), ("b".to_string(), 2)]);
}

#[test]
fn decode_unknown_message_returns_error() {
    let msg = DescriptorProto {
        name: Some("Exists".to_string()),
        field: vec![scalar_field("x", 1, Type::Int32)],
        ..Default::default()
    };
    let fds = build_fds("test.proto", vec![msg]);
    let err = decode_protobuf_to_value("NoSuchMessage", &fds, &[]).unwrap_err();
    assert!(matches!(err, DecoderError::SchemaDerivation(_)));
    assert_eq!(
        err.to_string(),
        "schema derivation failed: message descriptor not found"
    );
}

#[test]
fn decode_invalid_schema_data_returns_error() {
    let err = decode_protobuf_to_value("Foo", &[0xff, 0xff], &[]).unwrap_err();
    assert!(matches!(err, DecoderError::SchemaParse(_)));
}

#[test]
fn decode_proto3_optional_missing_is_null_by_default() {
    let msg = DescriptorProto {
        name: Some("WithOptional".to_string()),
        field: vec![proto3_optional_scalar_field("count", 1, Type::Int32, 0)],
        oneof_decl: vec![synthetic_oneof("_count")],
        ..Default::default()
    };
    let fds = build_fds("optional.proto", vec![msg]);

    let value = decode_protobuf_to_value("WithOptional", &fds, &[]).unwrap();
    let Value::Struct(fields) = value else {
        panic!("expected Struct");
    };
    assert!(matches!(fields[0], Value::Null));
}

#[test]
fn decode_proto3_optional_missing_is_default_in_legacy_policy() {
    let msg = DescriptorProto {
        name: Some("WithOptional".to_string()),
        field: vec![proto3_optional_scalar_field("count", 1, Type::Int32, 0)],
        oneof_decl: vec![synthetic_oneof("_count")],
        ..Default::default()
    };
    let fds = build_fds("optional.proto", vec![msg]);

    let value = decode_protobuf_to_value_with_policy(
        "WithOptional",
        &fds,
        &[],
        PresencePolicy::AlwaysDefault,
    )
    .unwrap();
    let Value::Struct(fields) = value else {
        panic!("expected Struct");
    };
    assert!(matches!(fields[0], Value::I32(0)));
}
