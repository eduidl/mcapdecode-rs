mod test_helpers;

use mcapdecode_core::{DataTypeDef, DecoderError, ElementDef, EnumVariant, FieldDef};
use mcapdecode_protobuf::{PresencePolicy, message_fields_to_field_defs, parse_message_descriptor};
use prost_types::{
    DescriptorProto,
    field_descriptor_proto::{Label, Type},
};
use test_helpers::*;

fn derive_schema(
    schema_name: &str,
    schema_data: &[u8],
    policy: PresencePolicy,
) -> Result<mcapdecode_core::FieldDefs, DecoderError> {
    let desc = parse_message_descriptor(schema_name, schema_data)?;
    message_fields_to_field_defs(schema_name, &desc, policy)
}

#[test]
fn scalar_fields() {
    let msg = DescriptorProto {
        name: Some("Scalars".to_string()),
        field: vec![
            scalar_field("f_double", 1, Type::Double),
            scalar_field("f_float", 2, Type::Float),
            scalar_field("f_int32", 3, Type::Int32),
            scalar_field("f_int64", 4, Type::Int64),
            scalar_field("f_uint32", 5, Type::Uint32),
            scalar_field("f_uint64", 6, Type::Uint64),
            scalar_field("f_sint32", 7, Type::Sint32),
            scalar_field("f_sint64", 8, Type::Sint64),
            scalar_field("f_fixed32", 9, Type::Fixed32),
            scalar_field("f_fixed64", 10, Type::Fixed64),
            scalar_field("f_sfixed32", 11, Type::Sfixed32),
            scalar_field("f_sfixed64", 12, Type::Sfixed64),
            scalar_field("f_bool", 13, Type::Bool),
            scalar_field("f_string", 14, Type::String),
            scalar_field("f_bytes", 15, Type::Bytes),
        ],
        ..Default::default()
    };
    let fds = build_fds("scalars.proto", vec![msg]);
    let schema = derive_schema("Scalars", &fds, PresencePolicy::PresenceAware).unwrap();

    let expected = vec![
        ("f_double", DataTypeDef::F64),
        ("f_float", DataTypeDef::F32),
        ("f_int32", DataTypeDef::I32),
        ("f_int64", DataTypeDef::I64),
        ("f_uint32", DataTypeDef::U32),
        ("f_uint64", DataTypeDef::U64),
        ("f_sint32", DataTypeDef::I32),
        ("f_sint64", DataTypeDef::I64),
        ("f_fixed32", DataTypeDef::U32),
        ("f_fixed64", DataTypeDef::U64),
        ("f_sfixed32", DataTypeDef::I32),
        ("f_sfixed64", DataTypeDef::I64),
        ("f_bool", DataTypeDef::Bool),
        ("f_string", DataTypeDef::String),
        ("f_bytes", DataTypeDef::Bytes),
    ];

    assert_eq!(schema.len(), expected.len());
    for (field, (name, dt)) in schema.iter().zip(expected.iter()) {
        assert_eq!(field.name, *name);
        assert_eq!(field.element.data_type, *dt);
        assert!(!field.element.nullable);
    }
}

#[test]
fn repeated_field_becomes_list() {
    let msg = DescriptorProto {
        name: Some("WithList".to_string()),
        field: vec![repeated_field("values", 1, Type::Int32)],
        ..Default::default()
    };
    let fds = build_fds("list.proto", vec![msg]);
    let schema = derive_schema("WithList", &fds, PresencePolicy::PresenceAware).unwrap();

    assert_eq!(schema.len(), 1);
    let field = &schema[0];
    assert_eq!(field.name, "values");
    assert_eq!(
        field.element.data_type,
        DataTypeDef::List(Box::new(ElementDef::new(DataTypeDef::I32, false)))
    );
}

#[test]
fn nested_message_becomes_struct() {
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
    let schema = derive_schema("Outer", &fds, PresencePolicy::PresenceAware).unwrap();

    assert_eq!(schema.len(), 1);
    let field = &schema[0];
    assert_eq!(field.name, "inner");
    assert_eq!(
        field.element.data_type,
        DataTypeDef::Struct(vec![FieldDef::new("x", DataTypeDef::I32, false)].into())
    );
    assert!(field.element.nullable);
}

#[test]
fn enum_field_preserves_values_and_displays_as_enum() {
    let color_enum = simple_enum("Color", &[("RED", 0), ("GREEN", 7), ("BLUE", 42)]);
    let msg = DescriptorProto {
        name: Some("WithEnum".to_string()),
        field: vec![enum_field("color", 1, ".Color")],
        ..Default::default()
    };
    let fds = build_fds_with_enums("enum.proto", vec![msg], vec![color_enum]);
    let schema = derive_schema("WithEnum", &fds, PresencePolicy::PresenceAware).unwrap();

    assert_eq!(schema.len(), 1);
    assert_eq!(schema[0].name, "color");
    let data_type = &schema[0].element.data_type;
    assert_eq!(
        data_type,
        &DataTypeDef::Enum(vec![
            EnumVariant::new("RED", 0),
            EnumVariant::new("GREEN", 7),
            EnumVariant::new("BLUE", 42),
        ])
    );
    assert_eq!(data_type.to_string(), "enum");
}

#[test]
fn map_field_becomes_map_type() {
    let entry = map_entry_message("LabelsEntry", Type::String, Type::String);
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
    let schema = derive_schema("WithMap", &fds, PresencePolicy::PresenceAware).unwrap();

    assert_eq!(schema.len(), 1);
    let field = &schema[0];
    assert_eq!(field.name, "labels");
    assert_eq!(
        field.element.data_type,
        DataTypeDef::Map {
            key: Box::new(ElementDef::new(DataTypeDef::String, false)),
            value: Box::new(ElementDef::new(DataTypeDef::String, false)),
        }
    );
}

#[test]
fn unknown_message_name_returns_error() {
    let msg = DescriptorProto {
        name: Some("Exists".to_string()),
        field: vec![scalar_field("x", 1, Type::Int32)],
        ..Default::default()
    };
    let fds = build_fds("test.proto", vec![msg]);
    let err = derive_schema("DoesNotExist", &fds, PresencePolicy::PresenceAware).unwrap_err();
    assert!(matches!(err, DecoderError::SchemaInvalid { .. }));
    assert!(err.to_string().contains("DoesNotExist"));
}

#[test]
fn invalid_schema_data_returns_error() {
    let err = derive_schema("Foo", &[0xff, 0xff, 0xff], PresencePolicy::PresenceAware).unwrap_err();
    assert!(matches!(err, DecoderError::SchemaParse { .. }));
}

#[test]
fn proto3_optional_is_nullable_in_presence_aware_policy() {
    let msg = DescriptorProto {
        name: Some("WithOptional".to_string()),
        field: vec![proto3_optional_scalar_field("count", 1, Type::Int32, 0)],
        oneof_decl: vec![synthetic_oneof("_count")],
        ..Default::default()
    };
    let fds = build_fds("optional.proto", vec![msg]);

    let schema = derive_schema("WithOptional", &fds, PresencePolicy::PresenceAware).unwrap();
    assert_eq!(schema.len(), 1);
    assert_eq!(schema[0].name, "count");
    assert!(schema[0].element.nullable);
}

#[test]
fn legacy_policy_fields_are_not_nullable() {
    let msg = DescriptorProto {
        name: Some("Scalars".to_string()),
        field: vec![scalar_field("x", 1, Type::Int32)],
        ..Default::default()
    };
    let fds = build_fds("legacy.proto", vec![msg]);

    let schema = derive_schema("Scalars", &fds, PresencePolicy::AlwaysDefault).unwrap();
    assert_eq!(schema.len(), 1);
    assert!(!schema[0].element.nullable);
}
