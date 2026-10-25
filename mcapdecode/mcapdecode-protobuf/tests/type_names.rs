#[allow(dead_code)]
mod test_helpers;

use mcapdecode_core::MessageDecoder;
use mcapdecode_protobuf::ProtobufDecoder;
use prost_types::{
    DescriptorProto,
    field_descriptor_proto::{Label, Type},
};
use test_helpers::*;

#[test]
fn named_fields_include_nested_repeated_messages_and_enums() {
    let child = DescriptorProto {
        name: Some("Child".into()),
        field: vec![
            enum_field("mode", 1, ".Mode"),
            scalar_field("x", 2, Type::Double),
        ],
        ..Default::default()
    };
    let root = DescriptorProto {
        name: Some("Root".into()),
        field: vec![
            message_field("child", 1, ".Child", Label::Optional),
            message_field("children", 2, ".Child", Label::Repeated),
            enum_field("mode", 3, ".Mode"),
        ],
        ..Default::default()
    };
    let data = build_fds_with_enums(
        "types.proto",
        vec![root, child],
        vec![simple_enum("Mode", &[("OFF", 0), ("ON", 1)])],
    );
    let decoder = ProtobufDecoder::new()
        .build_topic_decoder("Root", &data)
        .unwrap();
    let fields = decoder.field_defs();
    let names: Vec<_> = fields
        .iter()
        .map(|field| field.element.data_type.to_string())
        .collect();
    assert_eq!(names, ["Child", "Child[]", "Mode"]);
    let mcapdecode_core::DataTypeDef::List(element) = &fields[1].element.data_type else {
        panic!("expected repeated child");
    };
    assert_eq!(element.data_type.to_string(), "Child");
    assert_eq!(fields[1].element.to_string(), "Child[]");
    let mcapdecode_core::DataTypeDef::Struct(child) = &fields[0].element.data_type else {
        panic!("expected child struct");
    };
    assert_eq!(child.fields[0].element.data_type.to_string(), "Mode");
}
