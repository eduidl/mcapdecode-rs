use std::collections::HashMap;

use mcapdecode_core::DecoderError;
use mcapdecode_ros2_common::{
    ResolvedField, ResolvedSchema, ResolvedStruct, ResolvedType, Ros2Error, build_cdr_topic_decoder,
};

/// A struct `ex::msg::Node` whose only field is another `Node`.
fn recursive_schema(_name: &str, _text: &str) -> Result<ResolvedSchema, Ros2Error> {
    let root = vec!["ex".to_string(), "msg".to_string(), "Node".to_string()];
    let node = ResolvedStruct {
        fields: vec![ResolvedField {
            name: "child".into(),
            ty: ResolvedType::Struct(root.clone()),
            fixed_len: None,
        }],
    };
    Ok(ResolvedSchema {
        structs: HashMap::from([(root.clone(), node)]),
        root,
        enums: HashMap::new(),
    })
}

#[test]
fn recursive_struct_is_a_schema_error() {
    let Err(err) = build_cdr_topic_decoder("ex/msg/Node", b"", recursive_schema) else {
        panic!("expected an error")
    };
    assert!(
        matches!(err, DecoderError::SchemaDerivation(ref m) if m.contains("recursive struct")),
        "{err}"
    );
}
