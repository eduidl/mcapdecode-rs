use std::collections::HashMap;

use mcapdecode_core::DecoderError;
use mcapdecode_ros2_common::{
    ConstDef, FieldDef, PrimitiveType, ResolvedSchema, Ros2Error, StructDef, TypeExpr,
    ensure_builtin_structs, normalize_schema_name, resolve_for_cdr, split_schema_sections,
};

#[test]
fn schema_name_normalization_expands_legacy_message_names() {
    assert_eq!(
        normalize_schema_name("geometry_msgs/Pose").unwrap(),
        "geometry_msgs/msg/Pose"
    );
    assert_eq!(
        normalize_schema_name("example_interfaces/srv/AddTwoInts").unwrap(),
        "example_interfaces/srv/AddTwoInts"
    );
    assert!(normalize_schema_name("invalid").is_err());
}

#[test]
fn schema_sections_accept_official_and_compatible_delimiters() {
    let official_delimiter = "=".repeat(80);
    let schema = format!("root\n{official_delimiter}\ndependency\n===\nleaf");

    assert_eq!(
        split_schema_sections(&schema),
        vec![vec!["root"], vec!["dependency"], vec!["leaf"]]
    );
}

#[test]
fn one_or_two_equals_are_not_schema_delimiters() {
    let schema = "root\n=\nnot_a_delimiter\n==\nstill_root\n===\ndependency";

    assert_eq!(
        split_schema_sections(schema),
        vec![
            vec!["root", "=", "not_a_delimiter", "==", "still_root"],
            vec!["dependency"],
        ]
    );
}

#[test]
fn resolve_for_cdr_maps_invalid_utf8_to_schema_parse_error() {
    let error = resolve_for_cdr("example/msg/Message", &[0xff], resolve_empty_schema)
        .expect_err("invalid UTF-8 should be rejected");

    assert!(matches!(error, DecoderError::SchemaParse(_)));
}

#[test]
fn resolve_for_cdr_delegates_to_format_specific_resolver() {
    let resolved = resolve_for_cdr("example/msg/Message", b"schema", resolve_empty_schema)
        .expect("valid schema bytes should be delegated");

    assert_eq!(resolved.root, vec!["example", "msg", "Message"]);
}

#[test]
fn builtin_interfaces_share_the_same_field_shape() {
    let mut structs = HashMap::new();
    ensure_builtin_structs(&mut structs);

    let time = builtin_key("Time");
    let duration = builtin_key("Duration");
    assert_eq!(
        structs.get(&time).expect("Time should be injected").fields,
        builtin_time_fields()
    );
    assert_eq!(
        structs
            .get(&duration)
            .expect("Duration should be injected")
            .fields,
        builtin_time_fields()
    );
}

#[test]
fn builtin_interfaces_do_not_overwrite_explicit_definitions() {
    let duration = vec![
        "builtin_interfaces".to_string(),
        "msg".to_string(),
        "Duration".to_string(),
    ];
    let explicit_duration = StructDef {
        full_name: duration.clone(),
        fields: vec![FieldDef {
            name: "custom".to_string(),
            ty: TypeExpr::Primitive(PrimitiveType::U8),
            fixed_len: None,
        }],
        consts: Vec::<ConstDef>::new(),
    };
    let mut structs = HashMap::from([(duration.clone(), explicit_duration.clone())]);

    ensure_builtin_structs(&mut structs);

    assert_eq!(structs.get(&duration), Some(&explicit_duration));
}

fn builtin_key(type_name: &str) -> Vec<String> {
    vec![
        "builtin_interfaces".to_string(),
        "msg".to_string(),
        type_name.to_string(),
    ]
}

fn builtin_time_fields() -> Vec<FieldDef> {
    vec![
        FieldDef {
            name: "sec".to_string(),
            ty: TypeExpr::Primitive(PrimitiveType::I32),
            fixed_len: None,
        },
        FieldDef {
            name: "nanosec".to_string(),
            ty: TypeExpr::Primitive(PrimitiveType::U32),
            fixed_len: None,
        },
    ]
}

fn resolve_empty_schema(_: &str, _: &str) -> Result<ResolvedSchema, Ros2Error> {
    Ok(ResolvedSchema {
        root: vec![
            "example".to_string(),
            "msg".to_string(),
            "Message".to_string(),
        ],
        structs: HashMap::new(),
        enums: HashMap::new(),
    })
}
