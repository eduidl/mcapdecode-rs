use mcapdecode_core::{DataTypeDef, ElementDef, EnumDef, StructDef};

#[test]
fn named_definitions_display_original_names() {
    let named_types = [
        (
            DataTypeDef::Struct(StructDef {
                type_name: Some("example.Pose".into()),
                ..Default::default()
            }),
            "struct",
            "example.Pose",
        ),
        (
            DataTypeDef::Enum(EnumDef {
                type_name: Some("example.Mode".into()),
                ..Default::default()
            }),
            "enum",
            "example.Mode",
        ),
    ];
    for (data_type, kind, expected) in named_types {
        assert_eq!(data_type.type_name(), kind);
        assert_eq!(data_type.to_string(), expected);
    }
}

#[test]
fn collections_display_named_elements() {
    let pose = DataTypeDef::Struct(StructDef {
        type_name: Some("example.Pose".into()),
        ..Default::default()
    });
    let collections = [
        (
            DataTypeDef::List(Box::new(ElementDef::new(pose.clone(), false))),
            "list",
            "example.Pose[]",
        ),
        (
            DataTypeDef::BoundedList(Box::new(ElementDef::new(pose.clone(), false)), 4),
            "list",
            "example.Pose[<=4]",
        ),
        (
            DataTypeDef::Array(Box::new(ElementDef::new(pose.clone(), false)), 2),
            "array",
            "example.Pose[2]",
        ),
        (
            DataTypeDef::Map {
                key: Box::new(ElementDef::new(DataTypeDef::String, false)),
                value: Box::new(ElementDef::new(pose, true)),
            },
            "map",
            "map<string, example.Pose?>",
        ),
    ];
    for (data_type, kind, expected) in collections {
        assert_eq!(data_type.type_name(), kind);
        assert_eq!(data_type.to_string(), expected);
    }
}

#[test]
fn numeric_types_use_language_independent_names() {
    for (data_type, expected) in [
        (DataTypeDef::I8, "int8"),
        (DataTypeDef::I16, "int16"),
        (DataTypeDef::I32, "int32"),
        (DataTypeDef::I64, "int64"),
        (DataTypeDef::U8, "uint8"),
        (DataTypeDef::U16, "uint16"),
        (DataTypeDef::U32, "uint32"),
        (DataTypeDef::U64, "uint64"),
        (DataTypeDef::F32, "float32"),
        (DataTypeDef::F64, "float64"),
    ] {
        assert_eq!(data_type.type_name(), expected);
        assert_eq!(data_type.to_string(), expected);
    }
}

#[test]
fn anonymous_definitions_have_only_category_names() {
    for (data_type, expected) in [
        (DataTypeDef::Struct(StructDef::default()), "struct"),
        (DataTypeDef::Enum(EnumDef::default()), "enum"),
    ] {
        assert_eq!(data_type.type_name(), expected);
        assert_eq!(data_type.to_string(), expected);
    }
}
