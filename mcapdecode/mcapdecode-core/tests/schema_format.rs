use mcapdecode_core::{
    DataTypeDef, ElementDef, EnumDef, EnumVariant, FieldDef, FieldDefs, StructDef,
    format_field_defs,
};

#[test]
fn named_types_render_qualified_names_in_collections() -> Result<(), std::fmt::Error> {
    let pose = DataTypeDef::Struct(StructDef {
        type_name: Some("example.Pose".into()),
        fields: vec![FieldDef::new(
            "mode",
            DataTypeDef::Enum(EnumDef {
                type_name: Some("example.Mode".into()),
                variants: vec![EnumVariant::new("OFF", 0)],
            }),
            false,
        )]
        .into(),
    });
    let fields = vec![
        FieldDef::new("pose", pose.clone(), true),
        FieldDef::new(
            "poses",
            DataTypeDef::BoundedList(
                Box::new(ElementDef::new(
                    DataTypeDef::Array(Box::new(ElementDef::new(pose.clone(), true)), 2),
                    false,
                )),
                4,
            ),
            false,
        ),
        FieldDef::new(
            "lookup",
            DataTypeDef::Map {
                key: Box::new(ElementDef::new(DataTypeDef::String, false)),
                value: Box::new(ElementDef::new(
                    DataTypeDef::List(Box::new(ElementDef::new(pose, false))),
                    false,
                )),
            },
            false,
        ),
    ];

    assert_eq!(
        format_field_defs(&fields)?,
        "\
pose: example.Pose?
    mode: example.Mode
        OFF = 0
poses: example.Pose?[2][<=4]
    mode: example.Mode
        OFF = 0
lookup: map<string, example.Pose[]>
    mode: example.Mode
        OFF = 0
"
    );
    Ok(())
}

#[test]
fn nested_struct_keeps_compact_type_labels_and_indentation() -> Result<(), std::fmt::Error> {
    let fields = vec![FieldDef::new(
        "field_root",
        DataTypeDef::Struct(
            vec![
                FieldDef::new("field_a", DataTypeDef::F64, true),
                FieldDef::new(
                    "field_b",
                    DataTypeDef::Struct(
                        vec![FieldDef::new("field_c", DataTypeDef::String, true)].into(),
                    ),
                    true,
                ),
            ]
            .into(),
        ),
        true,
    )];

    let text = format_field_defs(&fields)?;
    let expected = "\
field_root: struct?
    field_a: float64?
    field_b: struct?
        field_c: string?
";
    assert_eq!(text, expected);
    Ok(())
}

#[test]
fn list_of_complex_item_is_rendered_as_block() -> Result<(), std::fmt::Error> {
    let fields = vec![FieldDef::new(
        "field_root",
        DataTypeDef::Struct(
            vec![FieldDef::new(
                "field_list",
                DataTypeDef::List(Box::new(ElementDef::new(
                    DataTypeDef::Struct(
                        vec![
                            FieldDef::new("item_a", DataTypeDef::I32, true),
                            FieldDef::new("item_b", DataTypeDef::String, true),
                        ]
                        .into(),
                    ),
                    true,
                ))),
                true,
            )]
            .into(),
        ),
        true,
    )];

    let text = format_field_defs(&fields)?;
    let expected = "\
field_root: struct?
    field_list: struct?[]?
        item_a: int32?
        item_b: string?
";
    assert_eq!(text, expected);
    Ok(())
}

#[test]
fn non_nullable_fields_do_not_get_nullable_suffix() -> Result<(), std::fmt::Error> {
    let fields = vec![FieldDef::new(
        "field_root",
        DataTypeDef::Struct(
            vec![
                FieldDef::new("field_a", DataTypeDef::F64, false),
                FieldDef::new(
                    "field_b",
                    DataTypeDef::List(Box::new(ElementDef::new(DataTypeDef::I32, false))),
                    false,
                ),
            ]
            .into(),
        ),
        false,
    )];

    let text = format_field_defs(&fields)?;
    let expected = "\
field_root: struct
    field_a: float64
    field_b: int32[]
";
    assert_eq!(text, expected);
    Ok(())
}

#[test]
fn arrays_and_maps_render_complete_types_on_the_parent_line() -> Result<(), std::fmt::Error> {
    let fields = vec![
        FieldDef::new(
            "samples",
            DataTypeDef::Array(Box::new(ElementDef::new(DataTypeDef::F64, false)), 9),
            false,
        ),
        FieldDef::new(
            "bounded_samples",
            DataTypeDef::BoundedList(Box::new(ElementDef::new(DataTypeDef::I16, false)), 4),
            false,
        ),
        FieldDef::new(
            "labels",
            DataTypeDef::Map {
                key: Box::new(ElementDef::new(DataTypeDef::String, false)),
                value: Box::new(ElementDef::new(DataTypeDef::I32, true)),
            },
            false,
        ),
    ];

    let text = format_field_defs(&fields)?;
    let expected = "\
samples: float64[9]
bounded_samples: int16[<=4]
labels: map<string, int32?>
";
    assert_eq!(text, expected);
    Ok(())
}

#[test]
fn enums_render_declared_wire_values() -> Result<(), std::fmt::Error> {
    let fields = vec![FieldDef::new(
        "color",
        DataTypeDef::Enum(vec![EnumVariant::new("UNKNOWN", 0), EnumVariant::new("BLUE", 7)].into()),
        false,
    )];

    let text = format_field_defs(&fields)?;
    assert_eq!(text, "color: enum\n    UNKNOWN = 0\n    BLUE = 7\n");
    Ok(())
}

#[test]
fn maps_expand_struct_bodies_without_item_labels() -> Result<(), std::fmt::Error> {
    let struct_key = DataTypeDef::Struct(vec![FieldDef::new("id", DataTypeDef::U32, false)].into());
    let struct_value =
        DataTypeDef::Struct(vec![FieldDef::new("enabled", DataTypeDef::Bool, false)].into());
    let fields = vec![
        FieldDef::new(
            "labels",
            DataTypeDef::Map {
                key: Box::new(ElementDef::new(DataTypeDef::String, false)),
                value: Box::new(ElementDef::new(struct_value.clone(), false)),
            },
            false,
        ),
        FieldDef::new(
            "index",
            DataTypeDef::Map {
                key: Box::new(ElementDef::new(struct_key, false)),
                value: Box::new(ElementDef::new(struct_value, false)),
            },
            false,
        ),
    ];

    let text = format_field_defs(&fields)?;
    let expected = "\
labels: map<string, struct>
    enabled: bool
index: map<struct, struct>
    @key: struct
        id: uint32
    @value: struct
        enabled: bool
";
    assert_eq!(text, expected);
    Ok(())
}

#[test]
fn field_defs_display_matches_formatter() -> Result<(), std::fmt::Error> {
    let fields: FieldDefs = vec![FieldDef::new("field_a", DataTypeDef::I32, false)].into();
    assert_eq!(fields.to_string(), format_field_defs(fields.as_slice())?);
    Ok(())
}

#[test]
fn element_display_suffixes_nullable() {
    assert_eq!(
        ElementDef::new(DataTypeDef::I32, false).to_string(),
        "int32"
    );
    assert_eq!(
        ElementDef::new(DataTypeDef::String, true).to_string(),
        "string?"
    );
    assert_eq!(
        ElementDef::new(
            DataTypeDef::Array(Box::new(ElementDef::new(DataTypeDef::U8, false)), 4),
            false
        )
        .to_string(),
        "uint8[4]"
    );
}

#[test]
fn primitive_type_names_are_language_neutral() {
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
fn nullable_elements_and_collections_are_distinct() {
    for (element_nullable, list_nullable, expected) in [
        (false, false, "uint8[]"),
        (true, false, "uint8?[]"),
        (false, true, "uint8[]?"),
        (true, true, "uint8?[]?"),
    ] {
        let element = ElementDef::new(
            DataTypeDef::List(Box::new(ElementDef::new(DataTypeDef::U8, element_nullable))),
            list_nullable,
        );
        assert_eq!(element.to_string(), expected);
    }
    let map = ElementDef::new(
        DataTypeDef::Map {
            key: Box::new(ElementDef::new(DataTypeDef::String, true)),
            value: Box::new(ElementDef::new(
                DataTypeDef::Array(Box::new(ElementDef::new(DataTypeDef::U8, true)), 2),
                true,
            )),
        },
        true,
    );
    assert_eq!(map.to_string(), "map<string?, uint8?[2]?>?");
}

#[test]
fn bounded_scalar_types_keep_their_bounds() {
    for (data_type, expected) in [
        (DataTypeDef::BoundedString(8), "string[<=8]"),
        (DataTypeDef::BoundedWString(8), "wstring[<=8]"),
        (DataTypeDef::BoundedBytes(8), "bytes[<=8]"),
    ] {
        assert_eq!(
            ElementDef::new(data_type, true).to_string(),
            format!("{expected}?")
        );
    }
}
