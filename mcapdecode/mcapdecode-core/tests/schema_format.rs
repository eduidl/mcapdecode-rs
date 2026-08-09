use mcapdecode_core::{
    DataTypeDef, ElementDef, EnumVariant, FieldDef, FieldDefs, format_field_defs,
};

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
field_root: optional struct
    field_a: optional f64
    field_b: optional struct
        field_c: optional string
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
field_root: optional struct
    field_list: optional struct?[]
        item_a: optional i32
        item_b: optional string
";
    assert_eq!(text, expected);
    Ok(())
}

#[test]
fn non_optional_fields_do_not_get_optional_prefix() -> Result<(), std::fmt::Error> {
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
    field_a: f64
    field_b: i32[]
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
samples: f64[9]
bounded_samples: i16[<=4]
labels: map<string, i32?>
";
    assert_eq!(text, expected);
    Ok(())
}

#[test]
fn enums_render_declared_wire_values() -> Result<(), std::fmt::Error> {
    let fields = vec![FieldDef::new(
        "color",
        DataTypeDef::Enum(vec![
            EnumVariant::new("UNKNOWN", 0),
            EnumVariant::new("BLUE", 7),
        ]),
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
        id: u32
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
fn element_display_prefixes_optional() {
    assert_eq!(ElementDef::new(DataTypeDef::I32, false).to_string(), "i32");
    assert_eq!(
        ElementDef::new(DataTypeDef::String, true).to_string(),
        "optional string"
    );
    assert_eq!(
        ElementDef::new(
            DataTypeDef::Array(Box::new(ElementDef::new(DataTypeDef::U8, false)), 4),
            false
        )
        .to_string(),
        "u8[4]"
    );
}
