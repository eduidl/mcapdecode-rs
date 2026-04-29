use mcapdecode_core::{DataTypeDef, ElementDef, FieldDef, FieldDefs, format_field_defs};

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
    field_list: optional list
        item: optional struct
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
    field_b: list
        item: i32
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
        "array[4]"
    );
}
