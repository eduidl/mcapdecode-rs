use std::sync::Arc;

use mcapdecode::{
    ChannelInfo,
    core::{
        DataTypeDef,
        DataTypeDef::{Bytes, I64, Map, String as StringType},
        ElementDef, EnumDef, EnumVariant, FieldDef, FieldDefs, StructDef, Value,
    },
};
use mcaptui::format::{
    format_detail_rows, format_json_detail_rows, format_raw_detail_rows, format_raw_schema_text,
    format_schema_text, format_timestamp,
};

fn sample_schema() -> FieldDefs {
    vec![
        FieldDef::new("stamp", I64, false),
        FieldDef::new(
            "nested",
            DataTypeDef::structure(
                vec![
                    FieldDef::new(
                        "items",
                        DataTypeDef::List(Box::new(ElementDef::new(I64, false))),
                        false,
                    ),
                    FieldDef::new(
                        "labels",
                        Map {
                            key: Box::new(ElementDef::new(StringType, false)),
                            value: Box::new(ElementDef::new(Bytes, false)),
                        },
                        false,
                    ),
                ]
                .into(),
            ),
            false,
        ),
    ]
    .into()
}

fn sample_value() -> Value {
    let schema = sample_schema();
    let DataTypeDef::Struct(nested) = &schema[1].element.data_type else {
        panic!("expected nested struct")
    };
    let nested = Value::structure(
        Arc::clone(nested),
        vec![
            Value::List(vec![Value::I64(1), Value::I64(2), Value::I64(3)]),
            Value::Map(vec![(
                Value::string("camera"),
                Value::Bytes(Arc::from(
                    [0_u8, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12].as_slice(),
                )),
            )]),
        ],
    );
    structure(schema, vec![Value::I64(42), nested])
}

/// A root struct value that carries `fields` as its schema.
fn structure(fields: FieldDefs, values: Vec<Value>) -> Value {
    Value::structure(Arc::new(StructDef::from(fields)), values)
}

fn sample_topic() -> ChannelInfo {
    ChannelInfo {
        id: 1,
        topic: "/camera/image".to_string(),
        message_count: Some(3),
        schema_name: Some("sensor_msgs/msg/Image".to_string()),
        schema_encoding: "ros2idl".to_string(),
        message_encoding: "cdr".to_string(),
        metadata: Default::default(),
    }
}

#[test]
fn timestamp_is_formatted_readably() {
    assert_eq!(
        format_timestamp(1_714_294_096_123_456_789),
        "2024-04-28 08:48:16.123"
    );
}

#[test]
fn detail_formatter_renders_struct_list_map_and_bytes() {
    let lines: Vec<_> = format_detail_rows(10, 20, &sample_value())
        .into_iter()
        .map(|row| row.text)
        .collect();

    assert!(lines.iter().any(|line| line == "@log_time: 10"));
    assert!(lines.iter().any(|line| line == "  stamp: 42"));
    assert!(lines.iter().any(|line| line == "  nested:"));
    assert!(lines.iter().any(|line| line == "    items: [3 items]"));
    assert!(
        lines
            .iter()
            .any(|line| line.contains("labels: [1 entries]"))
    );
    assert!(lines.iter().any(|line| line.contains("hex[00 01 02 03")));
}

#[test]
fn detail_formatter_truncates_large_collections() {
    let schema: FieldDefs = vec![
        FieldDef::new(
            "items",
            DataTypeDef::List(Box::new(ElementDef::new(I64, false))),
            false,
        ),
        FieldDef::new(
            "labels",
            Map {
                key: Box::new(ElementDef::new(StringType, false)),
                value: Box::new(ElementDef::new(I64, false)),
            },
            false,
        ),
    ]
    .into();
    let value = structure(
        schema,
        vec![
            Value::List((0_i64..40).map(Value::I64).collect()),
            Value::Map(
                (0_i64..35)
                    .map(|index| (Value::string(format!("k{index}")), Value::I64(index)))
                    .collect(),
            ),
        ],
    );

    let lines: Vec<_> = format_detail_rows(10, 20, &value)
        .into_iter()
        .map(|row| row.text)
        .collect();

    assert!(lines.iter().any(|line| line == "  items: [40 items]"));
    assert!(
        lines
            .iter()
            .any(|line| line == "    ... 8 more items omitted")
    );
    assert!(lines.iter().any(|line| line == "  labels: [35 entries]"));
    assert!(
        lines
            .iter()
            .any(|line| line == "    ... 3 more entries omitted")
    );
    assert!(!lines.iter().any(|line| line.contains("entry[34]")));
}

#[test]
fn schema_formatter_includes_metadata_and_fields() {
    let rendered = format_schema_text(&sample_topic(), &sample_schema());

    assert!(rendered.contains("topic: /camera/image"));
    assert!(rendered.contains("schema_name: sensor_msgs/msg/Image"));
    assert!(rendered.contains("schema_encoding: ros2idl"));
    assert!(rendered.contains("stamp: int64"));
    assert!(rendered.contains("nested:"));
}

#[test]
fn schema_formatter_uses_qualified_names_and_nullable_suffixes() {
    let fields = vec![FieldDef::new(
        "poses",
        DataTypeDef::List(Box::new(ElementDef::new(
            DataTypeDef::structure(StructDef {
                type_name: Some("example.Pose".into()),
                fields: vec![
                    FieldDef::new("x", DataTypeDef::F64, true),
                    FieldDef::new(
                        "mode",
                        DataTypeDef::Enum(EnumDef {
                            type_name: Some("example.Mode".into()),
                            variants: vec![EnumVariant::new("OFF", 0)],
                        }),
                        false,
                    ),
                ]
                .into(),
            }),
            true,
        ))),
        true,
    )]
    .into();
    let rendered = format_schema_text(&sample_topic(), &fields);
    assert!(rendered.ends_with(
        "poses: example.Pose?[]?\n    x: float64?\n    mode: example.Mode\n        OFF = 0\n"
    ));
}

#[test]
fn raw_detail_formatter_renders_full_hex_dump() {
    let rows = format_raw_detail_rows(10, 20, &[0x00, 0x01, 0x02, 0x03, 0xaa, 0xbb, 0xcc, 0xdd]);
    let lines: Vec<_> = rows.into_iter().map(|row| row.text).collect();

    assert!(lines.iter().any(|line| line == "@log_time: 10"));
    assert!(lines.iter().any(|line| line == "payload: [8 bytes]"));
    assert!(
        lines
            .iter()
            .any(|line| line == "  0000: 00 01 02 03 aa bb cc dd")
    );
}

#[test]
fn raw_schema_formatter_includes_mode_and_reason() {
    let rendered = format_raw_schema_text(
        &sample_topic(),
        "no decoder registered for schema_encoding='', message_encoding='cdr'",
    );

    assert!(rendered.contains("raw payload mode"));
    assert!(rendered.contains("reason: no decoder registered"));
    assert!(rendered.contains("payload: bytes"));
}

#[test]
fn schema_formatter_uses_nullable_suffixes() {
    let fields = vec![
        FieldDef::new("count", DataTypeDef::U32, true),
        FieldDef::new(
            "samples",
            DataTypeDef::List(Box::new(ElementDef::new(DataTypeDef::F64, true))),
            true,
        ),
    ]
    .into();
    let rendered = format_schema_text(&sample_topic(), &fields);
    assert!(rendered.ends_with("count: uint32?\nsamples: float64?[]?\n"));
}

#[test]
fn uint8_lists_show_their_schema_type_and_use_the_bytes_preview() {
    for ty in [DataTypeDef::Uint8List, DataTypeDef::BoundedUint8List(8)] {
        let schema: FieldDefs = vec![FieldDef::new("data", ty.clone(), false)].into();
        let value = structure(schema.clone(), vec![Value::Uint8List(Arc::from([0, 255]))]);
        let bytes = structure(schema.clone(), vec![Value::Bytes(Arc::from([0, 255]))]);
        let rows = format_detail_rows(0, 0, &value);
        let old_rows = format_detail_rows(0, 0, &bytes);
        assert_eq!(
            rows.iter().map(|r| &r.text).collect::<Vec<_>>(),
            old_rows.iter().map(|r| &r.text).collect::<Vec<_>>()
        );
        assert!(
            mcapdecode::core::format_field_defs(&schema)
                .unwrap()
                .contains(&ty.to_string())
        );
    }
}

#[test]
fn json_detail_rows_are_pretty_printed_in_message_key_order() {
    let value = Value::Struct(vec![Value::string(r#"{"z": 1.5, "a": [true]}"#)].into());

    let rows = format_json_detail_rows(10, 20, &value).unwrap();
    let lines: Vec<_> = rows.iter().map(|row| row.text.as_str()).collect();

    assert_eq!(
        lines,
        [
            "@log_time: 10",
            "@publish_time: 20",
            "payload:",
            "  {",
            "    \"z\": 1.5,",
            "    \"a\": [",
            "      true",
            "    ]",
            "  }",
        ]
    );
}

#[test]
fn json_detail_rows_leave_other_values_to_the_default_format() {
    for value in [
        Value::Struct(vec![Value::string("{broken")].into()),
        Value::Struct(vec![Value::I64(1)].into()),
        Value::Struct(vec![Value::string("{}"), Value::string("{}")].into()),
        Value::string("{}"),
    ] {
        assert!(format_json_detail_rows(0, 0, &value).is_none(), "{value:?}");
    }
}

#[test]
fn json_detail_rows_keep_float_values_exact() {
    let value = Value::Struct(vec![Value::string(r#"{"wy": -0.0009321063815277333}"#)].into());

    let rows = format_json_detail_rows(0, 0, &value).unwrap();

    assert!(
        rows.iter()
            .any(|row| row.text.contains("-0.0009321063815277333")),
        "{rows:?}"
    );
}
