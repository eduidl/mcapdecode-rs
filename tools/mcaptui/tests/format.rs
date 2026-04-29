use std::sync::Arc;

use mcapdecode::{
    TopicInfo,
    core::{
        DataTypeDef,
        DataTypeDef::{Bytes, I64, Map, String as StringType, Struct},
        ElementDef, FieldDef, FieldDefs, Value,
    },
};
use mcaptui::format::{
    format_detail_rows, format_raw_detail_rows, format_raw_schema_text, format_schema_text,
    format_timestamp,
};

fn sample_schema() -> FieldDefs {
    vec![
        FieldDef::new("stamp", I64, false),
        FieldDef::new(
            "nested",
            Struct(
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
    Value::Struct(vec![
        Value::I64(42),
        Value::Struct(vec![
            Value::List(vec![Value::I64(1), Value::I64(2), Value::I64(3)]),
            Value::Map(vec![(
                Value::string("camera"),
                Value::Bytes(Arc::from(
                    [0_u8, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12].as_slice(),
                )),
            )]),
        ]),
    ])
}

fn sample_topic() -> TopicInfo {
    TopicInfo {
        topic: "/camera/image".to_string(),
        message_count: Some(3),
        schema_name: Some("sensor_msgs/msg/Image".to_string()),
        schema_encoding: "ros2idl".to_string(),
        message_encoding: "cdr".to_string(),
        channel_count: 1,
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
    let lines: Vec<_> = format_detail_rows(10, 20, &sample_value(), &sample_schema())
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
fn schema_formatter_includes_metadata_and_fields() {
    let rendered = format_schema_text(&sample_topic(), &sample_schema());

    assert!(rendered.contains("topic: /camera/image"));
    assert!(rendered.contains("schema_name: sensor_msgs/msg/Image"));
    assert!(rendered.contains("schema_encoding: ros2idl"));
    assert!(rendered.contains("stamp: i64"));
    assert!(rendered.contains("nested:"));
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
