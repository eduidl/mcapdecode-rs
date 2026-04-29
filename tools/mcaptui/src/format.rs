use chrono::{DateTime, Utc};
use mcapdecode::{
    TopicInfo,
    core::{DataTypeDef, ElementDef, FieldDefs, Value},
};

use crate::app::DetailRow;

const BYTES_DETAIL_LEN: usize = 32;
const HEX_DUMP_WIDTH: usize = 16;

pub fn format_timestamp(timestamp_ns: u64) -> String {
    match i64::try_from(timestamp_ns) {
        Ok(timestamp_ns) => {
            let datetime = DateTime::<Utc>::from_timestamp_nanos(timestamp_ns);
            datetime.format("%Y-%m-%d %H:%M:%S%.3f").to_string()
        }
        Err(_) => timestamp_ns.to_string(),
    }
}

pub fn format_detail_rows(
    log_time: u64,
    publish_time: u64,
    value: &Value,
    field_defs: &FieldDefs,
) -> Vec<DetailRow> {
    let mut rows = vec![
        DetailRow::new(format!("@log_time: {log_time}"), None),
        DetailRow::new(format!("@publish_time: {publish_time}"), None),
        DetailRow::new("payload:", None),
    ];

    match value {
        Value::Struct(fields) => {
            for (index, field_value) in fields.iter().enumerate() {
                let fallback_name = format!("field_{index}");
                if let Some(field_def) = field_defs.get(index) {
                    push_field_lines(
                        &field_def.name,
                        field_value,
                        &field_def.element,
                        Some(field_def.name.as_str()),
                        1,
                        &mut rows,
                    );
                } else {
                    push_value_lines(
                        &fallback_name,
                        field_value,
                        None,
                        Some(fallback_name.as_str()),
                        1,
                        &mut rows,
                    );
                }
            }
        }
        other => push_value_lines("value", other, None, Some("value"), 1, &mut rows),
    }

    rows
}

pub fn format_schema_text(topic: &TopicInfo, field_defs: &FieldDefs) -> String {
    let schema_name = topic.schema_name.as_deref().unwrap_or("-");
    format!(
        "topic: {}\nschema_name: {}\nschema_encoding: {}\nmessage_encoding: {}\n\n{}",
        topic.topic, schema_name, topic.schema_encoding, topic.message_encoding, field_defs
    )
}

pub fn format_raw_schema_text(topic: &TopicInfo, reason: &str) -> String {
    let schema_name = topic.schema_name.as_deref().unwrap_or("-");
    format!(
        "topic: {}\nschema_name: {}\nschema_encoding: {}\nmessage_encoding: {}\n\nraw payload mode\nreason: {}\n\npayload: bytes",
        topic.topic, schema_name, topic.schema_encoding, topic.message_encoding, reason
    )
}

pub fn format_raw_detail_rows(log_time: u64, publish_time: u64, payload: &[u8]) -> Vec<DetailRow> {
    let mut rows = vec![
        DetailRow::new(format!("@log_time: {log_time}"), None),
        DetailRow::new(format!("@publish_time: {publish_time}"), None),
        DetailRow::new(
            format!("payload: [{} bytes]", payload.len()),
            Some("payload".to_string()),
        ),
    ];

    for (offset, chunk) in payload.chunks(HEX_DUMP_WIDTH).enumerate() {
        rows.push(DetailRow::new(
            format_hex_dump_line(offset * HEX_DUMP_WIDTH, chunk),
            None,
        ));
    }

    rows
}

fn push_field_lines(
    label: &str,
    value: &Value,
    element: &ElementDef,
    field_path: Option<&str>,
    indent: usize,
    rows: &mut Vec<DetailRow>,
) {
    push_value_lines(
        label,
        value,
        Some(&element.data_type),
        field_path,
        indent,
        rows,
    );
}

fn push_value_lines(
    label: &str,
    value: &Value,
    data_type: Option<&DataTypeDef>,
    field_path: Option<&str>,
    indent: usize,
    rows: &mut Vec<DetailRow>,
) {
    let pad = "  ".repeat(indent);
    match value {
        Value::Struct(fields) => {
            rows.push(DetailRow::new(
                format!("{pad}{label}:"),
                field_path.map(ToOwned::to_owned),
            ));
            let schema_fields = match data_type {
                Some(DataTypeDef::Struct(schema_fields)) => Some(schema_fields),
                _ => None,
            };
            for (index, field_value) in fields.iter().enumerate() {
                let fallback_name = format!("field_{index}");
                if let Some(field_def) = schema_fields.and_then(|defs| defs.get(index)) {
                    let child_path = field_path
                        .map(|path| format!("{path}.{}", field_def.name))
                        .unwrap_or_else(|| field_def.name.clone());
                    push_field_lines(
                        &field_def.name,
                        field_value,
                        &field_def.element,
                        Some(&child_path),
                        indent + 1,
                        rows,
                    );
                } else {
                    let fallback_path = field_path
                        .map(|path| format!("{path}.{fallback_name}"))
                        .unwrap_or_else(|| fallback_name.clone());
                    push_value_lines(
                        &fallback_name,
                        field_value,
                        None,
                        Some(&fallback_path),
                        indent + 1,
                        rows,
                    );
                }
            }
        }
        Value::List(items) | Value::Array(items) => {
            rows.push(DetailRow::new(
                format!("{pad}{label}: [{} items]", items.len()),
                field_path.map(ToOwned::to_owned),
            ));
            let child_type = match data_type {
                Some(DataTypeDef::List(element)) | Some(DataTypeDef::Array(element, _)) => {
                    Some(&element.data_type)
                }
                _ => None,
            };
            for (index, item) in items.iter().enumerate() {
                push_value_lines(
                    &format!("[{index}]"),
                    item,
                    child_type,
                    field_path,
                    indent + 1,
                    rows,
                );
            }
        }
        Value::Map(entries) => {
            rows.push(DetailRow::new(
                format!("{pad}{label}: [{} entries]", entries.len()),
                field_path.map(ToOwned::to_owned),
            ));
            let (key_type, value_type) = match data_type {
                Some(DataTypeDef::Map { key, value }) => {
                    (Some(&key.data_type), Some(&value.data_type))
                }
                _ => (None, None),
            };
            for (index, (key, value)) in entries.iter().enumerate() {
                rows.push(DetailRow::new(
                    format!("{}  entry[{index}]:", pad),
                    field_path.map(ToOwned::to_owned),
                ));
                push_value_lines("key", key, key_type, field_path, indent + 2, rows);
                push_value_lines("value", value, value_type, field_path, indent + 2, rows);
            }
        }
        _ => rows.push(DetailRow::new(
            format!("{pad}{label}: {}", format_scalar(value)),
            field_path.map(ToOwned::to_owned),
        )),
    }
}

fn format_scalar(value: &Value) -> String {
    match value {
        Value::Null => "null".to_string(),
        Value::Bool(value) => value.to_string(),
        Value::I8(value) => value.to_string(),
        Value::I16(value) => value.to_string(),
        Value::I32(value) => value.to_string(),
        Value::I64(value) => value.to_string(),
        Value::U8(value) => value.to_string(),
        Value::U16(value) => value.to_string(),
        Value::U32(value) => value.to_string(),
        Value::U64(value) => value.to_string(),
        Value::F32(value) => value.to_string(),
        Value::F64(value) => value.to_string(),
        Value::String(value) => format!("{value:?}"),
        Value::Bytes(value) => format_bytes(value, BYTES_DETAIL_LEN),
        Value::Struct(_) | Value::List(_) | Value::Array(_) | Value::Map(_) => {
            "<compound>".to_string()
        }
    }
}

fn format_bytes(bytes: &[u8], limit: usize) -> String {
    let mut rendered = String::from("hex[");
    for (index, byte) in bytes.iter().take(limit).enumerate() {
        if index > 0 {
            rendered.push(' ');
        }
        rendered.push_str(&format!("{byte:02x}"));
    }
    if bytes.len() > limit {
        if limit > 0 {
            rendered.push(' ');
        }
        rendered.push_str("...");
    }
    rendered.push(']');
    rendered
}

fn format_hex_dump_line(offset: usize, chunk: &[u8]) -> String {
    let hex = chunk
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<Vec<_>>()
        .join(" ");
    format!("  {offset:04x}: {hex}")
}
