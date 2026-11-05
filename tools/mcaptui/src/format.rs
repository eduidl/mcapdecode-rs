use chrono::{DateTime, Utc};
use mcapdecode::{
    ChannelInfo,
    core::{FieldDefs, StructValue, Value},
};

use crate::app::DetailRow;

const BYTES_DETAIL_LEN: usize = 32;
const DETAIL_COLLECTION_ITEM_LIMIT: usize = 32;
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

pub fn format_detail_rows(log_time: u64, publish_time: u64, value: &Value) -> Vec<DetailRow> {
    let mut rows = vec![
        DetailRow::new(format!("@log_time: {log_time}"), None),
        DetailRow::new(format!("@publish_time: {publish_time}"), None),
        DetailRow::new("payload:", None),
    ];

    match value {
        Value::Struct(fields) => push_struct_fields(fields, None, 1, &mut rows),
        other => push_value_lines("value", other, Some("value"), 1, &mut rows),
    }

    rows
}

/// Detail rows that pretty-print the text held by a `jsonschema` topic's single
/// string column. Returns `None` when the value is not valid JSON text, so that
/// the caller can show it as it is.
pub fn format_json_detail_rows(
    log_time: u64,
    publish_time: u64,
    value: &Value,
) -> Option<Vec<DetailRow>> {
    let Value::Struct(fields) = value else {
        return None;
    };
    let [Value::String(text)] = &fields[..] else {
        return None;
    };
    let json: serde_json::Value = serde_json::from_str(text).ok()?;
    let pretty = serde_json::to_string_pretty(&json).ok()?;

    let mut rows = vec![
        DetailRow::new(format!("@log_time: {log_time}"), None),
        DetailRow::new(format!("@publish_time: {publish_time}"), None),
        DetailRow::new("payload:", None),
    ];
    // Line numbers keep the scroll position while stepping through messages.
    rows.extend(
        pretty
            .lines()
            .enumerate()
            .map(|(index, line)| DetailRow::new(format!("  {line}"), Some(index.to_string()))),
    );
    Some(rows)
}

pub fn format_schema_text(topic: &ChannelInfo, field_defs: &FieldDefs) -> String {
    let schema_name = topic.schema_name.as_deref().unwrap_or("-");
    format!(
        "topic: {}\nschema_name: {}\nschema_encoding: {}\nmessage_encoding: {}\n\n{}",
        topic.topic, schema_name, topic.schema_encoding, topic.message_encoding, field_defs
    )
}

pub fn format_raw_schema_text(topic: &ChannelInfo, reason: &str) -> String {
    let schema_name = topic.schema_name.as_deref().unwrap_or("-");
    format!(
        "topic: {}\nschema_name: {}\nschema_encoding: {}\nmessage_encoding: {}\n\nraw payload mode\nreason: {}\n\npayload: bytes",
        topic.topic, schema_name, topic.schema_encoding, topic.message_encoding, reason
    )
}

/// Text for `jsonschema` topics, whose decoder never reads the schema, so its
/// `json: string` field says nothing about the message structure.
pub fn format_unsupported_schema_text(topic: &ChannelInfo) -> String {
    let schema_name = topic.schema_name.as_deref().unwrap_or("-");
    format!(
        "topic: {}\nschema_name: {}\nschema_encoding: {}\nmessage_encoding: {}\n\nJSON Schema is not supported\nmessages are shown as unparsed JSON text",
        topic.topic, schema_name, topic.schema_encoding, topic.message_encoding
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

/// Push one row group per field, named from the struct's own schema.
///
/// A positional struct without field names (built from `Vec<Value>`) has no
/// fields to show. Every decoder in this workspace attaches a schema.
fn push_struct_fields(
    fields: &StructValue,
    field_path: Option<&str>,
    indent: usize,
    rows: &mut Vec<DetailRow>,
) {
    for (name, value) in fields.fields().into_iter().flatten() {
        let child_path = field_path
            .map(|path| format!("{path}.{name}"))
            .unwrap_or_else(|| name.to_owned());
        push_value_lines(name, value, Some(&child_path), indent, rows);
    }
}

fn push_value_lines(
    label: &str,
    value: &Value,
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
            push_struct_fields(fields, field_path, indent + 1, rows);
        }
        Value::List(items) | Value::Array(items) => {
            rows.push(DetailRow::new(
                format!("{pad}{label}: [{} items]", items.len()),
                field_path.map(ToOwned::to_owned),
            ));
            for (index, item) in items.iter().take(DETAIL_COLLECTION_ITEM_LIMIT).enumerate() {
                push_value_lines(&format!("[{index}]"), item, field_path, indent + 1, rows);
            }
            push_truncated_summary(
                items.len(),
                DETAIL_COLLECTION_ITEM_LIMIT,
                "items",
                indent + 1,
                rows,
            );
        }
        Value::Map(entries) => {
            rows.push(DetailRow::new(
                format!("{pad}{label}: [{} entries]", entries.len()),
                field_path.map(ToOwned::to_owned),
            ));
            for (index, (key, value)) in entries
                .iter()
                .take(DETAIL_COLLECTION_ITEM_LIMIT)
                .enumerate()
            {
                rows.push(DetailRow::new(
                    format!("{}  entry[{index}]:", pad),
                    field_path.map(ToOwned::to_owned),
                ));
                push_value_lines("key", key, field_path, indent + 2, rows);
                push_value_lines("value", value, field_path, indent + 2, rows);
            }
            push_truncated_summary(
                entries.len(),
                DETAIL_COLLECTION_ITEM_LIMIT,
                "entries",
                indent + 1,
                rows,
            );
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
        Value::Bytes(value) | Value::Uint8List(value) => format_bytes(value, BYTES_DETAIL_LEN),
        Value::Struct(_) | Value::List(_) | Value::Array(_) | Value::Map(_) => {
            "<compound>".to_string()
        }
    }
}

fn push_truncated_summary(
    total: usize,
    limit: usize,
    noun: &str,
    indent: usize,
    rows: &mut Vec<DetailRow>,
) {
    let omitted = total.saturating_sub(limit);
    if omitted == 0 {
        return;
    }

    rows.push(DetailRow::new(
        format!("{}... {omitted} more {noun} omitted", "  ".repeat(indent)),
        None,
    ));
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
