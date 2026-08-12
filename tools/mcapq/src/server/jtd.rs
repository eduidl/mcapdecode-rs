use std::collections::BTreeMap;

use mcapdecode::{
    MetadataColumns, MetadataTimestampFormat,
    core::{DataTypeDef, ElementDef, FieldDefs},
};
use serde_json::{Map, Value, json};

/// The metadata column policy every schema is described against.
///
/// `timestamp_jtd` hard-codes the JTD form of the timestamp format chosen here,
/// so the two must change together.
fn metadata_columns() -> MetadataColumns {
    MetadataColumns::default().with_timestamp_format(MetadataTimestampFormat::UnixNanoseconds)
}

pub fn jtd_schema(
    fields: &FieldDefs,
    title: &str,
    schema_encoding: &str,
    message_encoding: &str,
) -> Result<Value, String> {
    let metadata_names = metadata_columns().names();
    let collisions = fields
        .iter()
        .filter(|field| metadata_names.iter().any(|name| name == &field.name))
        .map(|field| field.name.as_str())
        .collect::<Vec<_>>();
    if !collisions.is_empty() {
        return Err(format!(
            "payload fields collide with reserved metadata fields: {}",
            collisions.join(", ")
        ));
    }
    let mut properties = Map::new();
    for name in metadata_names {
        properties.insert(name, timestamp_jtd());
    }
    properties.extend(fields_jtd(fields));
    Ok(json!({
        "properties": properties,
        "metadata": {
            "title": title,
            "schema_encoding": schema_encoding,
            "message_encoding": message_encoding,
        },
    }))
}

fn timestamp_jtd() -> Value {
    with_metadata(
        json!({"type": "int64"}),
        [
            ("x-mcap-clock", json!("unix")),
            ("x-mcap-unit", json!("nanoseconds")),
        ],
    )
}

fn fields_jtd(fields: &FieldDefs) -> Map<String, Value> {
    fields
        .iter()
        .map(|field| (field.name.clone(), element_jtd(&field.element)))
        .collect()
}

fn element_jtd(element: &ElementDef) -> Value {
    let mut schema = data_type_jtd(&element.data_type);
    if element.nullable {
        schema
            .as_object_mut()
            .expect("JTD schema is always an object")
            .insert("nullable".to_string(), Value::Bool(true));
    }
    schema
}

fn data_type_jtd(data_type: &DataTypeDef) -> Value {
    match data_type {
        DataTypeDef::Null => with_metadata(json!({}), [("x-mcap-null-only", json!(true))]),
        DataTypeDef::Bool => json!({"type": "boolean"}),
        DataTypeDef::I8 => json!({"type": "int8"}),
        DataTypeDef::I16 => json!({"type": "int16"}),
        DataTypeDef::I32 => json!({"type": "int32"}),
        DataTypeDef::I64 => json!({"type": "int64"}),
        DataTypeDef::U8 => json!({"type": "uint8"}),
        DataTypeDef::U16 => json!({"type": "uint16"}),
        DataTypeDef::U32 => json!({"type": "uint32"}),
        DataTypeDef::U64 => json!({"type": "uint64"}),
        DataTypeDef::F32 => json!({"type": "float32"}),
        DataTypeDef::F64 => json!({"type": "float64"}),
        DataTypeDef::String => json!({"type": "string"}),
        DataTypeDef::WString => with_metadata(
            json!({"type": "string"}),
            [("x-mcap-original-type", json!("wstring"))],
        ),
        DataTypeDef::BoundedString(size) => with_metadata(
            json!({"type": "string"}),
            [
                ("x-mcap-original-type", json!("bounded_string")),
                ("x-mcap-max-length", json!(size)),
            ],
        ),
        DataTypeDef::BoundedWString(size) => with_metadata(
            json!({"type": "string"}),
            [
                ("x-mcap-original-type", json!("bounded_wstring")),
                ("x-mcap-max-length", json!(size)),
            ],
        ),
        DataTypeDef::Bytes => with_metadata(
            json!({"type": "string"}),
            [
                ("x-mcap-original-type", json!("bytes")),
                ("x-mcap-encoding", json!("hex")),
            ],
        ),
        DataTypeDef::BoundedBytes(size) => with_metadata(
            json!({"type": "string"}),
            [
                ("x-mcap-original-type", json!("bounded_bytes")),
                ("x-mcap-encoding", json!("hex")),
                ("x-mcap-max-items", json!(size)),
            ],
        ),
        DataTypeDef::Enum(variants) if variants.is_empty() => with_metadata(
            json!({"type": "string"}),
            [("x-mcap-original-type", json!("enum"))],
        ),
        DataTypeDef::Enum(variants) => with_metadata(
            json!({"enum": variants.iter().map(|variant| &variant.name).collect::<Vec<_>>() }),
            [
                ("x-mcap-original-type", json!("enum")),
                (
                    "x-mcap-enum-variants",
                    json!(
                        variants
                            .iter()
                            .map(|variant| json!({"name": variant.name, "value": variant.value}))
                            .collect::<Vec<_>>()
                    ),
                ),
            ],
        ),
        DataTypeDef::Struct(fields) => json!({"properties": fields_jtd(fields)}),
        DataTypeDef::List(element) => with_metadata(
            json!({"elements": element_jtd(element)}),
            [("x-mcap-original-type", json!("list"))],
        ),
        DataTypeDef::BoundedList(element, size) => with_metadata(
            json!({"elements": element_jtd(element)}),
            [
                ("x-mcap-original-type", json!("bounded_list")),
                ("x-mcap-max-items", json!(size)),
            ],
        ),
        DataTypeDef::Array(element, size) => with_metadata(
            json!({"elements": element_jtd(element)}),
            [
                ("x-mcap-original-type", json!("array")),
                ("x-mcap-fixed-length", json!(size)),
            ],
        ),
        DataTypeDef::Map { key, value } if is_string_map_key(key) => with_metadata(
            json!({"values": element_jtd(value)}),
            [("x-mcap-original-type", json!("map"))],
        ),
        DataTypeDef::Map { key, value } => with_metadata(
            json!({
                "elements": {
                    "properties": {
                        "key": element_jtd(key),
                        "value": element_jtd(value),
                    },
                },
            }),
            [("x-mcap-original-type", json!("map"))],
        ),
    }
}

fn is_string_map_key(key: &ElementDef) -> bool {
    matches!(
        key.data_type,
        DataTypeDef::String
            | DataTypeDef::WString
            | DataTypeDef::BoundedString(_)
            | DataTypeDef::BoundedWString(_)
    )
}

fn with_metadata<const N: usize>(schema: Value, entries: [(&str, Value); N]) -> Value {
    let metadata: BTreeMap<_, _> = entries
        .into_iter()
        .map(|(key, value)| (key.to_string(), value))
        .collect();
    let mut schema = schema;
    schema
        .as_object_mut()
        .expect("JTD schema is always an object")
        .insert("metadata".to_string(), json!(metadata));
    schema
}
