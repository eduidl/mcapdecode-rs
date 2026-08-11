use std::{collections::BTreeMap, path::PathBuf};

use clap::{Args, ValueEnum};
use mcapdecode::{
    McapReader,
    core::{DataTypeDef, ElementDef, FieldDefs, format_field_defs},
};
use serde_json::{Map, Value, json};

#[derive(Args)]
pub struct SchemaArgs {
    /// Path to the MCAP file.
    input: PathBuf,

    /// Topic whose schema to describe.
    #[arg(short, long)]
    topic: String,

    /// Schema representation to emit.
    #[arg(long, value_enum, default_value_t = SchemaFormat::Jtd)]
    format: SchemaFormat,
}

#[derive(Clone, Copy, ValueEnum)]
enum SchemaFormat {
    /// JSON Type Definition (RFC 8927).
    Jtd,
    /// mcapdecode's native FieldDefs text format.
    Native,
}

impl SchemaArgs {
    pub fn run(self) -> Result<(), String> {
        let reader = McapReader::builder().with_default_decoders().build();

        match self.format {
            SchemaFormat::Jtd => {
                let topic_schema = reader
                    .topic_schema(&self.input, &self.topic)
                    .map_err(|error| error.to_string())?;
                let output = jtd_schema(
                    &topic_schema.field_defs,
                    &topic_schema.info.schema_name.unwrap_or_default(),
                    &topic_schema.info.schema_encoding,
                    &topic_schema.info.message_encoding,
                );
                println!(
                    "{}",
                    serde_json::to_string(&output).map_err(|error| error.to_string())?
                );
            }
            SchemaFormat::Native => {
                let field_defs = reader
                    .topic_field_defs(&self.input, &self.topic)
                    .map_err(|error| error.to_string())?;
                let text = format_field_defs(&field_defs).map_err(|error| error.to_string())?;
                print!("{text}");
            }
        }

        Ok(())
    }
}

fn jtd_schema(
    fields: &FieldDefs,
    title: &str,
    schema_encoding: &str,
    message_encoding: &str,
) -> Value {
    json!({
        "properties": fields_jtd(fields),
        "metadata": {
            "title": title,
            "x-mcap": {
                "schema_encoding": schema_encoding,
                "message_encoding": message_encoding,
                "columns": ["log_time", "publish_time"]
            }
        }
    })
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
        DataTypeDef::WString => {
            with_metadata(json!({"type": "string"}), [("x-mcap-wide", json!(true))])
        }
        DataTypeDef::BoundedString(size) => with_metadata(
            json!({"type": "string"}),
            [("x-mcap-max-length", json!(size))],
        ),
        DataTypeDef::BoundedWString(size) => with_metadata(
            json!({"type": "string"}),
            [
                ("x-mcap-wide", json!(true)),
                ("x-mcap-max-length", json!(size)),
            ],
        ),
        DataTypeDef::Bytes => with_metadata(
            json!({"type": "string"}),
            [
                ("contentEncoding", json!("base64")),
                ("x-mcap-type", json!("bytes")),
            ],
        ),
        DataTypeDef::BoundedBytes(size) => with_metadata(
            json!({"type": "string"}),
            [
                ("contentEncoding", json!("base64")),
                ("x-mcap-type", json!("bytes")),
                ("x-mcap-max-items", json!(size)),
            ],
        ),
        DataTypeDef::Enum(variants) => {
            let enum_values = json!(
                variants
                    .iter()
                    .map(|variant| json!({"name": variant.name, "value": variant.value}))
                    .collect::<Vec<_>>()
            );
            let schema = if variants.is_empty() {
                json!({"type": "string"})
            } else {
                json!({"enum": variants.iter().map(|variant| &variant.name).collect::<Vec<_>>()})
            };
            with_metadata(schema, [("x-mcap-enum-values", enum_values)])
        }
        DataTypeDef::Struct(fields) => json!({"properties": fields_jtd(fields)}),
        DataTypeDef::List(element) => json!({"elements": element_jtd(element)}),
        DataTypeDef::BoundedList(element, size) => with_metadata(
            json!({"elements": element_jtd(element)}),
            [("x-mcap-max-items", json!(size))],
        ),
        DataTypeDef::Array(element, size) => with_metadata(
            json!({"elements": element_jtd(element)}),
            [("x-mcap-fixed-length", json!(size))],
        ),
        DataTypeDef::Map { key, value } if string_like(&key.data_type) => {
            json!({"values": element_jtd(value)})
        }
        DataTypeDef::Map { key, value } => with_metadata(
            json!({
                "elements": {"properties": {
                    "key": element_jtd(key),
                    "value": element_jtd(value)
                }}
            }),
            [("x-mcap-map", json!("entries"))],
        ),
    }
}

fn string_like(data_type: &DataTypeDef) -> bool {
    matches!(
        data_type,
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
