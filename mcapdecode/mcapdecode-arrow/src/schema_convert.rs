use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use mcapdecode_core::{DataTypeDef, ElementDef, FieldDef, FieldDefs};

// ---------------------------------------------------------------------------
// Convert FieldDef schema IR to Arrow schema (body fields only)
// ---------------------------------------------------------------------------

/// Converts `mcapdecode-core` schema IR into an Arrow `Schema`.
///
/// The input is expected to represent message body fields only. Metadata system
/// columns are not included in the returned schema; see
/// [`MessageBatchSchema`](crate::MessageBatchSchema) for the emitted schema.
pub fn field_defs_to_arrow_schema(fields: &FieldDefs) -> Schema {
    let arrow_fields: Vec<Field> = fields.iter().map(field_def_to_arrow_field).collect();
    Schema::new(arrow_fields)
}

fn field_def_to_arrow_field(f: &FieldDef) -> Field {
    Field::new(
        &f.name,
        element_def_to_datatype(&f.element),
        f.element.nullable,
    )
}

fn element_def_to_datatype(elem: &ElementDef) -> DataType {
    match &elem.data_type {
        DataTypeDef::Null => DataType::Null,
        DataTypeDef::Bool => DataType::Boolean,
        DataTypeDef::I8 => DataType::Int8,
        DataTypeDef::I16 => DataType::Int16,
        DataTypeDef::I32 => DataType::Int32,
        DataTypeDef::I64 => DataType::Int64,
        DataTypeDef::U8 => DataType::UInt8,
        DataTypeDef::U16 => DataType::UInt16,
        DataTypeDef::U32 => DataType::UInt32,
        DataTypeDef::U64 => DataType::UInt64,
        DataTypeDef::F32 => DataType::Float32,
        DataTypeDef::F64 => DataType::Float64,
        DataTypeDef::String
        | DataTypeDef::WString
        | DataTypeDef::BoundedString(_)
        | DataTypeDef::BoundedWString(_)
        | DataTypeDef::Enum(_) => DataType::Utf8,
        DataTypeDef::Bytes | DataTypeDef::BoundedBytes(_) => DataType::Binary,
        DataTypeDef::Struct(fields) => {
            let arrow_fields: Vec<Field> = fields.iter().map(field_def_to_arrow_field).collect();
            DataType::Struct(arrow_fields.into())
        }
        DataTypeDef::List(elem) | DataTypeDef::BoundedList(elem, _) => {
            let child_dt = element_def_to_datatype(elem);
            DataType::List(Arc::new(Field::new("item", child_dt, elem.nullable)))
        }
        DataTypeDef::Array(elem, size) => {
            let child_dt = element_def_to_datatype(elem);
            DataType::FixedSizeList(
                Arc::new(Field::new("item", child_dt, elem.nullable)),
                *size as i32,
            )
        }
        DataTypeDef::Map { key, value } => {
            let key_field = Field::new("key", element_def_to_datatype(key), key.nullable);
            let val_field = Field::new("value", element_def_to_datatype(value), value.nullable);
            let entry_struct = DataType::Struct(vec![key_field, val_field].into());
            let entry_field = Field::new("entries", entry_struct, false);
            DataType::Map(Arc::new(entry_field), false)
        }
    }
}
