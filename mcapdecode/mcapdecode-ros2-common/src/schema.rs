//! Conversion from a [`ResolvedSchema`] to Arrow-compatible [`FieldDefs`].
//!
//! This is the final step before handing the schema to `mcapdecode-arrow` for
//! building Arrow `RecordBatch`es.  The mapping rules are:
//!
//! | Resolved type          | Arrow `DataTypeDef`          |
//! |------------------------|------------------------------|
//! | Primitive              | corresponding scalar type    |
//! | Struct                 | `Struct(FieldDefs)`          |
//! | Enum                   | `Enum(variants)` (decoded as the variant name) |
//! | `sequence<uint8/octet>` | `Bytes`                     |
//! | Other sequence         | `List(element type)`         |
//! | BoundedString/WString  | `String`                     |
//! | Fixed-length field     | `Array(element type, n)`     |

use mcapdecode_core::{DataTypeDef, ElementDef, FieldDef, FieldDefs};

use crate::{
    ast::PrimitiveType,
    type_resolver::{ResolvedField, ResolvedSchema, ResolvedStruct, ResolvedType},
};

/// Convert the root struct of `schema` to [`FieldDefs`] for Arrow schema derivation.
pub fn resolved_schema_to_field_defs(schema: &ResolvedSchema) -> FieldDefs {
    let root_struct = schema
        .structs
        .get(&schema.root)
        .unwrap_or_else(|| panic!("Root struct {:?} not found", schema.root));
    resolved_struct_to_field_defs(schema, root_struct)
}

fn resolved_struct_to_field_defs(schema: &ResolvedSchema, st: &ResolvedStruct) -> FieldDefs {
    FieldDefs::new(
        st.fields
            .iter()
            .map(|f| resolved_field_to_field_def(schema, f))
            .collect(),
    )
}

fn resolved_field_to_field_def(schema: &ResolvedSchema, field: &ResolvedField) -> FieldDef {
    let inner_dt = resolved_type_to_data_type_def(schema, &field.ty);

    // Wrap in Array when the field has a fixed static length.
    let dt = match field.fixed_len {
        Some(n) => DataTypeDef::Array(Box::new(ElementDef::new(inner_dt, false)), n),
        None => inner_dt,
    };

    FieldDef::new(&field.name, dt, false)
}

fn resolved_type_to_data_type_def(schema: &ResolvedSchema, ty: &ResolvedType) -> DataTypeDef {
    match ty {
        ResolvedType::Primitive(p) => primitive_to_data_type_def(p),
        ResolvedType::Struct(name) => {
            let st = schema
                .structs
                .get(name)
                .unwrap_or_else(|| panic!("Struct {:?} not found", name));
            let fields = resolved_struct_to_field_defs(schema, st);
            DataTypeDef::Struct(fields)
        }
        // An unknown enum stays decodable (`decode_cdr_to_value` falls back to the raw
        // number), so it must not be treated as harder than a missing struct.
        ResolvedType::Enum(name) => {
            DataTypeDef::Enum(schema.enums.get(name).cloned().unwrap_or_default())
        }
        ResolvedType::Sequence { elem, .. }
            if matches!(
                elem.as_ref(),
                ResolvedType::Primitive(PrimitiveType::U8 | PrimitiveType::Octet)
            ) =>
        {
            DataTypeDef::Bytes
        }
        ResolvedType::Sequence { elem, .. } => DataTypeDef::List(Box::new(ElementDef::new(
            resolved_type_to_data_type_def(schema, elem),
            false,
        ))),
        ResolvedType::BoundedString(_) => DataTypeDef::String,
        ResolvedType::BoundedWString(_) => DataTypeDef::String,
    }
}

fn primitive_to_data_type_def(p: &PrimitiveType) -> DataTypeDef {
    match p {
        PrimitiveType::Bool => DataTypeDef::Bool,
        PrimitiveType::I8 => DataTypeDef::I8,
        PrimitiveType::I16 => DataTypeDef::I16,
        PrimitiveType::I32 => DataTypeDef::I32,
        PrimitiveType::I64 => DataTypeDef::I64,
        PrimitiveType::U8 | PrimitiveType::Octet => DataTypeDef::U8,
        PrimitiveType::U16 => DataTypeDef::U16,
        PrimitiveType::U32 => DataTypeDef::U32,
        PrimitiveType::U64 => DataTypeDef::U64,
        PrimitiveType::F32 => DataTypeDef::F32,
        PrimitiveType::F64 => DataTypeDef::F64,
        PrimitiveType::String => DataTypeDef::String,
        PrimitiveType::WString => DataTypeDef::String,
    }
}
