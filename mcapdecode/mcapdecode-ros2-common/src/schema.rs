//! Conversion from a [`ResolvedSchema`] to Arrow-compatible [`FieldDefs`].
//!
//! This is the final step before handing the schema to `mcapdecode-arrow` for
//! building Arrow `RecordBatch`es.  The mapping rules are:
//!
//! | Resolved type          | Arrow `DataTypeDef`          |
//! |------------------------|------------------------------|
//! | Primitive              | corresponding scalar type    |
//! | Struct                 | `Struct(StructDef)`          |
//! | Enum                   | `Enum(EnumDef)` (decoded as the variant name) |
//! | `sequence<uint8/octet>` | `Uint8List` / `BoundedUint8List`    |
//! | Other sequence         | `List` / `BoundedList`       |
//! | BoundedString/WString  | bounded string variants      |
//! | Fixed-length field     | `Array(element type, n)`     |

use std::collections::HashSet;

use mcapdecode_core::{DataTypeDef, ElementDef, EnumDef, FieldDef, FieldDefs, StructDef};

use crate::{
    ast::PrimitiveType,
    type_resolver::{ResolvedField, ResolvedSchema, ResolvedType},
};

/// Convert the root struct of `schema` to [`FieldDefs`] for Arrow schema derivation.
///
/// # Panics
///
/// Panics if `schema` references a struct it does not define, or if a struct
/// contains itself. Schemas produced by the type resolver never do the former.
pub fn resolved_schema_to_field_defs(schema: &ResolvedSchema) -> FieldDefs {
    try_resolved_schema_to_field_defs(schema).unwrap_or_else(|e| panic!("{e}"))
}

/// Fallible variant of [`resolved_schema_to_field_defs`].
pub(crate) fn try_resolved_schema_to_field_defs(
    schema: &ResolvedSchema,
) -> Result<FieldDefs, String> {
    resolved_struct_to_field_defs(schema, &schema.root, &mut HashSet::new())
}

/// `visiting` holds the structs on the current path, to detect recursion.
fn resolved_struct_to_field_defs<'s>(
    schema: &'s ResolvedSchema,
    name: &'s [String],
    visiting: &mut HashSet<&'s [String]>,
) -> Result<FieldDefs, String> {
    let st = schema
        .structs
        .get(name)
        .ok_or_else(|| format!("unknown struct: {}", name.join("::")))?;
    if !visiting.insert(name) {
        return Err(format!("recursive struct: {}", name.join("::")));
    }
    let fields = st
        .fields
        .iter()
        .map(|f| resolved_field_to_field_def(schema, f, visiting))
        .collect::<Result<Vec<_>, _>>()?;
    visiting.remove(name);
    Ok(FieldDefs::new(fields))
}

fn resolved_field_to_field_def<'s>(
    schema: &'s ResolvedSchema,
    field: &'s ResolvedField,
    visiting: &mut HashSet<&'s [String]>,
) -> Result<FieldDef, String> {
    let inner_dt = resolved_type_to_data_type_def(schema, &field.ty, visiting)?;

    // Wrap in Array when the field has a fixed static length.
    let dt = match field.fixed_len {
        Some(n) => DataTypeDef::Array(Box::new(ElementDef::new(inner_dt, false)), n),
        None => inner_dt,
    };

    Ok(FieldDef::new(&field.name, dt, false))
}

fn resolved_type_to_data_type_def<'s>(
    schema: &'s ResolvedSchema,
    ty: &'s ResolvedType,
    visiting: &mut HashSet<&'s [String]>,
) -> Result<DataTypeDef, String> {
    Ok(match ty {
        ResolvedType::Primitive(p) => primitive_to_data_type_def(p),
        ResolvedType::Struct(name) => {
            let fields = resolved_struct_to_field_defs(schema, name, visiting)?;
            DataTypeDef::Struct(StructDef {
                type_name: Some(name.join("/")),
                fields,
            })
        }
        // An unknown enum stays decodable (`decode_cdr_to_value` falls back to the raw
        // number), so it must not be treated as harder than a missing struct.
        ResolvedType::Enum(name) => DataTypeDef::Enum(EnumDef {
            type_name: Some(name.join("/")),
            variants: schema.enums.get(name).cloned().unwrap_or_default(),
        }),
        ResolvedType::Sequence { elem, max_len }
            if matches!(
                elem.as_ref(),
                ResolvedType::Primitive(PrimitiveType::U8 | PrimitiveType::Octet)
            ) =>
        {
            max_len.map_or(DataTypeDef::Uint8List, DataTypeDef::BoundedUint8List)
        }
        ResolvedType::Sequence { elem, max_len } => {
            let element = Box::new(ElementDef::new(
                resolved_type_to_data_type_def(schema, elem, visiting)?,
                false,
            ));
            match max_len {
                Some(size) => DataTypeDef::BoundedList(element, *size),
                None => DataTypeDef::List(element),
            }
        }
        ResolvedType::BoundedString(size) => DataTypeDef::BoundedString(*size),
        ResolvedType::BoundedWString(size) => DataTypeDef::BoundedWString(*size),
    })
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
        PrimitiveType::WString => DataTypeDef::WString,
    }
}
