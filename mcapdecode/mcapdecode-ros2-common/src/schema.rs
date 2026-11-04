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

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

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
    let mut builder = Builder {
        schema,
        built: HashMap::new(),
        visiting: HashSet::new(),
    };
    builder.struct_fields(&schema.root)
}

/// Builds each struct definition once, so every reference to the same struct
/// type shares one `Arc<StructDef>`.
struct Builder<'s> {
    schema: &'s ResolvedSchema,
    built: HashMap<&'s [String], Arc<StructDef>>,
    visiting: HashSet<&'s [String]>,
}

impl<'s> Builder<'s> {
    fn struct_fields(&mut self, name: &'s [String]) -> Result<FieldDefs, String> {
        let st = self
            .schema
            .structs
            .get(name)
            .ok_or_else(|| format!("unknown struct: {}", name.join("::")))?;
        if !self.visiting.insert(name) {
            return Err(format!("recursive struct: {}", name.join("::")));
        }
        let fields = st
            .fields
            .iter()
            .map(|f| self.field(f))
            .collect::<Result<Vec<_>, _>>()?;
        self.visiting.remove(name);
        Ok(FieldDefs::new(fields))
    }

    fn field(&mut self, field: &'s ResolvedField) -> Result<FieldDef, String> {
        let inner_dt = self.data_type(&field.ty)?;

        // Wrap in Array when the field has a fixed static length.
        let dt = match field.fixed_len {
            Some(n) => DataTypeDef::Array(Box::new(ElementDef::new(inner_dt, false)), n),
            None => inner_dt,
        };

        Ok(FieldDef::new(&field.name, dt, false))
    }

    fn data_type(&mut self, ty: &'s ResolvedType) -> Result<DataTypeDef, String> {
        Ok(match ty {
            ResolvedType::Primitive(p) => primitive_to_data_type_def(p),
            ResolvedType::Struct(name) => {
                if let Some(definition) = self.built.get(name.as_slice()) {
                    return Ok(DataTypeDef::Struct(Arc::clone(definition)));
                }
                let fields = self.struct_fields(name)?;
                let definition = Arc::new(StructDef {
                    type_name: Some(name.join("/")),
                    fields,
                });
                self.built.insert(name, Arc::clone(&definition));
                DataTypeDef::Struct(definition)
            }
            // An unknown enum stays decodable (the CDR decoder falls back to the raw
            // number), so it must not be treated as harder than a missing struct.
            ResolvedType::Enum(name) => DataTypeDef::Enum(EnumDef {
                type_name: Some(name.join("/")),
                variants: self.schema.enums.get(name).cloned().unwrap_or_default(),
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
                let element = Box::new(ElementDef::new(self.data_type(elem)?, false));
                match max_len {
                    Some(size) => DataTypeDef::BoundedList(element, *size),
                    None => DataTypeDef::List(element),
                }
            }
            ResolvedType::BoundedString(size) => DataTypeDef::BoundedString(*size),
            ResolvedType::BoundedWString(size) => DataTypeDef::BoundedWString(*size),
        })
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
        PrimitiveType::WString => DataTypeDef::WString,
    }
}
