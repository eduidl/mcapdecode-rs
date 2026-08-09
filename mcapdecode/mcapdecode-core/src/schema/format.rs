use std::fmt::{Error, Result, Write as _};

use super::{DataTypeDef, ElementDef, FieldDef};

/// Format field definitions as a type-annotated tree.
///
/// Each field's complete composite type is rendered on its own line. Only
/// struct and enum bodies are expanded below it, so collection element labels
/// do not obscure user-defined fields.
pub fn format_field_defs(fields: impl AsRef<[FieldDef]>) -> std::result::Result<String, Error> {
    let fields = fields.as_ref();
    let mut out = String::new();

    for field in fields {
        format_field(field, 0, &mut out)?;
    }

    Ok(out)
}

fn format_field(field: &FieldDef, indent: usize, out: &mut String) -> Result {
    let pad = " ".repeat(indent);
    writeln!(
        out,
        "{pad}{}: {}",
        field.name,
        format_element_type(&field.element)
    )?;
    format_data_type_body(&field.element.data_type, indent + 4, out)
}

fn format_element_type(element: &ElementDef) -> String {
    element.to_string()
}

fn format_data_type_body(data_type: &DataTypeDef, indent: usize, out: &mut String) -> Result {
    match data_type {
        DataTypeDef::Struct(fields) => {
            for child in fields.iter() {
                format_field(child, indent, out)?;
            }
        }
        DataTypeDef::Enum(variants) => {
            let pad = " ".repeat(indent);
            for variant in variants {
                writeln!(out, "{pad}{} = {}", variant.name, variant.value)?;
            }
        }
        DataTypeDef::List(element)
        | DataTypeDef::BoundedList(element, _)
        | DataTypeDef::Array(element, _) => format_data_type_body(&element.data_type, indent, out)?,
        DataTypeDef::Map { key, value } => format_map_body(key, value, indent, out)?,
        _ => {}
    }

    Ok(())
}

fn format_map_body(
    key: &ElementDef,
    value: &ElementDef,
    indent: usize,
    out: &mut String,
) -> Result {
    match (
        data_type_has_body(&key.data_type),
        data_type_has_body(&value.data_type),
    ) {
        (false, false) => {}
        // A scalar key is already fully described in map<K, V>; expand the
        // value body directly so ordinary maps stay compact.
        (false, true) => format_data_type_body(&value.data_type, indent, out)?,
        // Complex keys need an explicit branch because the following body
        // would otherwise be indistinguishable from the map value.
        (true, false) => format_labeled_body("@key", key, indent, out)?,
        (true, true) => {
            format_labeled_body("@key", key, indent, out)?;
            format_labeled_body("@value", value, indent, out)?;
        }
    }

    Ok(())
}

fn format_labeled_body(
    label: &str,
    element: &ElementDef,
    indent: usize,
    out: &mut String,
) -> Result {
    let pad = " ".repeat(indent);
    writeln!(out, "{pad}{label}: {}", format_element_type(element))?;
    format_data_type_body(&element.data_type, indent + 4, out)
}

fn data_type_has_body(data_type: &DataTypeDef) -> bool {
    match data_type {
        DataTypeDef::Struct(_) | DataTypeDef::Enum(_) => true,
        DataTypeDef::List(element)
        | DataTypeDef::BoundedList(element, _)
        | DataTypeDef::Array(element, _) => data_type_has_body(&element.data_type),
        DataTypeDef::Map { key, value } => {
            data_type_has_body(&key.data_type) || data_type_has_body(&value.data_type)
        }
        _ => false,
    }
}
