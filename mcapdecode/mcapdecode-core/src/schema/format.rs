use std::fmt::{Error, Result, Write as _};

use super::{DataTypeDef, ElementDef, FieldDef};

/// Format field definitions in a readable style:
/// primitive fields are rendered in one line, compound fields are pretty-printed.
/// Nested fields follow the same rule.
pub fn format_field_defs(fields: impl AsRef<[FieldDef]>) -> std::result::Result<String, Error> {
    let fields = fields.as_ref();
    let mut out = String::new();

    for field in fields.iter() {
        format_field(field, 0, &mut out)?;
    }

    Ok(out)
}

fn format_field(field: &FieldDef, indent: usize, out: &mut String) -> Result {
    format_labeled_element(&field.name, &field.element, indent, out)
}

fn format_data_type(data_type: &DataTypeDef, indent: usize, out: &mut String) -> Result {
    match data_type {
        DataTypeDef::Struct(fields) => {
            for child in fields.iter() {
                format_field(child, indent, out)?;
            }
        }
        DataTypeDef::List(elem) => {
            format_labeled_element("item", elem, indent, out)?;
        }
        DataTypeDef::Array(elem, size) => {
            let pad = " ".repeat(indent);
            format_labeled_element("item", elem, indent, out)?;
            writeln!(out, "{pad}size: {}", size)?;
        }
        DataTypeDef::Map { key, value } => {
            format_labeled_element("key", key, indent, out)?;
            format_labeled_element("value", value, indent, out)?;
        }
        _ => unreachable!("{data_type:?} is not a compound type"),
    }

    Ok(())
}

fn format_labeled_element(
    label: &str,
    element: &ElementDef,
    indent: usize,
    out: &mut String,
) -> Result {
    let pad = " ".repeat(indent);
    writeln!(out, "{pad}{label}: {element}")?;
    if !element.data_type.is_primitive() {
        format_data_type(&element.data_type, indent + 4, out)?;
    }
    Ok(())
}
