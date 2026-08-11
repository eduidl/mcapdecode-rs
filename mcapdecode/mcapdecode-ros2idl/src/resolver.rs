//! Ties together schema-bundle parsing, IDL parsing, and type resolution.

use mcapdecode_ros2_common::{
    ParsedSection, ResolvedSchema, Ros2Error, normalize_schema_name, resolve_parsed_section,
};

use crate::{
    parser::parse_idl_section,
    schema_bundle::{parse_schema_bundle, parse_schema_name},
};

/// Parse a multi-section IDL schema text and produce a fully resolved [`ResolvedSchema`].
///
/// Steps:
/// 1. Split `schema_text` into format-specific sections at `====` separator lines.
/// 2. Parse each section with [`parse_idl_section`] and merge results.
/// 3. Identify the root type from `schema_name`.
/// 4. Resolve all type references.
pub fn resolve_schema(schema_name: &str, schema_text: &str) -> Result<ResolvedSchema, Ros2Error> {
    let bundle = parse_schema_bundle(schema_name, schema_text)?;

    let mut merged = ParsedSection::default();
    for section in &bundle.sections {
        let parsed = parse_idl_section(&section.body)
            .map_err(|e| Ros2Error(format!("while parsing IDL section '{}': {e}", section.path)))?;
        for (k, v) in parsed.structs {
            merged.structs.insert(k, v);
        }
        for (k, v) in parsed.enums {
            merged.enums.insert(k, v);
        }
    }
    let root = bundle
        .main_section(&normalize_schema_name(schema_name)?)
        .ok_or_else(|| {
            Ros2Error(format!(
                "unable to determine root type for schema '{schema_name}'"
            ))
        })
        .and_then(|section| parse_schema_name(&section.path))?;

    resolve_parsed_section(merged, root)
}
