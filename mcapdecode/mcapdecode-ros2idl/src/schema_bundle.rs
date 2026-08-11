//! IDL-specific schema-bundle parsing.

use mcapdecode_ros2_common::{
    Ros2Error, SchemaBundle, SchemaSection, normalize_schema_name, split_schema_sections,
};

/// Parse a schema blob containing one or more `IDL:` sections.
pub fn parse_schema_bundle(
    schema_name: &str,
    schema_text: &str,
) -> Result<SchemaBundle, Ros2Error> {
    normalize_schema_name(schema_name)?;
    let sections = split_schema_sections(schema_text)
        .into_iter()
        .map(|section| parse_section(&section))
        .collect::<Result<Vec<_>, _>>()?;

    if sections.is_empty() {
        return Err(format!("no IDL sections found for schema '{schema_name}'").into());
    }

    Ok(SchemaBundle { sections })
}

/// Convert an IDL schema name into the resolver's qualified representation.
pub(crate) fn parse_schema_name(name: &str) -> Result<Vec<String>, Ros2Error> {
    normalize_schema_name(name).map(|name| name.split('/').map(ToString::to_string).collect())
}

fn parse_section(lines: &[&str]) -> Result<SchemaSection, Ros2Error> {
    let mut lines = lines
        .iter()
        .map(|line| line.trim())
        .filter(|line| !line.is_empty());
    let header = lines
        .next()
        .ok_or_else(|| Ros2Error("empty IDL section".to_string()))?;
    let path = header
        .strip_prefix("IDL:")
        .ok_or_else(|| Ros2Error(format!("missing `IDL:` header: {header}")))?
        .trim();
    if path.is_empty() {
        return Err("empty IDL path in section header".into());
    }

    Ok(SchemaSection {
        path: normalize_schema_name(path)?,
        body: lines.collect::<Vec<_>>().join("\n"),
    })
}
