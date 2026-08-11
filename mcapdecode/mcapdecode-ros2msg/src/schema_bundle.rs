//! MSG-specific schema-bundle parsing.

use mcapdecode_ros2_common::{
    Ros2Error, SchemaBundle, SchemaSection, normalize_schema_name, split_schema_sections,
};

/// Parse a schema blob containing an optional root and `MSG:` dependency sections.
pub fn parse_schema_bundle(
    schema_name: &str,
    schema_text: &str,
) -> Result<SchemaBundle, Ros2Error> {
    let root_path = normalize_schema_name(schema_name)?;
    let blocks = split_schema_sections(schema_text);
    let mut sections = Vec::with_capacity(blocks.len());

    for (index, block) in blocks.into_iter().enumerate() {
        sections.push(parse_section(
            &block,
            (index == 0).then(|| root_path.clone()),
        )?);
    }

    if sections.is_empty() {
        return Err(format!("no MSG sections found for schema '{schema_name}'").into());
    }

    Ok(SchemaBundle { sections })
}

fn parse_section(
    lines: &[&str],
    fallback_path: Option<String>,
) -> Result<SchemaSection, Ros2Error> {
    let first_idx = lines
        .iter()
        .position(|line| !line.trim().is_empty())
        .ok_or_else(|| Ros2Error("empty MSG section".to_string()))?;
    let header = lines[first_idx].trim();

    let (path, body_start) = if let Some(path) = header.strip_prefix("MSG:") {
        (normalize_schema_name(path.trim())?, first_idx + 1)
    } else if let Some(path) = fallback_path {
        (path, first_idx)
    } else {
        return Err(format!("missing `MSG:` header: {header}").into());
    };

    Ok(SchemaSection {
        path,
        body: lines[body_start..].join("\n"),
    })
}
