use mcapdecode_ros2_common::{ParsedSection, ResolvedSchema, Ros2Error, resolve_parsed_section};

use crate::{parse_msg, schema_bundle::SchemaBundle};

/// Parse a `.msg` schema text or schema bundle and produce a fully resolved schema.
pub fn resolve_schema(schema_name: &str, schema_text: &str) -> Result<ResolvedSchema, Ros2Error> {
    let bundle = SchemaBundle::parse(schema_name, schema_text)?;

    let mut merged = ParsedSection::default();
    for section in &bundle.sections {
        let parsed = parse_msg(&section.schema_name(), &section.body).map_err(|e| {
            Ros2Error(format!(
                "while parsing msg section '{}': {e}",
                section.path()
            ))
        })?;
        merged.structs.insert(parsed.full_name.clone(), parsed);
    }

    let root = bundle.main_type(schema_name).ok_or_else(|| {
        Ros2Error(format!(
            "unable to determine root type for schema '{schema_name}'"
        ))
    })?;

    resolve_parsed_section(merged, root)
}
