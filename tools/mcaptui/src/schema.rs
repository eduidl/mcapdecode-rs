use std::{collections::HashMap, path::Path};

use anyhow::{Context, Result};
use mcapdecode::{McapReader, McapReaderError, core::DecoderError};

use crate::{app, format};

pub(crate) type SchemaCache = HashMap<String, app::SchemaView>;

pub(crate) fn open_selected_schema(
    app: &mut app::App,
    reader: &McapReader,
    input: &Path,
    schema_cache: &mut SchemaCache,
) -> Result<()> {
    let row = app.selected_topic().context("no topic selected")?.clone();
    let schema_view = load_schema_view(reader, input, schema_cache, &row)?;
    app.set_schema_view(
        schema_view.topic.clone(),
        schema_view.title.clone(),
        schema_view.text.clone(),
    );
    app.set_status(format!("Showing schema for {}", row.label()));
    Ok(())
}

pub(crate) fn load_schema_view(
    reader: &McapReader,
    input: &Path,
    schema_cache: &mut SchemaCache,
    row: &app::TopicRow,
) -> Result<app::SchemaView> {
    let label = row.label();
    if let Some(schema_view) = schema_cache.get(&label) {
        return Ok(schema_view.clone());
    }

    let topic = &row.info;
    let title = match topic.schema_name.as_deref() {
        Some(schema_name) => format!("Schema: {label} ({schema_name})"),
        None => format!("Schema: {label}"),
    };
    let field_defs = match row.channel_id {
        Some(channel_id) => reader.field_defs_by_channel_id(input, channel_id),
        None => reader.topic_field_defs(input, &topic.topic),
    };
    let text = match field_defs {
        Ok(field_defs) => format::format_schema_text(topic, &field_defs),
        Err(error) if supports_raw_schema_view(&error) => {
            format::format_raw_schema_text(topic, &error.to_string())
        }
        Err(error) => {
            return Err(error)
                .with_context(|| format!("failed to load schema for topic '{label}'"));
        }
    };
    let schema_view = app::SchemaView {
        topic: label.clone(),
        title,
        text,
        line_count: 0,
    };
    let mut schema_view = schema_view;
    schema_view.line_count = schema_view.text.lines().count();
    schema_cache.insert(label, schema_view.clone());
    Ok(schema_view)
}

fn supports_raw_schema_view(error: &McapReaderError) -> bool {
    matches!(
        error,
        McapReaderError::NoDecoder { .. }
            | McapReaderError::SchemaNotAvailable
            | McapReaderError::Decoder(
                DecoderError::SchemaParse(_) | DecoderError::SchemaDerivation(_)
            )
    )
}
