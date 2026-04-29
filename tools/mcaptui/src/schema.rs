use std::{collections::HashMap, path::Path};

use anyhow::{Context, Result};
use mcapdecode::{McapReader, McapReaderError, TopicInfo};

use crate::{app, format};

pub(crate) type SchemaCache = HashMap<String, app::SchemaView>;

pub(crate) fn open_selected_schema(
    app: &mut app::App,
    reader: &McapReader,
    input: &Path,
    schema_cache: &mut SchemaCache,
) -> Result<()> {
    let topic = app
        .selected_topic()
        .map(|row| row.info.clone())
        .context("no topic selected")?;
    let schema_view = load_schema_view(reader, input, schema_cache, &topic)?;
    app.set_schema_view(
        schema_view.topic.clone(),
        schema_view.title.clone(),
        schema_view.text.clone(),
    );
    app.set_status(format!("Showing schema for {}", topic.topic));
    Ok(())
}

pub(crate) fn load_schema_view(
    reader: &McapReader,
    input: &Path,
    schema_cache: &mut SchemaCache,
    topic: &TopicInfo,
) -> Result<app::SchemaView> {
    if let Some(schema_view) = schema_cache.get(&topic.topic) {
        return Ok(schema_view.clone());
    }

    let title = match topic.schema_name.as_deref() {
        Some(schema_name) => format!("Schema: {} ({schema_name})", topic.topic),
        None => format!("Schema: {}", topic.topic),
    };
    let text = match reader.topic_field_defs(input, &topic.topic) {
        Ok(field_defs) => format::format_schema_text(topic, &field_defs),
        Err(error) if supports_raw_schema_view(&error) => {
            format::format_raw_schema_text(topic, &error.to_string())
        }
        Err(error) => {
            return Err(error)
                .with_context(|| format!("failed to load schema for topic '{}'", topic.topic));
        }
    };
    let schema_view = app::SchemaView {
        topic: topic.topic.clone(),
        title,
        text,
        line_count: 0,
    };
    let mut schema_view = schema_view;
    schema_view.line_count = schema_view.text.lines().count();
    schema_cache.insert(topic.topic.clone(), schema_view.clone());
    Ok(schema_view)
}

fn supports_raw_schema_view(error: &McapReaderError) -> bool {
    matches!(
        error,
        McapReaderError::NoDecoder { .. }
            | McapReaderError::SchemaNotAvailable { .. }
            | McapReaderError::SchemaDerivationFailed { .. }
    )
}
