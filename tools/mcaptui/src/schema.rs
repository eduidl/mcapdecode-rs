use std::collections::HashMap;

use anyhow::{Context, Result};
use mcapdecode::{McapReader, McapReaderError, core::DecoderError};

use crate::{app, format};

/// Channel IDs identify schemas even when display labels coincide.
pub(crate) type SchemaCache = HashMap<u16, app::SchemaView>;

pub(crate) fn open_selected_schema(
    app: &mut app::App,
    reader: &McapReader,
    schema_cache: &mut SchemaCache,
) -> Result<()> {
    let row = app.selected_topic().context("no topic selected")?.clone();
    let schema_view = load_schema_view(reader, schema_cache, &row)?;
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
    schema_cache: &mut SchemaCache,
    row: &app::TopicRow,
) -> Result<app::SchemaView> {
    let key = row.channel.id;
    if let Some(schema_view) = schema_cache.get(&key) {
        return Ok(schema_view.clone());
    }

    let label = row.label();
    let topic = &row.channel;
    let title = match topic.schema_name.as_deref() {
        Some(schema_name) => format!("Schema: {label} ({schema_name})"),
        None => format!("Schema: {label}"),
    };
    let text = if topic.schema_encoding == "jsonschema" && topic.message_encoding == "json" {
        format::format_unsupported_schema_text(topic)
    } else {
        let field_defs = reader.field_defs_by_channel_id(row.channel.id);
        match field_defs {
            Ok(field_defs) => format::format_schema_text(topic, &field_defs),
            Err(error) if supports_raw_fallback(&error) => {
                format::format_raw_schema_text(topic, &error.to_string())
            }
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("failed to load schema for topic '{label}'"));
            }
        }
    };
    let schema_view = app::SchemaView {
        topic: label,
        title,
        text,
        line_count: 0,
    };
    let mut schema_view = schema_view;
    schema_view.line_count = schema_view.text.lines().count();
    schema_cache.insert(key, schema_view.clone());
    Ok(schema_view)
}

pub(crate) fn supports_raw_fallback(error: &McapReaderError) -> bool {
    matches!(
        error,
        McapReaderError::NoDecoder { .. }
            | McapReaderError::SchemaNotAvailable
            | McapReaderError::Decoder(
                DecoderError::SchemaParse(_) | DecoderError::SchemaDerivation(_)
            )
    )
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use mcapdecode::{ChannelInfo, McapReader};

    fn reader() -> McapReader {
        McapReader::builder()
            .clear_decoders()
            .open(
                Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("../../mcapdecode/mcapdecode/tests/fixtures/with_summary.mcap"),
            )
            .unwrap()
    }

    use super::*;

    #[test]
    fn jsonschema_topics_report_the_schema_as_unsupported() {
        let row = app::TopicRow::from_channel(
            ChannelInfo {
                id: u16::MAX,
                topic: "/imu".to_string(),
                message_count: Some(1),
                schema_name: Some("schema-a".to_string()),
                schema_encoding: "jsonschema".to_string(),
                message_encoding: "json".to_string(),
                metadata: Default::default(),
            },
            false,
        );
        let mut cache = SchemaCache::default();

        let view = load_schema_view(&reader(), &mut cache, &row).unwrap();

        assert!(view.text.contains("JSON Schema is not supported"));
        assert!(!view.text.contains("json: string"));
    }

    #[test]
    fn jsonschema_with_other_message_encodings_goes_to_the_reader() {
        let row = app::TopicRow::from_channel(
            ChannelInfo {
                id: u16::MAX,
                topic: "/imu".to_string(),
                message_count: Some(1),
                schema_name: Some("schema-a".to_string()),
                schema_encoding: "jsonschema".to_string(),
                message_encoding: "cbor".to_string(),
                metadata: Default::default(),
            },
            false,
        );
        let mut cache = SchemaCache::default();

        // The topic is absent, so the unsupported text was not used.
        assert!(load_schema_view(&reader(), &mut cache, &row).is_err());
    }

    #[test]
    fn rows_with_the_same_label_do_not_share_a_cached_schema() {
        let channel_row = app::TopicRow::from_channel(
            ChannelInfo {
                id: 1,
                topic: "/foo".to_string(),
                message_count: Some(1),
                schema_name: None,
                schema_encoding: String::new(),
                message_encoding: String::new(),
                metadata: Default::default(),
            },
            true,
        );
        let topic_row = app::TopicRow::from_channel(
            ChannelInfo {
                id: u16::MAX,
                topic: "/foo #1".to_string(),
                message_count: Some(1),
                schema_name: None,
                schema_encoding: String::new(),
                message_encoding: String::new(),
                metadata: Default::default(),
            },
            false,
        );
        assert_eq!(channel_row.label(), topic_row.label());

        let cached = app::SchemaView {
            topic: channel_row.label(),
            title: "cached".to_string(),
            text: String::new(),
            line_count: 0,
        };
        let mut cache = SchemaCache::from([(1, cached)]);
        let reader = reader();

        let hit = load_schema_view(&reader, &mut cache, &channel_row).unwrap();
        assert_eq!(hit.title, "cached");
        // A cache miss queries the reader, where the topic is absent.
        assert!(load_schema_view(&reader, &mut cache, &topic_row).is_err());
    }
}
