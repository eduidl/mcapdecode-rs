use std::{path::Path, sync::Arc};

use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use mcap2arrow_arrow::{arrow_value_rows_to_record_batch, field_defs_to_arrow_schema};
use mcap2arrow_core::DecodedMessage;

use crate::{McapReader, McapReaderError, reader::TopicDecodeContext};

struct TopicBatchContext {
    decode: TopicDecodeContext,
    arrow_schema: SchemaRef,
}

pub trait McapReaderArrowExt {
    /// Read all messages for a topic and emit Arrow RecordBatches to callback.
    ///
    /// Chunks in the MCAP file are decompressed in parallel using rayon.
    /// Message decoding and Arrow conversion remain sequential.
    fn for_each_record_batch(
        &self,
        path: &Path,
        topic: &str,
        callback: impl FnMut(RecordBatch) -> Result<(), Box<dyn std::error::Error + Send + Sync>>,
    ) -> Result<(), McapReaderError>;
}

impl McapReaderArrowExt for McapReader {
    fn for_each_record_batch(
        &self,
        path: &Path,
        topic: &str,
        mut callback: impl FnMut(RecordBatch) -> Result<(), Box<dyn std::error::Error + Send + Sync>>,
    ) -> Result<(), McapReaderError> {
        let mmap = self.mmap_file(path)?;
        let summary = self.read_summary(path, &mmap)?;
        let context = resolve_topic_batch_context(self, &summary, topic)?;
        let mut rows = Vec::with_capacity(self.batch_size());
        self.for_each_decoded_message_impl(
            &mmap,
            &summary,
            &context.decode,
            topic,
            &mut |decoded| {
                push_decoded_message(
                    self.batch_size(),
                    &context.arrow_schema,
                    &mut rows,
                    decoded,
                    &mut callback,
                )
            },
        )?;

        flush_batch(&context.arrow_schema, &mut rows, &mut callback)
    }
}

fn resolve_topic_batch_context(
    reader: &McapReader,
    summary: &mcap::read::Summary,
    topic: &str,
) -> Result<TopicBatchContext, McapReaderError> {
    let decode = reader.resolve_topic_decode_context(summary, topic)?;

    if decode.field_defs.is_empty() {
        return Err(McapReaderError::EmptyDerivedSchema {
            topic: topic.to_string(),
            schema_name: get_schema_name(summary, topic)?,
        });
    }

    let arrow_schema = Arc::new(field_defs_to_arrow_schema(&decode.field_defs));

    Ok(TopicBatchContext {
        decode,
        arrow_schema,
    })
}

fn get_schema_name(summary: &mcap::read::Summary, topic: &str) -> Result<String, McapReaderError> {
    let mut channels = summary.channels.values().filter(|ch| ch.topic == topic);
    let channel = channels
        .next()
        .ok_or_else(|| McapReaderError::TopicNotFound {
            topic: topic.to_string(),
        })?;
    if channels.next().is_some() {
        return Err(McapReaderError::MultipleChannels {
            topic: topic.to_string(),
        });
    }

    let schema = channel
        .schema
        .as_ref()
        .ok_or_else(|| McapReaderError::SchemaNotAvailable {
            topic: channel.topic.clone(),
            channel_id: channel.id,
        })?;
    Ok(schema.name.clone())
}

fn flush_batch<F>(
    schema: &SchemaRef,
    rows: &mut Vec<DecodedMessage>,
    callback: &mut F,
) -> Result<(), McapReaderError>
where
    F: FnMut(RecordBatch) -> Result<(), Box<dyn std::error::Error + Send + Sync>>,
{
    if rows.is_empty() {
        return Ok(());
    }

    let batch = arrow_value_rows_to_record_batch(schema, rows.as_slice());
    rows.clear();
    callback(batch).map_err(McapReaderError::Callback)
}

fn push_decoded_message<F>(
    batch_size: usize,
    schema: &SchemaRef,
    rows: &mut Vec<DecodedMessage>,
    decoded: DecodedMessage,
    callback: &mut F,
) -> Result<(), McapReaderError>
where
    F: FnMut(RecordBatch) -> Result<(), Box<dyn std::error::Error + Send + Sync>>,
{
    rows.push(decoded);
    if rows.len() >= batch_size {
        flush_batch(schema, rows, callback)?;
    }
    Ok(())
}
