use std::{path::Path, sync::Arc};

use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use mcapdecode_arrow::{arrow_value_rows_to_record_batch, field_defs_to_arrow_schema};
use mcapdecode_core::DecodedMessage;

use crate::{McapReader, McapReaderError, ReadOptions};

impl McapReader {
    /// Read all messages for a topic and emit Arrow RecordBatches to callback.
    ///
    /// Chunks in the MCAP file are decompressed in parallel using rayon.
    /// Message decoding and Arrow conversion remain sequential.
    pub fn for_each_record_batch(
        &self,
        path: &Path,
        topic: &str,
        callback: impl FnMut(RecordBatch) -> Result<(), Box<dyn std::error::Error + Send + Sync>>,
    ) -> Result<(), McapReaderError> {
        self.for_each_record_batch_with_options(path, topic, &ReadOptions::default(), callback)
    }

    /// Read filtered messages for a topic and emit Arrow RecordBatches to callback.
    pub fn for_each_record_batch_with_options(
        &self,
        path: &Path,
        topic: &str,
        options: &ReadOptions,
        mut callback: impl FnMut(RecordBatch) -> Result<(), Box<dyn std::error::Error + Send + Sync>>,
    ) -> Result<(), McapReaderError> {
        self.with_prepared_topic(path, topic, |prepared| {
            if prepared.field_defs().is_empty() {
                return Err(McapReaderError::EmptyDerivedSchema {
                    topic: topic.to_string(),
                    schema_name: prepared.schema_name().to_string(),
                });
            }

            let schema = Arc::new(field_defs_to_arrow_schema(prepared.field_defs()));
            let batch_size = self.batch_size();
            let mut rows = Vec::with_capacity(batch_size);
            prepared.for_each_decoded_message_with_options(options, |decoded| {
                push_decoded_message(batch_size, &schema, &mut rows, decoded, &mut callback)
            })?;
            flush_batch(&schema, &mut rows, &mut callback).map_err(McapReaderError::Callback)
        })
    }
}

/// Errors surface as the callback's own error so the reader wraps them in
/// [`McapReaderError::Callback`] exactly once.
fn flush_batch<F>(
    schema: &SchemaRef,
    rows: &mut Vec<DecodedMessage>,
    callback: &mut F,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    F: FnMut(RecordBatch) -> Result<(), Box<dyn std::error::Error + Send + Sync>>,
{
    if rows.is_empty() {
        return Ok(());
    }

    let batch = arrow_value_rows_to_record_batch(schema, rows.as_slice());
    rows.clear();
    callback(batch)
}

fn push_decoded_message<F>(
    batch_size: usize,
    schema: &SchemaRef,
    rows: &mut Vec<DecodedMessage>,
    decoded: DecodedMessage,
    callback: &mut F,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    F: FnMut(RecordBatch) -> Result<(), Box<dyn std::error::Error + Send + Sync>>,
{
    rows.push(decoded);
    if rows.len() >= batch_size {
        flush_batch(schema, rows, callback)?;
    }
    Ok(())
}
