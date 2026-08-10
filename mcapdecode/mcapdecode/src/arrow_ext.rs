use std::{path::Path, sync::Arc};

use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use mcapdecode_arrow::{arrow_value_rows_to_record_batch, field_defs_to_arrow_schema};
use mcapdecode_core::DecodedMessage;

use crate::{McapReader, McapReaderError, ReadOptions};

/// Options for Arrow RecordBatch reads.
///
/// Batch size belongs here rather than on [`McapReaderBuilder`] because it
/// describes the shape of the Arrow output, not how the reader scans a file.
/// One reader can therefore serve reads that want different batch sizes.
///
/// [`McapReaderBuilder`]: crate::McapReaderBuilder
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordBatchOptions {
    /// Filtering and traversal options applied to the decoded messages.
    pub read_options: ReadOptions,
    /// Maximum number of decoded messages in one emitted RecordBatch.
    ///
    /// `0` is treated as `1`, so every decoded message is emitted immediately.
    pub batch_size: usize,
}

impl Default for RecordBatchOptions {
    fn default() -> Self {
        Self {
            read_options: ReadOptions::default(),
            batch_size: 1024,
        }
    }
}

impl RecordBatchOptions {
    fn effective_batch_size(&self) -> usize {
        self.batch_size.max(1)
    }
}

/// Adds Arrow RecordBatch output to [`McapReader`].
///
/// This is a trait rather than an inherent impl because the reader is defined
/// outside the crate that owns the Arrow conversion, so the orphan rule forbids
/// an inherent impl here. Import the trait to call these methods.
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

    /// Read messages for a topic and emit Arrow RecordBatches subject to
    /// `options`.
    fn for_each_record_batch_with_options(
        &self,
        path: &Path,
        topic: &str,
        options: &RecordBatchOptions,
        callback: impl FnMut(RecordBatch) -> Result<(), Box<dyn std::error::Error + Send + Sync>>,
    ) -> Result<(), McapReaderError>;
}

impl McapReaderArrowExt for McapReader {
    fn for_each_record_batch(
        &self,
        path: &Path,
        topic: &str,
        callback: impl FnMut(RecordBatch) -> Result<(), Box<dyn std::error::Error + Send + Sync>>,
    ) -> Result<(), McapReaderError> {
        self.for_each_record_batch_with_options(
            path,
            topic,
            &RecordBatchOptions::default(),
            callback,
        )
    }

    fn for_each_record_batch_with_options(
        &self,
        path: &Path,
        topic: &str,
        options: &RecordBatchOptions,
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
            let batch_size = options.effective_batch_size();
            let mut rows = Vec::with_capacity(batch_size);
            prepared.for_each_decoded_message_with_options(&options.read_options, |decoded| {
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
