use std::path::Path;

use arrow::record_batch::RecordBatch;
use mcapdecode_arrow::{MessageBatchSchema, MetadataColumns};
use mcapdecode_core::DecodedMessage;

use crate::{McapReader, McapReaderError, PreparedTopic, ReadOptions};

/// Options for Arrow RecordBatch reads.
///
/// Batch size belongs here rather than on [`McapReaderBuilder`] because it
/// describes the shape of the Arrow output, not how the reader scans a file.
/// One reader can therefore serve reads that want different batch sizes. The
/// metadata column naming is here for the same reason.
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
    /// Naming of the system metadata columns prepended to every batch.
    pub metadata: MetadataColumns,
}

impl Default for RecordBatchOptions {
    fn default() -> Self {
        Self {
            read_options: ReadOptions::default(),
            batch_size: 1024,
            metadata: MetadataColumns::default(),
        }
    }
}

impl RecordBatchOptions {
    fn effective_batch_size(&self) -> usize {
        self.batch_size.max(1)
    }
}

impl McapReader {
    /// The Arrow schema that [`Self::for_each_record_batch_with_options`] emits
    /// for `topic` under `options`.
    ///
    /// Callers that must state the schema up front (a DataFusion `MemTable`, a
    /// Parquet writer) should take it from here rather than deriving their own,
    /// so the declared schema and the emitted batches cannot disagree.
    pub fn topic_batch_schema(
        &self,
        path: &Path,
        topic: &str,
        options: &RecordBatchOptions,
    ) -> Result<MessageBatchSchema, McapReaderError> {
        self.with_prepared_topic(path, topic, |prepared| prepared.batch_schema(options))
    }

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
        self.for_each_record_batch_with_options(
            path,
            topic,
            &RecordBatchOptions::default(),
            callback,
        )
    }

    /// Read messages for a topic and emit Arrow RecordBatches subject to
    /// `options`.
    pub fn for_each_record_batch_with_options(
        &self,
        path: &Path,
        topic: &str,
        options: &RecordBatchOptions,
        mut callback: impl FnMut(RecordBatch) -> Result<(), Box<dyn std::error::Error + Send + Sync>>,
    ) -> Result<(), McapReaderError> {
        self.with_prepared_topic(path, topic, |prepared| {
            // Built once, before the read, so every batch shares one schema.
            let batch_schema = prepared.batch_schema(options)?;
            prepared.for_each_record_batch_with_schema(&batch_schema, options, &mut callback)
        })
    }
}

impl PreparedTopic<'_> {
    /// Emit batches using a caller-provided schema derived from this topic.
    ///
    /// Keeping schema derivation and message traversal on the same prepared
    /// topic guarantees that consumers which need the schema up front, such as
    /// DataFusion's `MemTable`, receive batches built with that exact schema.
    pub(crate) fn for_each_record_batch_with_schema(
        &self,
        batch_schema: &MessageBatchSchema,
        options: &RecordBatchOptions,
        callback: &mut impl FnMut(RecordBatch) -> Result<(), Box<dyn std::error::Error + Send + Sync>>,
    ) -> Result<(), McapReaderError> {
        let batch_size = options.effective_batch_size();
        let mut rows = Vec::with_capacity(batch_size);
        self.for_each_decoded_message_with_options_internal(
            &options.read_options,
            &mut |decoded| {
                push_decoded_message(batch_size, batch_schema, &mut rows, decoded, callback)
            },
        )?;
        flush_batch(batch_schema, &mut rows, callback)
    }
}

/// The single place the emitted Arrow schema is derived, so
/// [`McapReader::topic_batch_schema`] and the batches cannot disagree.
pub(crate) fn batch_schema_for(
    prepared: &PreparedTopic<'_>,
    options: &RecordBatchOptions,
) -> Result<MessageBatchSchema, McapReaderError> {
    if prepared.field_defs().is_empty() {
        return Err(McapReaderError::EmptyDerivedSchema {
            schema_name: prepared.schema_name().to_string(),
        });
    }
    Ok(MessageBatchSchema::from_field_defs(
        prepared.field_defs(),
        options.metadata.clone(),
    )?)
}

fn flush_batch<F>(
    batch_schema: &MessageBatchSchema,
    rows: &mut Vec<DecodedMessage>,
    callback: &mut F,
) -> Result<(), McapReaderError>
where
    F: FnMut(RecordBatch) -> Result<(), Box<dyn std::error::Error + Send + Sync>>,
{
    if rows.is_empty() {
        return Ok(());
    }

    let batch = batch_schema.to_record_batch(rows.as_slice())?;
    rows.clear();
    callback(batch).map_err(McapReaderError::Callback)
}

fn push_decoded_message<F>(
    batch_size: usize,
    batch_schema: &MessageBatchSchema,
    rows: &mut Vec<DecodedMessage>,
    decoded: DecodedMessage,
    callback: &mut F,
) -> Result<(), McapReaderError>
where
    F: FnMut(RecordBatch) -> Result<(), Box<dyn std::error::Error + Send + Sync>>,
{
    rows.push(decoded);
    if rows.len() >= batch_size {
        flush_batch(batch_schema, rows, callback)?;
    }
    Ok(())
}
