use std::path::Path;

use arrow::record_batch::RecordBatch;
use mcapdecode_arrow::{MessageBatchSchema, MetadataColumns};
use mcapdecode_core::DecodedMessage;

use crate::{McapReader, McapReaderError, ReadOptions};

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
    /// Callers that must state the schema up front, such as a Parquet writer,
    /// should take it from here rather than deriving their own,
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
        self.for_each_topic_record_batch_with_options(path, &[topic], options, |_, batch| {
            callback(batch)
        })
    }

    /// Read selected topics as Arrow batches, sharing chunk decompression.
    /// Rows remain in file order within each topic; empty topics emit no batches.
    /// Filtering, offset and limit apply independently to each topic.
    /// Duplicate topics are read once.
    pub fn for_each_topic_record_batch_with_options(
        &self,
        path: &Path,
        topics: &[&str],
        options: &RecordBatchOptions,
        mut callback: impl FnMut(
            &str,
            RecordBatch,
        ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>,
    ) -> Result<(), McapReaderError> {
        if topics.is_empty() {
            return Ok(());
        }
        self.with_topic_contexts(path, topics, |mmap, summary, topics, contexts| {
            let schemas: Vec<_> = contexts
                .iter()
                .map(|context| batch_schema_for_context(context, options))
                .collect::<Result<_, _>>()?;
            let batch_size = options.effective_batch_size();
            let mut rows: Vec<_> = topics
                .iter()
                .map(|_| Vec::with_capacity(batch_size))
                .collect();
            self.for_each_topic_decoded_message_impl(
                mmap,
                summary,
                contexts,
                &options.read_options,
                &mut |i, message| {
                    rows[i].push(message);
                    if rows[i].len() >= batch_size {
                        flush_batch(&schemas[i], &mut rows[i], &mut |batch| {
                            callback(topics[i], batch)
                        })?;
                    }
                    Ok(())
                },
            )?;
            for (i, row_buffer) in rows.iter_mut().enumerate() {
                flush_batch(&schemas[i], row_buffer, &mut |batch| {
                    callback(topics[i], batch)
                })?;
            }
            Ok(())
        })
    }
}

pub(crate) fn batch_schema_for_context(
    context: &crate::decode::TopicDecodeContext,
    options: &RecordBatchOptions,
) -> Result<MessageBatchSchema, McapReaderError> {
    if context.field_defs.is_empty() {
        return Err(McapReaderError::EmptyDerivedSchema {
            schema_name: context.schema_name.clone(),
        });
    }
    Ok(MessageBatchSchema::from_field_defs(
        &context.field_defs,
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
