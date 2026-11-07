use arrow::record_batch::RecordBatch;
use mcapdecode_arrow::{MessageBatchSchema, MetadataColumns};
use mcapdecode_core::DecodedMessage;

use crate::{McapReaderError, ReadOptions};

impl crate::internal_reader::InternalReader {
    pub(crate) fn for_each_record_batch_impl(
        &self,
        file: &crate::mcap_file::McapFile,
        contexts: &[crate::decode::TopicDecodeContext],
        read_options: &ReadOptions,
        batch_size: usize,
        metadata: &MetadataColumns,
        callback: &mut impl FnMut(
            usize,
            RecordBatch,
        ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>,
    ) -> Result<(), McapReaderError> {
        let schemas = contexts
            .iter()
            .map(|context| batch_schema_for_context(context, metadata))
            .collect::<Result<Vec<_>, _>>()?;
        let mut rows: Vec<_> = contexts
            .iter()
            .map(|_| Vec::with_capacity(batch_size))
            .collect();
        self.for_each_topic_decoded_message_impl(
            file,
            contexts,
            read_options,
            &mut |i, message| {
                rows[i].push(message);
                if rows[i].len() >= batch_size {
                    flush_batch(&schemas[i], &mut rows[i], &mut |batch| callback(i, batch))?;
                }
                Ok(())
            },
        )?;
        for (i, rows) in rows.iter_mut().enumerate() {
            flush_batch(&schemas[i], rows, &mut |batch| callback(i, batch))?;
        }
        Ok(())
    }
}

pub(crate) fn batch_schema_for_context(
    context: &crate::decode::TopicDecodeContext,
    metadata: &MetadataColumns,
) -> Result<MessageBatchSchema, McapReaderError> {
    if context.field_defs.is_empty() {
        return Err(McapReaderError::EmptyDerivedSchema {
            schema_name: context.schema_name.clone(),
        });
    }
    Ok(MessageBatchSchema::from_field_defs(
        &context.field_defs,
        metadata.clone(),
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
