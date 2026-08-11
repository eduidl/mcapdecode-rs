//! DataFusion table-provider support for [`crate::McapReader`].

use std::{path::Path, sync::Arc};

use datafusion::{catalog::TableProvider, datasource::MemTable};

use crate::{McapReader, McapReaderError, RecordBatchOptions};

impl McapReader {
    /// Materialize one decoded topic as a DataFusion table.
    ///
    /// This feature-gated convenience API owns all MCAP access. The returned
    /// provider is independent of the reader and can be registered with a
    /// `datafusion::execution::context::SessionContext`.
    pub fn datafusion_table(
        &self,
        path: &Path,
        topic: &str,
        options: &RecordBatchOptions,
    ) -> Result<Arc<dyn TableProvider>, McapReaderError> {
        self.with_prepared_topic(path, topic, |prepared| {
            let batch_schema = prepared.batch_schema(options)?;
            let schema = batch_schema.schema().clone();
            let mut batches = Vec::new();
            prepared.for_each_record_batch_with_schema(&batch_schema, options, &mut |batch| {
                batches.push(batch);
                Ok(())
            })?;
            Ok(Arc::new(MemTable::try_new(schema, vec![batches])?) as Arc<dyn TableProvider>)
        })
    }
}
