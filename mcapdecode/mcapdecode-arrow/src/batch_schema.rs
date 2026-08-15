//! The Arrow schema emitted for one topic, and the naming of its system columns.

use std::sync::Arc;

use arrow::{
    array::{ArrayRef, Int64Array, TimestampNanosecondArray},
    datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit},
};
use mcapdecode_core::{DecodedMessage, FieldDefs};

use crate::{error::ArrowConvertError, schema_convert::field_defs_to_arrow_schema};

/// Physical Arrow type used for MCAP record timestamps.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum MetadataTimestampFormat {
    /// Arrow's timezone-aware nanosecond timestamp type.
    #[default]
    TimestampNanosecond,
    /// A signed integer number of nanoseconds since the Unix epoch.
    UnixNanoseconds,
}

/// Naming and timestamp policy for the system metadata columns prepended to every batch.
///
/// The columns carry MCAP record fields that live outside the message payload,
/// so their names share a namespace with the payload's own top-level fields. A
/// prefix is how a caller keeps the two apart when they would otherwise collide.
/// The default is no prefix and Arrow nanosecond timestamps.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MetadataColumns {
    prefix: String,
    timestamp_format: MetadataTimestampFormat,
}

#[derive(Clone, Copy)]
enum MetadataColumn {
    LogTime,
    PublishTime,
}

impl MetadataColumn {
    const ALL: [Self; 2] = [Self::LogTime, Self::PublishTime];

    fn name(self) -> &'static str {
        match self {
            Self::LogTime => "log_time",
            Self::PublishTime => "publish_time",
        }
    }

    fn field(self, prefix: &str, timestamp_format: MetadataTimestampFormat) -> Field {
        let data_type = match timestamp_format {
            MetadataTimestampFormat::TimestampNanosecond => {
                DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from(crate::TIMESTAMP_TZ)))
            }
            MetadataTimestampFormat::UnixNanoseconds => DataType::Int64,
        };
        Field::new(
            format!("{}{name}", prefix, name = self.name()),
            data_type,
            false,
        )
    }

    fn values(self, rows: &[DecodedMessage]) -> Vec<i64> {
        match self {
            Self::LogTime => rows
                .iter()
                .map(|row| i64::try_from(row.log_time).expect("log_time exceeds i64::MAX"))
                .collect(),
            Self::PublishTime => rows
                .iter()
                .map(|row| i64::try_from(row.publish_time).expect("publish_time exceeds i64::MAX"))
                .collect(),
        }
    }

    fn array(self, rows: &[DecodedMessage], timestamp_format: MetadataTimestampFormat) -> ArrayRef {
        let values = self.values(rows);
        match timestamp_format {
            MetadataTimestampFormat::TimestampNanosecond => {
                Arc::new(TimestampNanosecondArray::from(values).with_timezone(crate::TIMESTAMP_TZ))
            }
            MetadataTimestampFormat::UnixNanoseconds => Arc::new(Int64Array::from(values)),
        }
    }
}

impl MetadataColumns {
    /// Prefix every metadata column name with `prefix`.
    pub fn with_prefix(prefix: impl Into<String>) -> Self {
        Self {
            prefix: prefix.into(),
            ..Self::default()
        }
    }

    /// Choose how the MCAP record timestamps are represented in Arrow batches.
    pub fn with_timestamp_format(mut self, timestamp_format: MetadataTimestampFormat) -> Self {
        self.timestamp_format = timestamp_format;
        self
    }

    /// The prefix applied to every metadata column.
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    /// The physical Arrow type used for the MCAP record timestamps.
    pub fn timestamp_format(&self) -> MetadataTimestampFormat {
        self.timestamp_format
    }

    /// The emitted column names, in the order they are emitted.
    pub fn names(&self) -> [String; 2] {
        MetadataColumn::ALL.map(|column| format!("{}{name}", self.prefix, name = column.name()))
    }

    fn fields(&self) -> Vec<Field> {
        MetadataColumn::ALL
            .iter()
            .map(|column| column.field(&self.prefix, self.timestamp_format))
            .collect()
    }

    pub(crate) fn arrays(&self, rows: &[DecodedMessage]) -> Vec<ArrayRef> {
        MetadataColumn::ALL
            .iter()
            .map(|column| column.array(rows, self.timestamp_format))
            .collect()
    }
}

/// The Arrow schema of one topic's `RecordBatch`es: metadata columns, then body
/// fields.
///
/// Both halves live in one value so that the schema handed to a consumer is by
/// construction the schema of the batches produced from it. Deriving the two
/// separately lets them drift, and the mismatch only surfaces at run time.
#[derive(Debug, Clone)]
pub struct MessageBatchSchema {
    schema: SchemaRef,
    body: SchemaRef,
    metadata: MetadataColumns,
}

impl MessageBatchSchema {
    /// Prepend `metadata`'s columns to a message body schema.
    ///
    /// # Errors
    /// Returns [`ArrowConvertError::MetadataColumnCollision`] if a body field
    /// has the same name as a metadata column. Both would be reachable only by
    /// position, so this is rejected rather than silently emitted.
    pub fn new(body: Schema, metadata: MetadataColumns) -> Result<Self, ArrowConvertError> {
        let names = metadata.names();
        let collisions: Vec<&str> = names
            .iter()
            .filter(|name| body.column_with_name(name).is_some())
            .map(String::as_str)
            .collect();
        if !collisions.is_empty() {
            return Err(ArrowConvertError::MetadataColumnCollision {
                names: collisions.join(", "),
            });
        }

        let mut fields = metadata.fields();
        fields.extend(body.fields().iter().map(|f| f.as_ref().clone()));
        Ok(Self {
            schema: Arc::new(Schema::new_with_metadata(fields, body.metadata().clone())),
            body: Arc::new(body),
            metadata,
        })
    }

    /// Convert `mcapdecode-core` schema IR into a batch schema.
    ///
    /// # Errors
    /// As [`Self::new`].
    pub fn from_field_defs(
        fields: &FieldDefs,
        metadata: MetadataColumns,
    ) -> Result<Self, ArrowConvertError> {
        Self::new(field_defs_to_arrow_schema(fields), metadata)
    }

    /// The emitted schema: metadata columns followed by body fields.
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// The message body schema, without metadata columns.
    pub fn body(&self) -> &SchemaRef {
        &self.body
    }

    /// The naming policy this schema was built with.
    pub fn metadata(&self) -> &MetadataColumns {
        &self.metadata
    }
}
