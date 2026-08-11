//! Conversion from decoded `DecodedMessage` rows to Arrow `RecordBatch`.
//!
//! The output schema comes from a [`MessageBatchSchema`], so the metadata
//! columns are named by the policy that schema was built with.

mod append;
mod builder;
mod scalar;

use arrow::{array::ArrayRef, datatypes::DataType, record_batch::RecordBatch};
use mcapdecode_core::{DecodedMessage, Value};

use crate::{batch_schema::MessageBatchSchema, error::ArrowConvertError};

impl MessageBatchSchema {
    /// Convert decoded rows to a `RecordBatch` with this schema.
    ///
    /// Metadata fields and arrays come from the same `MetadataColumns` column
    /// definitions, so their order and count cannot drift.
    ///
    /// # Errors
    /// Returns an error if `rows` is empty, if a value's shape does not match
    /// its field's Arrow data type, or if Arrow rejects the assembled batch.
    ///
    /// # Panics
    /// Panics if the body schema contains a data type unsupported by this
    /// crate's array builders, or a malformed `Map` entry field. It also
    /// panics if a row root value is neither `Struct` nor `Null`, or if
    /// `log_time` or `publish_time` exceeds `i64::MAX` nanoseconds.
    pub fn to_record_batch(
        &self,
        rows: &[DecodedMessage],
    ) -> Result<RecordBatch, ArrowConvertError> {
        if rows.is_empty() {
            return Err(ArrowConvertError::EmptyRows);
        }

        let body_fields = self.body().fields();
        let mut arrays = self.metadata().arrays(rows);

        for (i, field) in body_fields.iter().enumerate() {
            let values: Vec<&Value> = rows.iter().map(|r| extract_field(&r.value, i)).collect();
            arrays.push(build_array_from_values(field.data_type(), &values)?);
        }

        Ok(RecordBatch::try_new(self.schema().clone(), arrays)?)
    }
}

fn extract_field(root: &Value, field_index: usize) -> &Value {
    match root {
        Value::Struct(children) => children.get(field_index).unwrap_or(&Value::Null),
        Value::Null => &Value::Null,
        other => panic!("expected Struct or Null as message root, got {other:?}"),
    }
}

fn build_array_from_values(
    dt: &DataType,
    values: &[&Value],
) -> Result<ArrayRef, ArrowConvertError> {
    let capacity = match dt {
        DataType::List(_) | DataType::Map(_, _) => values.len().saturating_mul(4),
        _ => values.len(),
    };
    let mut builder = builder::make_builder(dt, capacity);
    for value in values {
        append::append_value_to_builder(&mut builder, dt, value)?;
    }
    Ok(builder.finish())
}
