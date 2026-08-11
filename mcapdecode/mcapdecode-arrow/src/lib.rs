//! Arrow integration layer for `mcapdecode`.
//!
//! This crate focuses on two responsibilities:
//! 1. Convert `mcapdecode-core` schema IR (`FieldDef`) to Arrow `Schema`.
//! 2. Convert decoded `DecodedMessage` rows into Arrow `RecordBatch`.
//!
//! `mcapdecode-arrow` intentionally keeps the public API small. Both entry
//! points hang off [`MessageBatchSchema`], which pairs a message body schema
//! with the naming of the system metadata columns prepended to it:
//! - [`MessageBatchSchema::from_field_defs`] for schema conversion.
//! - [`MessageBatchSchema::to_record_batch`] for row-to-batch conversion.
//!
//! Holding both in one value is what keeps the schema a consumer is handed
//! identical to the schema of the batches produced from it.
//!
//! Both conversions follow the conventions used by this project:
//! - Timestamp columns default to nanosecond `Timestamp` with `UTC`, or can be
//!   emitted as Unix-epoch-nanosecond `Int64` through [`MetadataColumns`].
//! - `RecordBatch` output prepends `log_time` and `publish_time`, named by a
//!   [`MetadataColumns`] policy that defaults to no prefix.
//!
//! # Typical Flow
//! ```rust
//! use mcapdecode_arrow::{MessageBatchSchema, MetadataColumns};
//! use mcapdecode_core::{DecodedMessage, FieldDefs};
//!
//! # fn main() -> Result<(), mcapdecode_arrow::ArrowConvertError> {
//! # let field_defs = FieldDefs::default();
//! # let rows: Vec<DecodedMessage> = vec![];
//! let batch_schema =
//!     MessageBatchSchema::from_field_defs(&field_defs, MetadataColumns::default())?;
//! // rows must not be empty.
//! if !rows.is_empty() {
//!     let _batch = batch_schema.to_record_batch(&rows)?;
//! }
//! # Ok(())
//! # }
//! ```
pub mod arrow_convert;
pub mod batch_schema;
pub mod error;
pub mod flatten;
pub mod json;
pub mod projection;
pub mod schema_convert;

/// Re-exports from [`batch_schema`].
pub use batch_schema::{MessageBatchSchema, MetadataColumns, MetadataTimestampFormat};
/// Re-export of [`error::ArrowConvertError`].
pub use error::ArrowConvertError;
/// Re-exports from [`flatten`].
pub use flatten::{
    ArrayPolicy, FlattenPolicy, ListPolicy, MapPolicy, StructPolicy, flatten_record_batch,
};
/// Re-export of [`json::JsonlWriter`].
pub use json::JsonlWriter;
/// Re-export of [`projection::project_record_batch`].
pub use projection::project_record_batch;
/// Re-export of [`schema_convert::field_defs_to_arrow_schema`].
pub use schema_convert::field_defs_to_arrow_schema;

pub(crate) const TIMESTAMP_TZ: &str = "+00:00";
