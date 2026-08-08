//! Arrow integration layer for `mcapdecode`.
//!
//! This crate focuses on two responsibilities:
//! 1. Convert `mcapdecode-core` schema IR (`FieldDef`) to Arrow `Schema`.
//! 2. Convert decoded `DecodedMessage` rows into Arrow `RecordBatch`.
//!
//! `mcapdecode-arrow` intentionally keeps the public API small and exposes only
//! two entry points:
//! - [`field_defs_to_arrow_schema`] for schema conversion.
//! - [`arrow_value_rows_to_record_batch`] / [`try_arrow_value_rows_to_record_batch`]
//!   for row-to-batch conversion.
//!
//! Both conversions follow the conventions used by this project:
//! - Timestamp columns are represented as nanosecond `Timestamp` with `UTC`.
//! - `RecordBatch` output prepends `@log_time` and `@publish_time`.
//!
//! # Typical Flow
//! ```rust
//! use mcapdecode_arrow::{arrow_value_rows_to_record_batch, field_defs_to_arrow_schema};
//! use mcapdecode_core::{DecodedMessage, FieldDefs};
//!
//! # let field_defs = FieldDefs::default();
//! # let rows: Vec<DecodedMessage> = vec![];
//! let body_schema = field_defs_to_arrow_schema(&field_defs);
//! // rows must not be empty.
//! if !rows.is_empty() {
//!     let _batch = arrow_value_rows_to_record_batch(&body_schema, &rows);
//! }
//! ```
pub mod arrow_convert;
pub mod error;
pub mod flatten;
pub mod projection;
pub mod schema_convert;

/// Re-export of [`arrow_convert::arrow_value_rows_to_record_batch`].
pub use arrow_convert::arrow_value_rows_to_record_batch;
/// Re-exports from [`arrow_convert`].
pub use arrow_convert::try_arrow_value_rows_to_record_batch;
/// Re-export of [`error::ArrowConvertError`].
pub use error::ArrowConvertError;
/// Re-exports from [`flatten`].
pub use flatten::{
    ArrayPolicy, FlattenPolicy, ListPolicy, MapPolicy, StructPolicy, flatten_record_batch,
};
/// Re-export of [`projection::project_record_batch`].
pub use projection::project_record_batch;
/// Re-export of [`schema_convert::field_defs_to_arrow_schema`].
pub use schema_convert::{field_defs_to_arrow_schema, field_defs_to_record_batch_schema};

pub(crate) const TIMESTAMP_TZ: &str = "+00:00";
