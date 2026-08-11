use arrow::error::ArrowError;
use mcapdecode_core::ValueTypeError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ArrowConvertError {
    #[error("Cannot create RecordBatch from empty rows")]
    EmptyRows,
    #[error(
        "metadata columns [{names}] collide with payload fields; set a metadata prefix to disambiguate"
    )]
    MetadataColumnCollision { names: String },
    #[error("value type mismatch: {0}")]
    ValueType(#[from] ValueTypeError),
    #[error(transparent)]
    Arrow(#[from] ArrowError),
}
