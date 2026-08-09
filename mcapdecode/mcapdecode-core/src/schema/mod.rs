//! Arrow-independent schema intermediate representation.

mod format;
mod types;

pub use format::format_field_defs;
pub use types::{DataTypeDef, ElementDef, EnumVariant, FieldDef, FieldDefs};
