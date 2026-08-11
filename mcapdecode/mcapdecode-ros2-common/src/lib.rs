//! Shared types, CDR decoding, and schema conversion used by both
//! `mcapdecode-ros2idl` and `mcapdecode-ros2msg`.
//!
//! Key components:
//! - [`ast`] — AST types produced by IDL/msg parsers
//! - [`cdr`] — CDR byte stream → [`mcapdecode_core::Value`] decoder
//! - [`schema`] — [`type_resolver::ResolvedSchema`] → [`mcapdecode_core::FieldDefs`] conversion
//! - [`type_resolver`] — type-name resolution and injection of ROS 2 builtin types

pub mod ast;
mod cdr;
mod decoder;
mod error;
mod schema;
mod schema_bundle;
mod topic_decoder;
mod type_resolver;

pub use ast::{ConstDef, EnumDef, FieldDef, ParsedSection, PrimitiveType, StructDef, TypeExpr};
pub use cdr::decode_cdr_to_value;
pub use decoder::{ResolveSchemaFn, build_cdr_topic_decoder, resolve_for_cdr};
pub use error::Ros2Error;
pub use schema::resolved_schema_to_field_defs;
pub use schema_bundle::{
    SchemaBundle, SchemaSection, normalize_schema_name, split_schema_sections,
};
pub use topic_decoder::Ros2CdrTopicDecoder;
pub use type_resolver::{
    ResolvedField, ResolvedSchema, ResolvedStruct, ResolvedType, ensure_builtin_structs,
    resolve_parsed_section, resolve_single_struct, resolve_struct,
};
