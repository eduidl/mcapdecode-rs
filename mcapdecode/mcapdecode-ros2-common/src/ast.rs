//! AST types shared between the IDL and msg parsers.
//!
//! Both parsers produce these types, which are then consumed by
//! [`crate::type_resolver`] to produce a fully resolved schema.

use std::collections::HashMap;

use mcapdecode_core::EnumVariant;

/// Scalar primitive types supported by ROS 2 IDL and .msg formats.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PrimitiveType {
    Bool,
    I8,
    I16,
    I32,
    I64,
    U8,
    U16,
    U32,
    U64,
    F32,
    F64,
    /// Unbounded UTF-8 string.
    String,
    /// Unbounded UTF-16 wide string (not yet supported in CDR decoding).
    WString,
    /// Alias for `U8`; corresponds to `octet` in IDL.
    Octet,
}

/// A type expression as it appears in an IDL struct field or .msg field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TypeExpr {
    Primitive(PrimitiveType),
    /// A named (possibly scoped) type, e.g. `["geometry_msgs", "msg", "Point"]`.
    Scoped(Vec<String>),
    /// A sequence type (`sequence<T, N>` / `T[<=N]` / `T[]`).
    Sequence {
        elem: Box<TypeExpr>,
        /// `None` for unbounded; `Some(n)` for bounded.
        max_len: Option<usize>,
    },
    /// `string<N>` — UTF-8 string with a maximum byte length of `N`.
    BoundedString(usize),
    /// `wstring<N>` — wide string with a maximum character count of `N`.
    BoundedWString(usize),
}

/// A single field inside a struct definition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FieldDef {
    pub name: String,
    pub ty: TypeExpr,
    /// `Some(n)` means the field is a fixed-length array of `n` elements.
    pub fixed_len: Option<usize>,
}

/// A constant defined inside a struct (`const T NAME = VALUE;`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConstDef {
    pub ty: TypeExpr,
    pub name: String,
    /// Raw string representation of the constant value as it appeared in the source.
    pub value: String,
}

/// A fully-parsed struct definition with its qualified name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StructDef {
    /// Fully-qualified name segments, e.g. `["geometry_msgs", "msg", "Point"]`.
    pub full_name: Vec<String>,
    pub fields: Vec<FieldDef>,
    pub consts: Vec<ConstDef>,
}

/// A fully-parsed enum definition with its qualified name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnumDef {
    /// Fully-qualified name segments.
    pub full_name: Vec<String>,
    /// Named numeric values in declaration order.
    pub variants: Vec<EnumVariant>,
}

/// All structs and enums extracted from one IDL section or .msg file.
#[derive(Debug, Clone, Default)]
pub struct ParsedSection {
    pub structs: HashMap<Vec<String>, StructDef>,
    pub enums: HashMap<Vec<String>, EnumDef>,
}
