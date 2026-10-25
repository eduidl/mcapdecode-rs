use std::{
    fmt::{Display, Formatter, Result},
    ops::Deref,
};

/// Arrow-independent data type definition for schema intermediate representation.
///
/// Variant names mirror [`Value`](crate::Value) for consistency (values ↔ types).
#[derive(Debug, Clone, PartialEq)]
pub enum DataTypeDef {
    Null,
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
    String,
    WString,
    BoundedString(usize),
    BoundedWString(usize),
    Bytes,
    BoundedBytes(usize),
    Enum(EnumDef),
    Struct(StructDef),
    List(Box<ElementDef>),
    BoundedList(Box<ElementDef>, usize),
    Array(Box<ElementDef>, usize),
    Map {
        key: Box<ElementDef>,
        value: Box<ElementDef>,
    },
}

impl DataTypeDef {
    pub fn is_primitive(&self) -> bool {
        !matches!(
            self,
            DataTypeDef::Struct(_)
                | DataTypeDef::List(_)
                | DataTypeDef::BoundedList(_, _)
                | DataTypeDef::Enum(_)
                | DataTypeDef::Array(_, _)
                | DataTypeDef::Map { .. }
        )
    }

    /// Category name, independent of qualified names, bounds, and nullability.
    pub fn type_name(&self) -> &'static str {
        match self {
            DataTypeDef::Null => "null",
            DataTypeDef::Bool => "bool",
            DataTypeDef::I8 => "int8",
            DataTypeDef::I16 => "int16",
            DataTypeDef::I32 => "int32",
            DataTypeDef::I64 => "int64",
            DataTypeDef::U8 => "uint8",
            DataTypeDef::U16 => "uint16",
            DataTypeDef::U32 => "uint32",
            DataTypeDef::U64 => "uint64",
            DataTypeDef::F32 => "float32",
            DataTypeDef::F64 => "float64",
            DataTypeDef::String => "string",
            DataTypeDef::WString => "wstring",
            DataTypeDef::BoundedString(_) => "string",
            DataTypeDef::BoundedWString(_) => "wstring",
            DataTypeDef::Bytes => "bytes",
            DataTypeDef::BoundedBytes(_) => "bytes",
            DataTypeDef::Enum(_) => "enum",
            DataTypeDef::Struct(_) => "struct",
            DataTypeDef::List(_) => "list",
            DataTypeDef::BoundedList(_, _) => "list",
            DataTypeDef::Array(_, _) => "array",
            DataTypeDef::Map { .. } => "map",
        }
    }
}

/// A struct definition with an optional qualified name from the source schema.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct StructDef {
    pub type_name: Option<String>,
    pub fields: FieldDefs,
}

impl From<FieldDefs> for StructDef {
    fn from(fields: FieldDefs) -> Self {
        Self {
            type_name: None,
            fields,
        }
    }
}

impl From<Vec<FieldDef>> for StructDef {
    fn from(fields: Vec<FieldDef>) -> Self {
        FieldDefs::new(fields).into()
    }
}

/// An enum definition with an optional qualified name from the source schema.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct EnumDef {
    pub type_name: Option<String>,
    pub variants: Vec<EnumVariant>,
}

impl From<Vec<EnumVariant>> for EnumDef {
    fn from(variants: Vec<EnumVariant>) -> Self {
        Self {
            type_name: None,
            variants,
        }
    }
}

/// A named numeric value in an enum definition.
///
/// The numeric value is preserved from the source schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnumVariant {
    pub name: String,
    pub value: i64,
}

impl EnumVariant {
    pub fn new(name: impl Into<String>, value: i64) -> Self {
        Self {
            name: name.into(),
            value,
        }
    }
}

impl Display for DataTypeDef {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match self {
            DataTypeDef::Struct(definition) => {
                f.write_str(definition.type_name.as_deref().unwrap_or("struct"))
            }
            DataTypeDef::Enum(definition) => {
                f.write_str(definition.type_name.as_deref().unwrap_or("enum"))
            }
            DataTypeDef::List(element) => write!(f, "{}[]", element),
            DataTypeDef::BoundedList(element, size) => {
                write!(f, "{}[<={size}]", element)
            }
            DataTypeDef::Array(element, size) => write!(f, "{}[{size}]", element),
            DataTypeDef::BoundedString(size) => write!(f, "string[<={size}]"),
            DataTypeDef::BoundedWString(size) => write!(f, "wstring[<={size}]"),
            DataTypeDef::BoundedBytes(size) => write!(f, "bytes[<={size}]"),
            DataTypeDef::Map { key, value } => {
                write!(f, "map<{}, {}>", key, value)
            }
            _ => f.write_str(self.type_name()),
        }
    }
}

/// Typed collection of [`FieldDef`] used for schema bodies and struct members.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct FieldDefs(pub Vec<FieldDef>);

impl FieldDefs {
    pub fn new(fields: Vec<FieldDef>) -> Self {
        Self(fields)
    }

    pub fn as_slice(&self) -> &[FieldDef] {
        &self.0
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = &FieldDef> {
        self.0.iter()
    }
}

impl From<Vec<FieldDef>> for FieldDefs {
    fn from(value: Vec<FieldDef>) -> Self {
        Self(value)
    }
}

impl From<FieldDefs> for Vec<FieldDef> {
    fn from(value: FieldDefs) -> Self {
        value.0
    }
}

impl AsRef<[FieldDef]> for FieldDefs {
    fn as_ref(&self) -> &[FieldDef] {
        self.as_slice()
    }
}

impl Deref for FieldDefs {
    type Target = [FieldDef];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl Display for FieldDefs {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        let text = super::format_field_defs(self.as_slice())?;
        f.write_str(&text)
    }
}

/// Arrow-independent nested element definition used in composite types.
#[derive(Debug, Clone, PartialEq)]
pub struct ElementDef {
    pub data_type: DataTypeDef,
    pub nullable: bool,
}

impl ElementDef {
    pub fn new(data_type: DataTypeDef, nullable: bool) -> Self {
        Self {
            data_type,
            nullable,
        }
    }
}

impl Display for ElementDef {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        if self.nullable {
            write!(f, "{}?", self.data_type)
        } else {
            Display::fmt(&self.data_type, f)
        }
    }
}

/// Arrow-independent field definition for schema intermediate representation.
#[derive(Debug, Clone, PartialEq)]
pub struct FieldDef {
    pub name: String,
    pub element: ElementDef,
}

impl FieldDef {
    pub fn new(name: impl Into<String>, data_type: DataTypeDef, nullable: bool) -> Self {
        Self {
            name: name.into(),
            element: ElementDef::new(data_type, nullable),
        }
    }
}
