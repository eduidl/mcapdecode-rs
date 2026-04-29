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
    Bytes,
    Struct(FieldDefs),
    List(Box<ElementDef>),
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
                | DataTypeDef::Array(_, _)
                | DataTypeDef::Map { .. }
        )
    }

    pub fn type_name(&self) -> &'static str {
        match self {
            DataTypeDef::Null => "null",
            DataTypeDef::Bool => "bool",
            DataTypeDef::I8 => "i8",
            DataTypeDef::I16 => "i16",
            DataTypeDef::I32 => "i32",
            DataTypeDef::I64 => "i64",
            DataTypeDef::U8 => "u8",
            DataTypeDef::U16 => "u16",
            DataTypeDef::U32 => "u32",
            DataTypeDef::U64 => "u64",
            DataTypeDef::F32 => "f32",
            DataTypeDef::F64 => "f64",
            DataTypeDef::String => "string",
            DataTypeDef::Bytes => "bytes",
            DataTypeDef::Struct(_) => "struct",
            DataTypeDef::List(_) => "list",
            DataTypeDef::Array(_, _) => "array",
            DataTypeDef::Map { .. } => "map",
        }
    }
}

impl Display for DataTypeDef {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match self {
            DataTypeDef::Array(_, size) => write!(f, "array[{size}]"),
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
            write!(f, "optional {}", self.data_type)
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
