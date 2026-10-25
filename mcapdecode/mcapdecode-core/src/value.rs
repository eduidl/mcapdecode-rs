//! Type-safe intermediate representation produced by message decoders.

use std::sync::Arc;

use crate::error::ValueTypeError;

/// Value produced by message decoders.
/// All types are explicit; no lossy conversions.
#[derive(Debug, Clone)]
pub enum Value {
    Null,
    Bool(bool),
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    F32(f32),
    F64(f64),
    String(Arc<str>),
    Bytes(Arc<[u8]>),
    Struct(Vec<Value>),
    List(Vec<Value>),
    /// Packed unsigned byte sequence, distinct from opaque binary data.
    Uint8List(Arc<[u8]>),
    Array(Vec<Value>),
    Map(Vec<(Value, Value)>),
}

impl Value {
    pub fn string(s: impl AsRef<str>) -> Self {
        Self::String(Arc::from(s.as_ref()))
    }

    pub fn try_i8(&self) -> Result<Option<i8>, ValueTypeError> {
        match self {
            Value::I8(v) => Ok(Some(*v)),
            Value::Null => Ok(None),
            _ => Err(self.type_mismatch("I8")),
        }
    }

    pub fn try_i16(&self) -> Result<Option<i16>, ValueTypeError> {
        match self {
            Value::I16(v) => Ok(Some(*v)),
            Value::Null => Ok(None),
            _ => Err(self.type_mismatch("I16")),
        }
    }

    pub fn try_i32(&self) -> Result<Option<i32>, ValueTypeError> {
        match self {
            Value::I32(v) => Ok(Some(*v)),
            Value::Null => Ok(None),
            _ => Err(self.type_mismatch("I32")),
        }
    }

    pub fn try_i64(&self) -> Result<Option<i64>, ValueTypeError> {
        match self {
            Value::I64(v) => Ok(Some(*v)),
            Value::Null => Ok(None),
            _ => Err(self.type_mismatch("I64")),
        }
    }

    pub fn try_u8(&self) -> Result<Option<u8>, ValueTypeError> {
        match self {
            Value::U8(v) => Ok(Some(*v)),
            Value::Null => Ok(None),
            _ => Err(self.type_mismatch("U8")),
        }
    }

    pub fn try_u16(&self) -> Result<Option<u16>, ValueTypeError> {
        match self {
            Value::U16(v) => Ok(Some(*v)),
            Value::Null => Ok(None),
            _ => Err(self.type_mismatch("U16")),
        }
    }

    pub fn try_u32(&self) -> Result<Option<u32>, ValueTypeError> {
        match self {
            Value::U32(v) => Ok(Some(*v)),
            Value::Null => Ok(None),
            _ => Err(self.type_mismatch("U32")),
        }
    }

    pub fn try_u64(&self) -> Result<Option<u64>, ValueTypeError> {
        match self {
            Value::U64(v) => Ok(Some(*v)),
            Value::Null => Ok(None),
            _ => Err(self.type_mismatch("U64")),
        }
    }

    pub fn try_bool(&self) -> Result<Option<bool>, ValueTypeError> {
        match self {
            Value::Bool(v) => Ok(Some(*v)),
            Value::Null => Ok(None),
            _ => Err(self.type_mismatch("Bool")),
        }
    }

    pub fn try_f32(&self) -> Result<Option<f32>, ValueTypeError> {
        match self {
            Value::F32(v) => Ok(Some(*v)),
            Value::Null => Ok(None),
            _ => Err(self.type_mismatch("F32")),
        }
    }

    pub fn try_f64(&self) -> Result<Option<f64>, ValueTypeError> {
        match self {
            Value::F64(v) => Ok(Some(*v)),
            Value::Null => Ok(None),
            _ => Err(self.type_mismatch("F64")),
        }
    }

    pub fn try_str(&self) -> Result<Option<&str>, ValueTypeError> {
        match self {
            Value::String(v) => Ok(Some(v.as_ref())),
            Value::Null => Ok(None),
            _ => Err(self.type_mismatch("String")),
        }
    }

    /// Borrow opaque bytes or a packed unsigned byte sequence as a byte slice.
    pub fn try_bytes(&self) -> Result<Option<&[u8]>, ValueTypeError> {
        match self {
            Value::Bytes(v) | Value::Uint8List(v) => Ok(Some(v.as_ref())),
            Value::Null => Ok(None),
            _ => Err(self.type_mismatch("Bytes")),
        }
    }

    pub fn type_mismatch(&self, expected: impl Into<String>) -> ValueTypeError {
        ValueTypeError::new(expected, self.variant_name())
    }

    fn variant_name(&self) -> &'static str {
        match self {
            Value::Null => "Null",
            Value::Bool(_) => "Bool",
            Value::I8(_) => "I8",
            Value::I16(_) => "I16",
            Value::I32(_) => "I32",
            Value::I64(_) => "I64",
            Value::U8(_) => "U8",
            Value::U16(_) => "U16",
            Value::U32(_) => "U32",
            Value::U64(_) => "U64",
            Value::F32(_) => "F32",
            Value::F64(_) => "F64",
            Value::String(_) => "String",
            Value::Bytes(_) => "Bytes",
            Value::Struct(_) => "Struct",
            Value::List(_) => "List",
            Value::Uint8List(_) => "Uint8List",
            Value::Array(_) => "Array",
            Value::Map(_) => "Map",
        }
    }
}
