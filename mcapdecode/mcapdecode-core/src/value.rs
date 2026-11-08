//! Type-safe intermediate representation produced by message decoders.

use std::sync::Arc;

use crate::{ValueAccessError, error::ValueTypeError, path::Segment};

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
    Struct(StructValue),
    List(Vec<Value>),
    /// Packed unsigned byte sequence, distinct from opaque binary data.
    Uint8List(Arc<[u8]>),
    Array(Vec<Value>),
    Map(Vec<(Value, Value)>),
}

/// The variant of a [`Value`], used to describe type mismatches.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ValueType {
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
    Struct,
    List,
    Uint8List,
    Array,
    Map,
}

impl ValueType {
    pub(crate) fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "Null" => Self::Null,
            "Bool" => Self::Bool,
            "I8" => Self::I8,
            "I16" => Self::I16,
            "I32" => Self::I32,
            "I64" => Self::I64,
            "U8" => Self::U8,
            "U16" => Self::U16,
            "U32" => Self::U32,
            "U64" => Self::U64,
            "F32" => Self::F32,
            "F64" => Self::F64,
            "String" => Self::String,
            "Bytes" => Self::Bytes,
            "Struct" => Self::Struct,
            "List" => Self::List,
            "Uint8List" => Self::Uint8List,
            "Array" => Self::Array,
            "Map" => Self::Map,
            _ => return None,
        })
    }

    /// The variant name, such as `"I32"`.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Null => "Null",
            Self::Bool => "Bool",
            Self::I8 => "I8",
            Self::I16 => "I16",
            Self::I32 => "I32",
            Self::I64 => "I64",
            Self::U8 => "U8",
            Self::U16 => "U16",
            Self::U32 => "U32",
            Self::U64 => "U64",
            Self::F32 => "F32",
            Self::F64 => "F64",
            Self::String => "String",
            Self::Bytes => "Bytes",
            Self::Struct => "Struct",
            Self::List => "List",
            Self::Uint8List => "Uint8List",
            Self::Array => "Array",
            Self::Map => "Map",
        }
    }
}

impl std::fmt::Display for ValueType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A decoded struct with its shared field definitions.
#[derive(Clone)]
pub struct StructValue {
    schema: Arc<crate::StructDef>,
    values: Vec<Value>,
}
impl StructValue {
    pub fn schema(&self) -> &Arc<crate::StructDef> {
        &self.schema
    }
    pub fn into_values(self) -> Vec<Value> {
        self.values
    }
    /// Pair each field name with its value.
    ///
    /// Fails with [`ValueAccessError::SchemaMismatch`] when the number of values
    /// differs from the number of schema fields, as for a positional struct
    /// built with `From<Vec<Value>>`.
    pub fn fields(&self) -> Result<impl Iterator<Item = (&str, &Value)>, ValueAccessError> {
        self.check_schema()?;
        Ok(self
            .schema
            .fields
            .iter()
            .zip(&self.values)
            .map(|(field, value)| (field.name.as_str(), value)))
    }
    pub(crate) fn check_schema(&self) -> Result<(), ValueAccessError> {
        if self.schema.fields.len() == self.values.len() {
            Ok(())
        } else {
            Err(ValueAccessError::SchemaMismatch)
        }
    }
}
impl From<Vec<Value>> for StructValue {
    /// Construct a positional struct without field names. Use `Value::structure`
    /// with a schema when named field access is required.
    fn from(values: Vec<Value>) -> Self {
        static EMPTY: std::sync::LazyLock<Arc<crate::StructDef>> =
            std::sync::LazyLock::new(|| Arc::new(crate::StructDef::default()));
        Self {
            schema: Arc::clone(&EMPTY),
            values,
        }
    }
}
impl std::fmt::Debug for StructValue {
    /// Print only the values, so that the shared schema is not repeated for
    /// every struct instance.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(&self.values).finish()
    }
}
impl std::ops::Deref for StructValue {
    type Target = [Value];
    fn deref(&self) -> &[Value] {
        &self.values
    }
}

impl Value {
    /// Construct a struct with a shared definition and values in schema order.
    ///
    /// `values` must hold exactly one value per schema field. This is checked
    /// only in debug builds, since decoders call this for every struct.
    pub fn structure(schema: Arc<crate::StructDef>, values: Vec<Value>) -> Self {
        debug_assert_eq!(
            schema.fields.len(),
            values.len(),
            "struct values must match the schema fields"
        );
        Self::Struct(StructValue { schema, values })
    }

    /// Access a named struct field or string map key, or a sequence element.
    pub fn get<I: crate::ValueIndex>(&self, index: I) -> Result<&Value, ValueAccessError> {
        index.get(self)
    }

    /// Follow a dotted path such as `"poses.0.position.x"` without a schema.
    ///
    /// A segment of only digits indexes a list or array; any other segment
    /// is a struct field or string map key, as with [`Value::get`]. Use
    /// [`crate::FieldPath`] to resolve a path once and apply it to many values.
    pub fn get_path(&self, path: &str) -> Result<&Value, ValueAccessError> {
        path.split('.')
            .try_fold(self, |value, segment| match Segment::parse(segment) {
                Segment::Name(name) => value.get(name),
                Segment::Index(index) => value.get(index),
            })
    }

    /// Look up a map key by value.
    pub fn get_key<K: crate::MapKey>(&self, key: K) -> Result<&Value, ValueAccessError> {
        let Self::Map(entries) = self else {
            return Err(self.exact_mismatch(ValueType::Map));
        };
        entries
            .iter()
            .find(|(candidate, _)| key.matches(candidate))
            .map(|(_, value)| value)
            .ok_or(ValueAccessError::NotFound)
    }

    /// Borrow elements of a list or fixed-size array.
    pub fn as_sequence(&self) -> Result<&[Value], ValueAccessError> {
        match self {
            Self::List(values) | Self::Array(values) => Ok(values),
            _ => Err(self.access_mismatch("List or Array")),
        }
    }

    /// Borrow map entries.
    pub fn as_map(&self) -> Result<&[(Value, Value)], ValueAccessError> {
        match self {
            Self::Map(entries) => Ok(entries),
            _ => Err(self.exact_mismatch(ValueType::Map)),
        }
    }

    pub(crate) fn access_mismatch(&self, expected: &'static str) -> ValueAccessError {
        if matches!(self, Self::Null) {
            ValueAccessError::NullValue
        } else {
            ValueAccessError::TypeMismatch(ValueTypeError::for_value(expected, self.type_name()))
        }
    }

    /// Like [`Value::access_mismatch`], but for an exact type, so that the
    /// error is built without parsing a type name.
    pub(crate) fn exact_mismatch(&self, expected: ValueType) -> ValueAccessError {
        if matches!(self, Self::Null) {
            ValueAccessError::NullValue
        } else {
            ValueAccessError::TypeMismatch(self.type_mismatch(expected))
        }
    }

    /// Whether this value is [`Value::Null`].
    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    /// Return `None` for [`Value::Null`] and `Some(self)` otherwise.
    ///
    /// Every `try_*` accessor reports null as [`ValueAccessError::NullValue`].
    /// Use this where null is an expected value, such as an unset optional
    /// field:
    ///
    /// ```
    /// # use mcapdecode_core::{Value, ValueAccessError};
    /// # fn main() -> Result<(), ValueAccessError> {
    /// let count = Value::Null;
    /// let count: Option<i32> = count.non_null().map(Value::try_i32).transpose()?;
    /// assert_eq!(count, None);
    /// # Ok(())
    /// # }
    /// ```
    pub fn non_null(&self) -> Option<&Value> {
        match self {
            Self::Null => None,
            _ => Some(self),
        }
    }

    /// Widen signed integers and unsigned integers up to `u32` to `i64`.
    /// `u64` is rejected even when the particular value would fit.
    pub fn try_widen_i64(&self) -> Result<i64, ValueAccessError> {
        match self {
            Self::I8(v) => Ok(i64::from(*v)),
            Self::I16(v) => Ok(i64::from(*v)),
            Self::I32(v) => Ok(i64::from(*v)),
            Self::I64(v) => Ok(*v),
            Self::U8(v) => Ok(i64::from(*v)),
            Self::U16(v) => Ok(i64::from(*v)),
            Self::U32(v) => Ok(i64::from(*v)),
            _ => Err(self.access_mismatch("integer safely representable as i64")),
        }
    }

    /// Widen an unsigned integer to `u64`. Signed integers are rejected,
    /// even when the particular value is non-negative.
    pub fn try_widen_u64(&self) -> Result<u64, ValueAccessError> {
        match self {
            Self::U8(v) => Ok(u64::from(*v)),
            Self::U16(v) => Ok(u64::from(*v)),
            Self::U32(v) => Ok(u64::from(*v)),
            Self::U64(v) => Ok(*v),
            _ => Err(self.access_mismatch("unsigned integer")),
        }
    }

    /// Widen f32 to f64 or return an f64, rejecting null and non-float values.
    pub fn try_widen_f64(&self) -> Result<f64, ValueAccessError> {
        match self {
            Self::F32(v) => Ok(f64::from(*v)),
            Self::F64(v) => Ok(*v),
            _ => Err(self.access_mismatch("float")),
        }
    }

    /// Convert any numeric value to `f64`. Unlike [`Value::try_widen_f64`], this
    /// also accepts integers, so `i64` and `u64` values beyond 2^53 may lose precision.
    pub fn try_lossy_f64(&self) -> Result<f64, ValueAccessError> {
        match self {
            Self::I8(v) => Ok(f64::from(*v)),
            Self::I16(v) => Ok(f64::from(*v)),
            Self::I32(v) => Ok(f64::from(*v)),
            Self::I64(v) => Ok(*v as f64),
            Self::U8(v) => Ok(f64::from(*v)),
            Self::U16(v) => Ok(f64::from(*v)),
            Self::U32(v) => Ok(f64::from(*v)),
            Self::U64(v) => Ok(*v as f64),
            Self::F32(v) => Ok(f64::from(*v)),
            Self::F64(v) => Ok(*v),
            _ => Err(self.access_mismatch("number")),
        }
    }

    pub fn string(s: impl AsRef<str>) -> Self {
        Self::String(Arc::from(s.as_ref()))
    }

    pub fn try_i8(&self) -> Result<i8, ValueAccessError> {
        match self {
            Value::I8(v) => Ok(*v),
            _ => Err(self.exact_mismatch(ValueType::I8)),
        }
    }

    pub fn try_i16(&self) -> Result<i16, ValueAccessError> {
        match self {
            Value::I16(v) => Ok(*v),
            _ => Err(self.exact_mismatch(ValueType::I16)),
        }
    }

    pub fn try_i32(&self) -> Result<i32, ValueAccessError> {
        match self {
            Value::I32(v) => Ok(*v),
            _ => Err(self.exact_mismatch(ValueType::I32)),
        }
    }

    pub fn try_i64(&self) -> Result<i64, ValueAccessError> {
        match self {
            Value::I64(v) => Ok(*v),
            _ => Err(self.exact_mismatch(ValueType::I64)),
        }
    }

    pub fn try_u8(&self) -> Result<u8, ValueAccessError> {
        match self {
            Value::U8(v) => Ok(*v),
            _ => Err(self.exact_mismatch(ValueType::U8)),
        }
    }

    pub fn try_u16(&self) -> Result<u16, ValueAccessError> {
        match self {
            Value::U16(v) => Ok(*v),
            _ => Err(self.exact_mismatch(ValueType::U16)),
        }
    }

    pub fn try_u32(&self) -> Result<u32, ValueAccessError> {
        match self {
            Value::U32(v) => Ok(*v),
            _ => Err(self.exact_mismatch(ValueType::U32)),
        }
    }

    pub fn try_u64(&self) -> Result<u64, ValueAccessError> {
        match self {
            Value::U64(v) => Ok(*v),
            _ => Err(self.exact_mismatch(ValueType::U64)),
        }
    }

    pub fn try_bool(&self) -> Result<bool, ValueAccessError> {
        match self {
            Value::Bool(v) => Ok(*v),
            _ => Err(self.exact_mismatch(ValueType::Bool)),
        }
    }

    pub fn try_f32(&self) -> Result<f32, ValueAccessError> {
        match self {
            Value::F32(v) => Ok(*v),
            _ => Err(self.exact_mismatch(ValueType::F32)),
        }
    }

    pub fn try_f64(&self) -> Result<f64, ValueAccessError> {
        match self {
            Value::F64(v) => Ok(*v),
            _ => Err(self.exact_mismatch(ValueType::F64)),
        }
    }

    pub fn try_str(&self) -> Result<&str, ValueAccessError> {
        match self {
            Value::String(v) => Ok(v.as_ref()),
            _ => Err(self.exact_mismatch(ValueType::String)),
        }
    }

    /// Borrow opaque bytes or a packed unsigned byte sequence as a byte slice.
    pub fn try_bytes(&self) -> Result<&[u8], ValueAccessError> {
        match self {
            Value::Bytes(v) | Value::Uint8List(v) => Ok(v.as_ref()),
            _ => Err(self.exact_mismatch(ValueType::Bytes)),
        }
    }

    /// Borrow the shared buffer behind opaque bytes or a packed unsigned byte
    /// sequence.
    ///
    /// Unlike [`Value::try_bytes`], cloning the returned `Arc` keeps the data
    /// alive beyond the `Value` without copying it.
    pub fn try_shared_bytes(&self) -> Result<&Arc<[u8]>, ValueAccessError> {
        match self {
            Value::Bytes(v) | Value::Uint8List(v) => Ok(v),
            _ => Err(self.exact_mismatch(ValueType::Bytes)),
        }
    }

    /// Describe this value as not being of the `expected` type.
    ///
    /// Use [`ValueTypeError::new`] to describe the expected type in free text.
    pub fn type_mismatch(&self, expected: ValueType) -> ValueTypeError {
        ValueTypeError::for_types(expected, self.type_name())
    }

    fn type_name(&self) -> ValueType {
        match self {
            Value::Null => ValueType::Null,
            Value::Bool(_) => ValueType::Bool,
            Value::I8(_) => ValueType::I8,
            Value::I16(_) => ValueType::I16,
            Value::I32(_) => ValueType::I32,
            Value::I64(_) => ValueType::I64,
            Value::U8(_) => ValueType::U8,
            Value::U16(_) => ValueType::U16,
            Value::U32(_) => ValueType::U32,
            Value::U64(_) => ValueType::U64,
            Value::F32(_) => ValueType::F32,
            Value::F64(_) => ValueType::F64,
            Value::String(_) => ValueType::String,
            Value::Bytes(_) => ValueType::Bytes,
            Value::Struct(_) => ValueType::Struct,
            Value::List(_) => ValueType::List,
            Value::Uint8List(_) => ValueType::Uint8List,
            Value::Array(_) => ValueType::Array,
            Value::Map(_) => ValueType::Map,
        }
    }
}
