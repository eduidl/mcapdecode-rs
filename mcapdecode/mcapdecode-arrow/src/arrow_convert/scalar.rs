use arrow::datatypes::{DataType, TimeUnit};
use mcapdecode_core::{Value, ValueType, ValueTypeError};

pub(super) enum ScalarValue<'a> {
    Null,
    Boolean(Option<bool>),
    Int8(Option<i8>),
    Int16(Option<i16>),
    Int32(Option<i32>),
    Int64(Option<i64>),
    UInt8(Option<u8>),
    UInt16(Option<u16>),
    UInt32(Option<u32>),
    UInt64(Option<u64>),
    Float32(Option<f32>),
    Float64(Option<f64>),
    Utf8(Option<&'a str>),
    Binary(Option<&'a [u8]>),
    TimestampNanosecond(Option<i64>),
}

/// Read an exactly typed scalar, mapping null to `None`.
///
/// Matches the value directly rather than going through the public accessors,
/// which report null as an error.
macro_rules! scalar {
    ($value:expr, $($pattern:pat => $out:expr),+ ; $expected:ident) => {
        match $value {
            $($pattern => Some($out),)+
            Value::Null => None,
            other => return Err(other.type_mismatch(ValueType::$expected)),
        }
    };
}

pub(super) fn scalar_value_for_datatype<'a>(
    dt: &DataType,
    value: &'a Value,
) -> Result<Option<ScalarValue<'a>>, ValueTypeError> {
    Ok(Some(match dt {
        DataType::Null => ScalarValue::Null,
        DataType::Boolean => ScalarValue::Boolean(scalar!(value, Value::Bool(v) => *v; Bool)),
        DataType::Int8 => ScalarValue::Int8(scalar!(value, Value::I8(v) => *v; I8)),
        DataType::Int16 => ScalarValue::Int16(scalar!(value, Value::I16(v) => *v; I16)),
        DataType::Int32 => ScalarValue::Int32(scalar!(value, Value::I32(v) => *v; I32)),
        DataType::Int64 => ScalarValue::Int64(scalar!(value, Value::I64(v) => *v; I64)),
        DataType::UInt8 => ScalarValue::UInt8(scalar!(value, Value::U8(v) => *v; U8)),
        DataType::UInt16 => ScalarValue::UInt16(scalar!(value, Value::U16(v) => *v; U16)),
        DataType::UInt32 => ScalarValue::UInt32(scalar!(value, Value::U32(v) => *v; U32)),
        DataType::UInt64 => ScalarValue::UInt64(scalar!(value, Value::U64(v) => *v; U64)),
        DataType::Float32 => ScalarValue::Float32(scalar!(value, Value::F32(v) => *v; F32)),
        DataType::Float64 => ScalarValue::Float64(scalar!(value, Value::F64(v) => *v; F64)),
        DataType::Utf8 => ScalarValue::Utf8(scalar!(value, Value::String(v) => v.as_ref(); String)),
        DataType::Binary => ScalarValue::Binary(
            scalar!(value, Value::Bytes(v) | Value::Uint8List(v) => v.as_ref(); Bytes),
        ),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            ScalarValue::TimestampNanosecond(scalar!(value, Value::I64(v) => *v; I64))
        }
        _ => return Ok(None),
    }))
}
