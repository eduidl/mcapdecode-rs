use arrow::{
    array::{
        ArrayBuilder, BinaryBuilder, BooleanBuilder, FixedSizeListBuilder, Float32Builder,
        Float64Builder, Int8Builder, Int16Builder, Int32Builder, Int64Builder, ListBuilder,
        MapBuilder, NullBuilder, StringBuilder, StructBuilder, TimestampNanosecondBuilder,
        UInt8Builder, UInt16Builder, UInt32Builder, UInt64Builder,
    },
    datatypes::DataType,
};
use mcapdecode_core::{Value, ValueType, ValueTypeError};

use super::scalar::{ScalarValue, scalar_value_for_datatype};

macro_rules! cast_builder {
    ($b:expr, $T:ty) => {
        $b.as_any_mut()
            .downcast_mut::<$T>()
            .expect(concat!("expected builder type: ", stringify!($T)))
    };
}

fn append_scalar_dyn(builder: &mut Box<dyn ArrayBuilder>, scalar: &ScalarValue<'_>) {
    match scalar {
        ScalarValue::Null => cast_builder!(builder, NullBuilder).append_null(),
        ScalarValue::Boolean(v) => cast_builder!(builder, BooleanBuilder).append_option(*v),
        ScalarValue::Int8(v) => cast_builder!(builder, Int8Builder).append_option(*v),
        ScalarValue::Int16(v) => cast_builder!(builder, Int16Builder).append_option(*v),
        ScalarValue::Int32(v) => cast_builder!(builder, Int32Builder).append_option(*v),
        ScalarValue::Int64(v) => cast_builder!(builder, Int64Builder).append_option(*v),
        ScalarValue::UInt8(v) => cast_builder!(builder, UInt8Builder).append_option(*v),
        ScalarValue::UInt16(v) => cast_builder!(builder, UInt16Builder).append_option(*v),
        ScalarValue::UInt32(v) => cast_builder!(builder, UInt32Builder).append_option(*v),
        ScalarValue::UInt64(v) => cast_builder!(builder, UInt64Builder).append_option(*v),
        ScalarValue::Float32(v) => cast_builder!(builder, Float32Builder).append_option(*v),
        ScalarValue::Float64(v) => cast_builder!(builder, Float64Builder).append_option(*v),
        ScalarValue::Utf8(v) => cast_builder!(builder, StringBuilder).append_option(*v),
        ScalarValue::Binary(v) => cast_builder!(builder, BinaryBuilder).append_option(*v),
        ScalarValue::TimestampNanosecond(v) => {
            cast_builder!(builder, TimestampNanosecondBuilder).append_option(*v)
        }
    }
}

fn append_list_elements(
    child_builder: &mut Box<dyn ArrayBuilder>,
    elem_dt: &DataType,
    value: &Value,
) -> Result<bool, ValueTypeError> {
    match value {
        Value::List(items) => {
            for item in items {
                append_value_to_builder(child_builder, elem_dt, item)?;
            }
            Ok(true)
        }
        Value::Null => Ok(false),
        _ => Err(value.type_mismatch(ValueType::List)),
    }
}

fn append_map_entries(
    map_builder: &mut MapBuilder<Box<dyn ArrayBuilder>, Box<dyn ArrayBuilder>>,
    fields: &arrow::datatypes::Fields,
    value: &Value,
) -> Result<bool, ValueTypeError> {
    match value {
        Value::Map(entries) => {
            for (key, map_value) in entries {
                append_value_to_builder(map_builder.keys(), fields[0].data_type(), key)?;
                append_value_to_builder(map_builder.values(), fields[1].data_type(), map_value)?;
            }
            Ok(true)
        }
        Value::Null => Ok(false),
        _ => Err(value.type_mismatch(ValueType::Map)),
    }
}

fn append_fixed_size_list_elements(
    child_builder: &mut Box<dyn ArrayBuilder>,
    elem_dt: &DataType,
    size: i32,
    value: &Value,
) -> Result<bool, ValueTypeError> {
    match value {
        Value::Array(items) => {
            if items.len() != size as usize {
                return Err(ValueTypeError::new(
                    format!("FixedSizeList(length={size})"),
                    format!("Array(length={})", items.len()),
                ));
            }
            for item in items {
                append_value_to_builder(child_builder, elem_dt, item)?;
            }
            Ok(true)
        }
        Value::Null => {
            for _ in 0..size {
                append_value_to_builder(child_builder, elem_dt, &Value::Null)?;
            }
            Ok(false)
        }
        _ => Err(value.type_mismatch(ValueType::Array)),
    }
}

pub(super) fn append_value_to_builder(
    builder: &mut Box<dyn ArrayBuilder>,
    dt: &DataType,
    value: &Value,
) -> Result<(), ValueTypeError> {
    if let Some(scalar) = scalar_value_for_datatype(dt, value)? {
        append_scalar_dyn(builder, &scalar);
        return Ok(());
    }

    match dt {
        DataType::List(field) => {
            let b = cast_builder!(builder, ListBuilder<Box<dyn ArrayBuilder>>);
            let valid = append_list_elements(b.values(), field.data_type(), value)?;
            b.append(valid);
        }
        DataType::FixedSizeList(field, size) => {
            let b = cast_builder!(builder, FixedSizeListBuilder<Box<dyn ArrayBuilder>>);
            let valid =
                append_fixed_size_list_elements(b.values(), field.data_type(), *size, value)?;
            b.append(valid);
        }
        DataType::Struct(fields) => {
            let b = cast_builder!(builder, StructBuilder);
            match value {
                Value::Struct(children) => {
                    for (i, field) in fields.iter().enumerate() {
                        let child_value = children.get(i).unwrap_or(&Value::Null);
                        append_value_to_struct_field(b, i, field.data_type(), child_value)?;
                    }
                    b.append(true);
                }
                Value::Null => {
                    for (i, field) in fields.iter().enumerate() {
                        append_value_to_struct_field(b, i, field.data_type(), &Value::Null)?;
                    }
                    b.append(false);
                }
                _ => return Err(value.type_mismatch(ValueType::Struct)),
            }
        }
        DataType::Map(entry_field, _) => {
            let b = cast_builder!(
                builder,
                MapBuilder<Box<dyn ArrayBuilder>, Box<dyn ArrayBuilder>>
            );
            let fields = match entry_field.data_type() {
                DataType::Struct(fields) if fields.len() == 2 => fields,
                other => panic!("Map entry field must be Struct with 2 fields, got: {other:?}"),
            };
            let valid = append_map_entries(b, fields, value)?;
            b.append(valid).expect("MapBuilder::append");
        }
        other => panic!("unsupported DataType in append_value_to_builder: {other:?}"),
    }
    Ok(())
}

fn append_value_to_struct_field(
    sb: &mut StructBuilder,
    index: usize,
    dt: &DataType,
    value: &Value,
) -> Result<(), ValueTypeError> {
    append_value_to_builder(&mut sb.field_builders_mut()[index], dt, value)
}
