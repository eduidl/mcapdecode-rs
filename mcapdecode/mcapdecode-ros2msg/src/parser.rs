//! Conversion from `re_ros_msg`'s AST to the shared [`StructDef`] representation.
//!
//! [`parse_msg`] is the single public entry point: it delegates parsing to the
//! `re_ros_msg` crate and then maps the resulting `MessageSpecification` into
//! the types understood by `mcapdecode-ros2-common`.

use mcapdecode_ros2_common::{
    ConstDef, FieldDef, PrimitiveType, Ros2Error, StructDef, TypeExpr, normalize_schema_name,
};
use re_ros_msg::{
    MessageSchema,
    message_spec::{
        ArraySize, BuiltInType, ComplexType, Constant, Field, MessageSpecification, Type,
    },
};
/// Parse .msg format and generate StructDef
pub fn parse_msg(schema_name: &str, msg_text: &str) -> Result<StructDef, Ros2Error> {
    // 1. Parse with re_ros_msg
    let schema = MessageSchema::parse(schema_name, msg_text)
        .map_err(|e| Ros2Error(format!("failed to parse msg schema '{schema_name}': {e}")))?;

    // 2. Parse schema_name to get full_name
    let full_name = parse_schema_name(schema_name)?;

    // 3. Convert MessageSpecification → StructDef
    convert_to_struct_def(full_name, schema.spec)
}

pub(crate) fn parse_schema_name(name: &str) -> Result<Vec<String>, Ros2Error> {
    // "geometry_msgs/msg/Point" → vec!["geometry_msgs", "msg", "Point"]
    // "geometry_msgs/Point" → vec!["geometry_msgs", "msg", "Point"]
    normalize_schema_name(name).map(|name| name.split('/').map(ToString::to_string).collect())
}

fn convert_to_struct_def(
    full_name: Vec<String>,
    spec: MessageSpecification,
) -> Result<StructDef, Ros2Error> {
    let fields = spec
        .fields
        .into_iter()
        .map(convert_field)
        .collect::<Result<Vec<_>, _>>()?;

    let consts = spec
        .constants
        .into_iter()
        .map(convert_constant)
        .collect::<Result<Vec<_>, _>>()?;

    Ok(StructDef {
        full_name,
        fields,
        consts,
    })
}

fn convert_field(field: Field) -> Result<FieldDef, Ros2Error> {
    let (ty, fixed_len) = convert_type(&field.ty)?;

    Ok(FieldDef {
        name: field.name,
        ty,
        fixed_len,
    })
}

fn convert_type(ty: &Type) -> Result<(TypeExpr, Option<usize>), Ros2Error> {
    match ty {
        Type::BuiltIn(builtin) => Ok((convert_builtin_type(builtin), None)),
        Type::Complex(complex) => {
            let scoped = convert_complex_type(complex);
            Ok((TypeExpr::Scoped(scoped), None))
        }
        Type::Array { ty: elem_ty, size } => {
            let (elem, elem_fixed) = convert_type(elem_ty)?;

            // If the element itself has fixed_len, that becomes the outer array
            if elem_fixed.is_some() {
                return Err("nested fixed arrays are not supported in ROS2".into());
            }

            match size {
                ArraySize::Fixed(n) => {
                    // Fixed array: field has fixed_len, type is the element
                    Ok((elem, Some(*n)))
                }
                ArraySize::Unbounded => {
                    // Dynamic sequence
                    Ok((
                        TypeExpr::Sequence {
                            elem: Box::new(elem),
                            max_len: None,
                        },
                        None,
                    ))
                }
                ArraySize::Bounded(n) => {
                    // Bounded sequence
                    Ok((
                        TypeExpr::Sequence {
                            elem: Box::new(elem),
                            max_len: Some(*n),
                        },
                        None,
                    ))
                }
            }
        }
    }
}

fn convert_builtin_type(ty: &BuiltInType) -> TypeExpr {
    let prim = match ty {
        BuiltInType::Bool => PrimitiveType::Bool,
        BuiltInType::Byte => PrimitiveType::U8,
        BuiltInType::Char => PrimitiveType::U8,
        BuiltInType::Int8 => PrimitiveType::I8,
        BuiltInType::UInt8 => PrimitiveType::U8,
        BuiltInType::Int16 => PrimitiveType::I16,
        BuiltInType::UInt16 => PrimitiveType::U16,
        BuiltInType::Int32 => PrimitiveType::I32,
        BuiltInType::UInt32 => PrimitiveType::U32,
        BuiltInType::Int64 => PrimitiveType::I64,
        BuiltInType::UInt64 => PrimitiveType::U64,
        BuiltInType::Float32 => PrimitiveType::F32,
        BuiltInType::Float64 => PrimitiveType::F64,
        BuiltInType::String(None) => PrimitiveType::String,
        BuiltInType::String(Some(n)) => {
            return TypeExpr::BoundedString(*n);
        }
        BuiltInType::WString(None) => PrimitiveType::WString,
        BuiltInType::WString(Some(n)) => {
            return TypeExpr::BoundedWString(*n);
        }
    };

    TypeExpr::Primitive(prim)
}

fn convert_complex_type(ty: &ComplexType) -> Vec<String> {
    match ty {
        ComplexType::Absolute { package, name } => {
            // "pkg/Type" → vec!["pkg", "msg", "Type"]
            vec![package.clone(), "msg".to_string(), name.clone()]
        }
        ComplexType::Relative { name } => {
            // "Type" → vec!["Type"]
            vec![name.clone()]
        }
    }
}

fn convert_constant(constant: Constant) -> Result<ConstDef, Ros2Error> {
    let ty = match &constant.ty {
        Type::BuiltIn(builtin) => convert_builtin_type(builtin),
        Type::Array { .. } => {
            return Err("constants cannot be arrays".into());
        }
        Type::Complex(_) => {
            return Err("constants must be primitive types".into());
        }
    };

    Ok(ConstDef {
        ty,
        name: constant.name,
        value: format!("{:?}", constant.value),
    })
}
