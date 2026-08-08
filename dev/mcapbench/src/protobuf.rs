//! Protobuf descriptor construction and message encoding for the generated models.

use prost::Message;
use prost_reflect::{DescriptorPool, DynamicMessage, Value as ProtoValue};
use prost_types::{
    DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet,
    field_descriptor_proto::{Label, Type},
};

use crate::{
    BenchResult,
    model::{FieldTy, Model, PACKAGE, Sample, StructDef},
};

/// Build a `FileDescriptorSet` with one message per struct in the model.
pub(crate) fn descriptor_set(model: &Model) -> Vec<u8> {
    let message_type = model
        .structs
        .iter()
        .map(|def| DescriptorProto {
            name: Some(def.name.into()),
            field: def
                .fields
                .iter()
                .enumerate()
                .map(|(index, (name, ty))| field_descriptor(index, name, ty))
                .collect(),
            ..Default::default()
        })
        .collect();

    FileDescriptorSet {
        file: vec![FileDescriptorProto {
            name: Some("bench.proto".into()),
            package: Some(PACKAGE.into()),
            syntax: Some("proto3".into()),
            message_type,
            ..Default::default()
        }],
    }
    .encode_to_vec()
}

fn field_descriptor(index: usize, name: &str, ty: &FieldTy) -> FieldDescriptorProto {
    let (typ, type_name, label) = match ty {
        FieldTy::U64 => (Type::Uint64, None, Label::Optional),
        FieldTy::F64Array(_) => (Type::Double, None, Label::Repeated),
        FieldTy::ByteSeq(_) => (Type::Bytes, None, Label::Optional),
        FieldTy::Str => (Type::String, None, Label::Optional),
        FieldTy::Struct(name) => (
            Type::Message,
            Some(format!(".{PACKAGE}.{name}")),
            Label::Optional,
        ),
    };
    FieldDescriptorProto {
        name: Some(name.into()),
        number: Some(index as i32 + 1),
        r#type: Some(typ.into()),
        type_name,
        label: Some(label.into()),
        ..Default::default()
    }
}

pub(crate) fn encode(
    pool: &DescriptorPool,
    model: &Model,
    def: &StructDef,
    sample: &Sample,
) -> BenchResult<DynamicMessage> {
    let descriptor = pool
        .get_message_by_name(&format!("{PACKAGE}.{}", def.name))
        .ok_or_else(|| format!("missing descriptor for {}", def.name))?;
    let mut message = DynamicMessage::new(descriptor);
    let Sample::Struct(values) = sample else {
        return Err("expected a struct sample".into());
    };
    for ((name, ty), value) in def.fields.iter().zip(values) {
        let proto_value = match (ty, value) {
            (FieldTy::U64, Sample::U64(v)) => ProtoValue::U64(*v),
            (FieldTy::F64Array(_), Sample::F64List(values)) => {
                ProtoValue::List(values.iter().map(|v| ProtoValue::F64(*v)).collect())
            }
            (FieldTy::ByteSeq(_), Sample::Bytes(bytes)) => {
                ProtoValue::Bytes(bytes::Bytes::from(bytes.clone()))
            }
            (FieldTy::Str, Sample::Str(s)) => ProtoValue::String(s.clone()),
            (FieldTy::Struct(struct_name), nested) => {
                ProtoValue::Message(encode(pool, model, model.get(struct_name), nested)?)
            }
            _ => return Err("sample does not match the model".into()),
        };
        message.set_field_by_name(name, proto_value);
    }
    Ok(message)
}
