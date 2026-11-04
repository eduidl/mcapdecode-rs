//! Convert a protobuf `DynamicMessage` into the intermediate [`Value`]
//! representation used by mcapdecode-core.

use std::sync::Arc;

use mcapdecode_core::{DataTypeDef, DecoderError, StructDef, Value};
use prost_reflect::{
    DynamicMessage, EnumDescriptor, Kind, MapKey, MessageDescriptor, Value as ProtoValue,
};

use crate::{PresencePolicy, schema::parse_message_descriptor};

/// Decode a message payload using an already-resolved [`MessageDescriptor`].
///
/// Both the standalone public functions and [`ProtobufDecoder`] converge here;
/// the decoder passes a cached descriptor so that `FileDescriptorSet` parsing
/// is not repeated on every message.
pub(crate) fn decode_from_descriptor(
    message_desc: &MessageDescriptor,
    message_data: &[u8],
    policy: PresencePolicy,
    schema: &Arc<StructDef>,
) -> Result<Value, DecoderError> {
    let dynamic_message = DynamicMessage::decode(message_desc.clone(), message_data)
        .map_err(|e| DecoderError::MessageDecode(Box::new(e)))?;
    Ok(message_to_value(
        &dynamic_message,
        message_desc,
        policy,
        schema,
    ))
}

/// Decode a serialized protobuf message into a [`Value`].
///
/// `schema_name` is the fully-qualified protobuf message name.
/// `schema_data` must be a valid serialized
/// `google.protobuf.FileDescriptorSet`.  `message_data` is the
/// wire-format encoded protobuf message.
///
/// This parses the descriptor set and derives the schema on every call. To
/// decode many messages of one type, build a topic decoder once with
/// [`ProtobufDecoder`](crate::ProtobufDecoder) and reuse it instead.
pub fn decode_protobuf_to_value(
    schema_name: &str,
    schema_data: &[u8],
    message_data: &[u8],
) -> Result<Value, DecoderError> {
    decode_protobuf_to_value_with_policy(
        schema_name,
        schema_data,
        message_data,
        PresencePolicy::PresenceAware,
    )
}

/// Decode a serialized protobuf message into a [`Value`] using a presence
/// policy.
///
/// This parses the descriptor set and derives the schema on every call. To
/// decode many messages of one type, build a topic decoder once with
/// [`ProtobufDecoder`](crate::ProtobufDecoder) and reuse it instead.
pub fn decode_protobuf_to_value_with_policy(
    schema_name: &str,
    schema_data: &[u8],
    message_data: &[u8],
    policy: PresencePolicy,
) -> Result<Value, DecoderError> {
    let desc = parse_message_descriptor(schema_name, schema_data)?;
    let schema = Arc::new(StructDef::from(crate::message_fields_to_field_defs(
        &desc, policy,
    )?));
    decode_from_descriptor(&desc, message_data, policy, &schema)
}

fn message_to_value(
    msg: &DynamicMessage,
    desc: &MessageDescriptor,
    policy: PresencePolicy,
    schema: &Arc<StructDef>,
) -> Value {
    let fields = desc
        .fields()
        .zip(schema.fields.iter())
        .map(|(field_desc, field)| {
            if matches!(policy, PresencePolicy::PresenceAware)
                && field_desc.supports_presence()
                && !msg.has_field(&field_desc)
            {
                return Value::Null;
            }
            let value = msg.get_field(&field_desc);
            proto_value_to_value(
                value.as_ref(),
                &field_desc.kind(),
                policy,
                &field.element.data_type,
            )
        })
        .collect();
    Value::structure(Arc::clone(schema), fields)
}

fn proto_value_to_value(
    value: &ProtoValue,
    kind: &Kind,
    policy: PresencePolicy,
    schema: &DataTypeDef,
) -> Value {
    match value {
        ProtoValue::Bool(v) => Value::Bool(*v),
        ProtoValue::I32(v) => Value::I32(*v),
        ProtoValue::I64(v) => Value::I64(*v),
        ProtoValue::U32(v) => Value::U32(*v),
        ProtoValue::U64(v) => Value::U64(*v),
        ProtoValue::F32(v) => Value::F32(*v),
        ProtoValue::F64(v) => Value::F64(*v),
        ProtoValue::String(s) => Value::String(Arc::from(s.as_str())),
        ProtoValue::Bytes(b) => Value::Bytes(Arc::from(b.as_ref())),
        ProtoValue::EnumNumber(n) => {
            let Kind::Enum(ed) = kind else {
                panic!("EnumNumber({n}) with non-Enum kind: {kind:?}")
            };
            enum_to_value(*n, ed)
        }
        ProtoValue::Message(m) => {
            let Kind::Message(md) = kind else {
                panic!("Message with non-Message kind: {kind:?}")
            };
            let DataTypeDef::Struct(def) = schema else {
                unreachable!("message schema must be a struct")
            };
            message_to_value(m, md, policy, def)
        }
        ProtoValue::List(items) => {
            let DataTypeDef::List(element) = schema else {
                unreachable!("list schema must be a list")
            };
            Value::List(
                items
                    .iter()
                    .map(|v| proto_value_to_value(v, kind, policy, &element.data_type))
                    .collect(),
            )
        }
        ProtoValue::Map(map) => {
            let DataTypeDef::Map {
                value: value_schema,
                ..
            } = schema
            else {
                unreachable!("map schema must be a map")
            };
            let value_kind = match kind {
                Kind::Message(entry_desc) if entry_desc.is_map_entry() => {
                    entry_desc.map_entry_value_field().kind()
                }
                _ => kind.clone(),
            };
            let entries = map
                .iter()
                .map(|(k, v)| {
                    (
                        map_key_to_value(k),
                        proto_value_to_value(v, &value_kind, policy, &value_schema.data_type),
                    )
                })
                .collect();
            Value::Map(entries)
        }
    }
}

fn enum_to_value(n: i32, ed: &EnumDescriptor) -> Value {
    let name = ed
        .get_value(n)
        .map(|v| v.name().to_string())
        .unwrap_or_else(|| n.to_string());
    Value::String(Arc::from(name))
}

fn map_key_to_value(k: &MapKey) -> Value {
    match k {
        MapKey::Bool(v) => Value::Bool(*v),
        MapKey::I32(v) => Value::I32(*v),
        MapKey::I64(v) => Value::I64(*v),
        MapKey::U32(v) => Value::U32(*v),
        MapKey::U64(v) => Value::U64(*v),
        MapKey::String(s) => Value::String(Arc::from(s.as_str())),
    }
}
