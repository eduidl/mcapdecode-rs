//! Convert a protobuf `FileDescriptorSet` into [`FieldDef`] schema.

use mcapdecode_core::{DataTypeDef, DecoderError, ElementDef, EnumVariant, FieldDef, FieldDefs};
use prost_reflect::{DescriptorPool, FieldDescriptor, Kind, MessageDescriptor};

use crate::PresencePolicy;

/// Parse `schema_data` (a serialized `google.protobuf.FileDescriptorSet`) and
/// look up the [`MessageDescriptor`] for `schema_name`.
///
/// [`ProtobufDecoder`] and helper APIs converge here.
pub fn parse_message_descriptor(
    schema_name: &str,
    schema_data: &[u8],
) -> Result<MessageDescriptor, DecoderError> {
    let pool = DescriptorPool::decode(schema_data).map_err(|e| DecoderError::SchemaParse {
        schema_name: schema_name.to_string(),
        source: Box::new(e),
    })?;
    pool.get_message_by_name(schema_name)
        .ok_or_else(|| DecoderError::SchemaInvalid {
            schema_name: schema_name.to_string(),
            detail: format!("message descriptor not found: '{schema_name}'"),
        })
}

/// Derive [`FieldDefs`] from an already-resolved [`MessageDescriptor`].
pub fn message_fields_to_field_defs(
    schema_name: &str,
    desc: &MessageDescriptor,
    policy: PresencePolicy,
) -> Result<FieldDefs, DecoderError> {
    desc.fields()
        .map(|f| field_descriptor_to_field_def(schema_name, &f, policy))
        .collect::<Result<Vec<_>, _>>()
        .map(Into::into)
}

fn field_descriptor_to_field_def(
    schema_name: &str,
    fd: &FieldDescriptor,
    policy: PresencePolicy,
) -> Result<FieldDef, DecoderError> {
    let inner_dt = kind_to_data_type_def(schema_name, fd, policy)?;

    let dt = if fd.is_list() {
        DataTypeDef::List(Box::new(ElementDef::new(inner_dt, false)))
    } else if fd.is_map() {
        let Kind::Message(entry_desc) = fd.kind() else {
            return Err(DecoderError::SchemaInvalid {
                schema_name: schema_name.to_string(),
                detail: format!(
                    "map field `{}` has non-message kind: {:?}",
                    fd.name(),
                    fd.kind()
                ),
            });
        };
        let key_field =
            entry_desc
                .get_field_by_name("key")
                .ok_or_else(|| DecoderError::SchemaInvalid {
                    schema_name: schema_name.to_string(),
                    detail: format!("map entry `{}` missing key field", fd.name()),
                })?;
        let value_field =
            entry_desc
                .get_field_by_name("value")
                .ok_or_else(|| DecoderError::SchemaInvalid {
                    schema_name: schema_name.to_string(),
                    detail: format!("map entry `{}` missing value field", fd.name()),
                })?;
        let key_dt = kind_to_data_type_def(schema_name, &key_field, policy)?;
        let val_dt = kind_to_data_type_def(schema_name, &value_field, policy)?;
        DataTypeDef::Map {
            key: Box::new(ElementDef::new(key_dt, false)),
            value: Box::new(ElementDef::new(val_dt, false)),
        }
    } else {
        inner_dt
    };

    let nullable = match policy {
        PresencePolicy::AlwaysDefault => false,
        PresencePolicy::PresenceAware => fd.supports_presence(),
    };
    Ok(FieldDef::new(fd.name(), dt, nullable))
}

fn kind_to_data_type_def(
    schema_name: &str,
    fd: &FieldDescriptor,
    policy: PresencePolicy,
) -> Result<DataTypeDef, DecoderError> {
    let dt = match fd.kind() {
        Kind::Double => DataTypeDef::F64,
        Kind::Float => DataTypeDef::F32,
        Kind::Int32 | Kind::Sint32 | Kind::Sfixed32 => DataTypeDef::I32,
        Kind::Int64 | Kind::Sint64 | Kind::Sfixed64 => DataTypeDef::I64,
        Kind::Uint32 | Kind::Fixed32 => DataTypeDef::U32,
        Kind::Uint64 | Kind::Fixed64 => DataTypeDef::U64,
        Kind::Bool => DataTypeDef::Bool,
        Kind::String => DataTypeDef::String,
        Kind::Bytes => DataTypeDef::Bytes,
        Kind::Enum(enum_desc) => DataTypeDef::Enum(
            enum_desc
                .values()
                .map(|value| EnumVariant::new(value.name(), i64::from(value.number())))
                .collect(),
        ),
        Kind::Message(msg_desc) => {
            let fields = message_fields_to_field_defs(schema_name, &msg_desc, policy)?;
            DataTypeDef::Struct(fields)
        }
    };
    Ok(dt)
}
