//! Convert a protobuf `FileDescriptorSet` into [`FieldDef`] schema.

use mcapdecode_core::{
    DataTypeDef, DecoderError, ElementDef, EnumDef, EnumVariant, FieldDef, FieldDefs, StructDef,
};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

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
    let pool =
        DescriptorPool::decode(schema_data).map_err(|e| DecoderError::SchemaParse(Box::new(e)))?;
    pool.get_message_by_name(schema_name)
        .ok_or_else(|| DecoderError::SchemaDerivation("message descriptor not found".to_string()))
}

/// Derive [`FieldDefs`] from an already-resolved [`MessageDescriptor`].
///
/// Returns [`DecoderError::SchemaDerivation`] for a message type that contains
/// itself (such as `google.protobuf.Value`), since a schema tree cannot
/// represent it.
pub fn message_fields_to_field_defs(
    desc: &MessageDescriptor,
    policy: PresencePolicy,
) -> Result<FieldDefs, DecoderError> {
    Builder {
        policy,
        built: HashMap::new(),
        visiting: HashSet::new(),
    }
    .message_fields(desc)
}

/// Builds each message definition once, so every reference to the same message
/// type shares one `Arc<StructDef>`.
struct Builder {
    policy: PresencePolicy,
    built: HashMap<String, Arc<StructDef>>,
    visiting: HashSet<String>,
}

impl Builder {
    fn message_fields(&mut self, desc: &MessageDescriptor) -> Result<FieldDefs, DecoderError> {
        if !self.visiting.insert(desc.full_name().to_owned()) {
            return Err(DecoderError::SchemaDerivation(format!(
                "recursive message type: {}",
                desc.full_name()
            )));
        }
        let fields = desc
            .fields()
            .map(|f| self.field(&f))
            .collect::<Result<Vec<_>, _>>()?;
        self.visiting.remove(desc.full_name());
        Ok(fields.into())
    }

    fn field(&mut self, fd: &FieldDescriptor) -> Result<FieldDef, DecoderError> {
        let dt = if fd.is_map() {
            let Kind::Message(entry_desc) = fd.kind() else {
                return Err(DecoderError::SchemaDerivation(format!(
                    "map field `{}` has non-message kind: {:?}",
                    fd.name(),
                    fd.kind()
                )));
            };
            let key_field = entry_desc.get_field_by_name("key").ok_or_else(|| {
                DecoderError::SchemaDerivation(format!(
                    "map entry `{}` missing key field",
                    fd.name()
                ))
            })?;
            let value_field = entry_desc.get_field_by_name("value").ok_or_else(|| {
                DecoderError::SchemaDerivation(format!(
                    "map entry `{}` missing value field",
                    fd.name()
                ))
            })?;
            let key_dt = self.kind(&key_field)?;
            let val_dt = self.kind(&value_field)?;
            DataTypeDef::Map {
                key: Box::new(ElementDef::new(key_dt, false)),
                value: Box::new(ElementDef::new(val_dt, false)),
            }
        } else if fd.is_list() {
            DataTypeDef::List(Box::new(ElementDef::new(self.kind(fd)?, false)))
        } else {
            self.kind(fd)?
        };

        let nullable = match self.policy {
            PresencePolicy::AlwaysDefault => false,
            PresencePolicy::PresenceAware => fd.supports_presence(),
        };
        Ok(FieldDef::new(fd.name(), dt, nullable))
    }

    fn kind(&mut self, fd: &FieldDescriptor) -> Result<DataTypeDef, DecoderError> {
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
            Kind::Enum(enum_desc) => DataTypeDef::Enum(EnumDef {
                type_name: Some(enum_desc.full_name().to_owned()),
                variants: enum_desc
                    .values()
                    .map(|value| EnumVariant::new(value.name(), i64::from(value.number())))
                    .collect(),
            }),
            Kind::Message(msg_desc) => {
                if let Some(definition) = self.built.get(msg_desc.full_name()) {
                    return Ok(DataTypeDef::Struct(Arc::clone(definition)));
                }
                let fields = self.message_fields(&msg_desc)?;
                let definition = Arc::new(StructDef {
                    type_name: Some(msg_desc.full_name().to_owned()),
                    fields,
                });
                self.built
                    .insert(msg_desc.full_name().to_owned(), Arc::clone(&definition));
                DataTypeDef::Struct(definition)
            }
        };
        Ok(dt)
    }
}
