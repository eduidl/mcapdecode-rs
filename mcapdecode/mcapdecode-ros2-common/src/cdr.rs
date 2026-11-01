//! CDR decoding to the Arrow-independent Value type.

use std::{fmt, sync::Arc};

use bytes::Buf;
use mcapdecode_core::{DecoderError, Value};

use crate::{
    ast::PrimitiveType,
    error::Ros2Error,
    type_resolver::{ResolvedField, ResolvedSchema, ResolvedType},
};

pub fn decode_cdr_to_value(schema: &ResolvedSchema, data: &[u8]) -> Result<Value, DecoderError> {
    let mut d = Decoder::new(data);
    d.read_encapsulation()
        .map_err(|detail| DecoderError::MessageDecode(detail.into()))?;
    let mut path = FieldPath::new(&schema.root);
    d.decode_struct(schema, &schema.root, &mut path)
        .map_err(|detail| DecoderError::MessageDecode(detail.into()))
}

/// The location being decoded, rendered only when an error reports it.
///
/// Decoding updates this for every field of every message, so it must not format
/// anything until then.
struct FieldPath<'s> {
    root: &'s [String],
    segments: Vec<PathSegment<'s>>,
}

enum PathSegment<'s> {
    Field(&'s str),
    Index(usize),
}

impl<'s> FieldPath<'s> {
    fn new(root: &'s [String]) -> Self {
        Self {
            root,
            segments: Vec::new(),
        }
    }
}

impl fmt::Display for FieldPath<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.root.join("."))?;
        for segment in &self.segments {
            match segment {
                PathSegment::Field(name) => write!(f, ".{name}")?,
                PathSegment::Index(index) => write!(f, "[{index}]")?,
            }
        }
        Ok(())
    }
}

fn primitive_align_size(p: &PrimitiveType) -> usize {
    match p {
        PrimitiveType::I16 | PrimitiveType::U16 => 2,
        PrimitiveType::I32 | PrimitiveType::U32 | PrimitiveType::F32 => 4,
        PrimitiveType::I64 | PrimitiveType::U64 | PrimitiveType::F64 => 8,
        _ => 1,
    }
}

struct Decoder<'a> {
    buf: &'a [u8],
    initial_len: usize,
    align_base: usize,
}

impl<'a> Decoder<'a> {
    fn new(buf: &'a [u8]) -> Self {
        let initial_len = buf.len();
        Self {
            buf,
            initial_len,
            align_base: 0,
        }
    }

    fn read_encapsulation(&mut self) -> Result<(), Ros2Error> {
        if self.buf.remaining() < 4 {
            return Err("incomplete encapsulation header".into());
        }
        let header = self.buf.get_u32_le();
        let endianness = (header >> 8) & 0xFF;
        if endianness != 0x01 {
            return Err(format!("unsupported CDR endianness: 0x{:02x}", endianness as u8).into());
        }
        self.align_base = 4;
        Ok(())
    }

    fn current_offset(&self) -> usize {
        self.initial_len - self.buf.remaining()
    }

    fn decode_struct<'s>(
        &mut self,
        schema: &'s ResolvedSchema,
        struct_name: &[String],
        path: &mut FieldPath<'s>,
    ) -> Result<Value, Ros2Error> {
        let s = schema
            .structs
            .get(struct_name)
            .ok_or_else(|| format!("unknown struct: {}", struct_name.join("::")))?;
        let mut fields = Vec::with_capacity(s.fields.len());
        if s.fields.is_empty() {
            // ROS 2 IDL forbids empty structs, so a `.msg`/`.idl` definition
            // with no fields gets a synthetic
            // `uint8 structure_needs_at_least_one_member` placeholder before
            // CDR serialisation. Every empty struct therefore consumes one
            // byte on the wire. See
            // https://design.ros2.org/articles/legacy_interface_definition.html
            self.align(1)?;
            self.buf.try_get_u8().map_err(|_| {
                Ros2Error(format!(
                    "unexpected EOF reading empty-struct placeholder at {path}"
                ))
            })?;
        }
        for field in &s.fields {
            path.segments.push(PathSegment::Field(&field.name));
            let v = self.decode_field(schema, field, path);
            path.segments.pop();
            let v = v?;
            fields.push(v);
        }
        Ok(Value::Struct(fields))
    }

    fn decode_field<'s>(
        &mut self,
        schema: &'s ResolvedSchema,
        field: &'s ResolvedField,
        path: &mut FieldPath<'s>,
    ) -> Result<Value, Ros2Error> {
        if let Some(n) = field.fixed_len {
            let mut arr = Vec::with_capacity(n);
            for i in 0..n {
                path.segments.push(PathSegment::Index(i));
                let v = self.decode_type(schema, &field.ty, path);
                path.segments.pop();
                arr.push(v?);
            }
            return Ok(Value::Array(arr));
        }
        self.decode_type(schema, &field.ty, path)
    }

    fn decode_type<'s>(
        &mut self,
        schema: &'s ResolvedSchema,
        ty: &'s ResolvedType,
        path: &mut FieldPath<'s>,
    ) -> Result<Value, Ros2Error> {
        match ty {
            ResolvedType::Primitive(p) => self.decode_primitive(p, path),
            ResolvedType::BoundedString(max) => {
                let s = self.decode_string(path)?;
                if s.len() > *max {
                    return Err(
                        format!("bounded string overflow at {path}: {} > {max}", s.len()).into(),
                    );
                }
                Ok(Value::String(Arc::from(s)))
            }
            ResolvedType::BoundedWString(_max) => {
                Err(format!("wstring not supported at {path}").into())
            }
            ResolvedType::Struct(name) => self.decode_struct(schema, name, path),
            ResolvedType::Enum(name) => {
                self.align(4)?;
                let raw = self
                    .buf
                    .try_get_u32_le()
                    .map_err(|_| Ros2Error(format!("unexpected EOF at {path}")))?;
                let s = match schema.enums.get(name) {
                    Some(vars) => vars
                        .iter()
                        // Enums are serialized as 32 bits, so only the low 32 bits of the
                        // declared value are on the wire (`@value(-1)` arrives as 0xFFFFFFFF).
                        .find(|variant| variant.value as u32 == raw)
                        .map(|variant| variant.name.clone())
                        .unwrap_or_else(|| raw.to_string()),
                    _ => raw.to_string(),
                };
                Ok(Value::String(Arc::from(s)))
            }
            ResolvedType::Sequence { elem, max_len } => {
                self.align(4)?;
                let len = self
                    .buf
                    .try_get_u32_le()
                    .map_err(|_| Ros2Error(format!("unexpected EOF at {path}")))?
                    as usize;
                if let Some(max) = max_len
                    && len > *max
                {
                    return Err(format!("sequence bound overflow at {path}: {len} > {max}").into());
                }
                if len > self.buf.remaining() {
                    return Err(format!(
                        "unexpected EOF at {path}: sequence length {len} exceeds remaining payload"
                    )
                    .into());
                }
                if matches!(
                    elem.as_ref(),
                    ResolvedType::Primitive(PrimitiveType::U8 | PrimitiveType::Octet)
                ) {
                    return Ok(Value::Uint8List(Arc::from(self.read_bytes(len, path)?)));
                }
                let mut out = Vec::with_capacity(len);
                for i in 0..len {
                    path.segments.push(PathSegment::Index(i));
                    let v = self.decode_type(schema, elem, path);
                    path.segments.pop();
                    out.push(v?);
                }
                Ok(Value::List(out))
            }
        }
    }

    fn decode_primitive(
        &mut self,
        p: &PrimitiveType,
        path: &FieldPath<'_>,
    ) -> Result<Value, Ros2Error> {
        self.align(primitive_align_size(p))?;
        let eof_err = || Ros2Error(format!("unexpected EOF at {path}"));

        Ok(match p {
            PrimitiveType::Bool => Value::Bool(self.buf.try_get_u8().map_err(|_| eof_err())? != 0),
            PrimitiveType::I8 => Value::I8(self.buf.try_get_i8().map_err(|_| eof_err())?),
            PrimitiveType::I16 => Value::I16(self.buf.try_get_i16_le().map_err(|_| eof_err())?),
            PrimitiveType::I32 => Value::I32(self.buf.try_get_i32_le().map_err(|_| eof_err())?),
            PrimitiveType::I64 => Value::I64(self.buf.try_get_i64_le().map_err(|_| eof_err())?),
            PrimitiveType::U8 | PrimitiveType::Octet => {
                Value::U8(self.buf.try_get_u8().map_err(|_| eof_err())?)
            }
            PrimitiveType::U16 => Value::U16(self.buf.try_get_u16_le().map_err(|_| eof_err())?),
            PrimitiveType::U32 => Value::U32(self.buf.try_get_u32_le().map_err(|_| eof_err())?),
            PrimitiveType::U64 => Value::U64(self.buf.try_get_u64_le().map_err(|_| eof_err())?),
            PrimitiveType::F32 => Value::F32(self.buf.try_get_f32_le().map_err(|_| eof_err())?),
            PrimitiveType::F64 => Value::F64(self.buf.try_get_f64_le().map_err(|_| eof_err())?),
            PrimitiveType::String => Value::String(Arc::from(self.decode_string(path)?)),
            PrimitiveType::WString => {
                return Err(format!("wstring not supported at {path}").into());
            }
        })
    }

    fn decode_string(&mut self, path: &FieldPath<'_>) -> Result<String, Ros2Error> {
        self.align(4)?;
        let len =
            self.buf
                .try_get_u32_le()
                .map_err(|_| Ros2Error(format!("unexpected EOF at {path}")))? as usize;
        if len == 0 {
            return Ok(String::new());
        }
        let bytes = self.read_bytes(len, path)?;
        if bytes.last() != Some(&0) {
            return Err(format!("string missing null terminator at {path}").into());
        }
        String::from_utf8(bytes[..len - 1].to_vec())
            .map_err(|e| Ros2Error(format!("invalid UTF-8 at {path}: {e}")))
    }

    fn align(&mut self, n: usize) -> Result<(), Ros2Error> {
        let relative_offset = self.current_offset() - self.align_base;
        let pad = (n - (relative_offset % n)) % n;
        if self.buf.remaining() < pad {
            return Err("buffer underflow while aligning".into());
        }
        self.buf.advance(pad);
        Ok(())
    }

    fn read_bytes(&mut self, n: usize, path: &FieldPath<'_>) -> Result<&'a [u8], Ros2Error> {
        if self.buf.remaining() < n {
            return Err(format!("unexpected EOF at {path}").into());
        }
        let (bytes, rest) = self.buf.split_at(n);
        self.buf = rest;
        Ok(bytes)
    }
}
