//! JSON Lines encoding for Arrow record batches produced by `mcapdecode`.

use std::{
    io::{self, Write},
    sync::Arc,
};

use arrow::{
    array::{
        Array, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array, MapArray,
        UInt32Array, UInt64Array,
    },
    datatypes::{DataType, FieldRef},
    error::ArrowError,
    json::{
        Encoder, EncoderFactory, EncoderOptions,
        writer::{LineDelimited, NullableEncoder, Writer, WriterBuilder, make_encoder},
    },
    record_batch::RecordBatch,
};

/// How non-finite floats (`NaN`, `Infinity`, `-Infinity`) are encoded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NonFiniteFloats {
    /// Encode them as `null`, which is Arrow's default and keeps the output
    /// parseable by strict JSON readers.
    Null,
    /// Encode them as the strings `"NaN"`, `"Infinity"`, and `"-Infinity"` so
    /// that the distinction between them survives the round trip.
    String,
}

/// A JSON Lines writer for decoded MCAP Arrow batches.
///
/// It preserves Arrow's default binary encoding and encodes non-finite floats
/// according to [`NonFiniteFloats`]. String-keyed maps are encoded as JSON
/// objects; maps with protobuf boolean or integer keys are encoded as
/// `[{"key": ..., "value": ...}]` entries to preserve their key types.
/// Configure timestamp column types through [`crate::MetadataColumns`] before
/// creating the record batch.
pub struct JsonlWriter<W: Write> {
    writer: Writer<W, LineDelimited>,
}

impl<W: Write> JsonlWriter<W> {
    /// Create a writer that uses the JSON representation shared by mcapdecode tools.
    pub fn new(writer: W, non_finite_floats: NonFiniteFloats) -> Self {
        let builder = WriterBuilder::new()
            .with_encoder_factory(Arc::new(McapJsonEncoderFactory { non_finite_floats }));
        Self {
            writer: builder.build(writer),
        }
    }

    /// Write one record batch as JSON Lines.
    pub fn write(&mut self, batch: &RecordBatch) -> Result<(), ArrowError> {
        self.writer.write(batch)
    }

    /// Finish writing and flush Arrow's internal state.
    pub fn finish(&mut self) -> Result<(), ArrowError> {
        self.writer.finish()
    }

    /// Flush the underlying output stream.
    pub fn flush(&mut self) -> io::Result<()> {
        self.writer.get_mut().flush()
    }
}

/// Customizes JSON encoding for configured non-finite floats and protobuf maps
/// with non-string keys. The factory is installed for every writer so that map
/// encoding is independent of the [`NonFiniteFloats`] setting.
#[derive(Debug)]
struct McapJsonEncoderFactory {
    non_finite_floats: NonFiniteFloats,
}

impl EncoderFactory for McapJsonEncoderFactory {
    fn make_default_encoder<'a>(
        &self,
        field: &'a FieldRef,
        array: &'a dyn Array,
        options: &'a EncoderOptions,
    ) -> Result<Option<NullableEncoder<'a>>, ArrowError> {
        let encoder: Box<dyn Encoder + 'a> = match array.data_type() {
            DataType::Float32 if self.non_finite_floats == NonFiniteFloats::String => {
                Box::new(Float32Encoder(
                    array
                        .as_any()
                        .downcast_ref::<Float32Array>()
                        .expect("Float32 data type must have a Float32Array"),
                ))
            }
            DataType::Float64 if self.non_finite_floats == NonFiniteFloats::String => {
                Box::new(Float64Encoder(
                    array
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .expect("Float64 data type must have a Float64Array"),
                ))
            }
            DataType::Map(_, _) => {
                let map = array
                    .as_any()
                    .downcast_ref::<MapArray>()
                    .expect("Map data type must have a MapArray");
                match MapEntryArrayEncoder::try_new(field, map, options)? {
                    Some(encoder) => Box::new(encoder),
                    None => return Ok(None),
                }
            }
            _ => return Ok(None),
        };
        Ok(Some(NullableEncoder::new(encoder, array.nulls().cloned())))
    }
}

/// JSON encoder for protobuf maps whose keys cannot be represented as JSON
/// object keys without converting them to strings.
///
/// `map<string, V>` is left to Arrow's object encoder. Protobuf's other map
/// key types are emitted as `[{"key": K, "value": V}]`, preserving `K`'s JSON
/// type at every nesting depth.
struct MapEntryArrayEncoder<'a> {
    map: &'a MapArray,
    keys: ProtobufMapKeyEncoder<'a>,
    values: NullableEncoder<'a>,
    explicit_nulls: bool,
}

impl<'a> MapEntryArrayEncoder<'a> {
    fn try_new(
        field: &'a FieldRef,
        map: &'a MapArray,
        options: &'a EncoderOptions,
    ) -> Result<Option<Self>, ArrowError> {
        let Some(keys) = ProtobufMapKeyEncoder::try_new(map.keys().as_ref()) else {
            return Ok(None);
        };

        if map.keys().nulls().is_some_and(|x| x.null_count() != 0) {
            return Err(ArrowError::InvalidArgumentError(
                "Encountered nulls in MapArray keys".to_string(),
            ));
        }
        if map.entries().nulls().is_some_and(|x| x.null_count() != 0) {
            return Err(ArrowError::InvalidArgumentError(
                "Encountered nulls in MapArray entries".to_string(),
            ));
        }

        Ok(Some(Self {
            map,
            keys,
            values: make_encoder(field, map.values().as_ref(), options)?,
            explicit_nulls: options.explicit_nulls(),
        }))
    }
}

impl Encoder for MapEntryArrayEncoder<'_> {
    fn encode(&mut self, index: usize, output: &mut Vec<u8>) {
        let start = self.map.offsets()[index] as usize;
        let end = self.map.offsets()[index + 1] as usize;
        let mut first = true;

        output.push(b'[');
        for index in start..end {
            let value_is_null = self.values.is_null(index);
            if value_is_null && !self.explicit_nulls {
                continue;
            }
            if !first {
                output.push(b',');
            }
            first = false;

            output.extend_from_slice(br#"{"key":"#);
            self.keys.encode(index, output);
            output.extend_from_slice(b",\"value\":");
            if value_is_null {
                output.extend_from_slice(b"null");
            } else {
                self.values.encode(index, output);
            }
            output.push(b'}');
        }
        output.push(b']');
    }
}

enum ProtobufMapKeyEncoder<'a> {
    Boolean(&'a BooleanArray),
    Int32(&'a Int32Array),
    Int64(&'a Int64Array),
    UInt32(&'a UInt32Array),
    UInt64(&'a UInt64Array),
}

impl<'a> ProtobufMapKeyEncoder<'a> {
    fn try_new(array: &'a dyn Array) -> Option<Self> {
        Some(match array.data_type() {
            DataType::Boolean => Self::Boolean(array.as_any().downcast_ref::<BooleanArray>()?),
            DataType::Int32 => Self::Int32(array.as_any().downcast_ref::<Int32Array>()?),
            DataType::Int64 => Self::Int64(array.as_any().downcast_ref::<Int64Array>()?),
            DataType::UInt32 => Self::UInt32(array.as_any().downcast_ref::<UInt32Array>()?),
            DataType::UInt64 => Self::UInt64(array.as_any().downcast_ref::<UInt64Array>()?),
            _ => return None,
        })
    }

    fn encode(&self, index: usize, output: &mut Vec<u8>) {
        let result = match self {
            Self::Boolean(array) => {
                output.extend_from_slice(if array.value(index) {
                    b"true"
                } else {
                    b"false"
                });
                Ok(())
            }
            Self::Int32(array) => write!(output, "{}", array.value(index)),
            Self::Int64(array) => write!(output, "{}", array.value(index)),
            Self::UInt32(array) => write!(output, "{}", array.value(index)),
            Self::UInt64(array) => write!(output, "{}", array.value(index)),
        };
        result.expect("writing JSON to memory cannot fail");
    }
}

struct Float32Encoder<'a>(&'a Float32Array);

impl Encoder for Float32Encoder<'_> {
    fn encode(&mut self, index: usize, output: &mut Vec<u8>) {
        write_float_json(self.0.value(index), output);
    }
}

struct Float64Encoder<'a>(&'a Float64Array);

impl Encoder for Float64Encoder<'_> {
    fn encode(&mut self, index: usize, output: &mut Vec<u8>) {
        write_float_json(self.0.value(index), output);
    }
}

fn write_float_json(value: impl Into<f64> + Copy + serde::Serialize, output: &mut Vec<u8>) {
    let value64 = value.into();
    if value64.is_finite() {
        serde_json::to_writer(output, &value).expect("writing JSON to memory cannot fail");
        return;
    }
    let text = if value64.is_nan() {
        "NaN"
    } else if value64.is_sign_positive() {
        "Infinity"
    } else {
        "-Infinity"
    };
    serde_json::to_writer(output, text).expect("writing JSON to memory cannot fail");
}
