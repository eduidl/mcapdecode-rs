//! JSON Lines encoding for Arrow record batches produced by `mcapdecode`.

use std::{
    io::{self, Write},
    sync::Arc,
};

use arrow::{
    array::{
        Array, ArrayAccessor, BinaryArray, BinaryViewArray, FixedSizeBinaryArray, Float32Array,
        Float64Array, LargeBinaryArray,
    },
    datatypes::{DataType, FieldRef},
    error::ArrowError,
    json::{
        Encoder, EncoderFactory, EncoderOptions,
        writer::{LineDelimited, NullableEncoder, Writer, WriterBuilder},
    },
    record_batch::RecordBatch,
};
use base64::{Engine as _, engine::general_purpose::STANDARD};

/// A JSON Lines writer for decoded MCAP Arrow batches.
///
/// It writes bytes as base64 strings and non-finite floats as `"NaN"`,
/// `"Infinity"`, or `"-Infinity"`. Configure timestamp column types through
/// [`crate::MetadataColumns`] before creating the record batch.
pub struct JsonlWriter<W: Write> {
    writer: Writer<W, LineDelimited>,
}

impl<W: Write> JsonlWriter<W> {
    /// Create a writer that uses the JSON representation shared by mcapdecode tools.
    pub fn new(writer: W) -> Self {
        Self {
            writer: WriterBuilder::new()
                .with_encoder_factory(Arc::new(McapJsonEncoderFactory))
                .build(writer),
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

/// Arrow's default JSON encoder converts non-finite floats to `null` and
/// binary values to hexadecimal. This factory keeps the representation stable
/// across tools and is used recursively for nested values as well.
#[derive(Debug)]
struct McapJsonEncoderFactory;

impl EncoderFactory for McapJsonEncoderFactory {
    fn make_default_encoder<'a>(
        &self,
        _field: &'a FieldRef,
        array: &'a dyn Array,
        _options: &'a EncoderOptions,
    ) -> Result<Option<NullableEncoder<'a>>, ArrowError> {
        let encoder: Box<dyn Encoder + 'a> = match array.data_type() {
            DataType::Float32 => Box::new(Float32Encoder(
                array
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .expect("Float32 data type must have a Float32Array"),
            )),
            DataType::Float64 => Box::new(Float64Encoder(
                array
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .expect("Float64 data type must have a Float64Array"),
            )),
            DataType::Binary => Box::new(Base64Encoder {
                values: array
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .expect("Binary data type must have a BinaryArray"),
            }),
            DataType::LargeBinary => Box::new(Base64Encoder {
                values: array
                    .as_any()
                    .downcast_ref::<LargeBinaryArray>()
                    .expect("LargeBinary data type must have a LargeBinaryArray"),
            }),
            DataType::BinaryView => Box::new(Base64Encoder {
                values: array
                    .as_any()
                    .downcast_ref::<BinaryViewArray>()
                    .expect("BinaryView data type must have a BinaryViewArray"),
            }),
            DataType::FixedSizeBinary(_) => Box::new(Base64Encoder {
                values: array
                    .as_any()
                    .downcast_ref::<FixedSizeBinaryArray>()
                    .expect("FixedSizeBinary data type must have a FixedSizeBinaryArray"),
            }),
            _ => return Ok(None),
        };
        Ok(Some(NullableEncoder::new(encoder, array.nulls().cloned())))
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

struct Base64Encoder<B> {
    values: B,
}

impl<'a, B> Encoder for Base64Encoder<B>
where
    B: ArrayAccessor<Item = &'a [u8]>,
{
    fn encode(&mut self, index: usize, output: &mut Vec<u8>) {
        serde_json::to_writer(output, &STANDARD.encode(self.values.value(index)))
            .expect("writing JSON to memory cannot fail");
    }
}
