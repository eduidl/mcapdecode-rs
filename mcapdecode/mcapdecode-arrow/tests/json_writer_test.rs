use std::sync::Arc;

use arrow::{
    array::{BinaryArray, Float32Array, Float64Array, Int64Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use mcapdecode_arrow::JsonlWriter;

#[test]
fn writes_mcap_json_conventions() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("log_time", DataType::Int64, false),
        Field::new("reading", DataType::Float32, false),
        Field::new("range", DataType::Float64, false),
        Field::new("nan", DataType::Float64, false),
        Field::new("bytes", DataType::Binary, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![123_i64])),
            Arc::new(Float32Array::from(vec![0.1_f32])),
            Arc::new(Float64Array::from(vec![f64::INFINITY])),
            Arc::new(Float64Array::from(vec![f64::NAN])),
            Arc::new(BinaryArray::from_iter_values([b"\0\x01\xff".as_slice()])),
        ],
    )
    .unwrap();

    let mut output = Vec::new();
    let mut writer = JsonlWriter::new(&mut output);
    writer.write(&batch).unwrap();
    writer.finish().unwrap();

    assert_eq!(
        String::from_utf8(output).unwrap(),
        "{\"log_time\":123,\"reading\":0.1,\"range\":\"Infinity\",\"nan\":\"NaN\",\"bytes\":\"AAH/\"}\n"
    );
}
