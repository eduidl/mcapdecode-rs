use std::sync::Arc;

use arrow::{
    array::{
        Array, ArrayRef, BinaryArray, BooleanBuilder, Float32Array, Float64Array, Int32Builder,
        Int64Array, Int64Builder, MapBuilder, StringBuilder, StructArray, UInt32Builder,
        UInt64Array, UInt64Builder,
    },
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use mcapdecode_arrow::{Integer64Encoding, JsonlWriter, JsonlWriterBuilder, NonFiniteFloats};

fn non_finite_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("log_time", DataType::Int64, false),
        Field::new("reading", DataType::Float32, false),
        Field::new("range", DataType::Float64, false),
        Field::new("nan", DataType::Float64, false),
        Field::new("bytes", DataType::Binary, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![123_i64])),
            Arc::new(Float32Array::from(vec![0.1_f32])),
            Arc::new(Float64Array::from(vec![f64::INFINITY])),
            Arc::new(Float64Array::from(vec![f64::NAN])),
            Arc::new(BinaryArray::from_iter_values([b"\0\x01\xff".as_slice()])),
        ],
    )
    .unwrap()
}

fn write_jsonl_with_explicit_nulls(
    batch: &RecordBatch,
    non_finite_floats: NonFiniteFloats,
    explicit_nulls: bool,
) -> String {
    let mut output = Vec::new();
    let mut writer = JsonlWriterBuilder::new()
        .with_non_finite_floats(non_finite_floats)
        .with_explicit_nulls(explicit_nulls)
        .build(&mut output);
    writer.write(batch).unwrap();
    writer.finish().unwrap();
    String::from_utf8(output).unwrap()
}

fn write_jsonl_with_integer64_encoding(
    batch: &RecordBatch,
    integer64_encoding: Integer64Encoding,
) -> String {
    let mut output = Vec::new();
    let mut writer = JsonlWriterBuilder::new()
        .with_integer64_encoding(integer64_encoding)
        .build(&mut output);
    writer.write(batch).unwrap();
    writer.finish().unwrap();
    String::from_utf8(output).unwrap()
}

fn write_jsonl(batch: &RecordBatch, non_finite_floats: NonFiniteFloats) -> String {
    let mut output = Vec::new();
    let mut writer = JsonlWriter::new(&mut output, non_finite_floats);
    writer.write(batch).unwrap();
    writer.finish().unwrap();
    String::from_utf8(output).unwrap()
}

#[test]
fn preserves_non_finite_floats_and_uses_arrow_binary_encoding() {
    assert_eq!(
        write_jsonl(&non_finite_batch(), NonFiniteFloats::String),
        "{\"log_time\":123,\"reading\":0.1,\"range\":\"Infinity\",\"nan\":\"NaN\",\"bytes\":\"0001ff\"}\n"
    );
}

#[test]
fn encodes_non_finite_floats_as_null_by_request() {
    assert_eq!(
        write_jsonl(&non_finite_batch(), NonFiniteFloats::Null),
        "{\"log_time\":123,\"reading\":0.1,\"range\":null,\"nan\":null,\"bytes\":\"0001ff\"}\n"
    );
}

#[test]
fn optionally_emits_null_struct_fields() {
    let nested = StructArray::from(vec![
        (
            Arc::new(Field::new("present", DataType::Int64, true)),
            Arc::new(Int64Array::from(vec![Some(42)])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("missing", DataType::Int64, true)),
            Arc::new(Int64Array::from(vec![None])) as ArrayRef,
        ),
    ]);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "nested",
            nested.data_type().clone(),
            false,
        )])),
        vec![Arc::new(nested)],
    )
    .unwrap();

    assert_eq!(
        write_jsonl(&batch, NonFiniteFloats::Null),
        "{\"nested\":{\"present\":42}}\n"
    );
    assert_eq!(
        write_jsonl_with_explicit_nulls(&batch, NonFiniteFloats::Null, true),
        "{\"nested\":{\"present\":42,\"missing\":null}}\n"
    );
}

#[test]
fn encodes_64_bit_integers_as_strings_when_configured() {
    let nested = StructArray::from(vec![
        (
            Arc::new(Field::new("signed", DataType::Int64, false)),
            Arc::new(Int64Array::from(vec![-42_i64])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("unsigned", DataType::UInt64, false)),
            Arc::new(UInt64Array::from(vec![u64::MAX])) as ArrayRef,
        ),
    ]);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("minimum", DataType::Int64, false),
            Field::new("maximum", DataType::UInt64, false),
            Field::new("nested", nested.data_type().clone(), false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![i64::MIN])),
            Arc::new(UInt64Array::from(vec![u64::MAX])),
            Arc::new(nested),
        ],
    )
    .unwrap();

    assert_eq!(
        write_jsonl_with_integer64_encoding(&batch, Integer64Encoding::String),
        "{\"minimum\":\"-9223372036854775808\",\"maximum\":\"18446744073709551615\",\"nested\":{\"signed\":\"-42\",\"unsigned\":\"18446744073709551615\"}}\n"
    );
}

#[test]
fn encodes_nested_protobuf_maps_as_json_objects() {
    let mut bool_map = MapBuilder::new(None, BooleanBuilder::new(), StringBuilder::new());
    bool_map.keys().append_value(true);
    bool_map.values().append_value("yes");
    bool_map.append(true).unwrap();

    let mut int32_map = MapBuilder::new(None, Int32Builder::new(), StringBuilder::new());
    int32_map.keys().append_value(-1);
    int32_map.values().append_value("negative");
    int32_map.keys().append_value(42);
    int32_map.values().append_value("answer");
    int32_map.append(true).unwrap();

    let mut int64_map = MapBuilder::new(None, Int64Builder::new(), StringBuilder::new());
    int64_map.keys().append_value(i64::MIN);
    int64_map.values().append_value("minimum");
    int64_map.append(true).unwrap();

    let mut uint32_map = MapBuilder::new(None, UInt32Builder::new(), StringBuilder::new());
    uint32_map.keys().append_value(u32::MAX);
    uint32_map.values().append_value("maximum");
    uint32_map.append(true).unwrap();

    let mut uint64_map = MapBuilder::new(None, UInt64Builder::new(), StringBuilder::new());
    uint64_map.keys().append_value(u64::MAX);
    uint64_map.values().append_value("maximum");
    uint64_map.append(true).unwrap();

    let mut string_map = MapBuilder::new(None, StringBuilder::new(), Int32Builder::new());
    string_map.keys().append_value("one");
    string_map.values().append_value(1);
    string_map.append(true).unwrap();

    let maps: Vec<(&str, ArrayRef)> = vec![
        ("bool_map", Arc::new(bool_map.finish())),
        ("int32_map", Arc::new(int32_map.finish())),
        ("int64_map", Arc::new(int64_map.finish())),
        ("uint32_map", Arc::new(uint32_map.finish())),
        ("uint64_map", Arc::new(uint64_map.finish())),
        ("string_map", Arc::new(string_map.finish())),
    ];
    let nested = StructArray::from(
        maps.iter()
            .map(|(name, array)| {
                (
                    Arc::new(Field::new(*name, array.data_type().clone(), false)),
                    array.clone(),
                )
            })
            .collect::<Vec<_>>(),
    );
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "nested",
            nested.data_type().clone(),
            false,
        )])),
        vec![Arc::new(nested)],
    )
    .unwrap();

    assert_eq!(
        write_jsonl(&batch, NonFiniteFloats::Null),
        concat!(
            r#"{"nested":{"bool_map":{"true":"yes"},"#,
            r#""int32_map":{"-1":"negative","42":"answer"},"#,
            r#""int64_map":{"-9223372036854775808":"minimum"},"#,
            r#""uint32_map":{"4294967295":"maximum"},"#,
            r#""uint64_map":{"18446744073709551615":"maximum"},"#,
            r#""string_map":{"one":1}}}"#,
            "\n",
        )
    );
}

#[test]
fn encodes_64_bit_protobuf_map_values_as_strings_when_configured() {
    let mut int64_map = MapBuilder::new(None, Int64Builder::new(), StringBuilder::new());
    int64_map.keys().append_value(i64::MIN);
    int64_map.values().append_value("minimum");
    int64_map.append(true).unwrap();

    let mut uint64_map = MapBuilder::new(None, UInt64Builder::new(), StringBuilder::new());
    uint64_map.keys().append_value(u64::MAX);
    uint64_map.values().append_value("maximum");
    uint64_map.append(true).unwrap();

    let mut int64_value_map = MapBuilder::new(None, Int32Builder::new(), Int64Builder::new());
    int64_value_map.keys().append_value(1);
    int64_value_map.values().append_value(i64::MIN);
    int64_value_map.append(true).unwrap();

    let int64_map = Arc::new(int64_map.finish());
    let uint64_map = Arc::new(uint64_map.finish());
    let int64_value_map = Arc::new(int64_value_map.finish());

    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("int64_map", int64_map.data_type().clone(), false),
            Field::new("uint64_map", uint64_map.data_type().clone(), false),
            Field::new(
                "int64_value_map",
                int64_value_map.data_type().clone(),
                false,
            ),
        ])),
        vec![int64_map, uint64_map, int64_value_map],
    )
    .unwrap();

    assert_eq!(
        write_jsonl_with_integer64_encoding(&batch, Integer64Encoding::String),
        "{\"int64_map\":{\"-9223372036854775808\":\"minimum\"},\"uint64_map\":{\"18446744073709551615\":\"maximum\"},\"int64_value_map\":{\"1\":\"-9223372036854775808\"}}\n"
    );
}
