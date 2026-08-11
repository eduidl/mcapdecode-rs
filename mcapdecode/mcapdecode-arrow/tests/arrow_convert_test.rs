use std::{collections::HashMap, sync::Arc};

use arrow::{
    array::{
        Array, FixedSizeListArray, Float32Array, Float64Array, Int32Array, ListArray, MapArray,
        StringArray, StructArray, TimestampNanosecondArray,
    },
    datatypes::{DataType, Field, Schema, TimeUnit},
    record_batch::RecordBatch,
};
use mcapdecode_arrow::{ArrowConvertError, MessageBatchSchema, MetadataColumns};
use mcapdecode_core::{DecodedMessage, Value};

fn make_row(log_time: u64, publish_time: u64, value: Value) -> DecodedMessage {
    DecodedMessage {
        log_time,
        publish_time,
        value,
    }
}

/// Panics on failure so the tests that assert on an error message can keep
/// using `#[should_panic]`; `try_to_batch` covers the fallible cases.
fn to_batch(body: &Arc<Schema>, rows: &[DecodedMessage]) -> RecordBatch {
    try_to_batch(body, rows).unwrap_or_else(|e| panic!("{e}"))
}

fn try_to_batch(
    body: &Arc<Schema>,
    rows: &[DecodedMessage],
) -> Result<RecordBatch, ArrowConvertError> {
    MessageBatchSchema::new(body.as_ref().clone(), MetadataColumns::default())
        .expect("test body schemas never collide with metadata columns")
        .to_record_batch(rows)
}

fn test_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("scalar_i32", DataType::Int32, true),
        Field::new(
            "list_i32",
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            true,
        ),
        Field::new(
            "arr_f32",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 3),
            true,
        ),
        Field::new(
            "arr_f64",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float64, true)), 2),
            true,
        ),
        Field::new(
            "map_str_i32",
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Field::new("key", DataType::Utf8, false),
                            Field::new("value", DataType::Int32, true),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
            true,
        ),
        Field::new(
            "nested",
            DataType::Struct(
                vec![
                    Field::new("a", DataType::Int32, true),
                    Field::new("b", DataType::Utf8, true),
                ]
                .into(),
            ),
            true,
        ),
    ]))
}

#[test]
fn arrow_value_rows_to_record_batch_mixed_types() {
    let schema = test_schema();
    let rows = vec![
        make_row(
            1_u64,
            10_u64,
            Value::Struct(vec![
                Value::I32(42),
                Value::List(vec![Value::I32(1), Value::I32(2)]),
                Value::Array(vec![Value::F32(1.0), Value::F32(2.0), Value::F32(3.0)]),
                Value::Array(vec![Value::F64(10.0), Value::F64(20.0)]),
                Value::Map(vec![
                    (Value::string("k1"), Value::I32(11)),
                    (Value::string("k2"), Value::I32(22)),
                ]),
                Value::Struct(vec![Value::I32(7), Value::string("ok")]),
            ]),
        ),
        make_row(
            2_u64,
            20_u64,
            Value::Struct(vec![
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Map(vec![]),
                Value::Struct(vec![Value::Null, Value::string("row2")]),
            ]),
        ),
        make_row(3_u64, 30_u64, Value::Null),
    ];

    let batch = to_batch(&schema, &rows);
    assert_eq!(batch.num_rows(), 3);
    assert_eq!(batch.num_columns(), 8);

    let log_time = batch
        .column(0)
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .unwrap();
    assert_eq!(log_time.value(0), 1);
    assert_eq!(log_time.value(2), 3);

    let scalar = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(scalar.value(0), 42);
    assert!(scalar.is_null(1));
    assert!(scalar.is_null(2));

    let list = batch
        .column(3)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert!(list.is_valid(0));
    assert!(list.is_null(1));
    assert!(list.is_null(2));
    assert_eq!(list.value_offsets(), &[0, 2, 2, 2]);

    let fixed32 = batch
        .column(4)
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .unwrap();
    assert!(fixed32.is_valid(0));
    assert!(fixed32.is_null(1));
    assert!(fixed32.is_null(2));
    let fixed32_values = fixed32
        .values()
        .as_any()
        .downcast_ref::<Float32Array>()
        .unwrap();
    assert_eq!(fixed32_values.value(0), 1.0);
    assert_eq!(fixed32_values.value(1), 2.0);
    assert_eq!(fixed32_values.value(2), 3.0);
    assert!(fixed32_values.is_null(3));
    assert!(fixed32_values.is_null(8));

    let fixed64 = batch
        .column(5)
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .unwrap();
    assert!(fixed64.is_valid(0));
    assert!(fixed64.is_null(1));
    assert!(fixed64.is_null(2));
    let fixed64_values = fixed64
        .values()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(fixed64_values.value(0), 10.0);
    assert_eq!(fixed64_values.value(1), 20.0);
    assert!(fixed64_values.is_null(2));
    assert!(fixed64_values.is_null(5));

    let map = batch.column(6).as_any().downcast_ref::<MapArray>().unwrap();
    assert!(map.is_valid(0));
    assert!(map.is_valid(1));
    assert!(map.is_null(2));
    assert_eq!(map.value_offsets(), &[0, 2, 2, 2]);

    let nested = batch
        .column(7)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    assert!(nested.is_valid(0));
    assert!(nested.is_valid(1));
    assert!(nested.is_null(2));
    let nested_a = nested
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let nested_b = nested
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(nested_a.value(0), 7);
    assert!(nested_a.is_null(1));
    assert_eq!(nested_b.value(0), "ok");
    assert_eq!(nested_b.value(1), "row2");
}

#[test]
#[should_panic(expected = "Cannot create RecordBatch from empty rows")]
fn empty_rows_panics() {
    let schema = test_schema();
    let rows: Vec<DecodedMessage> = Vec::new();
    to_batch(&schema, &rows);
}

#[test]
fn null_root_sets_body_columns_to_null() {
    let schema = test_schema();
    let rows = vec![make_row(10_u64, 20_u64, Value::Null)];

    let batch = to_batch(&schema, &rows);
    let scalar = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert!(scalar.is_null(0));

    let map = batch.column(6).as_any().downcast_ref::<MapArray>().unwrap();
    assert!(map.is_null(0));

    let nested = batch
        .column(7)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    assert!(nested.is_null(0));
}

#[test]
#[should_panic(expected = "expected Struct or Null as message root")]
fn non_struct_root_panics() {
    let schema = test_schema();
    let rows = vec![make_row(10_u64, 20_u64, Value::I32(123))];
    to_batch(&schema, &rows);
}

#[test]
#[should_panic(expected = "unsupported DataType for builder")]
fn unsupported_arrow_type_panics() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "unsupported",
        DataType::Duration(TimeUnit::Nanosecond),
        true,
    )]));

    let rows = vec![make_row(1_u64, 2_u64, Value::Struct(vec![Value::Null]))];
    to_batch(&schema, &rows);
}

#[test]
#[should_panic(expected = "log_time exceeds i64::MAX")]
fn out_of_range_log_time_panics_with_the_column_name() {
    let schema = Arc::new(Schema::empty());
    let rows = vec![make_row(u64::MAX, 1, Value::Struct(vec![]))];

    to_batch(&schema, &rows);
}

#[test]
#[should_panic(expected = "publish_time exceeds i64::MAX")]
fn out_of_range_publish_time_panics_with_the_column_name() {
    let schema = Arc::new(Schema::empty());
    let rows = vec![make_row(1, u64::MAX, Value::Struct(vec![]))];

    to_batch(&schema, &rows);
}

#[test]
fn batch_schema_preserves_body_schema_metadata() {
    let body = Schema::new_with_metadata(
        vec![Field::new("value", DataType::Int32, true)],
        HashMap::from([("source".to_string(), "fixture".to_string())]),
    );

    let batch_schema = MessageBatchSchema::new(body, MetadataColumns::default()).unwrap();

    assert_eq!(
        batch_schema.schema().metadata().get("source"),
        Some(&"fixture".to_string())
    );
}

#[test]
#[should_panic(
    expected = "value type mismatch: expected FixedSizeList(length=3), got Array(length=2)"
)]
fn fixed_size_list_length_mismatch_panics() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "arr_f32",
        DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 3),
        true,
    )]));

    let rows = vec![make_row(
        1_u64,
        2_u64,
        Value::Struct(vec![Value::Array(vec![Value::F32(1.0), Value::F32(2.0)])]),
    )];
    to_batch(&schema, &rows);
}

#[test]
fn fixed_size_list_length_mismatch_returns_error_in_try_api() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "arr_f32",
        DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 3),
        true,
    )]));

    let rows = vec![make_row(
        1_u64,
        2_u64,
        Value::Struct(vec![Value::Array(vec![Value::F32(1.0), Value::F32(2.0)])]),
    )];

    let err = try_to_batch(&schema, &rows).unwrap_err();
    assert!(matches!(err, ArrowConvertError::ValueType(_)));
    assert_eq!(
        err.to_string(),
        "value type mismatch: expected FixedSizeList(length=3), got Array(length=2)"
    );
}

#[test]
fn fixed_size_list_non_array_returns_error_in_try_api() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "arr_f32",
        DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 3),
        true,
    )]));

    let rows = vec![make_row(1_u64, 2_u64, Value::Struct(vec![Value::I32(99)]))];

    let err = try_to_batch(&schema, &rows).unwrap_err();
    assert!(matches!(err, ArrowConvertError::ValueType(_)));
    assert_eq!(
        err.to_string(),
        "value type mismatch: expected Array, got I32"
    );
}

#[test]
fn list_item_nullability_is_preserved() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "list_non_null_item",
        DataType::List(Arc::new(Field::new("item", DataType::Int32, false))),
        true,
    )]));
    let rows = vec![make_row(
        1_u64,
        2_u64,
        Value::Struct(vec![Value::List(vec![Value::I32(1), Value::I32(2)])]),
    )];

    let batch = to_batch(&schema, &rows);
    let batch_schema = batch.schema();
    let dt = batch_schema.field(2).data_type();
    assert_eq!(
        dt,
        &DataType::List(Arc::new(Field::new("item", DataType::Int32, false)))
    );
}

#[test]
fn map_value_nullability_is_preserved() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "map_str_i32_non_null_value",
        DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Field::new("key", DataType::Utf8, false),
                        Field::new("value", DataType::Int32, false),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        ),
        true,
    )]));
    let rows = vec![make_row(
        1_u64,
        2_u64,
        Value::Struct(vec![Value::Map(vec![(Value::string("k"), Value::I32(1))])]),
    )];

    let batch = to_batch(&schema, &rows);
    let batch_schema = batch.schema();
    let dt = batch_schema.field(2).data_type();
    assert_eq!(
        dt,
        &DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Field::new("key", DataType::Utf8, false),
                        Field::new("value", DataType::Int32, false),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        )
    );
}

#[test]
fn scalar_type_mismatch_returns_error_in_try_api() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "scalar_i8",
        DataType::Int8,
        true,
    )]));
    let rows = vec![make_row(1_u64, 2_u64, Value::Struct(vec![Value::I16(10)]))];

    let err = try_to_batch(&schema, &rows).unwrap_err();
    assert!(matches!(err, ArrowConvertError::ValueType(_)));
    assert_eq!(err.to_string(), "value type mismatch: expected I8, got I16");
}
