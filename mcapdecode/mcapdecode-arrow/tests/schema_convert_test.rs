use arrow::datatypes::{DataType, TimeUnit};
use mcapdecode_arrow::{field_defs_to_arrow_schema, field_defs_to_record_batch_schema};
use mcapdecode_core::{DataTypeDef, ElementDef, FieldDef, FieldDefs};

#[test]
fn field_defs_to_arrow_schema_converts_nested_types() {
    let fields = FieldDefs::from(vec![
        FieldDef::new("n", DataTypeDef::Null, true),
        FieldDef::new("b", DataTypeDef::Bool, false),
        FieldDef::new("i8", DataTypeDef::I8, false),
        FieldDef::new("i16", DataTypeDef::I16, false),
        FieldDef::new("i32", DataTypeDef::I32, false),
        FieldDef::new("i64", DataTypeDef::I64, false),
        FieldDef::new("u8", DataTypeDef::U8, false),
        FieldDef::new("u16", DataTypeDef::U16, false),
        FieldDef::new("u32", DataTypeDef::U32, false),
        FieldDef::new("u64", DataTypeDef::U64, false),
        FieldDef::new("f32", DataTypeDef::F32, false),
        FieldDef::new("f64", DataTypeDef::F64, false),
        FieldDef::new("s", DataTypeDef::String, true),
        FieldDef::new("bytes", DataTypeDef::Bytes, true),
        FieldDef {
            name: "list".to_string(),
            element: ElementDef::new(
                DataTypeDef::List(Box::new(ElementDef::new(DataTypeDef::I32, true))),
                true,
            ),
        },
        FieldDef {
            name: "array".to_string(),
            element: ElementDef::new(
                DataTypeDef::Array(Box::new(ElementDef::new(DataTypeDef::F64, false)), 4),
                false,
            ),
        },
        FieldDef {
            name: "map".to_string(),
            element: ElementDef::new(
                DataTypeDef::Map {
                    key: Box::new(ElementDef::new(DataTypeDef::String, false)),
                    value: Box::new(ElementDef::new(
                        DataTypeDef::Struct(
                            vec![FieldDef::new("v", DataTypeDef::I32, true)].into(),
                        ),
                        true,
                    )),
                },
                true,
            ),
        },
        FieldDef {
            name: "st".to_string(),
            element: ElementDef::new(
                DataTypeDef::Struct(
                    vec![
                        FieldDef::new("c1", DataTypeDef::I32, false),
                        FieldDef::new("c2", DataTypeDef::String, true),
                    ]
                    .into(),
                ),
                false,
            ),
        },
    ]);

    let schema = field_defs_to_arrow_schema(&fields);

    assert_eq!(schema.fields().len(), fields.len());
    assert_eq!(schema.field(0).data_type(), &DataType::Null);
    assert_eq!(schema.field(1).data_type(), &DataType::Boolean);
    assert_eq!(schema.field(2).data_type(), &DataType::Int8);
    assert_eq!(schema.field(3).data_type(), &DataType::Int16);
    assert_eq!(schema.field(4).data_type(), &DataType::Int32);
    assert_eq!(schema.field(5).data_type(), &DataType::Int64);
    assert_eq!(schema.field(6).data_type(), &DataType::UInt8);
    assert_eq!(schema.field(7).data_type(), &DataType::UInt16);
    assert_eq!(schema.field(8).data_type(), &DataType::UInt32);
    assert_eq!(schema.field(9).data_type(), &DataType::UInt64);
    assert_eq!(schema.field(10).data_type(), &DataType::Float32);
    assert_eq!(schema.field(11).data_type(), &DataType::Float64);
    assert_eq!(schema.field(12).data_type(), &DataType::Utf8);
    assert_eq!(schema.field(13).data_type(), &DataType::Binary);

    match schema.field(14).data_type() {
        DataType::List(item) => {
            assert_eq!(item.name(), "item");
            assert_eq!(item.data_type(), &DataType::Int32);
            assert!(item.is_nullable());
        }
        other => panic!("expected list, got {other:?}"),
    }

    match schema.field(15).data_type() {
        DataType::FixedSizeList(item, 4) => {
            assert_eq!(item.name(), "item");
            assert_eq!(item.data_type(), &DataType::Float64);
            assert!(!item.is_nullable());
        }
        other => panic!("expected fixed-size list, got {other:?}"),
    }

    match schema.field(16).data_type() {
        DataType::Map(entry, false) => match entry.data_type() {
            DataType::Struct(entries) => {
                assert_eq!(entries[0].name(), "key");
                assert_eq!(entries[0].data_type(), &DataType::Utf8);
                assert!(!entries[0].is_nullable());
                assert_eq!(entries[1].name(), "value");
                assert!(entries[1].is_nullable());
            }
            other => panic!("expected struct entry, got {other:?}"),
        },
        other => panic!("expected map, got {other:?}"),
    }

    match schema.field(17).data_type() {
        DataType::Struct(children) => {
            assert_eq!(children.len(), 2);
            assert_eq!(children[0].name(), "c1");
            assert_eq!(children[0].data_type(), &DataType::Int32);
            assert!(!children[0].is_nullable());
            assert_eq!(children[1].name(), "c2");
            assert_eq!(children[1].data_type(), &DataType::Utf8);
            assert!(children[1].is_nullable());
        }
        other => panic!("expected struct, got {other:?}"),
    }
}

#[test]
fn record_batch_schema_prepends_timestamp_columns() {
    let fields = FieldDefs::from(vec![FieldDef::new("value", DataTypeDef::I64, false)]);

    let schema = field_defs_to_record_batch_schema(&fields);

    assert_eq!(schema.field(0).name(), "@log_time");
    assert_eq!(
        schema.field(0).data_type(),
        &DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into()))
    );
    assert_eq!(schema.field(1).name(), "@publish_time");
    assert_eq!(schema.field(2).name(), "value");
}
