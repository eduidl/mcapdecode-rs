use arrow::datatypes::DataType;
use mcapdecode_arrow::field_defs_to_arrow_schema;
use mcapdecode_core::{DataTypeDef, ElementDef, EnumVariant, FieldDef, FieldDefs};

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
        FieldDef::new("bounded_string", DataTypeDef::BoundedString(4), false),
        FieldDef::new(
            "enum",
            DataTypeDef::Enum(vec![EnumVariant::new("A", 0), EnumVariant::new("B", 1)]),
            false,
        ),
        FieldDef::new("bounded_bytes", DataTypeDef::BoundedBytes(8), false),
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
    assert_eq!(schema.field(14).data_type(), &DataType::Utf8);
    assert_eq!(schema.field(15).data_type(), &DataType::Utf8);
    assert_eq!(schema.field(16).data_type(), &DataType::Binary);

    match schema.field(17).data_type() {
        DataType::List(item) => {
            assert_eq!(item.name(), "item");
            assert_eq!(item.data_type(), &DataType::Int32);
            assert!(item.is_nullable());
        }
        other => panic!("expected list, got {other:?}"),
    }

    match schema.field(18).data_type() {
        DataType::FixedSizeList(item, 4) => {
            assert_eq!(item.name(), "item");
            assert_eq!(item.data_type(), &DataType::Float64);
            assert!(!item.is_nullable());
        }
        other => panic!("expected fixed-size list, got {other:?}"),
    }

    match schema.field(19).data_type() {
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

    match schema.field(20).data_type() {
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
fn extended_string_and_collection_types_use_their_arrow_equivalents() {
    let fields = FieldDefs::from(vec![
        FieldDef::new("wstring", DataTypeDef::WString, false),
        FieldDef::new("bounded_wstring", DataTypeDef::BoundedWString(16), false),
        FieldDef::new(
            "bounded_list",
            DataTypeDef::BoundedList(Box::new(ElementDef::new(DataTypeDef::I16, false)), 8),
            false,
        ),
    ]);

    let schema = field_defs_to_arrow_schema(&fields);
    assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
    assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
    match schema.field(2).data_type() {
        DataType::List(item) => {
            assert_eq!(item.data_type(), &DataType::Int16);
            assert!(!item.is_nullable());
        }
        other => panic!("expected list, got {other:?}"),
    }
}
