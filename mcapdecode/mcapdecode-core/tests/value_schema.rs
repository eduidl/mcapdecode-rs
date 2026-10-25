use mcapdecode_core::{DataTypeDef, ElementDef, FieldDef, Value};

#[test]
fn value_string_creates_arc_str_value() {
    let value = Value::string("hello");
    match value {
        Value::String(s) => assert_eq!(&*s, "hello"),
        other => panic!("unexpected value variant: {:?}", other),
    }
}

#[test]
fn field_def_new_sets_all_fields() {
    let field = FieldDef::new("count", DataTypeDef::I64, false);
    assert_eq!(field.name, "count");
    assert!(matches!(field.element.data_type, DataTypeDef::I64));
    assert!(!field.element.nullable);
}

#[test]
fn element_def_new_sets_all_fields() {
    let element = ElementDef::new(DataTypeDef::I32, true);
    assert!(matches!(element.data_type, DataTypeDef::I32));
    assert!(element.nullable);
}

#[test]
fn packed_uint8_lists_are_distinct_from_bytes() {
    let list = Value::Uint8List(std::sync::Arc::from([0, 255]));
    let bytes = Value::Bytes(std::sync::Arc::from([0, 255]));
    assert_eq!(list.try_bytes().unwrap(), Some([0, 255].as_slice()));
    assert_eq!(Value::Null.try_bytes().unwrap(), None);
    assert!(Value::List(vec![]).try_bytes().is_err());
    assert_eq!(bytes.try_bytes().unwrap(), Some([0, 255].as_slice()));
    for (ty, display) in [
        (DataTypeDef::Uint8List, "uint8[]"),
        (DataTypeDef::BoundedUint8List(8), "uint8[<=8]"),
    ] {
        assert!(!ty.is_primitive());
        assert_eq!(ty.type_name(), "list");
        let fields = [FieldDef::new("data", ty, true)];
        assert_eq!(
            mcapdecode_core::format_field_defs(fields).unwrap(),
            format!("data: {display}?\n")
        );
    }
}
