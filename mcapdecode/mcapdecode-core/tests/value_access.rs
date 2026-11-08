use mcapdecode_core::{Value, ValueAccessError as E};

#[test]
fn sequences_and_maps_are_accessible() {
    let list = Value::List(vec![Value::F32(1.5)]);
    assert_eq!(list.get(0).unwrap().try_widen_f64(), Ok(1.5));
    assert!(matches!(list.get(1), Err(E::NotFound)));
    assert_eq!(list.as_sequence().unwrap().len(), 1);

    let array = Value::Array(vec![Value::U8(7)]);
    assert_eq!(array.get(0).unwrap().try_widen_u64(), Ok(7));

    let map = Value::Map(vec![(Value::string("p"), Value::I16(-3))]);
    assert_eq!(map.get_key("p").unwrap().try_widen_i64(), Ok(-3));
    assert!(matches!(map.get_key("missing"), Err(E::NotFound)));
    assert_eq!(map.as_map().unwrap().len(), 1);

    assert!(matches!(map.get(0), Err(E::TypeMismatch(_))));
    assert!(matches!(list.get_key("p"), Err(E::TypeMismatch(_))));
    assert!(matches!(Value::Null.get(0), Err(E::NullValue)));
    assert!(matches!(Value::Null.get_key("p"), Err(E::NullValue)));
}

#[test]
fn map_keys_match_by_value() {
    let numbers = Value::Map(vec![(Value::I32(-1), Value::U64(7))]);
    assert_eq!(numbers.get_key(-1_i64).unwrap().try_widen_u64(), Ok(7));
    assert!(matches!(numbers.get_key(1_u8), Err(E::NotFound)));

    let booleans = Value::Map(vec![(Value::Bool(true), Value::string("yes"))]);
    assert_eq!(booleans.get_key(true).unwrap().try_str(), Ok("yes"));
}

#[test]
fn numeric_conversions_are_checked_and_null_is_explicit() {
    for value in [
        Value::I8(1),
        Value::I16(1),
        Value::I32(1),
        Value::I64(1),
        Value::U8(1),
        Value::U16(1),
        Value::U32(1),
    ] {
        assert_eq!(value.try_widen_i64(), Ok(1));
    }
    for value in [Value::U8(1), Value::U16(1), Value::U32(1), Value::U64(1)] {
        assert_eq!(value.try_widen_u64(), Ok(1));
    }
    for value in [
        Value::I8(1),
        Value::I16(1),
        Value::I32(1),
        Value::I64(1),
        Value::I8(-1),
    ] {
        assert!(matches!(value.try_widen_u64(), Err(E::TypeMismatch(_))));
    }
    for value in [
        Value::U64(0),
        Value::U64(i64::MAX as u64),
        Value::U64(u64::MAX),
    ] {
        assert!(matches!(value.try_widen_i64(), Err(E::TypeMismatch(_))));
    }
    assert_eq!(Value::I64(i64::MIN).try_widen_i64(), Ok(i64::MIN));
    assert_eq!(
        Value::U32(u32::MAX).try_widen_i64(),
        Ok(i64::from(u32::MAX))
    );
    assert_eq!(Value::U64(u64::MAX).try_widen_u64(), Ok(u64::MAX));
    assert_eq!(Value::Null.try_widen_u64(), Err(E::NullValue));
    assert_eq!(Value::Null.try_widen_i64(), Err(E::NullValue));
    assert_eq!(Value::Null.try_widen_f64(), Err(E::NullValue));
    assert!(matches!(
        Value::I32(1).try_widen_f64(),
        Err(E::TypeMismatch(_))
    ));
    assert!(matches!(
        Value::F64(1.0).try_widen_i64(),
        Err(E::TypeMismatch(_))
    ));
    assert_eq!(Value::F32(1.5).try_widen_f64(), Ok(1.5));
    assert_eq!(Value::F64(1.5).try_widen_f64(), Ok(1.5));
    for value in [
        Value::I8(-2),
        Value::I16(-2),
        Value::I32(-2),
        Value::I64(-2),
        Value::F32(-2.0),
        Value::F64(-2.0),
    ] {
        assert_eq!(value.try_lossy_f64(), Ok(-2.0));
    }
    for value in [Value::U8(2), Value::U16(2), Value::U32(2), Value::U64(2)] {
        assert_eq!(value.try_lossy_f64(), Ok(2.0));
    }
    assert_eq!(Value::I64(i64::MIN).try_lossy_f64(), Ok(i64::MIN as f64));
    assert_eq!(Value::U64(u64::MAX).try_lossy_f64(), Ok(u64::MAX as f64));
    assert_eq!(Value::Null.try_lossy_f64(), Err(E::NullValue));
    assert!(matches!(
        Value::string("x").try_lossy_f64(),
        Err(E::TypeMismatch(_))
    ));
    assert_eq!(Value::string("hello").try_str(), Ok("hello"));
    assert_eq!(
        Value::Uint8List(vec![1, 2].into()).try_bytes(),
        Ok(&[1, 2][..])
    );
    assert!(matches!(Value::U8(1).try_u64(), Err(E::TypeMismatch(_))));
}

#[test]
fn shared_bytes_reuse_the_decoded_buffer() {
    let buffer: std::sync::Arc<[u8]> = std::sync::Arc::from([1, 2]);
    for value in [
        Value::Bytes(buffer.clone()),
        Value::Uint8List(buffer.clone()),
    ] {
        let shared = value.try_shared_bytes().unwrap();
        assert!(std::sync::Arc::ptr_eq(shared, &buffer));
    }
}

#[test]
fn exact_type_accessors_report_null_as_an_error() {
    assert_eq!(Value::F64(1.5).try_f64(), Ok(1.5));
    assert!(matches!(Value::I32(1).try_f64(), Err(E::TypeMismatch(_))));
    assert_eq!(Value::Null.try_f64(), Err(E::NullValue));
    assert_eq!(Value::Null.try_str(), Err(E::NullValue));
}

#[test]
fn non_null_makes_null_an_option() {
    fn read(value: &Value) -> Result<Option<i32>, E> {
        value.non_null().map(Value::try_i32).transpose()
    }
    assert_eq!(read(&Value::I32(3)), Ok(Some(3)));
    assert_eq!(read(&Value::Null), Ok(None));
    assert!(matches!(read(&Value::I64(3)), Err(E::TypeMismatch(_))));
    assert!(Value::Null.is_null());
    assert!(!Value::I32(0).is_null());
}
