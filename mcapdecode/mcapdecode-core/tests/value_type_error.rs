use mcapdecode_core::{Value, ValueTypeError};

#[test]
fn enum_type_names_preserve_error_messages_and_equality() {
    let cases = [
        (Value::Null, "Null"),
        (Value::Bool(true), "Bool"),
        (Value::I8(1), "I8"),
        (Value::I16(1), "I16"),
        (Value::I32(1), "I32"),
        (Value::I64(1), "I64"),
        (Value::U8(1), "U8"),
        (Value::U16(1), "U16"),
        (Value::U32(1), "U32"),
        (Value::U64(1), "U64"),
        (Value::F32(1.0), "F32"),
        (Value::F64(1.0), "F64"),
        (Value::string("x"), "String"),
        (Value::Bytes(vec![1].into()), "Bytes"),
        (Value::Struct(vec![].into()), "Struct"),
        (Value::List(vec![]), "List"),
        (Value::Uint8List(vec![1].into()), "Uint8List"),
        (Value::Array(vec![]), "Array"),
        (Value::Map(vec![]), "Map"),
    ];
    for (value, name) in cases {
        let error = value.type_mismatch("F64");
        assert_eq!(error, ValueTypeError::new("F64", name));
        assert_eq!(error.to_string(), format!("expected F64, got {name}"));
        assert_eq!(
            value
                .type_mismatch("custom expected description")
                .to_string(),
            format!("expected custom expected description, got {name}")
        );
    }
}

#[test]
fn scalar_accessors_preserve_success_null_and_type_errors() {
    macro_rules! check {
        ($method:ident, $name:literal, $value:expr, $expected:expr) => {
            assert_eq!($value.$method(), Ok(Some($expected)));
            assert_eq!(Value::Null.$method(), Ok(None));
            assert_eq!(
                Value::Map(vec![]).$method().unwrap_err(),
                ValueTypeError::new($name, "Map")
            );
        };
    }

    check!(try_bool, "Bool", Value::Bool(true), true);
    check!(try_i8, "I8", Value::I8(-1), -1);
    check!(try_i16, "I16", Value::I16(-1), -1);
    check!(try_i32, "I32", Value::I32(-1), -1);
    check!(try_i64, "I64", Value::I64(-1), -1);
    check!(try_u8, "U8", Value::U8(1), 1);
    check!(try_u16, "U16", Value::U16(1), 1);
    check!(try_u32, "U32", Value::U32(1), 1);
    check!(try_u64, "U64", Value::U64(1), 1);
    check!(try_f32, "F32", Value::F32(1.5), 1.5);
    check!(try_f64, "F64", Value::F64(1.5), 1.5);
    check!(try_str, "String", Value::string("x"), "x");
    check!(try_bytes, "Bytes", Value::Bytes(vec![1].into()), &[1][..]);
    check!(
        try_bytes,
        "Bytes",
        Value::Uint8List(vec![1].into()),
        &[1][..]
    );
    check!(
        try_shared_bytes,
        "Bytes",
        Value::Bytes(vec![1].into()),
        &std::sync::Arc::<[u8]>::from([1])
    );
    check!(
        try_shared_bytes,
        "Bytes",
        Value::Uint8List(vec![1].into()),
        &std::sync::Arc::<[u8]>::from([1])
    );
}

#[test]
fn errors_preserve_debug_display_clone_and_source() {
    #[derive(Debug)]
    struct ValueTypeError {
        expected: String,
        actual: String,
    }

    for (expected, actual) in [
        ("F64", "U8"),
        ("FixedSizeList(length=2)", "Array(length=1)"),
        ("F64", "unknown actual"),
        ("unknown expected", "U8"),
        ("", ""),
        ("quote \" and backslash \\ and newline\n", "型名"),
    ] {
        let old = ValueTypeError {
            expected: expected.into(),
            actual: actual.into(),
        };
        let error = mcapdecode_core::ValueTypeError::new(old.expected.clone(), old.actual.clone());
        assert_eq!(
            error.to_string(),
            format!("expected {expected}, got {actual}")
        );
        assert_eq!(error, error.clone());
        assert_eq!(format!("{error:?}"), format!("{old:?}"));
        assert_eq!(format!("{error:#?}"), format!("{old:#?}"));
        assert!(std::error::Error::source(&error).is_none());
    }
}
