use mcapdecode_core::Value;

#[test]
fn decoded_messages_support_named_access() {
    use mcapbench::{Encoding, PayloadCase, fixture};
    use mcapdecode_core::MessageDecoder;

    for encoding in [Encoding::Ros2idl, Encoding::Protobuf] {
        let f = fixture(PayloadCase::Nested, encoding).unwrap();
        let decoder: Box<dyn MessageDecoder> = match encoding {
            Encoding::Ros2idl => Box::new(mcapdecode_ros2idl::Ros2IdlDecoder::new()),
            Encoding::Protobuf => Box::new(mcapdecode_protobuf::ProtobufDecoder::new()),
            _ => unreachable!(),
        };
        let decoder = decoder
            .build_topic_decoder(&f.schema_name, &f.schema)
            .unwrap();
        let value = decoder.decode(&f.payload).unwrap();
        let second = decoder.decode(&f.payload).unwrap();
        let (Value::Struct(first_root), Value::Struct(second_root)) = (&value, &second) else {
            panic!("expected root structs");
        };
        assert!(std::sync::Arc::ptr_eq(
            first_root.schema(),
            second_root.schema()
        ));
        let (Value::Struct(first_nested), Value::Struct(second_nested)) =
            (&first_root[1], &second_root[1])
        else {
            panic!("expected nested structs");
        };
        assert!(std::sync::Arc::ptr_eq(
            first_nested.schema(),
            second_nested.schema()
        ));
        let mcapdecode_core::DataTypeDef::Struct(definition) =
            &first_root.schema().fields[1].element.data_type
        else {
            panic!("expected nested schema");
        };
        assert!(std::sync::Arc::ptr_eq(first_nested.schema(), definition));
        let result = value
            .get("b")
            .unwrap()
            .get("b")
            .unwrap()
            .get("b")
            .unwrap()
            .get("b")
            .unwrap()
            .try_widen_u64()
            .unwrap();
        let Value::Struct(root) = &value else {
            panic!("expected struct")
        };
        let Value::Struct(a) = &root[1] else {
            panic!("expected struct")
        };
        let Value::Struct(b) = &a[1] else {
            panic!("expected struct")
        };
        let Value::Struct(c) = &b[1] else {
            panic!("expected struct")
        };
        assert!(matches!(c[1], Value::U64(expected) if expected == result));
    }
}
