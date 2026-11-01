use mcapdecode_core::{
    DataTypeDef, DecoderError, ElementDef, MessageDecoder, MessageEncoding, SchemaEncoding,
};
use mcapdecode_rawjson::{COLUMN_NAME, RawJsonDecoder};

#[test]
fn handles_jsonschema_with_json_messages() {
    let key = RawJsonDecoder::new().encoding_key();
    assert_eq!(key.schema_encoding, SchemaEncoding::JsonSchema);
    assert_eq!(key.message_encoding, MessageEncoding::Json);
}

#[test]
fn exposes_one_string_column_whatever_the_schema() {
    for schema in [b"".as_slice(), b"not json", br#"{"type":"object"}"#] {
        let decoder = RawJsonDecoder::new()
            .build_topic_decoder("test.Msg", schema)
            .unwrap();
        let fields = decoder.field_defs();
        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].name, COLUMN_NAME);
        assert_eq!(
            fields[0].element,
            ElementDef::new(DataTypeDef::String, false)
        );
    }
}

#[test]
fn keeps_message_text_unchanged_without_parsing() {
    let decoder = RawJsonDecoder::new()
        .build_topic_decoder("test.Msg", b"")
        .unwrap();
    for message in [r#"{ "b": 1.50, "a": [1] }"#, "{broken", ""] {
        let value = decoder.decode(message.as_bytes()).unwrap();
        assert_eq!(
            format!("{value:?}"),
            format!("Struct([String({message:?})])")
        );
    }
}

#[test]
fn rejects_invalid_utf8() {
    let decoder = RawJsonDecoder::new()
        .build_topic_decoder("test.Msg", b"")
        .unwrap();
    assert!(matches!(
        decoder.decode(&[0xff]),
        Err(DecoderError::MessageDecode(_))
    ));
}
