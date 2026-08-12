use std::path::PathBuf;

use mcapdecode::{
    TopicDecodeStatus, TopicInfo,
    core::{
        DataTypeDef,
        DataTypeDef::{Bytes, I32, I64, Map},
        ElementDef, EnumVariant, FieldDef, FieldDefs,
    },
};
use mcapq::server::{jtd, metadata_columns, response};
use serde_json::json;

#[test]
fn jtd_describes_binary_fields_as_hex() {
    let fields: FieldDefs = vec![FieldDef::new("payload", Bytes, false)].into();

    let schema =
        jtd::jtd_schema(&fields, "Example", "ros2msg", "cdr", &metadata_columns()).unwrap();

    assert_eq!(schema["properties"]["payload"]["type"], "string");
    assert_eq!(
        schema["properties"]["payload"]["metadata"]["x-mcap-original-type"],
        "bytes"
    );
    assert_eq!(
        schema["properties"]["payload"]["metadata"]["x-mcap-encoding"],
        "hex"
    );
}

#[test]
fn jtd_preserves_enum_values_and_transformed_source_types() {
    let fields: FieldDefs = vec![
        FieldDef::new(
            "state",
            DataTypeDef::Enum(vec![
                EnumVariant::new("IDLE", 0),
                EnumVariant::new("RUNNING", 2),
            ]),
            false,
        ),
        FieldDef::new("wide", DataTypeDef::WString, false),
        FieldDef::new("short_name", DataTypeDef::BoundedString(16), false),
        FieldDef::new("payload", DataTypeDef::BoundedBytes(32), false),
        FieldDef::new(
            "samples",
            DataTypeDef::BoundedList(Box::new(ElementDef::new(I32, false)), 8),
            false,
        ),
        FieldDef::new(
            "values",
            DataTypeDef::List(Box::new(ElementDef::new(I32, false))),
            false,
        ),
        FieldDef::new(
            "coordinates",
            DataTypeDef::Array(Box::new(ElementDef::new(I32, false)), 3),
            false,
        ),
    ]
    .into();

    let schema =
        jtd::jtd_schema(&fields, "Example", "ros2msg", "cdr", &metadata_columns()).unwrap();
    assert_eq!(
        schema["properties"]["state"]["enum"],
        json!(["IDLE", "RUNNING"])
    );
    assert_eq!(
        schema["properties"]["state"]["metadata"]["x-mcap-enum-variants"],
        json!([
            {"name": "IDLE", "value": 0},
            {"name": "RUNNING", "value": 2},
        ])
    );
    for (field, original_type) in [
        ("state", "enum"),
        ("wide", "wstring"),
        ("short_name", "bounded_string"),
        ("payload", "bounded_bytes"),
        ("samples", "bounded_list"),
        ("values", "list"),
        ("coordinates", "array"),
    ] {
        assert_eq!(
            schema["properties"][field]["metadata"]["x-mcap-original-type"],
            original_type
        );
    }
}

#[test]
fn jtd_describes_string_and_non_string_keyed_maps() {
    let fields: FieldDefs = vec![
        FieldDef::new(
            "labels",
            Map {
                key: Box::new(ElementDef::new(DataTypeDef::String, false)),
                value: Box::new(ElementDef::new(I32, false)),
            },
            false,
        ),
        FieldDef::new(
            "indexed",
            Map {
                key: Box::new(ElementDef::new(I64, false)),
                value: Box::new(ElementDef::new(DataTypeDef::Bool, true)),
            },
            false,
        ),
    ]
    .into();

    let schema = jtd::jtd_schema(
        &fields,
        "Example",
        "protobuf",
        "protobuf",
        &metadata_columns(),
    )
    .unwrap();
    assert_eq!(schema["properties"]["labels"]["values"]["type"], "int32");
    assert_eq!(
        schema["properties"]["labels"]["metadata"]["x-mcap-original-type"],
        "map"
    );
    assert_eq!(
        schema["properties"]["indexed"]["elements"]["properties"]["key"]["type"],
        "int64"
    );
    assert_eq!(
        schema["properties"]["indexed"]["elements"]["properties"]["value"]["type"],
        "boolean"
    );
    assert_eq!(
        schema["properties"]["indexed"]["elements"]["properties"]["value"]["nullable"],
        true
    );
    assert_eq!(
        schema["properties"]["indexed"]["metadata"]["x-mcap-original-type"],
        "map"
    );
}

#[test]
fn jtd_rejects_payload_fields_that_collide_with_metadata() {
    let fields: FieldDefs = vec![FieldDef::new("log_time", I64, false)].into();
    let error =
        jtd::jtd_schema(&fields, "Example", "ros2msg", "cdr", &metadata_columns()).unwrap_err();
    assert!(error.contains("reserved metadata fields: log_time"));
}

#[test]
fn jtd_uses_the_metadata_column_policy_for_names_and_collisions() {
    let metadata = mcapdecode::MetadataColumns::with_prefix("_");
    let payload_log_time: FieldDefs = vec![FieldDef::new("log_time", I64, false)].into();
    let schema = jtd::jtd_schema(&payload_log_time, "Example", "ros2msg", "cdr", &metadata)
        .expect("the prefix keeps the payload log_time distinct");
    assert!(schema["properties"].get("_log_time").is_some());
    assert!(schema["properties"].get("_publish_time").is_some());
    assert!(schema["properties"].get("log_time").is_some());

    let collision: FieldDefs = vec![FieldDef::new("_log_time", I64, false)].into();
    let error = jtd::jtd_schema(&collision, "Example", "ros2msg", "cdr", &metadata).unwrap_err();
    assert!(error.contains("reserved metadata fields: _log_time"));
}

#[cfg(unix)]
#[test]
fn responses_serialize_non_utf8_paths_without_panicking() {
    use std::os::unix::ffi::OsStringExt;

    let path = PathBuf::from(std::ffi::OsString::from_vec(vec![b'/', 0xff]));
    let file = response::FileRef::new(&path, 0);
    assert!(
        serde_json::to_value(&file).unwrap()["path"]
            .as_str()
            .is_some()
    );
}

#[test]
fn resolve_path_rejects_relative_paths() {
    let error = mcapq::server::resolve_path(&[], "example.mcap").unwrap_err();
    assert_eq!(error, "MCAP path 'example.mcap' must be absolute");
}

#[test]
fn info_preserves_decoder_status() {
    let status = TopicDecodeStatus {
        topic: TopicInfo {
            topic: "/indexed".to_string(),
            message_count: Some(1),
            schema_name: Some("Example".to_string()),
            schema_encoding: "protobuf".to_string(),
            message_encoding: "protobuf".to_string(),
            channel_count: 1,
        },
        decodable: true,
        decode_error: None,
    };

    let topic = serde_json::to_value(response::topic_json(&status)).unwrap();
    assert_eq!(topic["decodable"], true);
    assert!(topic.get("decode_error").is_none());
}
