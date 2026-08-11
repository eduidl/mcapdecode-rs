use super::*;

#[test]
fn message_count_with_summary() {
    let reader = McapReader::new();
    let path = fixture_path("with_summary.mcap");

    assert_eq!(reader.message_count(&path, "/decoded").unwrap(), 2);
}

#[test]
fn message_count_no_summary_returns_error() {
    let reader = McapReader::new();
    let path = fixture_path("no_summary.mcap");
    assert!(matches!(
        reader.message_count(&path, "/decoded"),
        Err(McapReaderError::SummaryNotAvailable)
    ));
}

#[test]
fn message_count_unknown_topic_returns_error() {
    let reader = McapReader::new();
    let path = fixture_path("with_summary.mcap");
    assert!(matches!(
        reader.message_count(&path, "/unknown"),
        Err(McapReaderError::TopicNotFound)
    ));
}

fn collect_prepared_values(prepared: &PreparedTopic<'_>, options: &ReadOptions) -> Vec<i64> {
    let mut values = Vec::new();
    prepared
        .for_each_decoded_message_with_options(options, |message| {
            let Value::Struct(fields) = message.value else {
                panic!("expected struct payload");
            };
            let Value::I64(value) = fields[0] else {
                panic!("expected i64 field");
            };
            values.push(value);
            Ok(())
        })
        .unwrap();
    values
}

#[test]
fn with_prepared_topic_exposes_topic_metadata_and_derived_schema() {
    let fixture = write_chunked_fixture("prepared-metadata", &[br#"{"value":1}"#]);
    let reader = McapReader::builder()
        .with_decoder(Box::new(TestJsonDecoder))
        .build();

    reader
        .with_prepared_topic(fixture.path(), "/decoded", |prepared| {
            assert_eq!(prepared.topic(), "/decoded");
            assert_eq!(prepared.schema_name(), "test.Msg");
            assert!(!prepared.field_defs().is_empty());
            Ok(())
        })
        .unwrap();
}

#[test]
fn with_prepared_topic_scans_repeatedly_from_one_preparation() {
    let fixture = write_chunked_fixture(
        "prepared-repeat",
        &[br#"{"value":1}"#, br#"{"value":2}"#, br#"{"value":3}"#],
    );
    let reader = McapReader::builder()
        .with_decoder(Box::new(TestJsonDecoder))
        .build();

    let (all, limited) = reader
        .with_prepared_topic(fixture.path(), "/decoded", |prepared| {
            let all = collect_prepared_values(prepared, &ReadOptions::default());
            let limited = collect_prepared_values(
                prepared,
                &ReadOptions {
                    limit: Some(2),
                    ..ReadOptions::default()
                },
            );
            Ok((all, limited))
        })
        .unwrap();

    assert_eq!(all, vec![1, 2, 3]);
    assert_eq!(limited, vec![1, 2]);
}

#[test]
fn with_prepared_topic_matches_reader_level_filtered_read() {
    let fixture = write_chunked_fixture(
        "prepared-matches-reader",
        &[br#"{"value":1}"#, br#"{"value":2}"#, br#"{"value":3}"#],
    );
    let reader = McapReader::builder()
        .with_decoder(Box::new(TestJsonDecoder))
        .build();
    let options = ReadOptions {
        offset: 1,
        ..ReadOptions::default()
    };

    let prepared_values = reader
        .with_prepared_topic(fixture.path(), "/decoded", |prepared| {
            Ok(collect_prepared_values(prepared, &options))
        })
        .unwrap();

    assert_eq!(
        prepared_values,
        super::read_options::collect_values(&reader, fixture.path(), &options)
    );
}

#[test]
fn with_prepared_topic_reports_unknown_topic_without_running_callback() {
    let fixture = write_chunked_fixture("prepared-unknown-topic", &[br#"{"value":1}"#]);
    let reader = McapReader::builder()
        .with_decoder(Box::new(TestJsonDecoder))
        .build();

    let err = reader
        .with_prepared_topic::<()>(fixture.path(), "/unknown", |_prepared| {
            panic!("callback must not run when the topic cannot be resolved")
        })
        .unwrap_err();

    assert!(matches!(err, McapReaderError::TopicNotFound));
}

#[test]
fn builder_default_matches_new_without_decoders() {
    let new_reader = McapReader::new();
    let built_reader = McapReader::builder().build();
    let path = fixture_path("with_summary.mcap");

    assert_eq!(
        new_reader.message_count(&path, "/decoded").unwrap(),
        built_reader.message_count(&path, "/decoded").unwrap()
    );
}

#[test]
fn list_topics_returns_topic_metadata() {
    let reader = McapReader::new();
    let topics = reader
        .list_topics(&fixture_path("with_summary.mcap"))
        .unwrap();

    assert_eq!(
        topics,
        vec![
            TopicInfo {
                topic: "/decoded".to_string(),
                message_count: Some(2),
                schema_name: Some("test.Msg".to_string()),
                schema_encoding: "jsonschema".to_string(),
                message_encoding: "json".to_string(),
                channel_count: 1,
            },
            TopicInfo {
                topic: "/raw".to_string(),
                message_count: Some(1),
                schema_name: None,
                schema_encoding: String::new(),
                message_encoding: "application/octet-stream".to_string(),
                channel_count: 1,
            },
        ]
    );
}

#[test]
fn topic_schema_returns_metadata_and_field_defs_together() {
    let mut reader = McapReader::new();
    reader.register_decoder(Box::new(TestJsonDecoder));

    let topic_schema = reader
        .topic_schema(&fixture_path("with_summary.mcap"), "/decoded")
        .unwrap();

    assert_eq!(topic_schema.info.topic, "/decoded");
    assert_eq!(topic_schema.info.schema_name.as_deref(), Some("test.Msg"));
    assert_eq!(
        topic_schema.field_defs,
        vec![FieldDef::new("value", DataTypeDef::I64, true)].into()
    );
}

#[test]
fn list_topics_with_decode_status_reads_topic_metadata_and_decode_errors() {
    let reader = McapReader::new();
    let statuses = reader
        .list_topics_with_decode_status(&fixture_path("with_summary.mcap"))
        .unwrap();

    assert_eq!(
        statuses,
        vec![
            TopicDecodeStatus {
                topic: TopicInfo {
                    topic: "/decoded".to_string(),
                    message_count: Some(2),
                    schema_name: Some("test.Msg".to_string()),
                    schema_encoding: "jsonschema".to_string(),
                    message_encoding: "json".to_string(),
                    channel_count: 1,
                },
                decodable: false,
                decode_error: Some(
                    "no decoder registered for schema_encoding='jsonschema', message_encoding='json'"
                        .to_string(),
                ),
            },
            TopicDecodeStatus {
                topic: TopicInfo {
                    topic: "/raw".to_string(),
                    message_count: Some(1),
                    schema_name: None,
                    schema_encoding: String::new(),
                    message_encoding: "application/octet-stream".to_string(),
                    channel_count: 1,
                },
                decodable: false,
                decode_error: Some("schema not available".to_string()),
            },
        ]
    );
}

#[test]
fn list_topics_no_summary_returns_error() {
    let reader = McapReader::new();

    assert!(matches!(
        reader.list_topics(&fixture_path("no_summary.mcap")),
        Err(McapReaderError::SummaryNotAvailable)
    ));
}

#[test]
fn list_topics_aggregates_duplicate_channels() {
    let reader = McapReader::new();
    let fixture = write_duplicate_topic_fixture("duplicate-topic");
    let topics = reader.list_topics(fixture.path()).unwrap();

    assert_eq!(
        topics,
        vec![TopicInfo {
            topic: "/duplicate".to_string(),
            message_count: Some(2),
            schema_name: Some("test.Msg".to_string()),
            schema_encoding: "jsonschema".to_string(),
            message_encoding: "json".to_string(),
            channel_count: 2,
        }]
    );
}

#[test]
fn for_each_decoded_message_unknown_topic_returns_error() {
    let mut reader = McapReader::new();
    reader.register_decoder(Box::new(TestJsonDecoder));

    let err = reader
        .for_each_decoded_message(&fixture_path("with_summary.mcap"), "/unknown", |_message| {
            Ok(())
        })
        .unwrap_err();

    assert!(matches!(err, McapReaderError::TopicNotFound));
}

#[test]
fn for_each_decoded_message_errors_when_schema_is_missing() {
    let mut reader = McapReader::new();
    reader.register_decoder(Box::new(TestJsonDecoder));

    let err = reader
        .for_each_decoded_message(
            &fixture_path("with_summary.mcap"),
            "/raw",
            |_message| Ok(()),
        )
        .unwrap_err();

    assert!(matches!(err, McapReaderError::SchemaNotAvailable));
}

#[test]
fn for_each_decoded_message_without_decoder_returns_error() {
    let reader = McapReader::new();

    let err = reader
        .for_each_decoded_message(&fixture_path("with_summary.mcap"), "/decoded", |_message| {
            Ok(())
        })
        .unwrap_err();

    assert!(matches!(err, McapReaderError::NoDecoder { .. }));
}

#[test]
fn for_each_raw_message_reads_schema_less_topic_payloads() {
    let reader = McapReader::new();

    assert_eq!(
        collect_raw_payloads(&reader, &fixture_path("with_summary.mcap"), "/raw"),
        vec![vec![0x01, 0x02, 0x03]]
    );
}

#[test]
fn for_each_raw_message_unknown_topic_returns_error() {
    let reader = McapReader::new();

    let err = reader
        .for_each_raw_message(&fixture_path("with_summary.mcap"), "/unknown", |_message| {
            Ok(())
        })
        .unwrap_err();

    assert!(matches!(err, McapReaderError::TopicNotFound));
}

#[test]
fn for_each_raw_message_propagates_callback_error() {
    let reader = McapReader::new();

    let err = reader
        .for_each_raw_message(&fixture_path("with_summary.mcap"), "/raw", |_message| {
            Err("callback failed".into())
        })
        .unwrap_err();

    assert!(matches!(err, McapReaderError::Callback(_)));
    assert!(err.to_string().contains("callback failed"));
}

#[test]
fn for_each_decoded_message_parallel_matches_sequential_for_multi_chunk_fixture() {
    let fixture = write_chunked_fixture(
        "parallel-multi-chunk-decoded",
        &[
            br#"{"value":1}"#,
            br#"{"value":2}"#,
            br#"{"value":3}"#,
            br#"{"value":4}"#,
            br#"{"value":5}"#,
        ],
    );

    let parallel_reader = McapReader::builder()
        .with_decoder(Box::new(TestJsonDecoder))
        .with_parallel(true)
        .build();
    let sequential_reader = McapReader::builder()
        .with_decoder(Box::new(TestJsonDecoder))
        .with_parallel(false)
        .build();

    assert_eq!(
        collect_decoded_i64_values(&parallel_reader, fixture.path(), "/decoded"),
        collect_decoded_i64_values(&sequential_reader, fixture.path(), "/decoded")
    );
}

#[test]
fn for_each_decoded_message_parallel_propagates_decode_error() {
    let fixture = write_chunked_fixture(
        "parallel-decode-error-decoded",
        &[br#"{"value":1}"#, b"invalid", br#"{"value":3}"#],
    );

    let reader = McapReader::builder()
        .with_decoder(Box::new(TestJsonDecoder))
        .with_parallel(true)
        .build();

    let err = reader
        .for_each_decoded_message(fixture.path(), "/decoded", |_message| Ok(()))
        .unwrap_err();

    assert!(matches!(
        err,
        McapReaderError::Decoder(DecoderError::MessageDecode(_))
    ));
}

#[test]
fn for_each_decoded_message_propagates_callback_error() {
    let mut reader = McapReader::new();
    reader.register_decoder(Box::new(TestJsonDecoder));

    let err = reader
        .for_each_decoded_message(&fixture_path("with_summary.mcap"), "/decoded", |_message| {
            Err("callback failed".into())
        })
        .unwrap_err();

    let McapReaderError::Callback(inner) = &err else {
        panic!("expected a callback error, got {err:?}");
    };
    assert!(
        inner.downcast_ref::<McapReaderError>().is_none(),
        "the callback error must be wrapped exactly once"
    );
    assert_eq!(inner.to_string(), "callback failed");
}

#[test]
fn for_each_decoded_message_parallel_stops_after_callback_error() {
    let fixture = write_chunked_fixture(
        "parallel-callback-stop",
        &[
            br#"{"value":1}"#,
            br#"{"value":2}"#,
            br#"{"value":3}"#,
            br#"{"value":4}"#,
            br#"{"value":5}"#,
        ],
    );

    let reader = McapReader::builder()
        .with_decoder(Box::new(TestJsonDecoder))
        .with_parallel(true)
        .build();
    let mut visited = Vec::new();

    let err = reader
        .for_each_decoded_message(fixture.path(), "/decoded", |message| {
            if let Value::Struct(fields) = &message.value
                && let Some(Value::I64(value)) = fields.first()
            {
                visited.push(*value);
            }
            if visited.len() == 2 {
                return Err("callback failed".into());
            }
            Ok(())
        })
        .unwrap_err();

    assert!(matches!(err, McapReaderError::Callback(_)));
    assert_eq!(visited, vec![1, 2]);
}
