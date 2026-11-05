use super::*;

#[test]
fn opened_file_reuses_metadata_and_custom_decoders_without_reopening() {
    let fixture = write_chunked_fixture("opened-reuse", &[br#"{"value":42}"#]);
    let reader = McapReader::builder()
        .with_decoder(Box::new(TestJsonDecoder))
        .build();
    let topics = reader.list_topics(fixture.path()).unwrap();
    let channels = reader.list_channels(fixture.path()).unwrap();
    let count = reader.message_count(fixture.path(), "/decoded").unwrap();
    let fields = reader.topic_field_defs(fixture.path(), "/decoded").unwrap();
    assert_eq!(
        reader
            .field_defs_by_channel_id(fixture.path(), channels[0].id)
            .unwrap(),
        fields
    );
    let schema = reader.topic_schema(fixture.path(), "/decoded").unwrap();
    let statuses = reader
        .list_topics_with_decode_status(fixture.path())
        .unwrap();
    let opened = reader.open_file(fixture.path()).unwrap();
    #[cfg(unix)]
    fs::remove_file(fixture.path()).unwrap();

    for _ in 0..2 {
        assert_eq!(opened.topic_schema("/decoded").unwrap(), schema);
        assert_eq!(opened.list_topics_with_decode_status().unwrap(), statuses);
        assert_eq!(opened.list_topics(), topics);
        assert_eq!(opened.list_channels(), channels);
        assert_eq!(opened.message_count("/decoded").unwrap(), count);
        assert_eq!(opened.topic_field_defs("/decoded").unwrap(), fields);
        assert_eq!(
            opened.field_defs_by_channel_id(channels[0].id).unwrap(),
            fields
        );
    }
}

#[test]
fn opened_file_preserves_lookup_and_decoder_errors() {
    let path = fixture_path("with_summary.mcap");
    let reader = McapReader::new();
    let expected_statuses = reader.list_topics_with_decode_status(&path).unwrap();
    let opened = reader.open_file(&path).unwrap();
    assert_eq!(
        opened.list_topics_with_decode_status().unwrap(),
        expected_statuses
    );
    assert!(matches!(
        opened.topic_schema("/unknown"),
        Err(McapReaderError::TopicNotFound)
    ));
    assert!(matches!(
        opened.topic_schema("/decoded"),
        Err(McapReaderError::NoDecoder { .. })
    ));
    assert!(matches!(
        opened.topic_schema("/raw"),
        Err(McapReaderError::SchemaNotAvailable)
    ));
    assert!(matches!(
        opened.message_count("/unknown"),
        Err(McapReaderError::TopicNotFound)
    ));
    assert!(matches!(
        opened.topic_field_defs("/unknown"),
        Err(McapReaderError::TopicNotFound)
    ));
    assert!(matches!(
        opened.field_defs_by_channel_id(u16::MAX),
        Err(McapReaderError::ChannelNotFound)
    ));
    assert!(matches!(
        opened.topic_field_defs("/decoded"),
        Err(McapReaderError::NoDecoder { .. })
    ));
    assert!(matches!(
        opened.topic_field_defs("/raw"),
        Err(McapReaderError::SchemaNotAvailable)
    ));
    assert!(matches!(
        McapReader::open(fixture_path("no_summary.mcap")),
        Err(McapReaderError::SummaryNotAvailable)
    ));
}

#[test]
fn opened_file_requires_a_channel_for_ambiguous_topics() {
    let fixture = write_split_topic_fixture("opened-split");
    let reader = McapReader::builder()
        .with_decoder(Box::new(SchemaNamedFieldDecoder))
        .build();
    let expected: Vec<_> = reader
        .list_channels(fixture.path())
        .unwrap()
        .iter()
        .map(|channel| {
            reader
                .field_defs_by_channel_id(fixture.path(), channel.id)
                .unwrap()
        })
        .collect();
    let opened = reader.open_file(fixture.path()).unwrap();
    assert_eq!(opened.list_topics()[0].channel_count, 3);
    assert!(matches!(
        opened.topic_schema("/split"),
        Err(McapReaderError::MultipleChannels)
    ));
    assert_eq!(
        opened.list_topics_with_decode_status().unwrap(),
        reader
            .list_topics_with_decode_status(fixture.path())
            .unwrap()
    );
    assert!(matches!(
        opened.message_count("/split"),
        Err(McapReaderError::MultipleChannels)
    ));
    assert!(matches!(
        opened.topic_field_defs("/split"),
        Err(McapReaderError::MultipleChannels)
    ));
    for (channel, fields) in opened.list_channels().iter().zip(expected) {
        assert_eq!(opened.field_defs_by_channel_id(channel.id).unwrap(), fields);
    }
}

#[test]
fn opened_file_preserves_missing_statistics() {
    let fixture = TempFixture {
        path: temp_fixture_path("opened-no-stats"),
    };
    let mut writer = Writer::with_options(
        File::create(fixture.path()).unwrap(),
        WriteOptions::new().emit_statistics(false),
    )
    .unwrap();
    writer
        .add_channel(0, "/raw", "json", &BTreeMap::new())
        .unwrap();
    writer.finish().unwrap();
    let reader = McapReader::new();
    assert!(matches!(
        reader.message_count(fixture.path(), "/raw"),
        Err(McapReaderError::StatsNotAvailable)
    ));
    let opened = reader.open_file(fixture.path()).unwrap();
    assert_eq!(opened.list_topics()[0].message_count, None);
    assert_eq!(opened.list_channels()[0].message_count, None);
    assert!(matches!(
        opened.message_count("/raw"),
        Err(McapReaderError::StatsNotAvailable)
    ));
}

#[cfg(feature = "ros2msg")]
#[test]
fn open_enables_default_decoders() {
    let fixture = TempFixture {
        path: temp_fixture_path("opened-default-decoders"),
    };
    let mut writer = Writer::new(File::create(fixture.path()).unwrap()).unwrap();
    let schema = writer
        .add_schema("test/msg/Message", "ros2msg", b"int32 value\n")
        .unwrap();
    writer
        .add_channel(schema, "/decoded", "cdr", &BTreeMap::new())
        .unwrap();
    writer.finish().unwrap();
    let expected = McapReader::builder()
        .with_default_decoders()
        .build()
        .topic_field_defs(fixture.path(), "/decoded")
        .unwrap();
    let opened = McapReader::open(fixture.path()).unwrap();
    assert_eq!(opened.topic_field_defs("/decoded").unwrap(), expected);
}

#[cfg(feature = "arrow")]
#[test]
fn opened_batch_schema_matches_path_api_with_custom_metadata() {
    let fixture = write_chunked_fixture("opened-arrow", &[br#"{"value":42}"#]);
    let reader = McapReader::builder()
        .with_decoder(Box::new(TestJsonDecoder))
        .build();
    let options = mcapdecode::RecordBatchOptions {
        metadata: mcapdecode::MetadataColumns::with_prefix("mcap_")
            .with_timestamp_format(mcapdecode::MetadataTimestampFormat::UnixNanoseconds),
        ..Default::default()
    };
    let expected = reader
        .topic_batch_schema(fixture.path(), "/decoded", &options)
        .unwrap();
    let opened = reader.open_file(fixture.path()).unwrap();
    let actual = opened.topic_batch_schema("/decoded", &options).unwrap();
    assert_eq!(actual.schema(), expected.schema());
    assert!(matches!(
        opened.topic_batch_schema("/unknown", &Default::default()),
        Err(McapReaderError::TopicNotFound)
    ));
}

#[test]
fn reader_opens_multiple_files_with_independent_registration_snapshots() {
    let first_fixture = write_chunked_fixture("opened-first", &[br#"{"value":1}"#]);
    let second_fixture =
        write_chunked_fixture("opened-second", &[br#"{"value":2}"#, br#"{"value":3}"#]);
    let mut reader = McapReader::new();
    let unconfigured = reader.open_file(first_fixture.path()).unwrap();
    reader.register_decoder(Box::new(TestJsonDecoder));
    let first = reader.open_file(first_fixture.path()).unwrap();
    let second = reader.open_file(second_fixture.path()).unwrap();
    let fields: FieldDefs = vec![FieldDef::new("value", DataTypeDef::I64, true)].into();
    assert_eq!(first.topic_field_defs("/decoded").unwrap(), fields);
    assert_eq!(second.topic_field_defs("/decoded").unwrap(), fields);
    assert_eq!(first.message_count("/decoded").unwrap(), 1);
    assert_eq!(second.message_count("/decoded").unwrap(), 2);
    assert!(matches!(
        unconfigured.topic_field_defs("/decoded"),
        Err(McapReaderError::NoDecoder { .. })
    ));

    reader.register_decoder(Box::new(SchemaNamedFieldDecoder));
    assert_ne!(
        reader
            .topic_field_defs(first_fixture.path(), "/decoded")
            .unwrap(),
        fields
    );
    assert_eq!(first.topic_field_defs("/decoded").unwrap(), fields);
    assert_eq!(second.topic_field_defs("/decoded").unwrap(), fields);
}

#[test]
fn opened_file_reports_zero_for_an_empty_channel_with_statistics() {
    let fixture = multi_topic_fixture();
    let opened = McapReader::open(fixture.path()).unwrap();
    assert_eq!(opened.message_count("/empty").unwrap(), 0);
    assert_eq!(
        opened
            .list_topics()
            .iter()
            .find(|topic| topic.topic == "/empty")
            .unwrap()
            .message_count,
        Some(0)
    );
    assert_eq!(
        opened
            .list_channels()
            .iter()
            .find(|channel| channel.topic == "/empty")
            .unwrap()
            .message_count,
        Some(0)
    );
}
