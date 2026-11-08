use super::*;

#[test]
fn reader_reuses_metadata_and_custom_decoders_without_reopening() {
    let fixture = write_chunked_fixture("opened-reuse", &[br#"{"value":42}"#]);
    let builder = McapReader::builder()
        .clear_decoders()
        .with_decoder(TestJsonDecoder);
    let opened = builder.open(fixture.path()).unwrap();
    let topics = opened.topics();
    let channels = opened.channels();
    let count = topic_message_count(&opened, "/decoded").unwrap();
    let fields = topic_field_defs(&opened, "/decoded").unwrap();
    assert_eq!(count, Some(1));
    assert_eq!(
        fields,
        vec![FieldDef::new("value", DataTypeDef::I64, true)].into()
    );
    #[cfg(unix)]
    fs::remove_file(fixture.path()).unwrap();

    for _ in 0..2 {
        assert_eq!(opened.topics(), topics);
        assert_eq!(opened.channels(), channels);
        assert_eq!(topic_message_count(&opened, "/decoded").unwrap(), count);
        assert_eq!(topic_field_defs(&opened, "/decoded").unwrap(), fields);
        assert_eq!(channel_field_defs(&opened, channels[0].id).unwrap(), fields);
    }
}

#[test]
fn reader_preserves_lookup_and_decoder_errors() {
    let path = fixture_path("with_summary.mcap");
    let opened = McapReader::builder().clear_decoders().open(&path).unwrap();
    assert!(matches!(
        topic_field_defs(&opened, "/unknown"),
        Err(McapReaderError::TopicNotFound)
    ));
    assert!(matches!(
        channel_field_defs(&opened, u16::MAX),
        Err(McapReaderError::ChannelNotFound)
    ));
    assert!(matches!(
        topic_field_defs(&opened, "/decoded"),
        Err(McapReaderError::NoDecoder { .. })
    ));
    assert!(matches!(
        topic_field_defs(&opened, "/raw"),
        Err(McapReaderError::SchemaNotAvailable)
    ));
    assert!(matches!(
        McapReader::open(fixture_path("no_summary.mcap")),
        Err(McapReaderError::SummaryNotAvailable)
    ));
}

#[test]
fn reader_reports_each_channel_of_a_split_topic() {
    let fixture = write_split_topic_fixture("opened-split");
    let opened = McapReader::builder()
        .clear_decoders()
        .with_decoder(SchemaNamedFieldDecoder)
        .open(fixture.path())
        .unwrap();
    assert_eq!(opened.topics()[0].channel_count, 3);
    assert_eq!(opened.topics()[0].message_count, Some(4));
    let schemas = opened.read().topic("/split").field_defs().unwrap();
    let ids: Vec<_> = schemas.iter().map(|(channel, _)| channel.id).collect();
    let names: Vec<_> = schemas
        .iter()
        .map(|(_, field_defs)| field_defs[0].name.clone())
        .collect();
    assert_eq!(
        ids,
        opened.channels().iter().map(|c| c.id).collect::<Vec<_>>()
    );
    assert_eq!(names, ["first", "second", "empty"]);
}

#[test]
fn reader_preserves_missing_statistics() {
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
    let opened = McapReader::builder()
        .clear_decoders()
        .open(fixture.path())
        .unwrap();
    assert_eq!(opened.topics()[0].message_count, None);
    assert_eq!(opened.channels()[0].message_count, None);
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
    let expected = topic_field_defs(
        &McapReader::builder().open(fixture.path()).unwrap(),
        "/decoded",
    )
    .unwrap();
    let opened = McapReader::open(fixture.path()).unwrap();
    assert_eq!(topic_field_defs(&opened, "/decoded").unwrap(), expected);
}

#[cfg(feature = "arrow")]
#[test]
fn reader_batch_schema_uses_custom_metadata() {
    let fixture = write_chunked_fixture("opened-arrow", &[br#"{"value":42}"#]);
    let opened = McapReader::builder()
        .clear_decoders()
        .with_decoder(TestJsonDecoder)
        .open(fixture.path())
        .unwrap();
    let metadata = mcapdecode::MetadataColumns::with_prefix("mcap_")
        .with_timestamp_format(mcapdecode::MetadataTimestampFormat::UnixNanoseconds);
    let actual = topic_batch_schema(&opened, "/decoded", &metadata).unwrap();
    let names: Vec<_> = actual
        .schema()
        .fields()
        .iter()
        .take(2)
        .map(|field| field.name().as_str())
        .collect();
    assert_eq!(names, metadata.names());
    assert!(matches!(
        topic_batch_schema(&opened, "/unknown", &Default::default()),
        Err(McapReaderError::TopicNotFound)
    ));
}

#[test]
fn builder_opens_multiple_files_with_the_same_decoders() {
    let first_fixture = write_chunked_fixture("opened-first", &[br#"{"value":1}"#]);
    let second_fixture =
        write_chunked_fixture("opened-second", &[br#"{"value":2}"#, br#"{"value":3}"#]);
    let builder = McapReader::builder().clear_decoders();
    let unconfigured = builder.open(first_fixture.path()).unwrap();
    let builder = builder.with_decoder(TestJsonDecoder);
    let first = builder.open(first_fixture.path()).unwrap();
    let second = builder.open(second_fixture.path()).unwrap();
    let fields: FieldDefs = vec![FieldDef::new("value", DataTypeDef::I64, true)].into();
    assert_eq!(topic_field_defs(&first, "/decoded").unwrap(), fields);
    assert_eq!(topic_field_defs(&second, "/decoded").unwrap(), fields);
    assert_eq!(topic_message_count(&first, "/decoded").unwrap(), Some(1));
    assert_eq!(topic_message_count(&second, "/decoded").unwrap(), Some(2));
    assert!(matches!(
        topic_field_defs(&unconfigured, "/decoded"),
        Err(McapReaderError::NoDecoder { .. })
    ));
}

#[test]
fn reader_reports_zero_for_an_empty_channel_with_statistics() {
    let fixture = multi_topic_fixture();
    let opened = McapReader::open(fixture.path()).unwrap();
    assert_eq!(topic_message_count(&opened, "/empty").unwrap(), Some(0));
    assert_eq!(
        opened
            .topics()
            .iter()
            .find(|topic| topic.topic == "/empty")
            .unwrap()
            .message_count,
        Some(0)
    );
    assert_eq!(
        opened
            .channels()
            .iter()
            .find(|channel| channel.topic == "/empty")
            .unwrap()
            .message_count,
        Some(0)
    );
}

#[test]
fn reader_message_time_range_uses_statistics() {
    let fixture = write_chunked_fixture("time-range-stats", &[b"{}", b"{}", b"{}"]);
    let opened = McapReader::builder()
        .clear_decoders()
        .open(fixture.path())
        .unwrap();
    assert_eq!(opened.message_time_range(), Some(1..=3));
}

#[test]
fn reader_message_time_range_falls_back_to_chunk_indexes() {
    let fixture = multi_topic_fixture_with_options(
        WriteOptions::new()
            .chunk_size(Some(128))
            .emit_statistics(false),
    );
    let opened = McapReader::builder()
        .clear_decoders()
        .open(fixture.path())
        .unwrap();
    assert_eq!(opened.message_time_range(), Some(0..=1023));
}

#[test]
fn reader_message_time_range_is_none_without_messages() {
    let fixture = write_chunked_fixture("time-range-empty", &[]);
    let opened = McapReader::builder()
        .clear_decoders()
        .open(fixture.path())
        .unwrap();
    assert_eq!(opened.message_time_range(), None);
}

#[test]
fn reader_summary_exposes_statistics() {
    let fixture = write_chunked_fixture("summary", &[b"{}", b"{}", b"{}"]);
    let opened = McapReader::builder()
        .clear_decoders()
        .open(fixture.path())
        .unwrap();
    let stats = opened.summary().stats.as_ref().unwrap();
    assert_eq!(stats.message_count, 3);
    assert_eq!(opened.summary().channels.len(), 1);
}

/// Write a statistics-free file whose first chunk holds only schema and channel records.
fn write_leading_empty_chunk_fixture(log_times: &[u64]) -> TempFixture {
    let path = temp_fixture_path("leading-empty-chunk");
    let mut writer = Writer::with_options(
        File::create(&path).unwrap(),
        WriteOptions::new().emit_statistics(false),
    )
    .unwrap();
    let schema = writer.add_schema("test.Msg", "jsonschema", b"{}").unwrap();
    let channel_id = writer
        .add_channel(schema, "/a", "json", &BTreeMap::new())
        .unwrap();
    writer.flush().unwrap();
    for (sequence, &log_time) in log_times.iter().enumerate() {
        let header = MessageHeader {
            channel_id,
            sequence: sequence as u32,
            log_time,
            publish_time: log_time,
        };
        writer.write_to_known_channel(&header, b"{}").unwrap();
    }
    writer.finish().unwrap();
    TempFixture { path }
}

#[test]
fn reader_message_time_range_ignores_chunks_without_messages() {
    let fixture = write_leading_empty_chunk_fixture(&[100, 200]);
    let opened = McapReader::builder()
        .clear_decoders()
        .open(fixture.path())
        .unwrap();
    assert_eq!(opened.summary().chunk_indexes.len(), 2);
    assert_eq!(opened.message_time_range(), Some(100..=200));

    let fixture = write_leading_empty_chunk_fixture(&[]);
    let opened = McapReader::builder()
        .clear_decoders()
        .open(fixture.path())
        .unwrap();
    assert_eq!(opened.message_time_range(), None);
}
