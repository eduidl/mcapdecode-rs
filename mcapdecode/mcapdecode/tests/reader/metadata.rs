use super::*;

#[test]
fn reader_reuses_metadata_and_custom_decoders_without_reopening() {
    let fixture = write_chunked_fixture("opened-reuse", &[br#"{"value":42}"#]);
    let builder = McapReader::builder().with_decoder(Box::new(TestJsonDecoder));
    let opened = builder.open(fixture.path()).unwrap();
    let topics = opened.list_topics();
    let channels = opened.list_channels();
    let count = opened.message_count("/decoded").unwrap();
    let fields = opened.topic_field_defs("/decoded").unwrap();
    let schema = opened.topic_schema("/decoded").unwrap();
    let statuses = opened.list_topics_with_decode_status();
    assert_eq!(count, 1);
    assert_eq!(
        fields,
        vec![FieldDef::new("value", DataTypeDef::I64, true)].into()
    );
    #[cfg(unix)]
    fs::remove_file(fixture.path()).unwrap();

    for _ in 0..2 {
        assert_eq!(opened.topic_schema("/decoded").unwrap(), schema);
        assert_eq!(opened.list_topics_with_decode_status(), statuses);
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
fn reader_preserves_lookup_and_decoder_errors() {
    let path = fixture_path("with_summary.mcap");
    let opened = McapReader::builder().open(&path).unwrap();
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
fn reader_requires_a_channel_for_ambiguous_topics() {
    let fixture = write_split_topic_fixture("opened-split");
    let opened = McapReader::builder()
        .with_decoder(Box::new(SchemaNamedFieldDecoder))
        .open(fixture.path())
        .unwrap();
    assert_eq!(opened.list_topics()[0].channel_count, 3);
    assert!(matches!(
        opened.topic_schema("/split"),
        Err(McapReaderError::MultipleChannels)
    ));
    assert!(matches!(
        opened.message_count("/split"),
        Err(McapReaderError::MultipleChannels)
    ));
    assert!(matches!(
        opened.topic_field_defs("/split"),
        Err(McapReaderError::MultipleChannels)
    ));
    let names: Vec<_> = opened
        .list_channels()
        .iter()
        .map(|channel| {
            opened.field_defs_by_channel_id(channel.id).unwrap()[0]
                .name
                .clone()
        })
        .collect();
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
    let builder = McapReader::builder();
    assert!(matches!(
        builder.open(fixture.path()).unwrap().message_count("/raw"),
        Err(McapReaderError::StatsNotAvailable)
    ));
    let opened = builder.open(fixture.path()).unwrap();
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
        .open(fixture.path())
        .unwrap()
        .topic_field_defs("/decoded")
        .unwrap();
    let opened = McapReader::open(fixture.path()).unwrap();
    assert_eq!(opened.topic_field_defs("/decoded").unwrap(), expected);
}

#[cfg(feature = "arrow")]
#[test]
fn reader_batch_schema_uses_custom_metadata() {
    let fixture = write_chunked_fixture("opened-arrow", &[br#"{"value":42}"#]);
    let opened = McapReader::builder()
        .with_decoder(Box::new(TestJsonDecoder))
        .open(fixture.path())
        .unwrap();
    let metadata = mcapdecode::MetadataColumns::with_prefix("mcap_")
        .with_timestamp_format(mcapdecode::MetadataTimestampFormat::UnixNanoseconds);
    let actual = opened.topic_batch_schema("/decoded", &metadata).unwrap();
    let names: Vec<_> = actual
        .schema()
        .fields()
        .iter()
        .take(2)
        .map(|field| field.name().as_str())
        .collect();
    assert_eq!(names, metadata.names());
    assert!(matches!(
        opened.topic_batch_schema("/unknown", &Default::default()),
        Err(McapReaderError::TopicNotFound)
    ));
}

#[test]
fn builder_opens_multiple_files_with_the_same_decoders() {
    let first_fixture = write_chunked_fixture("opened-first", &[br#"{"value":1}"#]);
    let second_fixture =
        write_chunked_fixture("opened-second", &[br#"{"value":2}"#, br#"{"value":3}"#]);
    let builder = McapReader::builder();
    let unconfigured = builder.open(first_fixture.path()).unwrap();
    let builder = builder.with_decoder(Box::new(TestJsonDecoder));
    let first = builder.open(first_fixture.path()).unwrap();
    let second = builder.open(second_fixture.path()).unwrap();
    let fields: FieldDefs = vec![FieldDef::new("value", DataTypeDef::I64, true)].into();
    assert_eq!(first.topic_field_defs("/decoded").unwrap(), fields);
    assert_eq!(second.topic_field_defs("/decoded").unwrap(), fields);
    assert_eq!(first.message_count("/decoded").unwrap(), 1);
    assert_eq!(second.message_count("/decoded").unwrap(), 2);
    assert!(matches!(
        unconfigured.topic_field_defs("/decoded"),
        Err(McapReaderError::NoDecoder { .. })
    ));
}

#[test]
fn reader_reports_zero_for_an_empty_channel_with_statistics() {
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

#[test]
fn reader_message_time_range_uses_statistics() {
    let fixture = write_chunked_fixture("time-range-stats", &[b"{}", b"{}", b"{}"]);
    let opened = McapReader::builder().open(fixture.path()).unwrap();
    assert_eq!(opened.message_time_range(), Some(1..=3));
}

#[test]
fn reader_message_time_range_falls_back_to_chunk_indexes() {
    let fixture = multi_topic_fixture_with_options(
        WriteOptions::new()
            .chunk_size(Some(128))
            .emit_statistics(false),
    );
    let opened = McapReader::builder().open(fixture.path()).unwrap();
    assert_eq!(opened.message_time_range(), Some(0..=1023));
}

#[test]
fn reader_message_time_range_is_none_without_messages() {
    let fixture = write_chunked_fixture("time-range-empty", &[]);
    let opened = McapReader::builder().open(fixture.path()).unwrap();
    assert_eq!(opened.message_time_range(), None);
}

#[test]
fn reader_summary_exposes_statistics() {
    let fixture = write_chunked_fixture("summary", &[b"{}", b"{}", b"{}"]);
    let opened = McapReader::builder().open(fixture.path()).unwrap();
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
    let opened = McapReader::builder().open(fixture.path()).unwrap();
    assert_eq!(opened.summary().chunk_indexes.len(), 2);
    assert_eq!(opened.message_time_range(), Some(100..=200));

    let fixture = write_leading_empty_chunk_fixture(&[]);
    let opened = McapReader::builder().open(fixture.path()).unwrap();
    assert_eq!(opened.message_time_range(), None);
}
