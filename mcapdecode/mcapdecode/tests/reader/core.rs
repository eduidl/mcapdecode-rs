use super::*;

#[test]
fn reads_with_and_without_chunk_indexes() {
    for (use_chunks, indexes) in [(true, true), (true, false), (false, false)] {
        let path = temp_fixture_path("chunk-index-fallback");
        let fixture = TempFixture { path };
        let mut writer = Writer::with_options(
            File::create(fixture.path()).unwrap(),
            WriteOptions::new()
                .use_chunks(use_chunks)
                .emit_chunk_indexes(indexes),
        )
        .unwrap();
        let schema = writer.add_schema("test.Msg", "jsonschema", b"{}").unwrap();
        let channels: Vec<_> = ["/decoded", "/other"]
            .iter()
            .map(|topic| {
                writer
                    .add_channel(schema, topic, "json", &BTreeMap::new())
                    .unwrap()
            })
            .collect();
        for time in 1..=3 {
            for &channel_id in &channels {
                writer
                    .write_to_known_channel(
                        &MessageHeader {
                            channel_id,
                            sequence: 0,
                            log_time: time,
                            publish_time: time,
                        },
                        format!("{{\"value\":{time}}}").as_bytes(),
                    )
                    .unwrap();
            }
        }
        writer.finish().unwrap();
        let reader = McapReader::builder()
            .with_decoder(Box::new(TestJsonDecoder))
            .build()
            .open_file(fixture.path())
            .unwrap();
        let mut count = 0;
        reader
            .read()
            .topic("/decoded")
            .raw()
            .for_each(|_, _| {
                count += 1;
            })
            .unwrap();
        assert_eq!(count, 3);
        for parallel in [false, true] {
            let mut actual = Vec::new();
            reader
                .read()
                .topics(["/decoded", "/other"])
                .options(ReadOptions {
                    parallel: Some(parallel),
                    time_range: Some(TimeRange {
                        start: Some(2),
                        end: Some(3),
                    }),
                })
                .decoded()
                .for_each(|channel, message| {
                    actual.push((channel.topic.clone(), message.log_time));
                })
                .unwrap();
            assert_eq!(
                actual,
                [("/decoded".to_owned(), 2), ("/other".to_owned(), 2)]
            );
        }
    }
}

#[test]
fn raw_reads_include_messages_without_chunk_indexes() {
    for use_chunks in [false, true] {
        let fixture = multi_topic_fixture_with_options(
            WriteOptions::new()
                .use_chunks(use_chunks)
                .emit_chunk_indexes(false),
        );
        let reader = McapReader::open(fixture.path()).unwrap();
        for parallel in [false, true] {
            for (start, end, expected) in [
                (3, 8, vec![3, 4, 5, 6, 7]),
                (4, 6, vec![4, 5]),
                (3, 3, vec![]),
                (2000, 3000, vec![]),
            ] {
                let mut actual = BTreeMap::<String, Vec<u64>>::new();
                reader
                    .read()
                    .topics(["/first", "/second"])
                    .options(ReadOptions {
                        time_range: Some(TimeRange {
                            start: Some(start),
                            end: Some(end),
                        }),
                        parallel: Some(parallel),
                    })
                    .raw()
                    .for_each(|channel, message| {
                        assert_eq!(message.publish_time, message.log_time);
                        let value =
                            message.log_time + if channel.topic == "/second" { 100 } else { 0 };
                        assert_eq!(
                            message.data.as_ref(),
                            format!(r#"{{"value":{value}}}"#).as_bytes()
                        );
                        actual
                            .entry(channel.topic.clone())
                            .or_default()
                            .push(message.log_time);
                    })
                    .unwrap();
                for topic in ["/first", "/second"] {
                    assert_eq!(actual.get(topic).cloned().unwrap_or_default(), expected);
                }
            }
        }
        let mut calls = 0;
        let error = reader
            .read()
            .raw()
            .try_for_each(|_, _| {
                calls += 1;
                Err::<(), _>("stop reading".into())
            })
            .unwrap_err();
        assert_eq!(calls, 1);
        assert!(matches!(error, McapReaderError::Callback(_)));
    }
}

#[test]
fn raw_reads_match_with_parallel_overrides() {
    let fixture = multi_topic_fixture_with_indexes(false);
    let reader = split_reader(false).open_file(fixture.path()).unwrap();
    let mut results = Vec::new();
    for parallel in [false, true] {
        let mut messages = Vec::new();
        reader
            .read()
            .topics(["/first", "/second"])
            .options(ReadOptions {
                time_range: Some(TimeRange {
                    start: Some(4),
                    end: Some(8),
                }),
                parallel: Some(parallel),
            })
            .raw()
            .for_each(|channel, message| {
                messages.push((channel.id, message.log_time));
            })
            .unwrap();
        results.push(messages);
    }
    assert_eq!(results[0], results[1]);
    assert_eq!(results[0].len(), 8);
}

#[test]
fn opened_reader_default_selects_all_channels_even_with_duplicate_topics() {
    let fixture = write_split_topic_fixture("opened-all-channels");
    let reader = split_reader(false).open_file(fixture.path()).unwrap();
    let mut raw = BTreeMap::<u16, usize>::new();
    reader
        .read()
        .raw()
        .for_each(|channel, _| {
            *raw.entry(channel.id).or_default() += 1;
        })
        .unwrap();
    let mut decoded = BTreeMap::<u16, usize>::new();
    reader
        .read()
        .decoded()
        .for_each(|channel, _| {
            *decoded.entry(channel.id).or_default() += 1;
        })
        .unwrap();
    assert_eq!(raw, decoded);
    assert_eq!(raw.len(), 2);
    assert!(raw.values().all(|&count| count == 2));
}

#[test]
fn opened_reader_all_decoded_rejects_schema_less_channel() {
    let reader = McapReader::open(fixture_path("with_summary.mcap")).unwrap();
    assert!(matches!(
        reader.read().decoded().for_each(|_, _| {}),
        Err(McapReaderError::SchemaNotAvailable | McapReaderError::NoDecoder { .. })
    ));
    assert!(matches!(
        reader.read().topic("/raw").decoded().for_each(|_, _| {}),
        Err(McapReaderError::SchemaNotAvailable)
    ));
}

#[test]
fn opened_reader_empty_selection_emits_no_messages() {
    let reader = McapReader::open(fixture_path("with_summary.mcap")).unwrap();
    reader
        .read()
        .topics([] as [&str; 0])
        .raw()
        .for_each(|_, _| panic!("empty selection"))
        .unwrap();
    reader
        .read()
        .topics([] as [&str; 0])
        .decoded()
        .for_each(|_, _| panic!("empty selection"))
        .unwrap();
}

#[test]
fn opened_reader_selects_multiple_channels_and_reports_metadata() {
    let fixture = write_split_topic_fixture("opened-multiple-channels");
    let reader = split_reader(false).open_file(fixture.path()).unwrap();
    let channels = reader.list_channels();
    let ids = [channels[0].id, channels[1].id, channels[0].id];
    let options = ReadOptions {
        time_range: Some(TimeRange {
            start: Some(3),
            end: None,
        }),
        ..ReadOptions::default()
    };
    let mut raw = BTreeMap::new();
    reader
        .read()
        .channels(ids)
        .options(options.clone())
        .raw()
        .for_each(|channel, message| {
            assert_eq!(channel.topic, "/split");
            assert_eq!(channel.schema_name.as_deref(), Some("test.Msg"));
            raw.insert(channel.id, message.log_time);
        })
        .unwrap();
    let mut decoded = BTreeMap::new();
    reader
        .read()
        .channels(ids)
        .options(options)
        .decoded()
        .for_each(|channel, message| {
            assert_eq!(channel.topic, "/split");
            assert_eq!(channel.schema_name.as_deref(), Some("test.Msg"));
            decoded.insert(channel.id, message.log_time);
        })
        .unwrap();
    assert_eq!(raw, decoded);
    assert_eq!(raw.len(), 2);
    assert_eq!(raw[&ids[0]], 3);
    assert_eq!(raw[&ids[1]], 4);
    assert!(matches!(
        reader
            .read()
            .channels([ids[0], u16::MAX])
            .raw()
            .for_each(|_, _| panic!("invalid channel must be rejected before callbacks")),
        Err(McapReaderError::ChannelNotFound)
    ));
}

#[test]
fn opened_reader_exposes_schemas_and_counts() {
    let fixture = write_split_topic_fixture("opened-schema");
    let reader = split_reader(false).open_file(fixture.path()).unwrap();
    let channels = reader.list_channels();
    assert!(
        !reader
            .field_defs_by_channel_id(channels[0].id)
            .unwrap()
            .is_empty()
    );
    assert!(matches!(
        reader.topic_field_defs("/split"),
        Err(McapReaderError::MultipleChannels)
    ));
    assert!(matches!(
        reader.message_count("/split"),
        Err(McapReaderError::MultipleChannels)
    ));
    let fixture = multi_topic_fixture();
    let reader = split_reader(false).open_file(fixture.path()).unwrap();
    assert_eq!(reader.message_count("/first").unwrap(), 1024);
    assert!(!reader.topic_field_defs("/first").unwrap().is_empty());
}

#[test]
fn opened_reader_reuses_file_for_topic_and_channel_reads() {
    let fixture = write_split_topic_fixture("opened-reader");
    let reader = split_reader(false).open_file(fixture.path()).unwrap();
    let channels = reader.list_channels();
    assert_eq!(reader.list_topics().len(), 1);

    for _ in 0..2 {
        let mut times = Vec::new();
        reader
            .read()
            .channel(channels[0].id)
            .decoded()
            .for_each(|topic, message| {
                assert_eq!(topic.topic, "/split");
                times.push(message.log_time);
            })
            .unwrap();
        assert_eq!(times, [1, 3]);
    }
    assert!(matches!(
        reader
            .read()
            .topics(["/split"])
            .decoded()
            .for_each(|_, _| {}),
        Err(McapReaderError::MultipleChannels)
    ));
}

#[test]
fn opened_reader_selects_raw_topics_and_channel() {
    let fixture = write_split_topic_fixture("opened-raw");
    let reader = McapReader::open(fixture.path()).unwrap();
    let channel = reader.list_channels()[1].id;
    let mut times = Vec::new();
    reader
        .read()
        .channel(channel)
        .raw()
        .for_each(|topic, message| {
            assert_eq!(topic.topic, "/split");
            times.push(message.log_time);
        })
        .unwrap();
    assert_eq!(times, [2, 4]);
    assert!(matches!(
        reader.read().topics(["/split"]).raw().for_each(|_, _| {}),
        Err(McapReaderError::MultipleChannels)
    ));
}

#[test]
fn opened_reader_decoded_topics_apply_options_and_deduplicate() {
    let fixture = multi_topic_fixture();
    let reader = McapReader::builder()
        .with_decoder(Box::new(SchemaNamedFieldDecoder))
        .build()
        .open_file(fixture.path())
        .unwrap();
    let mut topics = Vec::new();
    reader
        .read()
        .topics(["/first", "/first", "/second"])
        .options(ReadOptions {
            time_range: Some(TimeRange {
                start: None,
                end: Some(1),
            }),
            ..ReadOptions::default()
        })
        .decoded()
        .for_each(|topic, _| {
            topics.push(topic.topic.clone());
        })
        .unwrap();
    assert_eq!(topics, ["/first", "/second"]);
}

#[test]
fn opened_reader_raw_and_decoded_share_filters() {
    for indexes in [false, true] {
        let fixture = multi_topic_fixture_with_indexes(indexes);
        let reader = split_reader(false).open_file(fixture.path()).unwrap();
        let options = ReadOptions {
            time_range: Some(TimeRange {
                start: Some(4),
                end: Some(6),
            }),
            ..ReadOptions::default()
        };
        let mut raw = BTreeMap::<String, Vec<u64>>::new();
        let mut decoded = BTreeMap::<String, Vec<u64>>::new();
        reader
            .read()
            .topics(["/first", "/second"])
            .options(options.clone())
            .raw()
            .for_each(|topic, message| {
                raw.entry(topic.topic.clone())
                    .or_default()
                    .push(message.log_time);
            })
            .unwrap();
        reader
            .read()
            .topics(["/first", "/second"])
            .options(options)
            .decoded()
            .for_each(|topic, message| {
                decoded
                    .entry(topic.topic.clone())
                    .or_default()
                    .push(message.log_time);
            })
            .unwrap();
        assert_eq!(raw, decoded);
        assert_eq!(raw["/first"], [4, 5]);
        assert_eq!(raw["/second"], [4, 5]);
    }
}

pub(super) fn split_reader(parallel: bool) -> McapReader {
    McapReader::builder()
        .with_decoder(Box::new(SchemaNamedFieldDecoder))
        .with_parallel(parallel)
        .build()
}

fn split_channel_ids(reader: &McapReader, path: &Path) -> Vec<u16> {
    reader
        .list_channels(path)
        .unwrap()
        .into_iter()
        .map(|channel| channel.id)
        .collect()
}

#[test]
fn list_channels_reports_each_channel_in_id_order() {
    let reader = McapReader::new();
    let fixture = write_split_topic_fixture("list-channels");

    let channels = reader.list_channels(fixture.path()).unwrap();

    assert!(channels.is_sorted_by_key(|channel| channel.id));
    assert_eq!(
        channels
            .iter()
            .map(|channel| channel.message_count)
            .collect::<Vec<_>>(),
        [Some(2), Some(2), Some(0)]
    );
    assert!(channels.iter().all(|channel| channel.topic == "/split"));
    assert_eq!(channels[0].schema_name.as_deref(), Some("test.Msg"));
    assert_eq!(channels[0].schema_encoding, "jsonschema");
    assert_eq!(channels[0].message_encoding, "json");
    assert_eq!(channels[0].metadata["source"], "left");
    assert!(channels[1].metadata.is_empty());
}

#[test]
fn list_channels_no_summary_returns_error() {
    let reader = McapReader::new();
    assert!(matches!(
        reader.list_channels(&fixture_path("no_summary.mcap")),
        Err(McapReaderError::SummaryNotAvailable)
    ));
}

#[test]
fn decoded_read_by_channel_id_returns_only_that_channel() {
    let fixture = write_split_topic_fixture("decoded-by-channel");
    for parallel in [false, true] {
        let reader = split_reader(parallel);
        let mut per_channel = Vec::new();
        for id in split_channel_ids(&reader, fixture.path()) {
            let mut log_times = Vec::new();
            reader
                .open_file(fixture.path())
                .unwrap()
                .read()
                .channel(id)
                .decoded()
                .for_each(|_, message| {
                    log_times.push(message.log_time);
                })
                .unwrap();
            per_channel.push(log_times);
        }

        assert_eq!(
            per_channel,
            [vec![1, 3], vec![2, 4], vec![]],
            "parallel={parallel}"
        );
    }
}

#[test]
fn decoded_read_by_channel_id_applies_options() {
    let fixture = write_split_topic_fixture("decoded-by-channel-options");
    let reader = split_reader(false);
    let id = split_channel_ids(&reader, fixture.path())[1];
    let mut log_times = Vec::new();

    reader
        .open_file(fixture.path())
        .unwrap()
        .read()
        .channel(id)
        .options(ReadOptions {
            time_range: Some(TimeRange {
                start: Some(3),
                end: None,
            }),
            ..ReadOptions::default()
        })
        .decoded()
        .for_each(|_, message| {
            log_times.push(message.log_time);
        })
        .unwrap();

    assert_eq!(log_times, [4]);
}

#[test]
fn raw_read_by_channel_id_returns_only_that_channel() {
    let fixture = write_split_topic_fixture("raw-by-channel");
    for parallel in [false, true] {
        let reader = McapReader::builder().with_parallel(parallel).build();
        let mut per_channel = Vec::new();
        for id in split_channel_ids(&reader, fixture.path()) {
            let mut log_times = Vec::new();
            reader
                .open_file(fixture.path())
                .unwrap()
                .read()
                .channel(id)
                .raw()
                .for_each(|_, message| {
                    log_times.push(message.log_time);
                })
                .unwrap();
            per_channel.push(log_times);
        }
        assert_eq!(per_channel, [vec![1, 3], vec![2, 4], vec![]]);
    }
}

#[test]
fn field_defs_by_channel_id_uses_that_channels_schema() {
    let fixture = write_split_topic_fixture("field-defs-by-channel");
    let reader = split_reader(true);
    let names: Vec<_> = split_channel_ids(&reader, fixture.path())
        .into_iter()
        .map(|id| {
            let field_defs = reader.field_defs_by_channel_id(fixture.path(), id).unwrap();
            field_defs[0].name.clone()
        })
        .collect();

    assert_eq!(names, ["first", "second", "empty"]);
}

#[test]
fn channel_id_reads_report_unknown_channel() {
    let fixture = write_split_topic_fixture("unknown-channel");
    let reader = split_reader(true);
    let unknown = u16::MAX;

    assert!(matches!(
        reader.field_defs_by_channel_id(fixture.path(), unknown),
        Err(McapReaderError::ChannelNotFound)
    ));
    assert!(matches!(
        reader
            .open_file(fixture.path())
            .unwrap()
            .read()
            .channel(unknown)
            .decoded()
            .for_each(|_, _| {}),
        Err(McapReaderError::ChannelNotFound)
    ));
    assert!(matches!(
        reader
            .open_file(fixture.path())
            .unwrap()
            .read()
            .channel(unknown)
            .raw()
            .for_each(|_, _| {}),
        Err(McapReaderError::ChannelNotFound)
    ));
}

#[test]
fn topic_reads_reject_a_topic_with_multiple_channels() {
    let fixture = write_split_topic_fixture("topic-multiple-channels");
    let reader = split_reader(true);

    assert!(matches!(
        reader.topic_field_defs(fixture.path(), "/split"),
        Err(McapReaderError::MultipleChannels)
    ));
    assert!(matches!(
        reader
            .open_file(fixture.path())
            .unwrap()
            .read()
            .topic("/split")
            .decoded()
            .for_each(|_, _| {}),
        Err(McapReaderError::MultipleChannels)
    ));
    assert!(matches!(
        reader
            .open_file(fixture.path())
            .unwrap()
            .read()
            .topic("/split")
            .raw()
            .for_each(|_, _| {}),
        Err(McapReaderError::MultipleChannels)
    ));
    assert!(matches!(
        reader.message_count(fixture.path(), "/split"),
        Err(McapReaderError::MultipleChannels)
    ));
}

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

#[test]
fn opened_topic_schema_enables_value_access() {
    let fixture = write_chunked_fixture("opened-value-view", &[br#"{"value":42}"#]);
    let reader = McapReader::builder()
        .with_decoder(Box::new(TestJsonDecoder))
        .build()
        .open_file(fixture.path())
        .unwrap();
    reader
        .read()
        .topic("/decoded")
        .decoded()
        .try_for_each(|_, message| {
            assert_eq!(message.value.get("value")?.try_widen_i64()?, 42);
            Ok(())
        })
        .unwrap();
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
fn decoded_read_unknown_topic_returns_error() {
    let mut reader = McapReader::new();
    reader.register_decoder(Box::new(TestJsonDecoder));

    let err = reader
        .open_file(fixture_path("with_summary.mcap"))
        .unwrap()
        .read()
        .topic("/unknown")
        .decoded()
        .for_each(|_, _message| {})
        .unwrap_err();

    assert!(matches!(err, McapReaderError::TopicNotFound));
}

#[test]
fn decoded_read_errors_when_schema_is_missing() {
    let mut reader = McapReader::new();
    reader.register_decoder(Box::new(TestJsonDecoder));

    let err = reader
        .open_file(fixture_path("with_summary.mcap"))
        .unwrap()
        .read()
        .topic("/raw")
        .decoded()
        .for_each(|_, _message| {})
        .unwrap_err();

    assert!(matches!(err, McapReaderError::SchemaNotAvailable));
}

#[test]
fn decoded_read_without_decoder_returns_error() {
    let reader = McapReader::new();

    let err = reader
        .open_file(fixture_path("with_summary.mcap"))
        .unwrap()
        .read()
        .topic("/decoded")
        .decoded()
        .for_each(|_, _message| {})
        .unwrap_err();

    assert!(matches!(err, McapReaderError::NoDecoder { .. }));
}

#[test]
fn raw_read_reads_schema_less_topic_payloads() {
    let reader = McapReader::new();

    assert_eq!(
        collect_raw_payloads(&reader, &fixture_path("with_summary.mcap"), "/raw"),
        vec![vec![0x01, 0x02, 0x03]]
    );
}

#[test]
fn raw_read_unknown_topic_returns_error() {
    let reader = McapReader::new();

    let err = reader
        .open_file(fixture_path("with_summary.mcap"))
        .unwrap()
        .read()
        .topic("/unknown")
        .raw()
        .for_each(|_, _message| {})
        .unwrap_err();

    assert!(matches!(err, McapReaderError::TopicNotFound));
}

#[test]
fn raw_read_propagates_callback_error() {
    let reader = McapReader::new();

    let err = reader
        .open_file(fixture_path("with_summary.mcap"))
        .unwrap()
        .read()
        .topic("/raw")
        .raw()
        .try_for_each(|_, _message| Err::<(), _>("callback failed".into()))
        .unwrap_err();

    assert!(matches!(err, McapReaderError::Callback(_)));
    assert!(err.to_string().contains("callback failed"));
}

#[test]
fn decoded_read_parallel_matches_sequential_for_multi_chunk_fixture() {
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
fn decoded_read_parallel_propagates_decode_error() {
    let fixture = write_chunked_fixture(
        "parallel-decode-error-decoded",
        &[br#"{"value":1}"#, b"invalid", br#"{"value":3}"#],
    );

    let reader = McapReader::builder()
        .with_decoder(Box::new(TestJsonDecoder))
        .with_parallel(true)
        .build();

    let err = reader
        .open_file(fixture.path())
        .unwrap()
        .read()
        .topic("/decoded")
        .decoded()
        .for_each(|_, _message| {})
        .unwrap_err();

    assert!(matches!(
        err,
        McapReaderError::Decoder(DecoderError::MessageDecode(_))
    ));
}

#[test]
fn decoded_read_propagates_callback_error() {
    let mut reader = McapReader::new();
    reader.register_decoder(Box::new(TestJsonDecoder));

    let err = reader
        .open_file(fixture_path("with_summary.mcap"))
        .unwrap()
        .read()
        .topic("/decoded")
        .decoded()
        .try_for_each(|_, _message| Err::<(), _>("callback failed".into()))
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
fn parallel_reader_callback_can_use_rayon() {
    let fixture = multi_topic_fixture();
    let reader = McapReader::builder()
        .with_decoder(Box::new(TestJsonDecoder))
        .with_parallel(true)
        .build();
    for topics in [&["/first"][..], &["/first", "/second"][..]] {
        let mut calls = 0;
        reader
            .open_file(fixture.path())
            .unwrap()
            .read()
            .topics(topics)
            .decoded()
            .for_each(|_, message| {
                let (log_time, publish_time) =
                    rayon::join(|| message.log_time, || message.publish_time);
                assert_eq!(log_time, publish_time);
                calls += 1;
            })
            .unwrap();
        assert_eq!(calls, 1024 * topics.len());
    }
}

#[test]
fn parallel_reader_stops_after_callback_error_across_windows() {
    let fixture = multi_topic_fixture();
    let reader = McapReader::builder()
        .with_decoder(Box::new(TestJsonDecoder))
        .with_parallel(true)
        .build();
    for topics in [&["/first"][..], &["/first", "/second"][..]] {
        let mut calls = 0;
        let error = reader
            .open_file(fixture.path())
            .unwrap()
            .read()
            .topics(topics)
            .decoded()
            .try_for_each(|_, _| {
                calls += 1;
                if calls == 500 {
                    return Err("stop reading".into());
                }
                Ok(())
            })
            .unwrap_err();
        assert!(matches!(error, McapReaderError::Callback(_)));
        assert_eq!(calls, 500);
    }
}

#[test]
fn decoded_read_parallel_stops_after_callback_error() {
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
        .open_file(fixture.path())
        .unwrap()
        .read()
        .topic("/decoded")
        .decoded()
        .try_for_each(|_, message| {
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

#[test]
#[cfg(all(feature = "ros2msg", feature = "ros2idl"))]
fn topic_schema_exposes_named_ros_types_without_message_payloads() {
    let schemas = [
        (
            "ros2msg",
            "builtin_interfaces/Time stamp\nbuiltin_interfaces/Time[2] fixed\nbuiltin_interfaces/Time[<=3] bounded\nbuiltin_interfaces/Time[] stamps\nuint32 count\n",
        ),
        (
            "ros2idl",
            "IDL: example/msg/Root\nmodule example { module msg { enum Mode { OFF, ON }; struct Root { Mode mode; builtin_interfaces::msg::Time stamp; builtin_interfaces::msg::Time fixed[2]; sequence<builtin_interfaces::msg::Time, 3> bounded; sequence<builtin_interfaces::msg::Time> stamps; uint32 count; }; }; };",
        ),
    ];
    for (encoding, schema) in schemas {
        let fixture = TempFixture {
            path: temp_fixture_path(encoding),
        };
        let mut writer = Writer::new(File::create(fixture.path()).unwrap()).unwrap();
        let id = writer
            .add_schema("example/msg/Root", encoding, schema.as_bytes())
            .unwrap();
        writer
            .add_channel(id, "/types", "cdr", &BTreeMap::new())
            .unwrap();
        writer.finish().unwrap();
        let reader = McapReader::builder().with_default_decoders().build();
        let metadata = reader.topic_schema(fixture.path(), "/types").unwrap();
        let displayed: Vec<_> = metadata
            .field_defs
            .iter()
            .map(|field| field.element.to_string())
            .collect();
        let mut expected_display = vec![
            "builtin_interfaces/msg/Time",
            "builtin_interfaces/msg/Time[2]",
            "builtin_interfaces/msg/Time[<=3]",
            "builtin_interfaces/msg/Time[]",
            "uint32",
        ];
        if encoding == "ros2idl" {
            expected_display.insert(0, "example/msg/Mode");
        }
        assert_eq!(displayed, expected_display, "{encoding}");
    }
}

#[test]
fn multi_topic_decoded_reads_work_without_arrow() {
    let fixture = multi_topic_fixture();
    for parallel in [false, true] {
        let reader = McapReader::builder()
            .with_decoder(Box::new(TestJsonDecoder))
            .with_parallel(parallel)
            .build();
        for options in [
            ReadOptions::default(),
            ReadOptions {
                time_range: Some(TimeRange {
                    start: Some(2),
                    end: Some(7),
                }),
                ..ReadOptions::default()
            },
            ReadOptions {
                time_range: Some(TimeRange {
                    start: Some(3),
                    end: Some(9),
                }),
                ..ReadOptions::default()
            },
        ] {
            let mut actual = BTreeMap::<String, Vec<(u64, u64, i64)>>::new();
            reader
                .open_file(fixture.path())
                .unwrap()
                .read()
                .topics(["/first", "/second", "/first", "/empty", "/second"])
                .options(options.clone())
                .decoded()
                .for_each(|channel, message| {
                    let Value::Struct(fields) = message.value else {
                        panic!("expected struct")
                    };
                    let Value::I64(value) = fields[0] else {
                        panic!("expected i64")
                    };
                    actual.entry(channel.topic.clone()).or_default().push((
                        message.log_time,
                        message.publish_time,
                        value,
                    ));
                })
                .unwrap();
            for topic in ["/first", "/second", "/empty"] {
                let mut expected = Vec::new();
                reader
                    .open_file(fixture.path())
                    .unwrap()
                    .read()
                    .topic(topic)
                    .options(options.clone())
                    .decoded()
                    .for_each(|_, message| {
                        let Value::Struct(fields) = message.value else {
                            panic!("expected struct")
                        };
                        let Value::I64(value) = fields[0] else {
                            panic!("expected i64")
                        };
                        expected.push((message.log_time, message.publish_time, value));
                    })
                    .unwrap();
                assert_eq!(actual.get(topic).cloned().unwrap_or_default(), expected);
            }
        }
        let mut calls = 0;
        let error = reader
            .open_file(fixture.path())
            .unwrap()
            .read()
            .topics(["/first", "/second"])
            .decoded()
            .try_for_each(|_, _| {
                calls += 1;
                Err::<(), _>("stop reading".into())
            })
            .unwrap_err();
        assert_eq!(calls, 1);
        let McapReaderError::Callback(inner) = error else {
            panic!("wrong error kind")
        };
        assert!(inner.downcast_ref::<McapReaderError>().is_none());
    }
}

#[test]
fn repeated_single_topic_is_read_once() {
    let fixture = multi_topic_fixture();
    let reader = McapReader::builder()
        .with_decoder(Box::new(TestJsonDecoder))
        .build();
    let options = ReadOptions::default();
    let mut topics = Vec::new();
    reader
        .open_file(fixture.path())
        .unwrap()
        .read()
        .topics(["/first", "/first"])
        .options(options)
        .decoded()
        .for_each(|channel, message| {
            topics.push((channel.topic.clone(), message.log_time));
        })
        .unwrap();
    assert_eq!(
        topics,
        (0..1024)
            .map(|time| ("/first".to_owned(), time))
            .collect::<Vec<_>>()
    );
}

#[test]
fn decoded_reads_scan_chunks_without_message_indexes() {
    let fixture = multi_topic_fixture_with_indexes(false);
    for parallel in [false, true] {
        let reader = McapReader::builder()
            .with_decoder(Box::new(TestJsonDecoder))
            .with_parallel(parallel)
            .build();
        let mut times = Vec::new();
        reader
            .open_file(fixture.path())
            .unwrap()
            .read()
            .topic("/first")
            .decoded()
            .for_each(|_, message| {
                times.push(message.log_time);
            })
            .unwrap();
        assert_eq!(times, (0..1024).collect::<Vec<_>>());

        for options in [
            ReadOptions::default(),
            ReadOptions {
                time_range: Some(TimeRange {
                    start: Some(3),
                    end: Some(9),
                }),
                ..Default::default()
            },
            ReadOptions {
                time_range: Some(TimeRange {
                    start: Some(4),
                    end: Some(6),
                }),
                ..Default::default()
            },
        ] {
            let mut actual = BTreeMap::<String, Vec<(u64, i64)>>::new();
            reader
                .open_file(fixture.path())
                .unwrap()
                .read()
                .topics(["/first", "/second"])
                .options(options.clone())
                .decoded()
                .for_each(|channel, message| {
                    let Value::Struct(fields) = message.value else {
                        panic!("expected struct")
                    };
                    let Value::I64(value) = fields[0] else {
                        panic!("expected i64")
                    };
                    actual
                        .entry(channel.topic.clone())
                        .or_default()
                        .push((message.log_time, value));
                })
                .unwrap();
            let range = options.time_range.map_or(0..1024, |range| {
                range.start.unwrap() as i64..range.end.unwrap() as i64
            });
            for (topic, offset) in [("/first", 0), ("/second", 100)] {
                assert_eq!(
                    actual[topic],
                    range
                        .clone()
                        .map(|i| (i as u64, i + offset))
                        .collect::<Vec<_>>()
                );
            }
        }
    }
}

#[test]
#[cfg(feature = "rawjson")]
fn default_decoders_expose_json_topics_unparsed() {
    let fixture = TempFixture {
        path: temp_fixture_path("json"),
    };
    let mut writer = Writer::new(File::create(fixture.path()).unwrap()).unwrap();
    let schema = writer.add_schema("test.Msg", "jsonschema", b"").unwrap();
    let channel = writer
        .add_channel(schema, "/json", "json", &BTreeMap::new())
        .unwrap();
    writer
        .write_to_known_channel(
            &MessageHeader {
                channel_id: channel,
                sequence: 0,
                log_time: 1,
                publish_time: 1,
            },
            br#"{"x": 7}"#,
        )
        .unwrap();
    writer.finish().unwrap();

    let reader = McapReader::builder().with_default_decoders().build();
    let metadata = reader.topic_schema(fixture.path(), "/json").unwrap();
    assert_eq!(metadata.field_defs[0].name, "json");
    assert_eq!(
        metadata.field_defs[0].element.data_type,
        DataTypeDef::String
    );

    let mut decoded = Vec::new();
    reader
        .open_file(fixture.path())
        .unwrap()
        .read()
        .topic("/json")
        .decoded()
        .try_for_each(|_, message| {
            decoded.push(message.value.get("json")?.try_str()?.unwrap().to_owned());
            Ok(())
        })
        .unwrap();
    assert_eq!(decoded, [r#"{"x": 7}"#]);
}
