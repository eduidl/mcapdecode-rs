use std::ops::Bound;

use mcapdecode::RawMessage;

use super::*;

const TOPICS: [&str; 2] = ["/a", "/b"];
/// The topic of each channel. `/a` spans two channels, both selected with the topic.
const CHANNEL_TOPICS: [&str; 3] = ["/a", "/b", "/a"];

/// `(channel index, log_time)` of each message in file order. Times go back and forth so
/// chunk time ranges overlap, and `/b` starts late.
fn messages() -> Vec<(usize, u64)> {
    let mut state = 7u64;
    (0..80)
        .map(|i| {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1);
            let time = (state >> 33) % 40;
            if i % 3 == 0 && time >= 10 {
                (1, time)
            } else {
                (i % 2 * 2, time)
            }
        })
        .collect()
}

/// Write [`messages`] whose payloads hold their position in file order.
fn write_fixture(name: &str, options: WriteOptions) -> TempFixture {
    let path = temp_fixture_path(name);
    let mut writer = Writer::with_options(
        File::create(&path).unwrap(),
        options.library("mcapdecode-test"),
    )
    .unwrap();
    let schema_id = writer
        .add_schema("test.Msg", "jsonschema", br#"{"type":"object"}"#)
        .unwrap();
    // Distinct metadata keeps the writer from merging the two `/a` channels.
    let channel_ids: Vec<_> = (CHANNEL_TOPICS.iter().enumerate())
        .map(|(i, topic)| {
            let metadata = BTreeMap::from([("index".to_owned(), i.to_string())]);
            writer
                .add_channel(schema_id, topic, "json", &metadata)
                .unwrap()
        })
        .collect();
    for (sequence, (channel, time)) in messages().into_iter().enumerate() {
        writer
            .write_to_known_channel(
                &MessageHeader {
                    channel_id: channel_ids[channel],
                    sequence: sequence as u32,
                    log_time: time,
                    publish_time: time,
                },
                format!(r#"{{"value":{sequence}}}"#).as_bytes(),
            )
            .unwrap();
    }
    writer.finish().unwrap();
    TempFixture { path }
}

/// Fixtures with and without chunk compression.
fn fixtures() -> Vec<TempFixture> {
    let base = || WriteOptions::new().chunk_size(Some(96));
    [base(), base().compression(None)]
        .into_iter()
        .map(|options| write_fixture("latest", options))
        .collect()
}

fn reader(path: &Path, parallel: bool) -> McapReader {
    McapReader::builder()
        .with_decoder(TestJsonDecoder)
        .with_parallel(parallel)
        .open(path)
        .unwrap()
}

type Range = (Bound<u64>, Bound<u64>);

/// `(channel id, log_time)` pairs returned by `latest`, checking that each payload is
/// that of a message at the returned time. Ties may return any of the tied messages.
fn latest(reader: &McapReader, range: Range) -> Vec<(u16, u64)> {
    let messages = messages();
    reader
        .read()
        .topics(TOPICS)
        .time_range(range)
        .raw()
        .latest()
        .unwrap()
        .into_iter()
        .map(|(info, message)| {
            let sequence = decode_test_value(&message.data).unwrap() as usize;
            assert_eq!(messages[sequence].1, message.log_time);
            (info.id, message.log_time)
        })
        .collect()
}

/// The expected `latest` result, found by reading every selected message.
fn expected(reader: &McapReader, range: Range) -> Vec<(u16, u64)> {
    let mut latest: BTreeMap<u16, RawMessage> = BTreeMap::new();
    reader
        .read()
        .topics(TOPICS)
        .time_range(range)
        .raw()
        .for_each(|info, message| {
            let replace = latest
                .get(&info.id)
                .is_none_or(|current| message.log_time > current.log_time);
            if replace {
                latest.insert(info.id, message);
            }
        })
        .unwrap();
    let channels = reader.read().topics(TOPICS).selected_channels().unwrap();
    assert_eq!(channels.len(), CHANNEL_TOPICS.len());
    channels
        .iter()
        .filter_map(|channel| {
            let message = latest.get(&channel.id)?;
            Some((channel.id, message.log_time))
        })
        .collect()
}

#[test]
fn latest_matches_a_full_read() {
    for fixture in fixtures() {
        for parallel in [false, true] {
            let reader = reader(fixture.path(), parallel);
            for start in [Unbounded, Included(14), Excluded(14)] {
                for time in 0..=45 {
                    for end in [Unbounded, Included(time), Excluded(time)] {
                        let range = (start, end);
                        assert_eq!(
                            latest(&reader, range),
                            expected(&reader, range),
                            "range {range:?}, parallel {parallel}",
                        );
                    }
                }
            }
        }
    }
}

#[test]
fn decoded_latest_decodes_the_latest_message() {
    let fixture = write_fixture("latest-decoded", WriteOptions::new().chunk_size(Some(96)));
    let reader = reader(fixture.path(), true);
    let raw = reader
        .read()
        .topics(TOPICS)
        .time_range(..=30)
        .raw()
        .latest()
        .unwrap();
    let decoded = reader
        .read()
        .topics(TOPICS)
        .time_range(..=30)
        .decoded()
        .latest()
        .unwrap();
    assert_eq!(raw.len(), decoded.len());
    for ((raw_info, raw), (info, message)) in raw.into_iter().zip(decoded) {
        assert_eq!(raw_info, info);
        assert_eq!(raw.log_time, message.log_time);
        assert_eq!(raw.publish_time, message.publish_time);
        let value = decode_test_value(&raw.data).unwrap();
        assert_eq!(
            message
                .value
                .get_path("value")
                .unwrap()
                .try_widen_i64()
                .unwrap(),
            value,
        );
    }
}

#[test]
fn decoded_latest_needs_no_decoder_for_channels_without_a_result() {
    let path = temp_fixture_path("latest-no-decoder");
    let fixture = TempFixture { path: path.clone() };
    let mut writer = Writer::new(File::create(&path).unwrap()).unwrap();
    let json = writer
        .add_schema("test.Msg", "jsonschema", br#"{"type":"object"}"#)
        .unwrap();
    let unknown = writer.add_schema("test.Other", "unknown", b"").unwrap();
    let metadata = BTreeMap::new();
    let a = writer.add_channel(json, "/a", "json", &metadata).unwrap();
    let c = writer
        .add_channel(unknown, "/c", "unknown", &metadata)
        .unwrap();
    for (channel_id, time) in [(a, 10), (c, 20)] {
        let header = MessageHeader {
            channel_id,
            sequence: 0,
            log_time: time,
            publish_time: time,
        };
        writer
            .write_to_known_channel(&header, br#"{"value":1}"#)
            .unwrap();
    }
    writer.finish().unwrap();

    for parallel in [false, true] {
        let reader = reader(fixture.path(), parallel);
        let read = |time| {
            reader
                .read()
                .topics(["/a", "/c"])
                .time_range(..=time)
                .decoded()
        };
        let decoded = read(15).latest().unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].0.topic, "/a");
        assert!(matches!(
            read(20).latest(),
            Err(McapReaderError::NoDecoder { .. })
        ));
    }
}

#[test]
fn latest_fails_without_indexes() {
    let base = || WriteOptions::new().chunk_size(Some(96));
    for options in [
        base().emit_message_indexes(false),
        base().emit_chunk_indexes(false),
    ] {
        let fixture = write_fixture("latest-no-index", options);
        let reader = reader(fixture.path(), true);
        let result = reader.read().topics(TOPICS).raw().latest();
        assert!(matches!(
            result,
            Err(McapReaderError::MessageIndexNotAvailable)
        ));
    }
}

/// Write `/a` messages at `times` into a file with the given options.
fn write_times(name: &str, options: WriteOptions, times: &[u64]) -> TempFixture {
    let path = temp_fixture_path(name);
    let mut writer = Writer::with_options(File::create(&path).unwrap(), options).unwrap();
    let schema_id = writer
        .add_schema("test.Msg", "jsonschema", br#"{"type":"object"}"#)
        .unwrap();
    let channel_id = writer
        .add_channel(schema_id, "/a", "json", &BTreeMap::new())
        .unwrap();
    for (sequence, &time) in times.iter().enumerate() {
        let header = MessageHeader {
            channel_id,
            sequence: sequence as u32,
            log_time: time,
            publish_time: time,
        };
        writer
            .write_to_known_channel(&header, br#"{"value":1}"#)
            .unwrap();
    }
    writer.finish().unwrap();
    TempFixture { path }
}

/// Without messages, a file may hold only an empty chunk or, unchunked, no chunk index.
#[test]
fn latest_of_a_file_without_messages_is_empty() {
    for options in [WriteOptions::new(), WriteOptions::new().use_chunks(false)] {
        let fixture = write_times("latest-no-messages", options, &[]);
        let reader = reader(fixture.path(), true);
        assert!(reader.read().topic("/a").raw().latest().unwrap().is_empty());
    }
}

/// An unindexed chunk whose messages are all at time zero reports zero for both times,
/// like an empty chunk, and must still fail rather than drop its messages.
#[test]
fn latest_fails_on_unindexed_time_zero_messages() {
    let options = WriteOptions::new().emit_message_indexes(false);
    let fixture = write_times("latest-time-zero", options, &[0, 0]);
    let reader = reader(fixture.path(), true);
    assert!(matches!(
        reader.read().topic("/a").raw().latest(),
        Err(McapReaderError::MessageIndexNotAvailable)
    ));
}

/// An empty selection reads nothing, so it succeeds like `for_each` even without indexes.
#[test]
fn latest_of_an_empty_selection_is_empty() {
    let options = WriteOptions::new().emit_chunk_indexes(false);
    let fixture = write_times("latest-empty-selection", options, &[1]);
    let reader = reader(fixture.path(), true);
    let empty: [u16; 0] = [];
    assert!(
        reader
            .read()
            .channels(empty)
            .raw()
            .latest()
            .unwrap()
            .is_empty()
    );
}
