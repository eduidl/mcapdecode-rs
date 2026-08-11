use std::{
    collections::BTreeMap,
    fs::{self, File},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

#[cfg(feature = "arrow")]
use arrow::array::Int64Array;
use mcap::{WriteOptions, Writer, records::MessageHeader};
use mcapdecode::{
    McapReader, McapReaderError, PreparedTopic, ReadOptions, TimeRange, TopicDecodeStatus,
    TopicInfo,
};
#[cfg(feature = "arrow")]
use mcapdecode::{MetadataColumns, RecordBatchOptions};
use mcapdecode_core::{
    DataTypeDef, DecoderError, EncodingKey, FieldDef, FieldDefs, MessageDecoder, MessageEncoding,
    SchemaEncoding, TopicDecoder, Value,
};
#[cfg(feature = "arrow")]
use memmap2::Mmap;

static TEMP_FIXTURE_COUNTER: AtomicUsize = AtomicUsize::new(0);

fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join(name)
}

struct TempFixture {
    path: PathBuf,
}

impl TempFixture {
    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempFixture {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

fn temp_fixture_path(name: &str) -> PathBuf {
    let id = TEMP_FIXTURE_COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "mcapdecode-{name}-{}-{id}.mcap",
        std::process::id()
    ))
}

fn write_chunked_fixture(name: &str, payloads: &[&[u8]]) -> TempFixture {
    let path = temp_fixture_path(name);
    let file = File::create(&path).unwrap();
    let mut writer = Writer::with_options(
        file,
        WriteOptions::new()
            .compression(None)
            .chunk_size(Some(1))
            .library("mcapdecode-test"),
    )
    .unwrap();
    let schema_id = writer
        .add_schema("test.Msg", "jsonschema", br#"{"type":"object"}"#)
        .unwrap();
    let channel_id = writer
        .add_channel(schema_id, "/decoded", "json", &BTreeMap::new())
        .unwrap();

    for (idx, payload) in payloads.iter().enumerate() {
        writer
            .write_to_known_channel(
                &MessageHeader {
                    channel_id,
                    sequence: idx as u32,
                    log_time: (idx + 1) as u64,
                    publish_time: (idx + 1) as u64,
                },
                payload,
            )
            .unwrap();
    }

    writer.finish().unwrap();
    TempFixture { path }
}

fn write_duplicate_topic_fixture(name: &str) -> TempFixture {
    let path = temp_fixture_path(name);
    let file = File::create(&path).unwrap();
    let mut writer = Writer::with_options(
        file,
        WriteOptions::new()
            .compression(None)
            .chunk_size(Some(1))
            .library("mcapdecode-test"),
    )
    .unwrap();
    let schema_id = writer
        .add_schema("test.Msg", "jsonschema", br#"{"type":"object"}"#)
        .unwrap();
    let first_metadata = BTreeMap::from([(String::from("source"), String::from("left"))]);
    let second_metadata = BTreeMap::from([(String::from("source"), String::from("right"))]);
    let first_channel_id = writer
        .add_channel(schema_id, "/duplicate", "json", &first_metadata)
        .unwrap();
    let second_channel_id = writer
        .add_channel(schema_id, "/duplicate", "json", &second_metadata)
        .unwrap();

    for (channel_id, value) in [(first_channel_id, 1_i64), (second_channel_id, 2_i64)] {
        writer
            .write_to_known_channel(
                &MessageHeader {
                    channel_id,
                    sequence: 0,
                    log_time: value as u64,
                    publish_time: value as u64,
                },
                format!(r#"{{"value":{value}}}"#).as_bytes(),
            )
            .unwrap();
    }

    writer.finish().unwrap();
    TempFixture { path }
}

#[cfg(feature = "arrow")]
fn chunk_index_count(path: &Path) -> usize {
    let file = File::open(path).unwrap();
    let mmap = unsafe { Mmap::map(&file) }.unwrap();
    let summary = mcap::Summary::read(&mmap).unwrap().unwrap();
    summary.chunk_indexes.len()
}

#[cfg(feature = "arrow")]
fn batch_options(batch_size: usize) -> RecordBatchOptions {
    RecordBatchOptions {
        batch_size,
        ..RecordBatchOptions::default()
    }
}

#[cfg(feature = "arrow")]
fn collect_i64_values(
    reader: &McapReader,
    path: &Path,
    topic: &str,
    options: &RecordBatchOptions,
) -> Vec<i64> {
    let mut values = Vec::new();
    reader
        .for_each_record_batch_with_options(path, topic, options, |batch| {
            let value_idx = batch
                .schema()
                .index_of("value")
                .expect("missing 'value' column");
            let values_col = batch
                .column(value_idx)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("expected Int64Array for 'value' column");

            for i in 0..values_col.len() {
                values.push(values_col.value(i));
            }
            Ok(())
        })
        .unwrap();
    values
}

#[cfg(feature = "arrow")]
fn collect_batch_rows(
    reader: &McapReader,
    path: &Path,
    topic: &str,
    options: &RecordBatchOptions,
) -> Vec<usize> {
    let mut batch_rows = Vec::new();
    reader
        .for_each_record_batch_with_options(path, topic, options, |batch| {
            batch_rows.push(batch.num_rows());
            Ok(())
        })
        .unwrap();
    batch_rows
}

fn collect_decoded_i64_values(reader: &McapReader, path: &Path, topic: &str) -> Vec<i64> {
    let mut values = Vec::new();
    reader
        .for_each_decoded_message(path, topic, |message| {
            let value = match message.value {
                Value::Struct(mut fields) => match fields.remove(0) {
                    Value::I64(value) => value,
                    other => panic!("expected I64 field, got {other:?}"),
                },
                other => panic!("expected struct payload, got {other:?}"),
            };
            values.push(value);
            Ok(())
        })
        .unwrap();
    values
}

fn collect_raw_payloads(reader: &McapReader, path: &Path, topic: &str) -> Vec<Vec<u8>> {
    let mut payloads = Vec::new();
    reader
        .for_each_raw_message(path, topic, |message| {
            payloads.push(message.data.to_vec());
            Ok(())
        })
        .unwrap();
    payloads
}

fn decode_test_value(message_data: &[u8]) -> Result<i64, DecoderError> {
    let text = std::str::from_utf8(message_data).map_err(|source| DecoderError::MessageDecode {
        schema_name: "test.Msg".to_string(),
        source: Box::new(source),
    })?;

    for key in ["\"value\":", "\"x\":"] {
        if let Some(start) = text.find(key) {
            let digits: String = text[start + key.len()..]
                .chars()
                .take_while(|c| c.is_ascii_digit() || *c == '-')
                .collect();
            if let Ok(value) = digits.parse::<i64>() {
                return Ok(value);
            }
        }
    }

    Err(DecoderError::MessageDecode {
        schema_name: "test.Msg".to_string(),
        source: "missing integer field".into(),
    })
}

struct TestJsonDecoder;
struct TestJsonTopicDecoder {
    field_defs: FieldDefs,
}

impl MessageDecoder for TestJsonDecoder {
    fn encoding_key(&self) -> EncodingKey {
        EncodingKey::new(SchemaEncoding::JsonSchema, MessageEncoding::Json)
    }

    fn build_topic_decoder(
        &self,
        _schema_name: &str,
        _schema_data: &[u8],
    ) -> Result<Box<dyn TopicDecoder>, DecoderError> {
        Ok(Box::new(TestJsonTopicDecoder {
            field_defs: vec![FieldDef::new("value", DataTypeDef::I64, true)].into(),
        }))
    }
}

impl TopicDecoder for TestJsonTopicDecoder {
    fn decode(&self, message_data: &[u8]) -> Result<Value, DecoderError> {
        Ok(Value::Struct(vec![Value::I64(decode_test_value(
            message_data,
        )?)]))
    }

    fn field_defs(&self) -> &FieldDefs {
        &self.field_defs
    }
}

/// Derives a payload field named `log_time`, which is what a metadata column
/// is called under the default naming.
#[cfg(feature = "arrow")]
struct CollidingJsonDecoder;

#[cfg(feature = "arrow")]
impl MessageDecoder for CollidingJsonDecoder {
    fn encoding_key(&self) -> EncodingKey {
        EncodingKey::new(SchemaEncoding::JsonSchema, MessageEncoding::Json)
    }

    fn build_topic_decoder(
        &self,
        _schema_name: &str,
        _schema_data: &[u8],
    ) -> Result<Box<dyn TopicDecoder>, DecoderError> {
        Ok(Box::new(TestJsonTopicDecoder {
            field_defs: vec![FieldDef::new("log_time", DataTypeDef::I64, true)].into(),
        }))
    }
}

/// Declares an integer field but produces a string to exercise Arrow conversion
/// failures at the public reader boundary.
#[cfg(feature = "arrow")]
struct MismatchedJsonDecoder;

#[cfg(feature = "arrow")]
impl MessageDecoder for MismatchedJsonDecoder {
    fn encoding_key(&self) -> EncodingKey {
        EncodingKey::new(SchemaEncoding::JsonSchema, MessageEncoding::Json)
    }

    fn build_topic_decoder(
        &self,
        _schema_name: &str,
        _schema_data: &[u8],
    ) -> Result<Box<dyn TopicDecoder>, DecoderError> {
        Ok(Box::new(MismatchedJsonTopicDecoder))
    }
}

#[cfg(feature = "arrow")]
struct MismatchedJsonTopicDecoder;

#[cfg(feature = "arrow")]
impl TopicDecoder for MismatchedJsonTopicDecoder {
    fn decode(&self, _message_data: &[u8]) -> Result<Value, DecoderError> {
        Ok(Value::Struct(vec![Value::string("not an integer")]))
    }

    fn field_defs(&self) -> &FieldDefs {
        static FIELDS: std::sync::OnceLock<FieldDefs> = std::sync::OnceLock::new();
        FIELDS.get_or_init(|| vec![FieldDef::new("value", DataTypeDef::I64, true)].into())
    }
}

struct CountingTestJsonDecoder(Arc<AtomicUsize>);

impl MessageDecoder for CountingTestJsonDecoder {
    fn encoding_key(&self) -> EncodingKey {
        EncodingKey::new(SchemaEncoding::JsonSchema, MessageEncoding::Json)
    }

    fn build_topic_decoder(
        &self,
        _schema_name: &str,
        _schema_data: &[u8],
    ) -> Result<Box<dyn TopicDecoder>, DecoderError> {
        Ok(Box::new(CountingTestJsonTopicDecoder {
            count: Arc::clone(&self.0),
            field_defs: vec![FieldDef::new("value", DataTypeDef::I64, true)].into(),
        }))
    }
}

struct CountingTestJsonTopicDecoder {
    count: Arc<AtomicUsize>,
    field_defs: FieldDefs,
}

impl TopicDecoder for CountingTestJsonTopicDecoder {
    fn decode(&self, message_data: &[u8]) -> Result<Value, DecoderError> {
        self.count.fetch_add(1, Ordering::Relaxed);
        Ok(Value::Struct(vec![Value::I64(decode_test_value(
            message_data,
        )?)]))
    }

    fn field_defs(&self) -> &FieldDefs {
        &self.field_defs
    }
}

#[test]
fn read_options_filter_time_range_with_exclusive_end() {
    let fixture = write_chunked_fixture(
        "time-range",
        &[br#"{"value":1}"#, br#"{"value":2}"#, br#"{"value":3}"#],
    );
    let reader = McapReader::builder()
        .with_decoder(Box::new(TestJsonDecoder))
        .build();
    let mut values = Vec::new();

    reader
        .for_each_decoded_message_with_options(
            fixture.path(),
            "/decoded",
            &ReadOptions {
                time_range: Some(TimeRange {
                    start: Some(2),
                    end: Some(3),
                }),
                ..ReadOptions::default()
            },
            |message| {
                let Value::Struct(fields) = message.value else {
                    panic!("expected struct payload");
                };
                let Value::I64(value) = fields[0] else {
                    panic!("expected i64 field");
                };
                values.push(value);
                Ok(())
            },
        )
        .unwrap();

    assert_eq!(values, vec![2]);
}

#[test]
fn read_options_limit_stops_without_parallel_speculation() {
    let fixture = write_chunked_fixture(
        "limit-stop",
        &[
            br#"{"value":1}"#,
            br#"{"value":2}"#,
            br#"{"value":3}"#,
            br#"{"value":4}"#,
        ],
    );
    let decode_count = Arc::new(AtomicUsize::new(0));
    let reader = McapReader::builder()
        .with_decoder(Box::new(CountingTestJsonDecoder(Arc::clone(&decode_count))))
        .with_parallel(true)
        .build();
    let mut values = Vec::new();

    reader
        .for_each_decoded_message_with_options(
            fixture.path(),
            "/decoded",
            &ReadOptions {
                limit: Some(2),
                ..ReadOptions::default()
            },
            |message| {
                let Value::Struct(fields) = message.value else {
                    panic!("expected struct payload");
                };
                let Value::I64(value) = fields[0] else {
                    panic!("expected i64 field");
                };
                values.push(value);
                Ok(())
            },
        )
        .unwrap();

    assert_eq!(values, vec![1, 2]);
    assert_eq!(decode_count.load(Ordering::Relaxed), 2);
}

#[test]
fn read_options_parallel_overrides_reader_setting_in_both_directions() {
    let payloads: &[&[u8]] = &[
        br#"{"value":1}"#,
        br#"{"value":2}"#,
        br#"{"value":3}"#,
        br#"{"value":4}"#,
    ];
    let sequential_fixture = write_chunked_fixture("parallel-override-off", payloads);
    let parallel_fixture = write_chunked_fixture("parallel-override-on", payloads);

    // A parallel reader forced onto the sequential path, and vice versa.
    let parallel_reader = McapReader::builder()
        .with_decoder(Box::new(TestJsonDecoder))
        .with_parallel(true)
        .build();
    let sequential_reader = McapReader::builder()
        .with_decoder(Box::new(TestJsonDecoder))
        .with_parallel(false)
        .build();

    let forced_sequential = collect_values(
        &parallel_reader,
        sequential_fixture.path(),
        &ReadOptions {
            parallel: Some(false),
            ..ReadOptions::default()
        },
    );
    let forced_parallel = collect_values(
        &sequential_reader,
        parallel_fixture.path(),
        &ReadOptions {
            parallel: Some(true),
            ..ReadOptions::default()
        },
    );

    assert_eq!(forced_sequential, vec![1, 2, 3, 4]);
    assert_eq!(forced_parallel, vec![1, 2, 3, 4]);
}

#[test]
fn read_options_parallel_defaults_to_reader_setting() {
    let fixture = write_chunked_fixture(
        "parallel-default",
        &[br#"{"value":1}"#, br#"{"value":2}"#, br#"{"value":3}"#],
    );
    let reader = McapReader::builder()
        .with_decoder(Box::new(TestJsonDecoder))
        .with_parallel(false)
        .build();

    let values = collect_values(&reader, fixture.path(), &ReadOptions::default());

    assert_eq!(values, vec![1, 2, 3]);
    assert_eq!(ReadOptions::default().parallel, None);
}

#[test]
fn read_options_limit_overrides_requested_parallel_read() {
    let fixture = write_chunked_fixture(
        "parallel-limit-override",
        &[
            br#"{"value":1}"#,
            br#"{"value":2}"#,
            br#"{"value":3}"#,
            br#"{"value":4}"#,
        ],
    );
    let decode_count = Arc::new(AtomicUsize::new(0));
    let reader = McapReader::builder()
        .with_decoder(Box::new(CountingTestJsonDecoder(Arc::clone(&decode_count))))
        .with_parallel(false)
        .build();

    let values = collect_values(
        &reader,
        fixture.path(),
        &ReadOptions {
            limit: Some(2),
            parallel: Some(true),
            ..ReadOptions::default()
        },
    );

    // The limit wins, so the read still stops early instead of decoding everything.
    assert_eq!(values, vec![1, 2]);
    assert_eq!(decode_count.load(Ordering::Relaxed), 2);
}

#[test]
fn read_options_offset_skips_leading_messages_without_decoding_them_on_sequential_path() {
    let fixture = write_chunked_fixture(
        "offset-page",
        &[
            br#"{"value":1}"#,
            br#"{"value":2}"#,
            br#"{"value":3}"#,
            br#"{"value":4}"#,
        ],
    );
    let decode_count = Arc::new(AtomicUsize::new(0));
    let reader = McapReader::builder()
        .with_decoder(Box::new(CountingTestJsonDecoder(Arc::clone(&decode_count))))
        .with_parallel(false)
        .build();

    let values = collect_values(
        &reader,
        fixture.path(),
        &ReadOptions {
            offset: 1,
            ..ReadOptions::default()
        },
    );

    // The parallel path decodes a chunk before knowing its position, so this
    // saving is specific to the sequential path.
    assert_eq!(values, vec![2, 3, 4]);
    assert_eq!(decode_count.load(Ordering::Relaxed), 3);
}

#[test]
fn read_options_offset_preserves_order_on_parallel_path() {
    let fixture = write_chunked_fixture(
        "offset-parallel",
        &[
            br#"{"value":1}"#,
            br#"{"value":2}"#,
            br#"{"value":3}"#,
            br#"{"value":4}"#,
        ],
    );
    let reader = McapReader::builder()
        .with_decoder(Box::new(TestJsonDecoder))
        .with_parallel(true)
        .build();

    let values = collect_values(
        &reader,
        fixture.path(),
        &ReadOptions {
            offset: 2,
            ..ReadOptions::default()
        },
    );

    assert_eq!(values, vec![3, 4]);
}

#[test]
fn read_options_offset_past_end_yields_no_messages() {
    let fixture = write_chunked_fixture("offset-past-end", &[br#"{"value":1}"#, br#"{"value":2}"#]);
    let reader = McapReader::builder()
        .with_decoder(Box::new(TestJsonDecoder))
        .build();

    let values = collect_values(
        &reader,
        fixture.path(),
        &ReadOptions {
            offset: 10,
            ..ReadOptions::default()
        },
    );

    assert!(values.is_empty());
}

#[test]
fn read_options_offset_counts_messages_after_time_range_filtering() {
    let fixture = write_chunked_fixture(
        "offset-after-time-range",
        &[
            br#"{"value":1}"#,
            br#"{"value":2}"#,
            br#"{"value":3}"#,
            br#"{"value":4}"#,
        ],
    );
    let reader = McapReader::builder()
        .with_decoder(Box::new(TestJsonDecoder))
        .build();

    // log_time is index + 1, so the range selects values 2, 3 and 4.
    let values = collect_values(
        &reader,
        fixture.path(),
        &ReadOptions {
            time_range: Some(TimeRange {
                start: Some(2),
                end: Some(5),
            }),
            offset: 1,
            ..ReadOptions::default()
        },
    );

    assert_eq!(values, vec![3, 4]);
}

fn collect_values(reader: &McapReader, path: &Path, options: &ReadOptions) -> Vec<i64> {
    let mut values = Vec::new();
    reader
        .for_each_decoded_message_with_options(path, "/decoded", options, |message| {
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
        Err(McapReaderError::SummaryNotAvailable { .. })
    ));
}

#[test]
fn message_count_unknown_topic_returns_error() {
    let reader = McapReader::new();
    let path = fixture_path("with_summary.mcap");
    assert!(matches!(
        reader.message_count(&path, "/unknown"),
        Err(McapReaderError::TopicNotFound { .. })
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
        collect_values(&reader, fixture.path(), &options)
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

    assert!(matches!(err, McapReaderError::TopicNotFound { .. }));
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
                    "no decoder registered for schema_encoding='jsonschema', message_encoding='json' on topic '/decoded'"
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
                decode_error: Some(
                    "schema not available for topic '/raw' (channel id 2)".to_string(),
                ),
            },
        ]
    );
}

#[test]
fn list_topics_no_summary_returns_error() {
    let reader = McapReader::new();

    assert!(matches!(
        reader.list_topics(&fixture_path("no_summary.mcap")),
        Err(McapReaderError::SummaryNotAvailable { .. })
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

#[cfg(feature = "arrow")]
#[test]
fn for_each_record_batch_without_decoder_returns_error() {
    let reader = McapReader::new();
    let err = reader
        .for_each_record_batch(&fixture_path("with_summary.mcap"), "/decoded", |_batch| {
            Ok(())
        })
        .unwrap_err();
    assert!(matches!(err, McapReaderError::NoDecoder { .. }));
}

#[cfg(feature = "arrow")]
#[test]
fn for_each_record_batch_errors_when_decoder_is_missing_contains_message() {
    let reader = McapReader::builder().build();

    let err = reader
        .for_each_record_batch(&fixture_path("with_summary.mcap"), "/decoded", |_batch| {
            Ok(())
        })
        .unwrap_err();

    assert!(err.to_string().contains("no decoder registered"));
}

#[cfg(feature = "arrow")]
#[test]
fn for_each_record_batch_unknown_topic_returns_error() {
    let mut reader = McapReader::new();
    reader.register_decoder(Box::new(TestJsonDecoder));

    let err = reader
        .for_each_record_batch(&fixture_path("with_summary.mcap"), "/unknown", |_batch| {
            Ok(())
        })
        .unwrap_err();
    assert!(matches!(
        err,
        McapReaderError::TopicNotFound { ref topic } if topic == "/unknown"
    ));
}

#[cfg(feature = "arrow")]
#[test]
fn for_each_record_batch_errors_when_schema_is_missing() {
    let mut reader = McapReader::new();
    reader.register_decoder(Box::new(TestJsonDecoder));
    let err = reader
        .for_each_record_batch(&fixture_path("with_summary.mcap"), "/raw", |_batch| Ok(()))
        .unwrap_err();
    assert!(matches!(
        err,
        McapReaderError::SchemaNotAvailable { ref topic, .. } if topic == "/raw"
    ));
}

#[cfg(feature = "arrow")]
#[test]
fn for_each_record_batch_propagates_callback_error() {
    let mut reader = McapReader::new();
    reader.register_decoder(Box::new(TestJsonDecoder));
    let err = reader
        .for_each_record_batch(&fixture_path("with_summary.mcap"), "/decoded", |_batch| {
            Err("callback failed".into())
        })
        .unwrap_err();
    let McapReaderError::Callback(inner) = &err else {
        panic!("expected a callback error, got {err:?}");
    };
    // `Callback` is `#[error(transparent)]`, so a doubly wrapped error would
    // still render as "callback failed".
    assert!(
        inner.downcast_ref::<McapReaderError>().is_none(),
        "the callback error must be wrapped exactly once"
    );
    assert_eq!(inner.to_string(), "callback failed");
}

#[cfg(feature = "arrow")]
#[test]
fn for_each_record_batch_reports_arrow_conversion_errors_separately() {
    let fixture = write_chunked_fixture("arrow-conversion-error", &[br#"{"value":1}"#]);
    let reader = McapReader::builder()
        .with_decoder(Box::new(MismatchedJsonDecoder))
        .build();

    let err = reader
        .for_each_record_batch(fixture.path(), "/decoded", |_batch| Ok(()))
        .unwrap_err();

    assert!(matches!(err, McapReaderError::ArrowConvert(_)));
}

#[cfg(feature = "arrow")]
#[test]
fn for_each_record_batch_emits_batches_by_batch_size() {
    let reader = McapReader::builder()
        .with_decoder(Box::new(TestJsonDecoder))
        .build();

    let batch_rows = collect_batch_rows(
        &reader,
        &fixture_path("with_summary.mcap"),
        "/decoded",
        &batch_options(1),
    );

    assert_eq!(batch_rows, vec![1, 1]);
}

#[cfg(feature = "arrow")]
#[test]
fn for_each_record_batch_treats_zero_batch_size_as_one() {
    let reader = McapReader::builder()
        .with_decoder(Box::new(TestJsonDecoder))
        .build();

    let batch_rows = collect_batch_rows(
        &reader,
        &fixture_path("with_summary.mcap"),
        "/decoded",
        &batch_options(0),
    );

    assert_eq!(batch_rows, vec![1, 1]);
}

#[cfg(feature = "arrow")]
#[test]
fn for_each_record_batch_defaults_to_a_single_batch() {
    let reader = McapReader::builder()
        .with_decoder(Box::new(TestJsonDecoder))
        .build();

    let mut batch_rows = Vec::new();
    reader
        .for_each_record_batch(&fixture_path("with_summary.mcap"), "/decoded", |batch| {
            batch_rows.push(batch.num_rows());
            Ok(())
        })
        .unwrap();

    assert_eq!(batch_rows, vec![2]);
}

#[cfg(feature = "arrow")]
#[test]
fn for_each_record_batch_flushes_final_partial_batch() {
    let reader = McapReader::builder()
        .with_decoder(Box::new(TestJsonDecoder))
        .build();

    let batch_rows = collect_batch_rows(
        &reader,
        &fixture_path("with_summary.mcap"),
        "/decoded",
        &batch_options(3),
    );

    assert_eq!(batch_rows, vec![2]);
}

#[cfg(feature = "arrow")]
#[test]
fn for_each_record_batch_propagates_callback_error_with_builder_decoder() {
    let reader = McapReader::builder()
        .with_decoder(Box::new(TestJsonDecoder))
        .build();

    let err = reader
        .for_each_record_batch_with_options(
            &fixture_path("with_summary.mcap"),
            "/decoded",
            &batch_options(1),
            |_batch| Err("callback failed".into()),
        )
        .unwrap_err();

    assert!(err.to_string().contains("callback failed"));
}

#[cfg(feature = "arrow")]
#[test]
fn register_shared_decoder_decodes_messages() {
    let mut reader = McapReader::new();
    reader.register_shared_decoder(Arc::new(TestJsonDecoder));

    let values = collect_i64_values(
        &reader,
        &fixture_path("with_summary.mcap"),
        "/decoded",
        &RecordBatchOptions::default(),
    );
    assert_eq!(values, vec![1, 2]);
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

    assert!(matches!(
        err,
        McapReaderError::TopicNotFound { ref topic } if topic == "/unknown"
    ));
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

    assert!(matches!(
        err,
        McapReaderError::SchemaNotAvailable { ref topic, .. } if topic == "/raw"
    ));
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

    assert!(matches!(
        err,
        McapReaderError::TopicNotFound { ref topic } if topic == "/unknown"
    ));
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

    assert!(matches!(err, McapReaderError::MessageDecodeFailed { .. }));
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

#[cfg(feature = "arrow")]
#[test]
fn for_each_record_batch_parallel_matches_sequential_for_multi_chunk_fixture() {
    let fixture = write_chunked_fixture(
        "parallel-multi-chunk",
        &[
            br#"{"value":1}"#,
            br#"{"value":2}"#,
            br#"{"value":3}"#,
            br#"{"value":4}"#,
            br#"{"value":5}"#,
        ],
    );
    assert!(chunk_index_count(fixture.path()) > 1);

    let parallel_reader = McapReader::builder()
        .with_decoder(Box::new(TestJsonDecoder))
        .with_parallel(true)
        .build();
    let sequential_reader = McapReader::builder()
        .with_decoder(Box::new(TestJsonDecoder))
        .with_parallel(false)
        .build();
    let options = batch_options(2);

    assert_eq!(
        collect_i64_values(&parallel_reader, fixture.path(), "/decoded", &options),
        collect_i64_values(&sequential_reader, fixture.path(), "/decoded", &options)
    );
    assert_eq!(
        collect_batch_rows(&parallel_reader, fixture.path(), "/decoded", &options),
        collect_batch_rows(&sequential_reader, fixture.path(), "/decoded", &options)
    );
}

#[cfg(feature = "arrow")]
#[test]
fn for_each_record_batch_parallel_propagates_decode_error_for_multi_chunk_fixture() {
    let fixture = write_chunked_fixture(
        "parallel-decode-error",
        &[br#"{"value":1}"#, b"invalid", br#"{"value":3}"#],
    );
    assert!(chunk_index_count(fixture.path()) > 1);

    let reader = McapReader::builder()
        .with_decoder(Box::new(TestJsonDecoder))
        .with_parallel(true)
        .build();

    let err = reader
        .for_each_record_batch(fixture.path(), "/decoded", |_batch| Ok(()))
        .unwrap_err();

    assert!(matches!(err, McapReaderError::MessageDecodeFailed { .. }));
}

#[cfg(feature = "arrow")]
#[test]
fn topic_batch_schema_matches_the_schema_of_emitted_batches() {
    // Consumers that must declare a schema up front (a DataFusion MemTable)
    // take it from `topic_batch_schema`. If the two ever diverge the mismatch
    // only shows up at run time deep inside the consumer, so pin it here.
    let fixture = write_chunked_fixture("batch-schema-agreement", &[br#"{"value":1}"#]);
    let reader = McapReader::builder()
        .with_decoder(Box::new(TestJsonDecoder))
        .build();

    for metadata in [
        MetadataColumns::default(),
        MetadataColumns::with_prefix("@"),
    ] {
        let options = RecordBatchOptions {
            metadata: metadata.clone(),
            ..RecordBatchOptions::default()
        };
        let declared = reader
            .topic_batch_schema(fixture.path(), "/decoded", &options)
            .unwrap();
        let prepared_declared = reader
            .with_prepared_topic(fixture.path(), "/decoded", |prepared| {
                prepared.batch_schema(&options)
            })
            .unwrap();
        assert_eq!(prepared_declared.schema(), declared.schema());

        let mut emitted = Vec::new();
        reader
            .for_each_record_batch_with_options(fixture.path(), "/decoded", &options, |batch| {
                emitted.push(batch.schema());
                Ok(())
            })
            .unwrap();

        assert!(!emitted.is_empty());
        for schema in emitted {
            assert_eq!(&schema, declared.schema());
        }

        let names: Vec<&str> = declared
            .schema()
            .fields()
            .iter()
            .take(2)
            .map(|field| field.name().as_str())
            .collect();
        assert_eq!(names, metadata.names());
    }
}

#[cfg(feature = "arrow")]
#[test]
fn topic_batch_schema_rejects_a_payload_field_shadowing_a_metadata_column() {
    let fixture = write_chunked_fixture("batch-schema-collision", &[br#"{"value":1}"#]);
    let reader = McapReader::builder()
        .with_decoder(Box::new(CollidingJsonDecoder))
        .build();

    let err = reader
        .topic_batch_schema(fixture.path(), "/decoded", &RecordBatchOptions::default())
        .unwrap_err();
    assert!(matches!(
        err,
        McapReaderError::ArrowConvert(
            mcapdecode_arrow::ArrowConvertError::MetadataColumnCollision { ref names }
        ) if names == "log_time"
    ));

    // A prefix moves the metadata columns out of the payload's way.
    let options = RecordBatchOptions {
        metadata: MetadataColumns::with_prefix("_"),
        ..RecordBatchOptions::default()
    };
    let schema = reader
        .topic_batch_schema(fixture.path(), "/decoded", &options)
        .unwrap();
    assert_eq!(schema.schema().field(0).name(), "_log_time");
    assert_eq!(schema.schema().field(2).name(), "log_time");
}
