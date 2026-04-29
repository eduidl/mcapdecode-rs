#[cfg(feature = "arrow")]
use std::sync::Arc;
use std::{
    collections::BTreeMap,
    fs::{self, File},
    path::{Path, PathBuf},
    sync::atomic::{AtomicUsize, Ordering},
};

#[cfg(feature = "arrow")]
use arrow::array::Int64Array;
use mcap::{WriteOptions, Writer, records::MessageHeader};
#[cfg(feature = "arrow")]
use mcapdecode::McapReaderArrowExt;
use mcapdecode::{McapReader, McapReaderError, TopicInfo};
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
fn collect_i64_values(reader: &McapReader, path: &Path, topic: &str) -> Vec<i64> {
    let mut values = Vec::new();
    reader
        .for_each_record_batch(path, topic, |batch| {
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
fn collect_batch_rows(reader: &McapReader, path: &Path, topic: &str) -> Vec<usize> {
    let mut batch_rows = Vec::new();
    reader
        .for_each_record_batch(path, topic, |batch| {
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
    let reader = McapReader::builder().with_batch_size(1).build();

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
    assert!(matches!(err, McapReaderError::Callback(_)));
    assert!(err.to_string().contains("callback failed"));
}

#[cfg(feature = "arrow")]
#[test]
fn for_each_record_batch_emits_batches_by_batch_size() {
    let reader = McapReader::builder()
        .with_decoder(Box::new(TestJsonDecoder))
        .with_batch_size(1)
        .build();

    let mut batch_rows = Vec::new();
    reader
        .for_each_record_batch(&fixture_path("with_summary.mcap"), "/decoded", |batch| {
            batch_rows.push(batch.num_rows());
            Ok(())
        })
        .unwrap();

    assert_eq!(batch_rows, vec![1, 1]);
}

#[cfg(feature = "arrow")]
#[test]
fn for_each_record_batch_flushes_final_partial_batch() {
    let reader = McapReader::builder()
        .with_decoder(Box::new(TestJsonDecoder))
        .with_batch_size(3)
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
fn for_each_record_batch_propagates_callback_error_with_builder_decoder() {
    let reader = McapReader::builder()
        .with_decoder(Box::new(TestJsonDecoder))
        .with_batch_size(1)
        .build();

    let err = reader
        .for_each_record_batch(&fixture_path("with_summary.mcap"), "/decoded", |_batch| {
            Err("callback failed".into())
        })
        .unwrap_err();

    assert!(err.to_string().contains("callback failed"));
}

#[cfg(feature = "arrow")]
#[test]
fn register_shared_decoder_decodes_messages() {
    let mut reader = McapReader::new();
    reader.register_shared_decoder(Arc::new(TestJsonDecoder));

    let values = collect_i64_values(&reader, &fixture_path("with_summary.mcap"), "/decoded");
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

    assert!(matches!(err, McapReaderError::Callback(_)));
    assert!(err.to_string().contains("callback failed"));
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
        .with_batch_size(2)
        .with_parallel(true)
        .build();
    let sequential_reader = McapReader::builder()
        .with_decoder(Box::new(TestJsonDecoder))
        .with_batch_size(2)
        .with_parallel(false)
        .build();

    assert_eq!(
        collect_i64_values(&parallel_reader, fixture.path(), "/decoded"),
        collect_i64_values(&sequential_reader, fixture.path(), "/decoded")
    );
    assert_eq!(
        collect_batch_rows(&parallel_reader, fixture.path(), "/decoded"),
        collect_batch_rows(&sequential_reader, fixture.path(), "/decoded")
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
