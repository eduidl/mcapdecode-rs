use std::{
    collections::BTreeMap,
    fs::{self, File},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use mcap::{WriteOptions, Writer, records::MessageHeader};
use mcapdecode::{
    McapReader, McapReaderError, PreparedTopic, ReadOptions, TimeRange, TopicDecodeStatus,
    TopicInfo,
};
use mcapdecode_core::{
    DataTypeDef, DecoderError, EncodingKey, FieldDef, FieldDefs, MessageDecoder, MessageEncoding,
    SchemaEncoding, TopicDecoder, Value,
};

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

fn multi_topic_fixture() -> TempFixture {
    let path = temp_fixture_path("multi-topic");
    let mut writer = Writer::with_options(
        File::create(&path).unwrap(),
        WriteOptions::new().chunk_size(Some(128)),
    )
    .unwrap();
    let schema = writer
        .add_schema("test.Msg", "jsonschema", br#"{"type":"object"}"#)
        .unwrap();
    let channels: Vec<_> = ["/first", "/second", "/empty"]
        .iter()
        .map(|topic| {
            writer
                .add_channel(schema, topic, "json", &BTreeMap::new())
                .unwrap()
        })
        .collect();
    // Enough interleaved chunks to exercise backpressure when a callback fails.
    for i in 0..1024 {
        for (topic_index, &channel_id) in channels[..2].iter().enumerate() {
            let data = format!("{{\"value\":{}}}", i + topic_index * 100);
            writer
                .write_to_known_channel(
                    &MessageHeader {
                        channel_id,
                        sequence: i as u32,
                        log_time: i as u64,
                        publish_time: i as u64,
                    },
                    data.as_bytes(),
                )
                .unwrap();
        }
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
    let text = std::str::from_utf8(message_data)
        .map_err(|source| DecoderError::MessageDecode(Box::new(source)))?;

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

    Err(DecoderError::MessageDecode("missing integer field".into()))
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

mod core;
mod read_options;

#[cfg(feature = "arrow")]
mod record_batches;
