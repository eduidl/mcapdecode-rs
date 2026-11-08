use arrow::array::Int64Array;
use mcapdecode::{ArrowRead, MetadataColumns};
use memmap2::Mmap;

use super::*;

#[test]
fn raw_decoded_and_arrow_use_the_same_selection() {
    use arrow::array::TimestampNanosecondArray;

    for indexes in [false, true] {
        let fixture = multi_topic_fixture_with_indexes(indexes);
        for parallel in [false, true] {
            let reader = McapReader::builder()
                .clear_decoders()
                .with_decoder(Box::new(SchemaNamedFieldDecoder))
                .with_parallel(parallel)
                .open(fixture.path())
                .unwrap();
            for (start, end) in [(3, 9), (5, 9), (4, 6), (3, 3)] {
                let mut raw = BTreeMap::<u16, Vec<u64>>::new();
                let mut decoded = BTreeMap::<u16, Vec<u64>>::new();
                let mut batches = BTreeMap::<u16, Vec<u64>>::new();
                reader
                    .read()
                    .topics(["/first", "/second"])
                    .time_range(start..end)
                    .raw()
                    .for_each(|channel, message| {
                        raw.entry(channel.id).or_default().push(message.log_time);
                    })
                    .unwrap();
                reader
                    .read()
                    .topics(["/first", "/second"])
                    .time_range(start..end)
                    .decoded()
                    .for_each(|channel, message| {
                        decoded
                            .entry(channel.id)
                            .or_default()
                            .push(message.log_time);
                    })
                    .unwrap();
                reader
                    .read()
                    .topics(["/first", "/second"])
                    .time_range(start..end)
                    .arrow()
                    .batch_size(2)
                    .for_each(|channel, batch| {
                        let times = batch
                            .column(0)
                            .as_any()
                            .downcast_ref::<TimestampNanosecondArray>()
                            .unwrap();
                        batches
                            .entry(channel.id)
                            .or_default()
                            .extend(times.values().iter().map(|&time| time as u64));
                    })
                    .unwrap();
                assert_eq!(
                    raw, decoded,
                    "indexes={indexes}, parallel={parallel}, start={start}, end={end}"
                );
                assert_eq!(
                    raw, batches,
                    "indexes={indexes}, parallel={parallel}, start={start}, end={end}"
                );
            }
        }
    }
}

#[test]
fn reader_arrow_empty_selection_and_unsupported_channel() {
    let reader = McapReader::open(fixture_path("with_summary.mcap")).unwrap();
    reader
        .read()
        .topics([] as [&str; 0])
        .arrow()
        .for_each(|_, _| panic!("empty selection"))
        .unwrap();
    assert!(matches!(
        reader.read().arrow().for_each(|_, _| {}),
        Err(McapReaderError::SchemaNotAvailable)
    ));
}

#[test]
fn reader_arrow_distinguishes_channels_with_the_same_topic() {
    let fixture = write_split_topic_fixture("opened-arrow-channels");
    let reader = McapReader::builder()
        .clear_decoders()
        .with_decoder(Box::new(SchemaNamedFieldDecoder))
        .open(fixture.path())
        .unwrap();
    let channels = reader.channels();
    let mut rows = BTreeMap::new();
    reader
        .read()
        .channels([channels[0].id, channels[1].id])
        .arrow()
        .for_each(|channel, batch| {
            assert_eq!(channel.topic, "/split");
            assert_eq!(channel.schema_name.as_deref(), Some("test.Msg"));
            rows.insert(channel.id, batch.num_rows());
        })
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[&channels[0].id], 2);
    assert_eq!(rows[&channels[1].id], 2);
}

fn chunk_index_count(path: &Path) -> usize {
    let file = File::open(path).unwrap();
    let mmap = unsafe { Mmap::map(&file) }.unwrap();
    let summary = mcap::Summary::read(&mmap).unwrap().unwrap();
    summary.chunk_indexes.len()
}

fn collect_i64_values(read: ArrowRead<'_>) -> Vec<i64> {
    let mut values = Vec::new();
    read.for_each(|_, batch| {
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
    })
    .unwrap();
    values
}

fn collect_batch_rows(read: ArrowRead<'_>) -> Vec<usize> {
    let mut batch_rows = Vec::new();
    read.for_each(|_, batch| {
        batch_rows.push(batch.num_rows());
    })
    .unwrap();
    batch_rows
}

/// Derives a payload field named `log_time`, which is what a metadata column
/// is called under the default naming.
struct CollidingJsonDecoder;

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
            schema: Arc::new(vec![FieldDef::new("log_time", DataTypeDef::I64, true)].into()),
        }))
    }
}

/// Declares an integer field but produces a string to exercise Arrow conversion
/// failures at the public reader boundary.
struct MismatchedJsonDecoder;

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

struct MismatchedJsonTopicDecoder;

impl TopicDecoder for MismatchedJsonTopicDecoder {
    fn decode(&self, _message_data: &[u8]) -> Result<Value, DecoderError> {
        Ok(Value::Struct(vec![Value::string("not an integer")].into()))
    }

    fn field_defs(&self) -> &FieldDefs {
        static FIELDS: std::sync::OnceLock<FieldDefs> = std::sync::OnceLock::new();
        FIELDS.get_or_init(|| vec![FieldDef::new("value", DataTypeDef::I64, true)].into())
    }
}

#[test]
fn arrow_read_without_decoder_returns_error() {
    let builder = McapReader::builder().clear_decoders();
    let err = builder
        .open(fixture_path("with_summary.mcap"))
        .unwrap()
        .read()
        .topic("/decoded")
        .arrow()
        .for_each(|_, _batch| {})
        .unwrap_err();
    assert!(matches!(err, McapReaderError::NoDecoder { .. }));
}

#[test]
fn arrow_read_errors_when_decoder_is_missing_contains_message() {
    let builder = McapReader::builder().clear_decoders();

    let err = builder
        .open(fixture_path("with_summary.mcap"))
        .unwrap()
        .read()
        .topic("/decoded")
        .arrow()
        .for_each(|_, _batch| {})
        .unwrap_err();

    assert!(err.to_string().contains("no decoder registered"));
}

#[test]
fn arrow_read_unknown_topic_returns_error() {
    let builder = McapReader::builder()
        .clear_decoders()
        .with_decoder(Box::new(TestJsonDecoder));

    let err = builder
        .open(fixture_path("with_summary.mcap"))
        .unwrap()
        .read()
        .topic("/unknown")
        .arrow()
        .for_each(|_, _batch| {})
        .unwrap_err();
    assert!(matches!(err, McapReaderError::TopicNotFound));
}

#[test]
fn arrow_read_errors_when_schema_is_missing() {
    let builder = McapReader::builder()
        .clear_decoders()
        .with_decoder(Box::new(TestJsonDecoder));
    let err = builder
        .open(fixture_path("with_summary.mcap"))
        .unwrap()
        .read()
        .topic("/raw")
        .arrow()
        .for_each(|_, _batch| {})
        .unwrap_err();
    assert!(matches!(err, McapReaderError::SchemaNotAvailable));
}

#[test]
fn arrow_read_propagates_callback_error() {
    let builder = McapReader::builder()
        .clear_decoders()
        .with_decoder(Box::new(TestJsonDecoder));
    let err = builder
        .open(fixture_path("with_summary.mcap"))
        .unwrap()
        .read()
        .topic("/decoded")
        .arrow()
        .try_for_each(|_, _batch| Err::<(), _>("callback failed".into()))
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

#[test]
fn arrow_read_reports_arrow_conversion_errors_separately() {
    let fixture = write_chunked_fixture("arrow-conversion-error", &[br#"{"value":1}"#]);
    let builder = McapReader::builder()
        .clear_decoders()
        .with_decoder(Box::new(MismatchedJsonDecoder));

    let err = builder
        .open(fixture.path())
        .unwrap()
        .read()
        .topic("/decoded")
        .arrow()
        .for_each(|_, _batch| {})
        .unwrap_err();

    assert!(matches!(err, McapReaderError::ArrowConvert(_)));
}

#[test]
fn arrow_read_emits_batches_by_batch_size() {
    let builder = McapReader::builder()
        .clear_decoders()
        .with_decoder(Box::new(TestJsonDecoder));

    let file = builder.open(fixture_path("with_summary.mcap")).unwrap();
    let batch_rows = collect_batch_rows(file.read().topic("/decoded").arrow().batch_size(1));

    assert_eq!(batch_rows, vec![1, 1]);
}

#[test]
fn arrow_read_treats_zero_batch_size_as_one() {
    let builder = McapReader::builder()
        .clear_decoders()
        .with_decoder(Box::new(TestJsonDecoder));

    let file = builder.open(fixture_path("with_summary.mcap")).unwrap();
    let batch_rows = collect_batch_rows(file.read().topic("/decoded").arrow().batch_size(0));

    assert_eq!(batch_rows, vec![1, 1]);
}

#[test]
fn arrow_read_defaults_to_a_single_batch() {
    let builder = McapReader::builder()
        .clear_decoders()
        .with_decoder(Box::new(TestJsonDecoder));

    let mut batch_rows = Vec::new();
    builder
        .open(fixture_path("with_summary.mcap"))
        .unwrap()
        .read()
        .topic("/decoded")
        .arrow()
        .for_each(|_, batch| {
            batch_rows.push(batch.num_rows());
        })
        .unwrap();

    assert_eq!(batch_rows, vec![2]);
}

#[test]
fn arrow_read_flushes_final_partial_batch() {
    let builder = McapReader::builder()
        .clear_decoders()
        .with_decoder(Box::new(TestJsonDecoder));

    let file = builder.open(fixture_path("with_summary.mcap")).unwrap();
    let batch_rows = collect_batch_rows(file.read().topic("/decoded").arrow().batch_size(3));

    assert_eq!(batch_rows, vec![2]);
}

#[test]
fn arrow_read_propagates_callback_error_with_builder_decoder() {
    let builder = McapReader::builder()
        .clear_decoders()
        .with_decoder(Box::new(TestJsonDecoder));

    let err = builder
        .open(fixture_path("with_summary.mcap"))
        .unwrap()
        .read()
        .topic("/decoded")
        .arrow()
        .batch_size(1)
        .try_for_each(|_, _batch| Err::<(), _>("callback failed".into()))
        .unwrap_err();

    assert!(err.to_string().contains("callback failed"));
}

#[test]
fn shared_decoder_decodes_messages() {
    let builder = McapReader::builder()
        .clear_decoders()
        .with_shared_decoder(Arc::new(TestJsonDecoder));

    let file = builder.open(fixture_path("with_summary.mcap")).unwrap();
    let values = collect_i64_values(file.read().topic("/decoded").arrow());
    assert_eq!(values, vec![1, 2]);
}

#[test]
fn arrow_read_parallel_matches_sequential_for_multi_chunk_fixture() {
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

    let parallel_builder = McapReader::builder()
        .clear_decoders()
        .with_decoder(Box::new(TestJsonDecoder))
        .with_parallel(true);
    let sequential_builder = McapReader::builder()
        .clear_decoders()
        .with_decoder(Box::new(TestJsonDecoder))
        .with_parallel(false);
    let parallel = parallel_builder.open(fixture.path()).unwrap();
    let sequential = sequential_builder.open(fixture.path()).unwrap();
    fn read(file: &mcapdecode::McapReader) -> ArrowRead<'_> {
        file.read().topic("/decoded").arrow().batch_size(2)
    }

    assert_eq!(
        collect_i64_values(read(&parallel)),
        collect_i64_values(read(&sequential))
    );
    assert_eq!(
        collect_batch_rows(read(&parallel)),
        collect_batch_rows(read(&sequential))
    );
}

#[test]
fn arrow_read_parallel_propagates_decode_error_for_multi_chunk_fixture() {
    let fixture = write_chunked_fixture(
        "parallel-decode-error",
        &[br#"{"value":1}"#, b"invalid", br#"{"value":3}"#],
    );
    assert!(chunk_index_count(fixture.path()) > 1);

    let builder = McapReader::builder()
        .clear_decoders()
        .with_decoder(Box::new(TestJsonDecoder))
        .with_parallel(true);

    let err = builder
        .open(fixture.path())
        .unwrap()
        .read()
        .topic("/decoded")
        .arrow()
        .for_each(|_, _batch| {})
        .unwrap_err();

    assert!(matches!(
        err,
        McapReaderError::Decoder(DecoderError::MessageDecode(_))
    ));
}

#[test]
fn arrow_schemas_match_the_schema_of_emitted_batches() {
    // Consumers that must declare a schema up front take it from
    // `ArrowRead::schemas`. If the two ever diverge the mismatch only shows up
    // at run time deep inside the consumer, so pin it here.
    let fixture = write_chunked_fixture("batch-schema-agreement", &[br#"{"value":1}"#]);
    let file = McapReader::builder()
        .clear_decoders()
        .with_decoder(Box::new(TestJsonDecoder))
        .open(fixture.path())
        .unwrap();

    for metadata in [
        MetadataColumns::default(),
        MetadataColumns::with_prefix("@"),
    ] {
        let arrow = file
            .read()
            .topic("/decoded")
            .arrow()
            .metadata(metadata.clone());
        let (_, declared) = arrow.schemas().unwrap().remove(0);

        let mut emitted = Vec::new();
        arrow
            .for_each(|_, batch| {
                emitted.push(batch.schema());
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

#[test]
fn arrow_schemas_reject_a_payload_field_shadowing_a_metadata_column() {
    let fixture = write_chunked_fixture("batch-schema-collision", &[br#"{"value":1}"#]);
    let file = McapReader::builder()
        .clear_decoders()
        .with_decoder(Box::new(CollidingJsonDecoder))
        .open(fixture.path())
        .unwrap();

    let err = topic_batch_schema(&file, "/decoded", &MetadataColumns::default()).unwrap_err();
    assert!(matches!(
        err,
        McapReaderError::ArrowConvert(
            mcapdecode_arrow::ArrowConvertError::MetadataColumnCollision { ref names }
        ) if names == "log_time"
    ));

    // A prefix moves the metadata columns out of the payload's way.
    let schema = topic_batch_schema(&file, "/decoded", &MetadataColumns::with_prefix("_")).unwrap();
    assert_eq!(schema.schema().field(0).name(), "_log_time");
    assert_eq!(schema.schema().field(2).name(), "log_time");
}

#[test]
fn multi_topic_batches_match_individual_reads_and_per_topic_filters() {
    let fixture = multi_topic_fixture();
    for parallel in [false, true] {
        let builder = McapReader::builder()
            .clear_decoders()
            .with_decoder(Box::new(TestJsonDecoder))
            .with_parallel(parallel);
        for range in [
            (Unbounded, Unbounded),
            (Included(3), Excluded(12)),
            (Included(3), Excluded(8)),
            (Included(0), Excluded(0)),
        ] {
            let file = builder.open(fixture.path()).unwrap();
            let mut actual = BTreeMap::<String, Vec<i64>>::new();
            file.read()
                .topics(["/first", "/second", "/first", "/empty", "/second"])
                .time_range(range)
                .arrow()
                .batch_size(3)
                .for_each(|channel, batch| {
                    let index = batch.schema().index_of("value").unwrap();
                    let array = batch
                        .column(index)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    assert!(batch.num_rows() <= 3);
                    actual
                        .entry(channel.topic.clone())
                        .or_default()
                        .extend(array.values());
                })
                .unwrap();
            for topic in ["/first", "/second", "/empty"] {
                assert_eq!(
                    actual.get(topic).cloned().unwrap_or_default(),
                    collect_i64_values(
                        file.read()
                            .topic(topic)
                            .time_range(range)
                            .arrow()
                            .batch_size(3)
                    )
                );
            }
        }
    }
}

#[test]
fn multi_topic_read_validates_before_callbacks_and_propagates_callback_failure() {
    let fixture = multi_topic_fixture();
    let builder = McapReader::builder()
        .clear_decoders()
        .with_decoder(Box::new(TestJsonDecoder));
    {
        let mut called = false;
        let err = builder
            .open(fixture.path())
            .unwrap()
            .read()
            .topics(["/first", "/unknown"])
            .arrow()
            .batch_size(1)
            .for_each(|_, _| {
                called = true;
            })
            .unwrap_err();
        assert!(!called);
        assert!(matches!(err, McapReaderError::TopicNotFound));
    }
    let mut calls = 0;
    let err = builder
        .open(fixture.path())
        .unwrap()
        .read()
        .topics(["/first", "/second"])
        .arrow()
        .batch_size(1)
        .try_for_each(|_, _| {
            calls += 1;
            Err::<(), _>("stop writing".into())
        })
        .unwrap_err();
    assert_eq!(calls, 1);
    let McapReaderError::Callback(inner) = err else {
        panic!("wrong error kind")
    };
    assert!(inner.downcast_ref::<McapReaderError>().is_none());
    assert_eq!(inner.to_string(), "stop writing");
}

#[test]
fn repeated_single_topic_emits_one_batch() {
    let fixture = multi_topic_fixture();
    let builder = McapReader::builder()
        .clear_decoders()
        .with_decoder(Box::new(TestJsonDecoder));
    let mut topics = Vec::new();
    builder
        .open(fixture.path())
        .unwrap()
        .read()
        .topics(["/first", "/first"])
        .arrow()
        .for_each(|channel, batch| {
            assert_eq!(batch.num_rows(), 1024);
            let index = batch.schema().index_of("value").unwrap();
            let values = batch
                .column(index)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            assert_eq!(values.values().as_ref(), &(0..1024).collect::<Vec<i64>>());
            topics.push(channel.topic.clone());
        })
        .unwrap();
    assert_eq!(topics, ["/first"]);
}

#[test]
fn record_batch_reads_scan_chunks_without_message_indexes() {
    let fixture = multi_topic_fixture_with_indexes(false);
    let builder = McapReader::builder()
        .clear_decoders()
        .with_decoder(Box::new(TestJsonDecoder));
    let mut actual = BTreeMap::<String, Vec<i64>>::new();
    builder
        .open(fixture.path())
        .unwrap()
        .read()
        .topics(["/first", "/second"])
        .arrow()
        .batch_size(256)
        .for_each(|channel, batch| {
            let index = batch.schema().index_of("value").unwrap();
            let values = batch
                .column(index)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            actual
                .entry(channel.topic.clone())
                .or_default()
                .extend(values.values());
        })
        .unwrap();
    assert_eq!(actual["/first"], (0..1024).collect::<Vec<_>>());
    assert_eq!(actual["/second"], (100..1124).collect::<Vec<_>>());
}

#[test]
fn reader_arrow_uses_shared_filters() {
    let fixture = multi_topic_fixture();
    let reader = McapReader::builder()
        .clear_decoders()
        .with_decoder(Box::new(SchemaNamedFieldDecoder))
        .with_parallel(false)
        .open(fixture.path())
        .unwrap();
    let mut rows = BTreeMap::<String, usize>::new();
    reader
        .read()
        .topics(["/first", "/second"])
        .time_range(4..6)
        .arrow()
        .batch_size(1)
        .for_each(|topic, batch| {
            assert_eq!(batch.num_rows(), 1);
            *rows.entry(topic.topic.clone()).or_default() += batch.num_rows();
        })
        .unwrap();
    assert_eq!(rows["/first"], 2);
    assert_eq!(rows["/second"], 2);
}

#[test]
fn reader_exposes_arrow_schema() {
    let fixture = multi_topic_fixture();
    let reader = McapReader::builder()
        .clear_decoders()
        .with_decoder(Box::new(SchemaNamedFieldDecoder))
        .open(fixture.path())
        .unwrap();
    assert!(topic_batch_schema(&reader, "/first", &MetadataColumns::default()).is_ok());
}
