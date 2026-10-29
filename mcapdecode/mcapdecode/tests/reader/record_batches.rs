use arrow::array::Int64Array;
use mcapdecode::{MetadataColumns, RecordBatchOptions};
use memmap2::Mmap;

use super::*;

fn chunk_index_count(path: &Path) -> usize {
    let file = File::open(path).unwrap();
    let mmap = unsafe { Mmap::map(&file) }.unwrap();
    let summary = mcap::Summary::read(&mmap).unwrap().unwrap();
    summary.chunk_indexes.len()
}

fn batch_options(batch_size: usize) -> RecordBatchOptions {
    RecordBatchOptions {
        batch_size,
        ..RecordBatchOptions::default()
    }
}

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
            field_defs: vec![FieldDef::new("log_time", DataTypeDef::I64, true)].into(),
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
        Ok(Value::Struct(vec![Value::string("not an integer")]))
    }

    fn field_defs(&self) -> &FieldDefs {
        static FIELDS: std::sync::OnceLock<FieldDefs> = std::sync::OnceLock::new();
        FIELDS.get_or_init(|| vec![FieldDef::new("value", DataTypeDef::I64, true)].into())
    }
}

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

#[test]
fn for_each_record_batch_unknown_topic_returns_error() {
    let mut reader = McapReader::new();
    reader.register_decoder(Box::new(TestJsonDecoder));

    let err = reader
        .for_each_record_batch(&fixture_path("with_summary.mcap"), "/unknown", |_batch| {
            Ok(())
        })
        .unwrap_err();
    assert!(matches!(err, McapReaderError::TopicNotFound));
}

#[test]
fn for_each_record_batch_errors_when_schema_is_missing() {
    let mut reader = McapReader::new();
    reader.register_decoder(Box::new(TestJsonDecoder));
    let err = reader
        .for_each_record_batch(&fixture_path("with_summary.mcap"), "/raw", |_batch| Ok(()))
        .unwrap_err();
    assert!(matches!(err, McapReaderError::SchemaNotAvailable));
}

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

    assert!(matches!(
        err,
        McapReaderError::Decoder(DecoderError::MessageDecode(_))
    ));
}

#[test]
fn topic_batch_schema_matches_the_schema_of_emitted_batches() {
    // Consumers that must declare a schema up front take it from
    // `topic_batch_schema`. If the two ever diverge the mismatch only shows up
    // at run time deep inside the consumer, so pin it here.
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

#[test]
fn multi_topic_batches_match_individual_reads_and_per_topic_filters() {
    let fixture = multi_topic_fixture();
    for parallel in [false, true] {
        let reader = McapReader::builder()
            .with_decoder(Box::new(TestJsonDecoder))
            .with_parallel(parallel)
            .build();
        for read_options in [
            ReadOptions::default(),
            ReadOptions {
                time_range: Some(TimeRange {
                    start: Some(3),
                    end: Some(12),
                }),
                ..ReadOptions::default()
            },
            ReadOptions {
                offset: 3,
                limit: Some(5),
                ..ReadOptions::default()
            },
            ReadOptions {
                limit: Some(0),
                ..ReadOptions::default()
            },
        ] {
            let options = RecordBatchOptions {
                batch_size: 3,
                read_options,
                ..RecordBatchOptions::default()
            };
            let mut actual = BTreeMap::<String, Vec<i64>>::new();
            reader
                .for_each_topic_record_batch_with_options(
                    fixture.path(),
                    &["/first", "/second", "/first", "/empty", "/second"],
                    &options,
                    |topic, batch| {
                        let index = batch.schema().index_of("value").unwrap();
                        let array = batch
                            .column(index)
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap();
                        assert!(batch.num_rows() <= 3);
                        actual
                            .entry(topic.into())
                            .or_default()
                            .extend(array.values());
                        Ok(())
                    },
                )
                .unwrap();
            for topic in ["/first", "/second", "/empty"] {
                assert_eq!(
                    actual.get(topic).cloned().unwrap_or_default(),
                    collect_i64_values(&reader, fixture.path(), topic, &options)
                );
            }
        }
    }
}

#[test]
fn multi_topic_read_validates_before_callbacks_and_propagates_callback_failure() {
    let fixture = multi_topic_fixture();
    let reader = McapReader::builder()
        .with_decoder(Box::new(TestJsonDecoder))
        .build();
    let options = batch_options(1);
    {
        let mut called = false;
        let err = reader
            .for_each_topic_record_batch_with_options(
                fixture.path(),
                &["/first", "/unknown"],
                &options,
                |_, _| {
                    called = true;
                    Ok(())
                },
            )
            .unwrap_err();
        assert!(!called);
        assert!(matches!(err, McapReaderError::TopicNotFound));
    }
    let mut calls = 0;
    let err = reader
        .for_each_topic_record_batch_with_options(
            fixture.path(),
            &["/first", "/second"],
            &options,
            |_, _| {
                calls += 1;
                Err("stop writing".into())
            },
        )
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
    let reader = McapReader::builder()
        .with_decoder(Box::new(TestJsonDecoder))
        .build();
    let options = RecordBatchOptions {
        read_options: ReadOptions {
            limit: Some(1),
            ..Default::default()
        },
        ..Default::default()
    };
    let mut topics = Vec::new();
    reader
        .for_each_topic_record_batch_with_options(
            fixture.path(),
            &["/first", "/first"],
            &options,
            |topic, batch| {
                assert_eq!(batch.num_rows(), 1);
                topics.push(topic.to_owned());
                Ok(())
            },
        )
        .unwrap();
    assert_eq!(topics, ["/first"]);
}

#[test]
fn record_batch_reads_scan_chunks_without_message_indexes() {
    let fixture = multi_topic_fixture_with_indexes(false);
    let reader = McapReader::builder()
        .with_decoder(Box::new(TestJsonDecoder))
        .build();
    let mut actual = BTreeMap::<String, Vec<i64>>::new();
    reader
        .for_each_topic_record_batch_with_options(
            fixture.path(),
            &["/first", "/second"],
            &batch_options(256),
            |topic, batch| {
                let index = batch.schema().index_of("value").unwrap();
                let values = batch
                    .column(index)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                actual
                    .entry(topic.into())
                    .or_default()
                    .extend(values.values());
                Ok(())
            },
        )
        .unwrap();
    assert_eq!(actual["/first"], (0..1024).collect::<Vec<_>>());
    assert_eq!(actual["/second"], (100..1124).collect::<Vec<_>>());
}
