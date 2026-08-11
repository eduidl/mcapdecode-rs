use super::*;

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
    assert!(matches!(err, McapReaderError::TopicNotFound));
}

#[cfg(feature = "arrow")]
#[test]
fn for_each_record_batch_errors_when_schema_is_missing() {
    let mut reader = McapReader::new();
    reader.register_decoder(Box::new(TestJsonDecoder));
    let err = reader
        .for_each_record_batch(&fixture_path("with_summary.mcap"), "/raw", |_batch| Ok(()))
        .unwrap_err();
    assert!(matches!(err, McapReaderError::SchemaNotAvailable));
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

    assert!(matches!(
        err,
        McapReaderError::Decoder(DecoderError::MessageDecode(_))
    ));
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
