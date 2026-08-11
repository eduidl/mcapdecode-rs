use super::*;

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

pub(super) fn collect_values(reader: &McapReader, path: &Path, options: &ReadOptions) -> Vec<i64> {
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
