use super::*;

#[test]
fn read_options_filter_time_range_with_exclusive_end() {
    let fixture = write_chunked_fixture(
        "time-range",
        &[br#"{"value":1}"#, br#"{"value":2}"#, br#"{"value":3}"#],
    );
    let builder = McapReader::builder().with_decoder(Box::new(TestJsonDecoder));
    let mut values = Vec::new();

    builder
        .open(fixture.path())
        .unwrap()
        .read()
        .topic("/decoded")
        .options(ReadOptions {
            time_range: Some(TimeRange {
                start: Some(2),
                end: Some(3),
            }),
            ..ReadOptions::default()
        })
        .decoded()
        .for_each(|_, message| {
            let Value::Struct(fields) = message.value else {
                panic!("expected struct payload");
            };
            let Value::I64(value) = fields[0] else {
                panic!("expected i64 field");
            };
            values.push(value);
        })
        .unwrap();

    assert_eq!(values, vec![2]);
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
    let parallel_builder = McapReader::builder()
        .with_decoder(Box::new(TestJsonDecoder))
        .with_parallel(true);
    let sequential_builder = McapReader::builder()
        .with_decoder(Box::new(TestJsonDecoder))
        .with_parallel(false);

    let forced_sequential = collect_values(
        &parallel_builder,
        sequential_fixture.path(),
        &ReadOptions {
            parallel: Some(false),
            ..ReadOptions::default()
        },
    );
    let forced_parallel = collect_values(
        &sequential_builder,
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
    let builder = McapReader::builder()
        .with_decoder(Box::new(TestJsonDecoder))
        .with_parallel(false);

    let values = collect_values(&builder, fixture.path(), &ReadOptions::default());

    assert_eq!(values, vec![1, 2, 3]);
    assert_eq!(ReadOptions::default().parallel, None);
}

pub(super) fn collect_values(
    builder: &McapReaderBuilder,
    path: &Path,
    options: &ReadOptions,
) -> Vec<i64> {
    let mut values = Vec::new();
    builder
        .open(path)
        .unwrap()
        .read()
        .topic("/decoded")
        .options(options.clone())
        .decoded()
        .for_each(|_, message| {
            let Value::Struct(fields) = message.value else {
                panic!("expected struct payload");
            };
            let Value::I64(value) = fields[0] else {
                panic!("expected i64 field");
            };
            values.push(value);
        })
        .unwrap();
    values
}
