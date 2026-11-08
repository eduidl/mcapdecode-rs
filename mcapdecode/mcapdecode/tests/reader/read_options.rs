use super::*;

#[test]
fn read_options_filter_time_range_with_exclusive_end() {
    let fixture = write_chunked_fixture(
        "time-range",
        &[br#"{"value":1}"#, br#"{"value":2}"#, br#"{"value":3}"#],
    );
    let builder = McapReader::builder()
        .clear_decoders()
        .with_decoder(TestJsonDecoder);
    let mut values = Vec::new();

    builder
        .open(fixture.path())
        .unwrap()
        .read()
        .topic("/decoded")
        .time_range(2..3)
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
        .clear_decoders()
        .with_decoder(TestJsonDecoder)
        .with_parallel(true);
    let sequential_builder = McapReader::builder()
        .clear_decoders()
        .with_decoder(TestJsonDecoder)
        .with_parallel(false);

    let forced_sequential =
        collect_values(&parallel_builder, sequential_fixture.path(), Some(false));
    let forced_parallel = collect_values(&sequential_builder, parallel_fixture.path(), Some(true));

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
        .clear_decoders()
        .with_decoder(TestJsonDecoder)
        .with_parallel(false);

    let values = collect_values(&builder, fixture.path(), None);

    assert_eq!(values, vec![1, 2, 3]);
}

#[test]
fn time_range_accepts_inclusive_and_open_ranges() {
    let fixture = write_chunked_fixture(
        "time-range-bounds",
        &[br#"{"value":1}"#, br#"{"value":2}"#, br#"{"value":3}"#],
    );
    let reader = McapReader::builder()
        .clear_decoders()
        .with_decoder(TestJsonDecoder)
        .open(fixture.path())
        .unwrap();
    let read = |select: &dyn Fn(Read<'_>) -> Read<'_>| {
        let mut values = Vec::new();
        select(reader.read().topic("/decoded"))
            .decoded()
            .for_each(|_, message| {
                let Value::Struct(fields) = message.value else {
                    panic!("expected struct payload");
                };
                values.push(fields[0].try_i64().unwrap().unwrap());
            })
            .unwrap();
        values
    };

    assert_eq!(read(&|r| r.time_range(2..=3)), vec![2, 3]);
    assert_eq!(read(&|r| r.time_range(2..)), vec![2, 3]);
    assert_eq!(read(&|r| r.time_range(..=1)), vec![1]);
    assert_eq!(
        read(&|r| r.time_range((Excluded(1), Included(3)))),
        vec![2, 3]
    );
    assert_eq!(
        read(&|r| r.time_range((Excluded(1), Unbounded))),
        vec![2, 3]
    );
    assert_eq!(read(&|r| r.time_range(..)), vec![1, 2, 3]);
    assert_eq!(read(&|r| r.time_range(2..2)), Vec::<i64>::new());
    assert_eq!(read(&|r| r.time_range(1..3).time_range(3..)), vec![3]);
}

pub(super) fn collect_values(
    builder: &McapReaderBuilder,
    path: &Path,
    parallel: Option<bool>,
) -> Vec<i64> {
    let mut values = Vec::new();
    let reader = builder.open(path).unwrap();
    let read = reader.read().topic("/decoded");
    let read = match parallel {
        Some(parallel) => read.parallel(parallel),
        None => read,
    };
    read.decoded()
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
