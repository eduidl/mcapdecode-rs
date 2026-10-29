use std::{collections::BTreeMap, fs, path::Path, process::Command};

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::Value;

mod common;

#[test]
fn convert_rejects_a_metadata_prefix_containing_a_dot() {
    let output = Command::new(env!("CARGO_BIN_EXE_transmcap"))
        .args([
            "convert",
            "ignored.mcap",
            "--topic",
            "/test",
            "--metadata-prefix",
            "mcap.",
        ])
        .output()
        .unwrap();

    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("metadata prefix must not contain '.'")
    );
}

#[test]
fn convert_jsonl_keeps_metadata_timestamps_by_default() {
    let fixture = common::fixture();
    let output = Command::new(env!("CARGO_BIN_EXE_transmcap"))
        .args([
            "convert",
            fixture.to_str().unwrap(),
            "--topic",
            "/demo/velocity",
            "--metadata-prefix",
            "",
        ])
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let row: Value = serde_json::from_str(
        String::from_utf8(output.stdout)
            .unwrap()
            .lines()
            .next()
            .unwrap(),
    )
    .unwrap();
    assert!(row["log_time"].is_string());
    assert!(row["publish_time"].is_string());
}

#[test]
fn convert_accepts_jsonl_explicit_null() {
    let fixture = common::fixture();
    let output = Command::new(env!("CARGO_BIN_EXE_transmcap"))
        .args([
            "convert",
            fixture.to_str().unwrap(),
            "--topic",
            "/demo/velocity",
            "--metadata-prefix",
            "",
            "--jsonl-explicit-null",
        ])
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn convert_rejects_jsonl_explicit_null_for_non_jsonl_formats() {
    for format in ["csv", "parquet"] {
        let output = Command::new(env!("CARGO_BIN_EXE_transmcap"))
            .args([
                "convert",
                "ignored.mcap",
                "--topic",
                "/test",
                "--format",
                format,
                "--jsonl-explicit-null",
            ])
            .output()
            .unwrap();

        assert!(
            !output.status.success(),
            "format {format} unexpectedly succeeded"
        );
        assert!(
            String::from_utf8_lossy(&output.stderr)
                .contains("--jsonl-explicit-null requires --format jsonl or jsonl-ext")
        );
    }
}

#[test]
fn convert_jsonl_ext_is_available_for_special_float_encoding() {
    let fixture = common::fixture();
    let output = Command::new(env!("CARGO_BIN_EXE_transmcap"))
        .args([
            "convert",
            fixture.to_str().unwrap(),
            "--topic",
            "/demo/velocity",
            "--format",
            "jsonl-ext",
            "--metadata-prefix",
            "",
        ])
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let row: Value = serde_json::from_str(
        String::from_utf8(output.stdout)
            .unwrap()
            .lines()
            .next()
            .unwrap(),
    )
    .unwrap();
    assert!(row["log_time"].is_string());
    assert!(row["publish_time"].is_string());
}

#[test]
fn convert_time_ns_writes_unix_nanoseconds() {
    let fixture = common::fixture();
    let output = Command::new(env!("CARGO_BIN_EXE_transmcap"))
        .args([
            "convert",
            fixture.to_str().unwrap(),
            "--topic",
            "/demo/velocity",
            "--metadata-prefix",
            "",
            "--time-ns",
        ])
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let row: Value = serde_json::from_str(
        String::from_utf8(output.stdout)
            .unwrap()
            .lines()
            .next()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(row["log_time"], 1_716_025_282_050_000_000_i64);
    assert_eq!(row["publish_time"], 1_716_025_282_050_000_000_i64);
}

#[test]
fn convert_csv_writes_a_header_and_records() {
    let fixture = common::fixture();
    let output = Command::new(env!("CARGO_BIN_EXE_transmcap"))
        .args([
            "convert",
            fixture.to_str().unwrap(),
            "--topic",
            "/demo/velocity",
            "--format",
            "csv",
            "--metadata-prefix",
            "",
        ])
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let csv = String::from_utf8(output.stdout).unwrap();
    assert!(csv.starts_with("log_time,publish_time,"));
    assert!(
        csv.lines().count() > 1,
        "CSV output did not contain any records"
    );
}

#[test]
fn convert_parquet_requires_an_output_path() {
    let fixture = common::fixture();
    let output = Command::new(env!("CARGO_BIN_EXE_transmcap"))
        .args([
            "convert",
            fixture.to_str().unwrap(),
            "--topic",
            "/demo/velocity",
            "--format",
            "parquet",
        ])
        .output()
        .unwrap();

    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("Parquet output requires -o <file>"));
}

#[test]
fn convert_parquet_writes_a_nonempty_file() {
    let fixture = common::fixture();
    let path = common::temporary_output_path("parquet");
    let output = Command::new(env!("CARGO_BIN_EXE_transmcap"))
        .args([
            "convert",
            fixture.to_str().unwrap(),
            "--topic",
            "/demo/velocity",
            "--format",
            "parquet",
            "--output",
            path.to_str().unwrap(),
        ])
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(fs::metadata(&path).unwrap().len() > 0);
    fs::remove_file(path).unwrap();
}

#[test]
fn convert_parquet_writes_an_empty_file_with_the_selected_schema() {
    let input = common::temporary_output_path("mcap");
    write_empty_mcap(&input, true);

    for (options, expected_columns) in [
        (
            vec![],
            vec![
                "@log_time",
                "@publish_time",
                "velocity.x",
                "velocity.y",
                "velocity.z",
            ],
        ),
        (
            vec![
                "--metadata-prefix",
                "mcap_",
                "--time-ns",
                "--fields",
                "mcap_log_time,velocity.x",
                "--parallel",
            ],
            vec!["mcap_log_time", "velocity.x"],
        ),
    ] {
        let path = common::temporary_output_path("parquet");
        let output = Command::new(env!("CARGO_BIN_EXE_transmcap"))
            .arg("convert")
            .arg(&input)
            .args(["--topic", "/empty", "--format", "parquet", "--output"])
            .arg(&path)
            .args(&options)
            .output()
            .unwrap();

        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let builder =
            ParquetRecordBatchReaderBuilder::try_new(fs::File::open(&path).unwrap()).unwrap();
        assert_eq!(builder.metadata().file_metadata().num_rows(), 0);
        let columns: Vec<_> = builder
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        assert_eq!(columns, expected_columns);
        if options.contains(&"--time-ns") {
            assert_eq!(
                builder.schema().field(0).data_type(),
                &arrow::datatypes::DataType::Int64
            );
        }
        assert_eq!(builder.build().unwrap().count(), 0);
        fs::remove_file(path).unwrap();
    }

    fs::remove_file(input).unwrap();
}

#[test]
fn convert_parquet_rejects_an_empty_topic_without_a_schema() {
    let input = common::temporary_output_path("mcap");
    let path = common::temporary_output_path("parquet");
    write_empty_mcap(&input, false);
    let output = Command::new(env!("CARGO_BIN_EXE_transmcap"))
        .arg("convert")
        .arg(&input)
        .args(["--topic", "/empty", "--format", "parquet", "--output"])
        .arg(&path)
        .output()
        .unwrap();

    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("schema not available"));
    assert!(!path.exists());
    fs::remove_file(input).unwrap();
}

fn write_empty_mcap(path: &Path, with_schema: bool) {
    let mut writer = mcap::Writer::new(fs::File::create(path).unwrap()).unwrap();
    let schema_id = if with_schema {
        writer
            .add_schema(
                "test_msgs/msg/Velocity",
                "ros2msg",
                b"geometry_msgs/Vector3 velocity\n\
              ================================================================================\n\
              MSG: geometry_msgs/Vector3\n\
              float64 x\nfloat64 y\nfloat64 z\n",
            )
            .unwrap()
    } else {
        0
    };
    writer
        .add_channel(schema_id, "/empty", "cdr", &BTreeMap::new())
        .unwrap();
    writer.finish().unwrap();
}

#[test]
fn convert_rejects_a_list_flatten_size_without_the_matching_policy() {
    let output = Command::new(env!("CARGO_BIN_EXE_transmcap"))
        .args([
            "convert",
            "ignored.mcap",
            "--topic",
            "/test",
            "--list-flatten-size",
            "2",
        ])
        .output()
        .unwrap();

    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr)
            .contains("--list-flatten-size requires --list-policy flatten-fixed")
    );
}

#[test]
fn convert_rejects_multiple_topics() {
    let output = Command::new(env!("CARGO_BIN_EXE_transmcap"))
        .args([
            "convert",
            "ignored.mcap",
            "--topic",
            "/first",
            "--topic",
            "/second",
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
}

fn write_multi_topic_mcap(path: &Path) {
    write_multi_topic_mcap_with_count(path, 32);
}

fn write_multi_topic_mcap_with_count(path: &Path, count: u32) {
    write_multi_topic_mcap_with_stats(path, count, true);
}

fn write_multi_topic_mcap_with_stats(path: &Path, count: u32, stats: bool) {
    let mut writer = mcap::Writer::with_options(
        fs::File::create(path).unwrap(),
        mcap::WriteOptions::new()
            .chunk_size(Some(256))
            .emit_statistics(stats),
    )
    .unwrap();
    let schema = writer
        .add_schema("test_msgs/msg/Sample", "ros2msg", b"float64 value\n")
        .unwrap();
    let channels: Vec<_> = ["/first", "/second", "/empty"]
        .iter()
        .map(|topic| {
            writer
                .add_channel(schema, topic, "cdr", &BTreeMap::new())
                .unwrap()
        })
        .collect();
    for i in 0..count {
        for (topic_index, &channel_id) in channels[..2].iter().enumerate() {
            let mut payload = vec![0, 1, 0, 0];
            payload.extend_from_slice(&(i as f64 + topic_index as f64 * 100.0).to_le_bytes());
            writer
                .write_to_known_channel(
                    &mcap::records::MessageHeader {
                        channel_id,
                        sequence: i,
                        log_time: i as u64,
                        publish_time: i as u64,
                    },
                    &payload,
                )
                .unwrap();
        }
    }
    writer.finish().unwrap();
}

#[test]
fn batch_reads_without_summary_statistics() {
    let input = common::temporary_output_path("mcap");
    let config = common::temporary_output_path("json");
    let output = common::temporary_output_path("parquet");
    write_multi_topic_mcap_with_stats(&input, 32, false);
    fs::write(
        &config,
        serde_json::json!({"topics":[{"topic":"/first", "output":output, "format":"parquet"}]})
            .to_string(),
    )
    .unwrap();
    let result = Command::new(env!("CARGO_BIN_EXE_transmcap"))
        .arg("batch")
        .arg(&input)
        .arg("--config")
        .arg(&config)
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let reader =
        ParquetRecordBatchReaderBuilder::try_new(fs::File::open(&output).unwrap()).unwrap();
    assert_eq!(reader.metadata().file_metadata().num_rows(), 32);
    for path in [&input, &config, &output] {
        fs::remove_file(path).unwrap();
    }
}

#[test]
fn batch_preserves_a_full_final_batch() {
    let input = common::temporary_output_path("mcap");
    let config = common::temporary_output_path("json");
    let output = common::temporary_output_path("parquet");
    write_multi_topic_mcap_with_count(&input, 1024);
    fs::write(
        &config,
        serde_json::json!({"topics":[{"topic":"/first", "output":output, "format":"parquet"}]})
            .to_string(),
    )
    .unwrap();
    let result = Command::new(env!("CARGO_BIN_EXE_transmcap"))
        .arg("batch")
        .arg(&input)
        .arg("--config")
        .arg(&config)
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let reader =
        ParquetRecordBatchReaderBuilder::try_new(fs::File::open(&output).unwrap()).unwrap();
    assert_eq!(reader.metadata().file_metadata().num_rows(), 1024);
    for path in [&input, &config, &output] {
        fs::remove_file(path).unwrap();
    }
}

#[test]
fn batch_matches_individual_conversions() {
    let input = common::temporary_output_path("mcap");
    let config = common::temporary_output_path("json");
    write_multi_topic_mcap(&input);
    for parallel in [false, true] {
        let first = common::temporary_output_path("parquet");
        let second = common::temporary_output_path("csv");
        let empty = common::temporary_output_path("parquet");
        let settings = serde_json::json!({"defaults":{"format":"parquet", "time_ns":true, "fields":["@log_time", "value"]}, "topics":[
            {"topic":"/first", "output":first},
            {"topic":"/second", "output":second, "format":"csv", "fields":["value"]},
            {"topic":"/empty", "output":empty}
        ]});
        fs::write(&config, settings.to_string()).unwrap();
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_transmcap"));
        cmd.arg("batch").arg(&input).arg("--config").arg(&config);
        if parallel {
            cmd.arg("--parallel");
        }
        let output = cmd.output().unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        for (topic, actual, format, fields) in [
            ("/first", &first, "parquet", "@log_time,value"),
            ("/second", &second, "csv", "value"),
            ("/empty", &empty, "parquet", "@log_time,value"),
        ] {
            let expected = common::temporary_output_path(format);
            let output = Command::new(env!("CARGO_BIN_EXE_transmcap"))
                .arg("convert")
                .arg(&input)
                .args([
                    "--topic",
                    topic,
                    "--format",
                    format,
                    "--time-ns",
                    "--fields",
                    fields,
                    "--output",
                ])
                .arg(&expected)
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            if format == "csv" {
                assert_eq!(fs::read(actual).unwrap(), fs::read(&expected).unwrap());
            } else {
                let read = |path: &Path| {
                    let reader =
                        ParquetRecordBatchReaderBuilder::try_new(fs::File::open(path).unwrap())
                            .unwrap();
                    let schema = reader.schema().clone();
                    let rows = reader.metadata().file_metadata().num_rows();
                    (
                        schema,
                        rows,
                        reader
                            .build()
                            .unwrap()
                            .collect::<Result<Vec<_>, _>>()
                            .unwrap(),
                    )
                };
                assert_eq!(read(actual), read(&expected));
            }
            fs::remove_file(expected).unwrap();
        }
        for path in [&first, &second, &empty] {
            fs::remove_file(path).unwrap();
        }
    }
    fs::remove_file(input).unwrap();
    fs::remove_file(config).unwrap();
}

#[test]
fn batch_rejects_invalid_settings_before_writing() {
    let input = common::temporary_output_path("mcap");
    let config = common::temporary_output_path("json");
    let output = common::temporary_output_path("jsonl");
    write_multi_topic_mcap(&input);
    let first = serde_json::json!({"topic":"/first", "output":output});
    for content in [
        "{".to_owned(),
        serde_json::json!({"topics":[]}).to_string(),
        serde_json::json!({"topics":[first, {"topic":"/second"}]}).to_string(),
        serde_json::json!({"topics":[first, first]}).to_string(),
        serde_json::json!({"topics":[first, {"topic":"/first", "output":"ignored"}]}).to_string(),
        serde_json::json!({"topics":[first, {"topic":"/second", "output":output}]}).to_string(),
        serde_json::json!({"topics":[first, {"topic":"/missing", "output":"ignored"}]}).to_string(),
        serde_json::json!({"topics":[first, {"topic":"/second", "output":input}]}).to_string(),
        serde_json::json!({"topics":[first, {"topic":"/second", "output":"ignored", "unknown":true}]}).to_string(),
        serde_json::json!({"topics":[first, {"topic":"/second", "output":"ignored", "list_flatten_size":2}]}).to_string(),
    ] {
        fs::write(&config, &content).unwrap();
        let result = Command::new(env!("CARGO_BIN_EXE_transmcap"))
            .arg("batch").arg(&input).arg("--config").arg(&config).output().unwrap();
        assert!(!result.status.success(), "{content}");
        assert!(!output.exists(), "{content}");
    }
    fs::remove_file(input).unwrap();
    fs::remove_file(config).unwrap();
}

#[test]
fn batch_and_convert_apply_the_same_per_topic_policies() {
    let input = common::temporary_output_path("mcap");
    let config = common::temporary_output_path("json");
    let output = common::temporary_output_path("csv");
    let output_dropped = common::temporary_output_path("csv");
    let mut writer = mcap::Writer::new(fs::File::create(&input).unwrap()).unwrap();
    let schema = writer
        .add_schema(
            "test_msgs/msg/Arrays",
            "ros2msg",
            b"float64[] values\nfloat64[2] fixed\n",
        )
        .unwrap();
    let channel = writer
        .add_channel(schema, "/arrays", "cdr", &BTreeMap::new())
        .unwrap();
    let other = writer
        .add_channel(schema, "/other", "cdr", &BTreeMap::new())
        .unwrap();
    let mut payload = vec![0, 1, 0, 0];
    payload.extend_from_slice(&2_u32.to_le_bytes());
    payload.extend_from_slice(&[0; 4]);
    for value in [1.0_f64, 2.0, 3.0, 4.0] {
        payload.extend_from_slice(&value.to_le_bytes());
    }
    for channel in [channel, other] {
        writer
            .write_to_known_channel(
                &mcap::records::MessageHeader {
                    channel_id: channel,
                    sequence: 0,
                    log_time: 42,
                    publish_time: 42,
                },
                &payload,
            )
            .unwrap();
    }
    writer.finish().unwrap();
    fs::write(&config, serde_json::json!({"defaults": {"format":"csv", "list_policy":"flatten-fixed", "list_flatten_size":2, "array_policy":"flatten", "map_policy":"drop", "metadata_prefix":"mcap_", "time_ns":true}, "topics":[{"topic":"/arrays", "output":output, "fields":["mcap_log_time", "values", "fixed"]}, {"topic":"/other", "output":output_dropped, "fields":["mcap_log_time", "values", "fixed"], "array_policy":"drop"}]}).to_string()).unwrap();
    let batch = Command::new(env!("CARGO_BIN_EXE_transmcap"))
        .arg("batch")
        .arg(&input)
        .arg("--config")
        .arg(&config)
        .output()
        .unwrap();
    assert!(
        batch.status.success(),
        "{}",
        String::from_utf8_lossy(&batch.stderr)
    );
    let single = Command::new(env!("CARGO_BIN_EXE_transmcap"))
        .arg("convert")
        .arg(&input)
        .args([
            "--topic",
            "/arrays",
            "--format",
            "csv",
            "--list-policy",
            "flatten-fixed",
            "--list-flatten-size",
            "2",
            "--array-policy",
            "flatten",
            "--map-policy",
            "drop",
            "--metadata-prefix",
            "mcap_",
            "--time-ns",
            "--fields",
            "mcap_log_time,values,fixed",
        ])
        .output()
        .unwrap();
    assert!(
        single.status.success(),
        "{}",
        String::from_utf8_lossy(&single.stderr)
    );
    let csv = fs::read(&output).unwrap();
    assert_eq!(csv, single.stdout);
    assert_eq!(
        String::from_utf8(csv).unwrap(),
        "mcap_log_time,values.0,values.1,fixed.0,fixed.1\n42,1.0,2.0,3.0,4.0\n"
    );
    assert_eq!(
        fs::read_to_string(&output_dropped).unwrap(),
        "mcap_log_time,values.0,values.1\n42,1.0,2.0\n"
    );
    for path in [&input, &config, &output, &output_dropped] {
        fs::remove_file(path).unwrap();
    }
}

#[test]
fn batch_and_convert_reject_the_same_invalid_settings() {
    let config = common::temporary_output_path("json");
    let output = common::temporary_output_path("jsonl");
    for (json, args, message) in [
        (
            serde_json::json!({"metadata_prefix":"bad."}),
            vec!["--metadata-prefix", "bad."],
            "metadata prefix must not contain '.'",
        ),
        (
            serde_json::json!({"list_flatten_size":2}),
            vec!["--list-flatten-size", "2"],
            "--list-flatten-size requires --list-policy flatten-fixed",
        ),
        (
            serde_json::json!({"format":"csv", "jsonl_explicit_null":true}),
            vec!["--format", "csv", "--jsonl-explicit-null"],
            "--jsonl-explicit-null requires --format jsonl or jsonl-ext",
        ),
    ] {
        fs::write(
            &config,
            serde_json::json!({"defaults":json, "topics":[{"topic":"/test", "output":output}]})
                .to_string(),
        )
        .unwrap();
        let batch = Command::new(env!("CARGO_BIN_EXE_transmcap"))
            .args(["batch", "ignored.mcap", "--config"])
            .arg(&config)
            .output()
            .unwrap();
        let single = Command::new(env!("CARGO_BIN_EXE_transmcap"))
            .args(["convert", "ignored.mcap", "--topic", "/test"])
            .args(args)
            .output()
            .unwrap();
        for result in [batch, single] {
            assert!(!result.status.success());
            assert!(
                String::from_utf8_lossy(&result.stderr).contains(message),
                "{}",
                String::from_utf8_lossy(&result.stderr)
            );
        }
        assert!(!output.exists());
    }
    fs::remove_file(config).unwrap();
}
