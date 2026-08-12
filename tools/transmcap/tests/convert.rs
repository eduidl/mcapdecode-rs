use std::{fs, process::Command};

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
fn convert_accepts_explicit_null_for_jsonl() {
    let fixture = common::fixture();
    let output = Command::new(env!("CARGO_BIN_EXE_transmcap"))
        .args([
            "convert",
            fixture.to_str().unwrap(),
            "--topic",
            "/demo/velocity",
            "--metadata-prefix",
            "",
            "--explicit-null",
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
