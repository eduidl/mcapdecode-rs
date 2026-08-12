use std::{
    collections::BTreeMap,
    fs::{self, File},
    path::PathBuf,
    process::Command,
    sync::atomic::{AtomicUsize, Ordering},
};

use mcap::{WriteOptions, Writer};
use serde_json::Value;

static FIXTURE_COUNTER: AtomicUsize = AtomicUsize::new(0);

struct Fixture(PathBuf);

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

fn schema_fixture() -> Fixture {
    schema_fixture_with_schema(
        br#"
================================================================================
IDL: ex/msg/Sample
module ex {
  module msg {
    enum State {
      IDLE,
      RUNNING
    };
    struct Sample {
      float32 reading;
      sequence<uint16, 4> values;
      uint8 data[3];
      string<16> label;
      State state;
    };
  };
};
"#,
    )
}

fn schema_fixture_with_schema(schema: &[u8]) -> Fixture {
    let id = FIXTURE_COUNTER.fetch_add(1, Ordering::Relaxed);
    let path = std::env::temp_dir().join(format!("mcapq-schema-{}-{id}.mcap", std::process::id()));
    let file = File::create(&path).unwrap();
    let mut writer = Writer::with_options(file, WriteOptions::new().library("mcapq-test")).unwrap();
    let schema_id = writer
        .add_schema("ex/msg/Sample", "ros2idl", schema)
        .unwrap();
    writer
        .add_channel(schema_id, "/sample", "cdr", &BTreeMap::new())
        .unwrap();
    writer.finish().unwrap();
    Fixture(path)
}

fn run(fixture: &Fixture, format: Option<&str>) -> std::process::Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_mcapq"));
    command.args(["schema", fixture.0.to_str().unwrap(), "--topic", "/sample"]);
    if let Some(format) = format {
        command.args(["--format", format]);
    }
    command.output().unwrap()
}

#[test]
fn schema_defaults_to_jtd_and_preserves_constraints() {
    let fixture = schema_fixture();
    let output = run(&fixture, None);

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let schema: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(schema["metadata"]["title"], "ex/msg/Sample");
    assert_eq!(schema["metadata"]["x-mcap"]["schema_encoding"], "ros2idl");
    assert_eq!(
        schema["metadata"]["x-mcap"]["columns"],
        serde_json::json!(["log_time", "publish_time"])
    );
    assert_eq!(schema["properties"]["reading"]["type"], "float32");
    assert_eq!(
        schema["properties"]["values"]["metadata"]["x-mcap-max-items"],
        4
    );
    assert_eq!(
        schema["properties"]["data"]["metadata"]["x-mcap-fixed-length"],
        3
    );
    assert_eq!(
        schema["properties"]["label"]["metadata"]["x-mcap-max-length"],
        16
    );
    assert_eq!(
        schema["properties"]["state"]["enum"],
        serde_json::json!(["IDLE", "RUNNING"])
    );
    assert_eq!(
        schema["properties"]
            .as_object()
            .unwrap()
            .keys()
            .collect::<Vec<_>>(),
        vec!["reading", "values", "data", "label", "state"]
    );
}

#[test]
fn schema_supports_native_format() {
    let fixture = schema_fixture();
    let native = run(&fixture, Some("native"));
    assert!(native.status.success());
    assert!(
        String::from_utf8(native.stdout)
            .unwrap()
            .contains("state: enum\n    IDLE = 0\n    RUNNING = 1")
    );
}

#[test]
fn schema_errors_are_json() {
    let id = FIXTURE_COUNTER.fetch_add(1, Ordering::Relaxed);
    let missing_path =
        std::env::temp_dir().join(format!("mcapq-missing-{}-{id}.mcap", std::process::id()));
    let output = Command::new(env!("CARGO_BIN_EXE_mcapq"))
        .args([
            "schema",
            missing_path.to_str().unwrap(),
            "--topic",
            "/sample",
        ])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(1));
    let error: Value = serde_json::from_slice(&output.stderr).unwrap();
    assert_eq!(error["error"]["code"], "runtime_error");
}

#[test]
fn schema_help_does_not_advertise_a_metadata_prefix_without_a_batch_output_command() {
    let output = Command::new(env!("CARGO_BIN_EXE_mcapq"))
        .args(["schema", "--help"])
        .output()
        .unwrap();

    assert!(output.status.success());
    let help = String::from_utf8(output.stdout).unwrap();
    assert!(!help.contains("metadata-prefix"));
}
