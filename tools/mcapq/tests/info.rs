use std::{path::PathBuf, process::Command};

use serde_json::Value;

fn fixture_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../mcapdecode/mcapdecode/tests/fixtures/with_summary.mcap")
}

#[test]
fn info_outputs_topic_metadata_as_json() {
    let output = Command::new(env!("CARGO_BIN_EXE_mcapq"))
        .arg("info")
        .arg(fixture_path())
        .output()
        .unwrap();

    assert!(output.status.success());
    assert!(output.stderr.is_empty());

    let info: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert!(info["file"]["size_bytes"].as_u64().is_some());
    assert_eq!(
        info["topics"],
        serde_json::json!([
            {
                "topic": "/decoded",
                "schema": "test.Msg",
                "count": 2,
                "decodable": false,
                "decode_error": "no decoder registered for schema_encoding='jsonschema', message_encoding='json' on topic '/decoded'"
            },
            {
                "topic": "/raw",
                "schema": null,
                "count": 1,
                "decodable": false,
                "decode_error": "schema not available for topic '/raw' (channel id 2)"
            }
        ])
    );
}

#[test]
fn missing_file_uses_json_error_and_runtime_exit_code() {
    let output = Command::new(env!("CARGO_BIN_EXE_mcapq"))
        .args(["info", "/tmp/mcapq-info-does-not-exist.mcap"])
        .output()
        .unwrap();

    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    let error: Value = serde_json::from_slice(&output.stderr).unwrap();
    assert_eq!(error["error"]["code"], "runtime_error");
}

#[test]
fn help_writes_help_to_stdout_and_exits_successfully() {
    let output = Command::new(env!("CARGO_BIN_EXE_mcapq"))
        .arg("--help")
        .output()
        .unwrap();

    assert!(output.status.success());
    assert!(output.stderr.is_empty());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("Usage: mcapq <COMMAND>"));
    assert!(stdout.contains("info"));
    assert!(stdout.contains("schema"));
}
