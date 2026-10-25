use std::{fs, process::Command};

mod common;

#[test]
fn schema_writes_the_topic_schema_to_stdout() {
    let fixture = common::fixture();
    let output = Command::new(env!("CARGO_BIN_EXE_transmcap"))
        .args([
            "schema",
            fixture.to_str().unwrap(),
            "--topic",
            "/demo/velocity",
        ])
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        String::from_utf8(output.stdout).unwrap(),
        "speed_mps: float64\ngear: string\n\n"
    );
}

#[test]
fn schema_writes_the_topic_schema_to_a_file() {
    let fixture = common::fixture();
    let path = common::temporary_output_path("txt");
    let output = Command::new(env!("CARGO_BIN_EXE_transmcap"))
        .args([
            "schema",
            fixture.to_str().unwrap(),
            "--topic",
            "/demo/velocity",
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
    assert_eq!(
        fs::read_to_string(&path).unwrap(),
        "speed_mps: float64\ngear: string\n\n"
    );
    fs::remove_file(path).unwrap();
}

#[test]
fn schema_rejects_an_unknown_topic() {
    let fixture = common::fixture();
    let output = Command::new(env!("CARGO_BIN_EXE_transmcap"))
        .args(["schema", fixture.to_str().unwrap(), "--topic", "/unknown"])
        .output()
        .unwrap();

    assert!(!output.status.success());
}
