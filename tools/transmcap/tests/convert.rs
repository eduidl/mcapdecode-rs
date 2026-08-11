use std::process::Command;

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
