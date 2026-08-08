//! Contracts for writing fixtures out: rejected shapes, and concurrent generation.

use std::{path::PathBuf, thread};

use mcapbench::{
    Encoding, FileShape, Layout, PayloadCase, ensure_generated, generate, generated_path,
};

fn temp_output(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!("mcapbench-test-{}-{name}.mcap", std::process::id()))
}

#[test]
fn zero_chunk_bytes_is_rejected() {
    // The writer would flush a chunk per message, quietly producing a file with a
    // completely different shape (and size) than the benchmark asked for.
    let output = temp_output("zero-chunk");
    let result = generate(
        &output,
        PayloadCase::Flat,
        Encoding::Ros2idl,
        FileShape {
            chunk_bytes: 0,
            ..FileShape::default()
        },
    );
    assert!(result.is_err(), "zero chunk_bytes should be rejected");
    let _ = std::fs::remove_file(&output);
}

#[test]
fn out_of_range_selectivity_is_rejected() {
    let output = temp_output("bad-select");
    let result = generate(
        &output,
        PayloadCase::Flat,
        Encoding::Ros2idl,
        FileShape {
            select_percent: 101,
            ..FileShape::default()
        },
    );
    assert!(
        result.is_err(),
        "select_percent above 100 should be rejected"
    );
    let _ = std::fs::remove_file(&output);
}

/// Threads racing for the same missing fixture must not write over each other: each
/// stages its own file and renames it into place, so the cached fixture is always a
/// complete one.
#[test]
fn concurrent_generation_yields_one_complete_fixture() {
    let shape = FileShape {
        // A shape no other test uses, so this really does start from a missing fixture.
        select_percent: 37,
        layout: Layout::Clustered,
        ..FileShape::default()
    };
    let path = generated_path(PayloadCase::Flat, Encoding::Ros2idl, shape).unwrap();
    let _ = std::fs::remove_file(&path);

    let paths: Vec<PathBuf> = thread::scope(|scope| {
        let handles: Vec<_> = (0..4)
            .map(|_| {
                scope.spawn(move || {
                    ensure_generated(PayloadCase::Flat, Encoding::Ros2idl, shape).unwrap()
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });

    assert!(paths.iter().all(|p| *p == path));
    let published = std::fs::metadata(&path).unwrap().len();

    // Regenerating alone must produce exactly the same size; a torn write would not.
    std::fs::remove_file(&path).unwrap();
    let alone = ensure_generated(PayloadCase::Flat, Encoding::Ros2idl, shape).unwrap();
    assert_eq!(std::fs::metadata(&alone).unwrap().len(), published);

    // No staging file may survive a successful run.
    let leftovers: Vec<_> = std::fs::read_dir(mcapbench::fixture_dir())
        .unwrap()
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .filter(|name| name.starts_with(".tmp-"))
        .collect();
    assert!(
        leftovers.is_empty(),
        "staging files left behind: {leftovers:?}"
    );

    let _ = std::fs::remove_file(&path);
}
