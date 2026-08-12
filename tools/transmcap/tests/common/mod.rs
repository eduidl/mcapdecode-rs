use std::{
    path::PathBuf,
    sync::atomic::{AtomicUsize, Ordering},
};

static TEMP_FILE_COUNTER: AtomicUsize = AtomicUsize::new(0);

pub fn fixture() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/transmcap-demo.mcap")
}

pub fn temporary_output_path(extension: &str) -> PathBuf {
    let counter = TEMP_FILE_COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "transmcap-test-{}-{counter}.{extension}",
        std::process::id()
    ))
}
