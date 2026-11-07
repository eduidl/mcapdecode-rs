use std::ops::ControlFlow;

use super::{core::split_reader, *};

/// The multi-topic fixture without chunks, with sequential chunk reads, and with parallel chunk reads.
fn stop_cases() -> Vec<(TempFixture, OpenedMcap)> {
    [(false, false), (true, false), (true, true)]
        .into_iter()
        .map(|(use_chunks, parallel)| {
            let fixture = multi_topic_fixture_with_options(
                WriteOptions::new()
                    .use_chunks(use_chunks)
                    .chunk_size(Some(128)),
            );
            let file = split_reader(parallel).open_file(fixture.path()).unwrap();
            (fixture, file)
        })
        .collect()
}

fn stop_after(calls: &mut usize, limit: usize) -> ControlFlow<()> {
    *calls += 1;
    if *calls == limit {
        ControlFlow::Break(())
    } else {
        ControlFlow::Continue(())
    }
}

#[test]
fn raw_try_for_each_stops_without_error() {
    for (_fixture, file) in stop_cases() {
        let mut calls = 0;
        file.read()
            .raw()
            .try_for_each(|_, _| Ok(stop_after(&mut calls, 10)))
            .unwrap();
        assert_eq!(calls, 10);
    }
}

#[test]
fn decoded_try_for_each_stops_without_error() {
    for (_fixture, file) in stop_cases() {
        let mut log_times = Vec::new();
        file.read()
            .topic("/first")
            .decoded()
            .try_for_each(|_, message| {
                log_times.push(message.log_time);
                Ok(if log_times.len() == 10 {
                    ControlFlow::Break(())
                } else {
                    ControlFlow::Continue(())
                })
            })
            .unwrap();
        assert_eq!(log_times, (0..10).collect::<Vec<_>>());
    }
}

#[test]
fn try_for_each_propagates_callback_error() {
    for (_fixture, file) in stop_cases() {
        let error = file
            .read()
            .decoded()
            .try_for_each(|_, _| Err::<(), _>("callback failed".into()))
            .unwrap_err();
        assert!(matches!(error, McapReaderError::Callback(_)));
    }
}

#[cfg(feature = "arrow")]
#[test]
fn arrow_try_for_each_stops_without_flushing_buffered_rows() {
    for (_fixture, file) in stop_cases() {
        // Full batches are emitted while reading.
        let mut calls = 0;
        file.read()
            .arrow()
            .batch_size(4)
            .try_for_each(|_, _| Ok(stop_after(&mut calls, 3)))
            .unwrap();
        assert_eq!(calls, 3);

        // Every batch is a final flush, one per selected channel.
        let mut calls = 0;
        file.read()
            .topics(["/first", "/second"])
            .arrow()
            .batch_size(2048)
            .try_for_each(|_, _| Ok(stop_after(&mut calls, 1)))
            .unwrap();
        assert_eq!(calls, 1);
    }
}
