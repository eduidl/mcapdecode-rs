//! Chunk traversal and ordered delivery, independent of message decoding.

use std::{
    collections::BTreeMap,
    io,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
};

use rayon::prelude::*;

use crate::{error::McapReaderError, mcap_file::McapFile};

/// Process each message while its chunk is being read, then deliver results in chunk order.
/// In parallel mode `process` runs on workers and `callback` runs on the calling thread.
pub(crate) fn for_each_chunk<'a, T: Send>(
    file: &McapFile,
    chunks: impl Iterator<Item = &'a mcap::records::ChunkIndex>,
    parallel: bool,
    process: impl Fn(&mcap::Message<'_>) -> Result<Option<T>, McapReaderError> + Sync,
    callback: &mut impl FnMut(T) -> Result<(), McapReaderError>,
) -> Result<(), McapReaderError> {
    let read_chunk = |chunk,
                      cancelled: &dyn Fn() -> bool,
                      emit: &mut dyn FnMut(T) -> Result<(), McapReaderError>| {
        for message in file.summary.stream_chunk(&file.mmap, chunk)? {
            if cancelled() {
                break;
            }
            if let Some(result) = process(&message?)? {
                emit(result)?;
            }
        }
        Ok::<_, McapReaderError>(())
    };
    if !parallel {
        for chunk in chunks {
            read_chunk(chunk, &|| false, callback)?;
        }
        return Ok(());
    }

    let chunks: Vec<_> = chunks.collect();
    let chunk_count = chunks.len();
    let cancelled = AtomicBool::new(false);
    std::thread::scope(|scope| -> Result<(), McapReaderError> {
        // Bound decoded chunks without blocking Rayon workers on result delivery.
        let (sender, receiver) = mpsc::channel();
        let (resume, ready) = mpsc::sync_channel(1);
        let window_size = rayon::current_num_threads().max(1) * 4;
        resume.send(()).expect("receiver is alive");
        let cancelled_ref = &cancelled;
        scope.spawn(move || {
            for (window_index, window) in chunks.chunks(window_size).enumerate() {
                let sent = window.par_iter().enumerate().try_for_each_with(
                    &sender,
                    |sender, (i, chunk)| {
                        if cancelled_ref.load(Ordering::Relaxed) {
                            return Ok(());
                        }
                        let mut messages = Vec::new();
                        let result = read_chunk(
                            chunk,
                            &|| cancelled_ref.load(Ordering::Relaxed),
                            &mut |message| {
                                messages.push(message);
                                Ok(())
                            },
                        )
                        .map(|()| messages);
                        sender.send((window_index * window_size + i, result))
                    },
                );
                if sent.is_err() || ready.recv().is_err() {
                    break;
                }
            }
        });
        let mut next = 0;
        let mut pending = BTreeMap::new();
        while next < chunk_count {
            let (position, result) = receiver.recv().map_err(|_| {
                McapReaderError::Io(io::Error::other(
                    "parallel decode worker disconnected unexpectedly",
                ))
            })?;
            pending.insert(position, result);
            while let Some(result) = pending.remove(&next) {
                let messages = match result {
                    Ok(messages) => messages,
                    Err(error) => {
                        cancelled.store(true, Ordering::Relaxed);
                        return Err(error);
                    }
                };
                for message in messages {
                    if let Err(error) = callback(message) {
                        cancelled.store(true, Ordering::Relaxed);
                        return Err(error);
                    }
                }
                next += 1;
                if next % window_size == 0 || next == chunk_count {
                    let _ = resume.send(());
                }
            }
        }
        Ok(())
    })
}
