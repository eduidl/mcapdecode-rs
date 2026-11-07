//! Message selection, chunk traversal and ordered delivery, independent of message decoding.

use std::{
    collections::BTreeMap,
    io,
    ops::ControlFlow,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
};

use rayon::prelude::*;

use crate::{TimeRange, error::McapReaderError, mcap_file::McapFile};

/// Process messages of the selected channels within `time_range`, skipping chunks that hold none.
/// `process` and `callback` receive the message channel's position in `channel_ids`.
/// Reading stops without error once `callback` returns [`ControlFlow::Break`].
pub(crate) fn for_each_selected_message<T: Send>(
    file: &McapFile,
    channel_ids: &[u16],
    time_range: Option<TimeRange>,
    parallel: bool,
    process: impl Fn(usize, &mcap::Message<'_>) -> Result<T, McapReaderError> + Sync,
    callback: &mut impl FnMut(usize, T) -> Result<ControlFlow<()>, McapReaderError>,
) -> Result<(), McapReaderError> {
    if channel_ids.is_empty() {
        return Ok(());
    }
    // A single channel avoids the per-message map lookup.
    if let [channel_id] = *channel_ids {
        return for_each_message_of(
            file,
            |id| (id == channel_id).then_some(0),
            time_range,
            parallel,
            process,
            callback,
        );
    }
    let indexes: BTreeMap<_, _> = channel_ids
        .iter()
        .enumerate()
        .map(|(i, &id)| (id, i))
        .collect();
    for_each_message_of(
        file,
        |id| indexes.get(&id).copied(),
        time_range,
        parallel,
        process,
        callback,
    )
}

fn for_each_message_of<T: Send>(
    file: &McapFile,
    index_of: impl Fn(u16) -> Option<usize> + Sync,
    time_range: Option<TimeRange>,
    parallel: bool,
    process: impl Fn(usize, &mcap::Message<'_>) -> Result<T, McapReaderError> + Sync,
    callback: &mut impl FnMut(usize, T) -> Result<ControlFlow<()>, McapReaderError>,
) -> Result<(), McapReaderError> {
    let chunks = file.summary.chunk_indexes.iter().filter(|chunk| {
        let offsets = &chunk.message_index_offsets;
        (offsets.is_empty() || offsets.keys().any(|&id| index_of(id).is_some()))
            && time_range.is_none_or(|range| range.overlaps_chunk(chunk))
    });
    for_each_chunk(
        file,
        chunks,
        parallel,
        |message| {
            let Some(i) = index_of(message.channel.id) else {
                return Ok(None);
            };
            if time_range.is_some_and(|range| !range.contains(message.log_time)) {
                return Ok(None);
            }
            Ok(Some((i, process(i, message)?)))
        },
        &mut |(i, result)| callback(i, result),
    )
}

/// Process each message while its chunk is being read, then deliver results in chunk order.
/// In parallel mode `process` runs on workers and `callback` runs on the calling thread.
/// Without chunk indexes, scan the file sequentially instead.
fn for_each_chunk<'a, T: Send>(
    file: &McapFile,
    chunks: impl Iterator<Item = &'a mcap::records::ChunkIndex>,
    parallel: bool,
    process: impl Fn(&mcap::Message<'_>) -> Result<Option<T>, McapReaderError> + Sync,
    callback: &mut impl FnMut(T) -> Result<ControlFlow<()>, McapReaderError>,
) -> Result<(), McapReaderError> {
    if file.summary.chunk_indexes.is_empty() {
        for message in mcap::MessageStream::new(&file.mmap)? {
            if let Some(result) = process(&message?)?
                && callback(result)?.is_break()
            {
                break;
            }
        }
        return Ok(());
    }
    let read_chunk =
        |chunk,
         cancelled: &dyn Fn() -> bool,
         emit: &mut dyn FnMut(T) -> Result<ControlFlow<()>, McapReaderError>| {
            for message in file.summary.stream_chunk(&file.mmap, chunk)? {
                if cancelled() {
                    break;
                }
                if let Some(result) = process(&message?)?
                    && emit(result)?.is_break()
                {
                    return Ok(ControlFlow::Break(()));
                }
            }
            Ok::<_, McapReaderError>(ControlFlow::Continue(()))
        };
    if !parallel {
        for chunk in chunks {
            if read_chunk(chunk, &|| false, callback)?.is_break() {
                break;
            }
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
                                Ok(ControlFlow::Continue(()))
                            },
                        )
                        .map(|_| messages);
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
                    match callback(message) {
                        Ok(ControlFlow::Continue(())) => {}
                        Ok(ControlFlow::Break(())) => {
                            cancelled.store(true, Ordering::Relaxed);
                            return Ok(());
                        }
                        Err(error) => {
                            cancelled.store(true, Ordering::Relaxed);
                            return Err(error);
                        }
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
