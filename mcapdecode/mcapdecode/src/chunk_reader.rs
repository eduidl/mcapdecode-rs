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

use crate::{McapReader, McapReaderError, types::TimeRange};

/// What a chunk holds, resolved once when the file is opened.
///
/// Whether a chunk has messages and whether it has message indexes is decided by
/// this, not by the [`mcap::records::ChunkIndex`] fields.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ChunkContents {
    /// No message records, e.g. only schema and channel records.
    Empty,
    /// Messages, with a message index for each channel in the chunk.
    Indexed,
    /// Messages without message indexes; which channels appear is unknown.
    Unindexed,
}

/// Classify each chunk index of `summary`, in the same order.
///
/// A chunk index without message index offsets and with both message times at zero
/// is either empty or holds only messages logged at time zero. Only such chunks are
/// read, and only until their first message.
pub(crate) fn classify_chunks(summary: &mcap::read::Summary, mcap: &[u8]) -> Vec<ChunkContents> {
    summary
        .chunk_indexes
        .iter()
        .map(|chunk| {
            if !chunk.message_index_offsets.is_empty() {
                ChunkContents::Indexed
            } else if chunk.message_start_time != 0 || chunk.message_end_time != 0 {
                ChunkContents::Unindexed
            } else {
                // A chunk that cannot be read is left to fail when its messages are read.
                let has_message = summary
                    .stream_chunk(mcap, chunk)
                    .map_or(true, |mut messages| messages.next().is_some());
                if has_message {
                    ChunkContents::Unindexed
                } else {
                    ChunkContents::Empty
                }
            }
        })
        .collect()
}

/// Process messages of the selected channels within `time_range`, skipping chunks that hold none.
/// `process` and `callback` receive the message channel's position in `channel_ids`.
/// Reading stops without error once `callback` returns [`ControlFlow::Break`].
pub(crate) fn for_each_selected_message<T: Send>(
    reader: &McapReader,
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
            reader,
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
        reader,
        |id| indexes.get(&id).copied(),
        time_range,
        parallel,
        process,
        callback,
    )
}

fn for_each_message_of<T: Send>(
    reader: &McapReader,
    index_of: impl Fn(u16) -> Option<usize> + Sync,
    time_range: Option<TimeRange>,
    parallel: bool,
    process: impl Fn(usize, &mcap::Message<'_>) -> Result<T, McapReaderError> + Sync,
    callback: &mut impl FnMut(usize, T) -> Result<ControlFlow<()>, McapReaderError>,
) -> Result<(), McapReaderError> {
    let chunks = reader
        .message_chunks()
        .filter(|(chunk, contents)| {
            (*contents == ChunkContents::Unindexed
                || chunk
                    .message_index_offsets
                    .keys()
                    .any(|&id| index_of(id).is_some()))
                && time_range.is_none_or(|range| range.overlaps_chunk(chunk))
        })
        .map(|(chunk, _)| chunk);
    for_each_chunk(
        reader,
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
    reader: &McapReader,
    chunks: impl Iterator<Item = &'a mcap::records::ChunkIndex>,
    parallel: bool,
    process: impl Fn(&mcap::Message<'_>) -> Result<Option<T>, McapReaderError> + Sync,
    callback: &mut impl FnMut(T) -> Result<ControlFlow<()>, McapReaderError>,
) -> Result<(), McapReaderError> {
    if reader.summary().chunk_indexes.is_empty() {
        for message in mcap::MessageStream::new(reader.mmap())? {
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
            for message in reader.summary().stream_chunk(reader.mmap(), chunk)? {
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
                        );
                        sender.send((window_index * window_size + i, (messages, result)))
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
            while let Some((messages, result)) = pending.remove(&next) {
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
                if let Err(error) = result {
                    cancelled.store(true, Ordering::Relaxed);
                    return Err(error);
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
