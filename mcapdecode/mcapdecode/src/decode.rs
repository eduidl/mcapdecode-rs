//! Chunk selection and message decoding for the sequential and parallel paths.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    io,
    path::Path,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
};

use mcapdecode_core::{DecodedMessage, FieldDefs, TopicDecoder};
use rayon::prelude::*;

use crate::{
    McapReader, ReadOptions, error::McapReaderError, internal_reader::InternalReader,
    mcap_file::McapFile,
};

impl InternalReader {
    pub(crate) fn for_each_decoded_message_impl<F>(
        &self,
        file: &McapFile,
        context: &TopicDecodeContext,
        options: &ReadOptions,
        callback: &mut F,
    ) -> Result<(), McapReaderError>
    where
        F: FnMut(DecodedMessage) -> Result<(), McapReaderError>,
    {
        let chunks = file.summary.chunk_indexes.iter().filter(|chunk| {
            let offsets = &chunk.message_index_offsets;
            (offsets.is_empty() || offsets.contains_key(&context.channel_id))
                && options
                    .time_range
                    .is_none_or(|range| range.overlaps_chunk(chunk))
        });
        self.for_each_selected(
            file,
            chunks,
            options,
            |message| (message.channel.id == context.channel_id).then_some(((), context)),
            &mut |(), message| callback(message),
        )
    }

    pub(crate) fn for_each_topic_decoded_message_impl(
        &self,
        file: &McapFile,
        contexts: &[TopicDecodeContext],
        options: &ReadOptions,
        callback: &mut impl FnMut(usize, DecodedMessage) -> Result<(), McapReaderError>,
    ) -> Result<(), McapReaderError> {
        if let [context] = contexts {
            return self.for_each_decoded_message_impl(file, context, options, &mut |message| {
                callback(0, message)
            });
        }
        let channels: HashMap<_, _> = contexts
            .iter()
            .enumerate()
            .map(|(i, context)| (context.channel_id, i))
            .collect();
        let chunks = file.summary.chunk_indexes.iter().filter(|chunk| {
            let offsets = &chunk.message_index_offsets;
            (offsets.is_empty() || offsets.keys().any(|id| channels.contains_key(id)))
                && options
                    .time_range
                    .is_none_or(|range| range.overlaps_chunk(chunk))
        });
        self.for_each_selected(
            file,
            chunks,
            options,
            |message| {
                let &i = channels.get(&message.channel.id)?;
                Some((i, &contexts[i]))
            },
            callback,
        )
    }

    fn for_each_selected<'a, K: Send, F>(
        &self,
        file: &McapFile,
        chunks: impl Iterator<Item = &'a mcap::records::ChunkIndex>,
        options: &ReadOptions,
        select: impl Fn(&mcap::Message<'_>) -> Option<(K, &'a TopicDecodeContext)> + Sync,
        callback: &mut F,
    ) -> Result<(), McapReaderError>
    where
        F: FnMut(K, DecodedMessage) -> Result<(), McapReaderError>,
    {
        let decode_chunk =
            |chunk,
             cancelled: &dyn Fn() -> bool,
             emit: &mut dyn FnMut(K, DecodedMessage) -> Result<(), McapReaderError>| {
                for message in file.summary.stream_chunk(&file.mmap, chunk)? {
                    if cancelled() {
                        break;
                    }
                    let message = message?;
                    if let Some((key, context)) = select(&message)
                        && options
                            .time_range
                            .is_none_or(|range| range.contains(message.log_time))
                    {
                        emit(
                            key,
                            DecodedMessage {
                                log_time: message.log_time,
                                publish_time: message.publish_time,
                                value: context.decoder.decode(&message.data)?,
                            },
                        )?;
                    }
                }
                Ok::<_, McapReaderError>(())
            };
        if !options.parallel.unwrap_or(self.parallel) {
            for chunk in chunks {
                decode_chunk(chunk, &|| false, callback)?;
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
                            let result = decode_chunk(
                                chunk,
                                &|| cancelled_ref.load(Ordering::Relaxed),
                                &mut |key, message| {
                                    messages.push((key, message));
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
                    for (key, message) in messages {
                        if let Err(error) = callback(key, message) {
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
}

impl McapReader {
    /// Read several topics while decompressing each selected chunk only once.
    /// The callback runs on the calling thread, in file order within each topic.
    /// Order between topics is unspecified. Duplicate topics are read once.
    pub fn for_each_topic_decoded_message(
        &self,
        path: &Path,
        topics: &[&str],
        callback: impl FnMut(
            &str,
            DecodedMessage,
        ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>,
    ) -> Result<(), McapReaderError> {
        self.for_each_topic_decoded_message_with_options(
            path,
            topics,
            &ReadOptions::default(),
            callback,
        )
    }

    /// Read selected topics with log-time filtering and parallel read options.
    /// Duplicate topics are read once.
    pub fn for_each_topic_decoded_message_with_options(
        &self,
        path: &Path,
        topics: &[&str],
        options: &ReadOptions,
        mut callback: impl FnMut(
            &str,
            DecodedMessage,
        ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>,
    ) -> Result<(), McapReaderError> {
        if topics.is_empty() {
            return Ok(());
        }
        self.with_topic_contexts(path, topics, |file, topics, contexts| {
            self.inner.for_each_topic_decoded_message_impl(
                file,
                contexts,
                options,
                &mut |i, message| callback(topics[i], message).map_err(McapReaderError::Callback),
            )
        })
    }

    pub(crate) fn with_topic_contexts<T>(
        &self,
        path: &Path,
        topics: &[&str],
        callback: impl FnOnce(&McapFile, &[&str], &[TopicDecodeContext]) -> Result<T, McapReaderError>,
    ) -> Result<T, McapReaderError> {
        let file = McapFile::open(path)?;
        let mut seen = HashSet::new();
        let topics: Vec<_> = topics
            .iter()
            .copied()
            .filter(|topic| seen.insert(*topic))
            .collect();
        let contexts: Vec<_> = topics
            .iter()
            .map(|topic| {
                self.inner
                    .resolve_topic_decode_context(&file.summary, topic)
            })
            .collect::<Result<_, _>>()?;
        callback(&file, &topics, &contexts)
    }
}

pub(crate) struct TopicDecodeContext {
    pub(crate) channel_id: u16,
    pub(crate) schema_name: String,
    pub(crate) decoder: Box<dyn TopicDecoder>,
    pub(crate) field_defs: FieldDefs,
}
