//! Chunk selection and message decoding for the sequential and parallel paths.

use std::{
    collections::BTreeMap,
    io,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
};

use mcapdecode_core::{DecodedMessage, FieldDefs, TopicDecoder};
use memmap2::Mmap;

use crate::{McapReader, ReadOptions, TimeRange, error::McapReaderError};

/// Chunk- and message-level predicates shared by the sequential and parallel
/// read paths.
///
/// The two paths differ in how they schedule decoding, but they must agree on
/// which chunks and messages belong to a read, so both select through this.
#[derive(Debug, Clone, Copy)]
struct MessageFilter {
    channel_id: u16,
    time_range: Option<TimeRange>,
}

impl MessageFilter {
    fn new(context: &TopicDecodeContext, options: &ReadOptions) -> Self {
        Self {
            channel_id: context.channel_id,
            time_range: options.time_range,
        }
    }

    /// Rejected chunks are never decompressed, so this is the read's cheapest
    /// filter and the only one that saves I/O.
    fn accepts_chunk(self, chunk_index: &mcap::records::ChunkIndex) -> bool {
        chunk_index
            .message_index_offsets
            .contains_key(&self.channel_id)
            && self
                .time_range
                .is_none_or(|range| range.overlaps_chunk(chunk_index))
    }

    /// A chunk can hold messages from other channels and from outside the time
    /// range, so surviving [`MessageFilter::accepts_chunk`] is not enough.
    fn accepts_message(self, message: &mcap::Message<'_>) -> bool {
        message.channel.id == self.channel_id
            && self
                .time_range
                .is_none_or(|range| range.contains(message.log_time))
    }
}

impl McapReader {
    pub(crate) fn for_each_decoded_message_impl<F>(
        &self,
        mmap: &Mmap,
        summary: &mcap::read::Summary,
        context: &TopicDecodeContext,
        request: DecodeRequest<'_>,
        callback: &mut F,
    ) -> Result<(), McapReaderError>
    where
        F: FnMut(DecodedMessage) -> Result<(), McapReaderError>,
    {
        // TODO(P2a): When a limit is set, use per-channel MessageIndex entries to
        // identify the minimal set of chunks that can satisfy it, then decode that
        // bounded set in parallel. Message indexes are optional in MCAP, so files
        // without them must retain this sequential, immediate-stop fallback.
        let parallel = request.options.parallel.unwrap_or(self.parallel);
        if parallel && request.options.limit.is_none() {
            self.for_each_decoded_message_parallel(mmap, summary, context, request, callback)
        } else {
            self.for_each_decoded_message_sequential(mmap, summary, context, request, callback)
        }
    }

    fn for_each_decoded_message_parallel<F>(
        &self,
        mmap: &Mmap,
        summary: &mcap::read::Summary,
        context: &TopicDecodeContext,
        request: DecodeRequest<'_>,
        callback: &mut F,
    ) -> Result<(), McapReaderError>
    where
        F: FnMut(DecodedMessage) -> Result<(), McapReaderError>,
    {
        use rayon::prelude::*;

        let filter = MessageFilter::new(context, request.options);
        let chunk_indexes: Vec<_> = Self::matching_chunk_indexes(summary, filter).collect();
        let chunk_count = chunk_indexes.len();
        let cancelled = Arc::new(AtomicBool::new(false));
        let (sender, receiver) = mpsc::channel();

        std::thread::scope(|scope| -> Result<(), McapReaderError> {
            let worker_sender = sender.clone();
            let worker = scope.spawn(|| {
                chunk_indexes.into_par_iter().enumerate().for_each_with(
                    worker_sender,
                    |sender, (position, chunk_index)| {
                        let result = self.decode_chunk_messages(
                            mmap,
                            summary,
                            context,
                            chunk_index,
                            request,
                            &cancelled,
                        );
                        let _ = sender.send((position, result));
                    },
                );
            });
            drop(sender);

            let mut next_position = 0usize;
            let mut skipped = 0usize;
            let mut pending = BTreeMap::new();
            while next_position < chunk_count {
                let (position, result) = receiver.recv().map_err(|_| {
                    McapReaderError::Io(io::Error::other(
                        "parallel decode worker disconnected unexpectedly",
                    ))
                })?;
                pending.insert(position, result);

                while let Some(result) = pending.remove(&next_position) {
                    let chunk_messages = match result {
                        Ok(messages) => messages,
                        Err(error) => {
                            cancelled.store(true, Ordering::Relaxed);
                            return Err(error);
                        }
                    };
                    for decoded in chunk_messages {
                        // Chunks are decoded before their position in the topic
                        // is known, so the offset can only be applied here.
                        if skipped < request.options.offset {
                            skipped += 1;
                            continue;
                        }
                        if let Err(error) = callback(decoded) {
                            cancelled.store(true, Ordering::Relaxed);
                            return Err(error);
                        }
                    }
                    next_position += 1;
                }
            }
            let _ = worker.join();
            Ok(())
        })?;

        Ok(())
    }

    fn matching_chunk_indexes(
        summary: &mcap::read::Summary,
        filter: MessageFilter,
    ) -> impl Iterator<Item = &mcap::records::ChunkIndex> {
        summary
            .chunk_indexes
            .iter()
            .filter(move |chunk_index| filter.accepts_chunk(chunk_index))
    }

    fn decode_chunk_messages(
        &self,
        mmap: &Mmap,
        summary: &mcap::read::Summary,
        context: &TopicDecodeContext,
        chunk_index: &mcap::records::ChunkIndex,
        request: DecodeRequest<'_>,
        cancelled: &AtomicBool,
    ) -> Result<Vec<DecodedMessage>, McapReaderError> {
        if cancelled.load(Ordering::Relaxed) {
            return Ok(Vec::new());
        }

        let filter = MessageFilter::new(context, request.options);
        let mut decoded_messages = Vec::new();
        for msg_result in summary.stream_chunk(mmap, chunk_index)? {
            if cancelled.load(Ordering::Relaxed) {
                return Ok(decoded_messages);
            }

            let msg = msg_result?;
            if !filter.accepts_message(&msg) {
                continue;
            }
            decoded_messages.push(self.decode_message(
                context,
                request.topic,
                msg.log_time,
                msg.publish_time,
                &msg.data,
            )?);
        }

        Ok(decoded_messages)
    }

    fn for_each_decoded_message_sequential<F>(
        &self,
        mmap: &Mmap,
        summary: &mcap::read::Summary,
        context: &TopicDecodeContext,
        request: DecodeRequest<'_>,
        callback: &mut F,
    ) -> Result<(), McapReaderError>
    where
        F: FnMut(DecodedMessage) -> Result<(), McapReaderError>,
    {
        if request.options.limit == Some(0) {
            return Ok(());
        }
        let filter = MessageFilter::new(context, request.options);
        let mut skipped = 0usize;
        let mut emitted = 0usize;
        for chunk_index in Self::matching_chunk_indexes(summary, filter) {
            for message in summary.stream_chunk(mmap, chunk_index)? {
                let message = message?;
                if !filter.accepts_message(&message) {
                    continue;
                }
                // Skipping here keeps the offset messages out of the decoder.
                if skipped < request.options.offset {
                    skipped += 1;
                    continue;
                }

                let decoded = self.decode_message(
                    context,
                    request.topic,
                    message.log_time,
                    message.publish_time,
                    &message.data,
                )?;
                callback(decoded)?;
                emitted += 1;
                if request.options.limit.is_some_and(|limit| emitted >= limit) {
                    return Ok(());
                }
            }
        }

        Ok(())
    }

    fn decode_message(
        &self,
        context: &TopicDecodeContext,
        topic: &str,
        log_time: u64,
        publish_time: u64,
        data: &[u8],
    ) -> Result<DecodedMessage, McapReaderError> {
        let value =
            context
                .decoder
                .decode(data)
                .map_err(|e| McapReaderError::MessageDecodeFailed {
                    topic: topic.to_string(),
                    source: e,
                })?;

        Ok(DecodedMessage {
            log_time,
            publish_time,
            value,
        })
    }
}

pub(crate) struct TopicDecodeContext {
    pub(crate) channel_id: u16,
    pub(crate) schema_name: String,
    pub(crate) decoder: Box<dyn TopicDecoder>,
    pub(crate) field_defs: FieldDefs,
}

#[derive(Clone, Copy)]
pub(crate) struct DecodeRequest<'a> {
    pub(crate) topic: &'a str,
    pub(crate) options: &'a ReadOptions,
}
