use std::{
    io,
    ops::ControlFlow,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Receiver, TryRecvError},
    },
    thread,
    time::{Duration, Instant},
};

use anyhow::Result;
use mcapdecode::{DecodedMessage, McapReader, McapReaderError, RawMessage, Value};

use crate::{
    app::{App, LoadedMessage, TopicRow},
    format,
};

const MAX_LOADER_EVENTS_PER_FRAME: usize = 32;
const MESSAGE_BATCH_SIZE: usize = 64;
const MESSAGE_BATCH_MAX_LATENCY: Duration = Duration::from_millis(40);

#[derive(Debug, Default)]
pub(crate) struct LoaderDrainResult {
    pub(crate) state_changed: bool,
    pub(crate) hit_limit: bool,
}

pub(crate) struct TopicLoader {
    receiver: Receiver<LoaderEvent>,
    cancel: Arc<AtomicBool>,
    join_handle: thread::JoinHandle<()>,
}

enum LoaderEvent {
    Messages(Vec<LoadedMessage>),
    Finished { topic: String },
    Failed { topic: String, error: String },
}

impl TopicLoader {
    pub(crate) fn spawn(reader: Arc<McapReader>, row: TopicRow) -> Self {
        let (sender, receiver) = mpsc::channel();
        let cancel = Arc::new(AtomicBool::new(false));
        let cancel_for_thread = Arc::clone(&cancel);
        let label = row.label();

        let join_handle = thread::spawn(move || {
            // Deriving the schema up front tells whether the topic can be
            // decoded, or has to be shown as raw payload bytes.
            let field_defs = reader
                .read()
                .channel(row.channel.id)
                .field_defs()
                .map(|mut schemas| schemas.remove(0).1);
            let load_result = match field_defs {
                Ok(_) => load_topic(&sender, &cancel_for_thread, to_loaded_message, |callback| {
                    reader
                        .read()
                        .channel(row.channel.id)
                        .decoded()
                        .try_for_each(|_, message| callback(message))
                }),
                Err(error) if error.is_unsupported_channel() => load_topic(
                    &sender,
                    &cancel_for_thread,
                    to_loaded_raw_message,
                    |callback| {
                        reader
                            .read()
                            .channel(row.channel.id)
                            .raw()
                            .try_for_each(|_, message| callback(message))
                    },
                ),
                Err(error) => Err(anyhow::Error::new(error)
                    .context(format!("failed to load schema for topic '{label}'"))),
            };

            match load_result {
                Ok(()) => {
                    let _ = sender.send(LoaderEvent::Finished { topic: label });
                }
                Err(error) => {
                    let _ = sender.send(LoaderEvent::Failed {
                        topic: label,
                        error: error.to_string(),
                    });
                }
            }
        });

        Self {
            receiver,
            cancel,
            join_handle,
        }
    }
}

pub(crate) fn drain_loader_events(
    app: &mut App,
    loader: &mut Option<TopicLoader>,
) -> LoaderDrainResult {
    let mut result = LoaderDrainResult::default();
    let mut next_loader = loader.take();

    if let Some(active_loader) = next_loader.take() {
        let mut processed = 0usize;
        loop {
            if processed >= MAX_LOADER_EVENTS_PER_FRAME {
                next_loader = Some(active_loader);
                result.hit_limit = true;
                break;
            }

            match active_loader.receiver.try_recv() {
                Ok(LoaderEvent::Messages(messages)) => {
                    app.append_loaded_messages(messages);
                    result.state_changed = true;
                    processed += 1;
                }
                Ok(LoaderEvent::Finished { topic }) => {
                    app.finish_loading(&topic);
                    let _ = active_loader.join_handle.join();
                    result.state_changed = true;
                    break;
                }
                Ok(LoaderEvent::Failed { topic, error }) => {
                    app.fail_loading(format!("Failed to load {topic}: {error}"));
                    let _ = active_loader.join_handle.join();
                    result.state_changed = true;
                    break;
                }
                Err(TryRecvError::Empty) => {
                    next_loader = Some(active_loader);
                    break;
                }
                Err(TryRecvError::Disconnected) => {
                    let _ = active_loader.join_handle.join();
                    app.fail_loading("Loader disconnected unexpectedly");
                    result.state_changed = true;
                    break;
                }
            }
        }
    }

    *loader = next_loader;
    result
}

pub(crate) fn cancel_loader(loader: &mut Option<TopicLoader>) {
    if let Some(active_loader) = loader.take() {
        active_loader.cancel.store(true, Ordering::Relaxed);
        thread::spawn(move || {
            let _ = active_loader.join_handle.join();
        });
    }
}

fn to_loaded_message(index: usize, message: DecodedMessage) -> LoadedMessage {
    LoadedMessage {
        index,
        log_time: message.log_time,
        publish_time: message.publish_time,
        log_time_display: format::format_timestamp(message.log_time),
        publish_time_display: format::format_timestamp(message.publish_time),
        value: message.value,
        detail_rows: None,
        detail_text: None,
    }
}

fn load_topic<T>(
    sender: &mpsc::Sender<LoaderEvent>,
    cancel: &AtomicBool,
    convert: impl Fn(usize, T) -> LoadedMessage,
    read: impl FnOnce(
        &mut dyn FnMut(T) -> Result<ControlFlow<()>, Box<dyn std::error::Error + Send + Sync>>,
    ) -> Result<(), McapReaderError>,
) -> Result<()> {
    let mut pending = Vec::new();
    let mut last_flush = Instant::now();
    let mut next_index = 0usize;
    let load_result = read(&mut |message| {
        if cancel.load(Ordering::Relaxed) {
            return Ok(ControlFlow::Break(()));
        }

        pending.push(convert(next_index, message));
        next_index += 1;
        flush_pending_messages(sender, &mut pending, &mut last_flush)?;
        Ok(ControlFlow::Continue(()))
    });

    if !pending.is_empty() {
        let _ = sender.send(LoaderEvent::Messages(pending));
    }

    load_result.map_err(anyhow::Error::from)
}

fn to_loaded_raw_message(index: usize, message: RawMessage) -> LoadedMessage {
    let detail_rows = format::format_raw_detail_rows(
        message.log_time,
        message.publish_time,
        message.data.as_ref(),
    );
    let detail_text = detail_rows
        .iter()
        .map(|row| row.text.as_str())
        .collect::<Vec<_>>()
        .join("\n");

    LoadedMessage {
        index,
        log_time: message.log_time,
        publish_time: message.publish_time,
        log_time_display: format::format_timestamp(message.log_time),
        publish_time_display: format::format_timestamp(message.publish_time),
        value: Value::Bytes(message.data),
        detail_rows: Some(detail_rows),
        detail_text: Some(detail_text),
    }
}

fn flush_pending_messages(
    sender: &mpsc::Sender<LoaderEvent>,
    pending: &mut Vec<LoadedMessage>,
    last_flush: &mut Instant,
) -> Result<()> {
    if !should_flush_messages(pending.len(), last_flush.elapsed()) {
        return Ok(());
    }

    let batch = std::mem::take(pending);
    sender
        .send(LoaderEvent::Messages(batch))
        .map_err(|send_error| io::Error::other(send_error.to_string()))?;
    *last_flush = Instant::now();
    Ok(())
}

fn should_flush_messages(pending_len: usize, elapsed_since_flush: Duration) -> bool {
    pending_len >= MESSAGE_BATCH_SIZE
        || (pending_len > 0 && elapsed_since_flush >= MESSAGE_BATCH_MAX_LATENCY)
}
