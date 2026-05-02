use std::{
    io,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Receiver, TryRecvError},
    },
    thread,
    time::{Duration, Instant},
};

use anyhow::Result;
use mcapdecode::{
    McapReader, McapReaderError, RawMessage, TopicInfo,
    core::{DataTypeDef, DecodedMessage, FieldDef, FieldDefs, Value},
};

use crate::{
    app::{App, LoadedMessage},
    format,
};

const CANCELLED_ERROR: &str = "__mcaptui_loader_cancelled__";
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
    FieldDefs(mcapdecode::core::FieldDefs),
    Messages(Vec<LoadedMessage>),
    Finished { topic: String },
    Failed { topic: String, error: String },
}

impl TopicLoader {
    pub(crate) fn spawn(input: PathBuf, topic: TopicInfo, parallel: bool) -> Self {
        let (sender, receiver) = mpsc::channel();
        let cancel = Arc::new(AtomicBool::new(false));
        let cancel_for_thread = Arc::clone(&cancel);
        let topic_name = topic.topic.clone();

        let join_handle = thread::spawn(move || {
            let reader = McapReader::builder()
                .with_default_decoders()
                .with_parallel(parallel)
                .build();

            let load_result = match reader.topic_field_defs(&input, &topic_name) {
                Ok(field_defs) => load_decoded_topic(
                    &reader,
                    &input,
                    &topic_name,
                    field_defs,
                    &sender,
                    &cancel_for_thread,
                ),
                Err(error) if supports_raw_fallback(&error) => {
                    load_raw_topic(&reader, &input, &topic_name, &sender, &cancel_for_thread)
                }
                Err(error) => Err(anyhow::Error::new(error)
                    .context(format!("failed to load schema for topic '{}'", topic_name))),
            };

            match load_result {
                Ok(()) => {
                    let _ = sender.send(LoaderEvent::Finished { topic: topic_name });
                }
                Err(error) if error.to_string() == CANCELLED_ERROR => {}
                Err(error) => {
                    let _ = sender.send(LoaderEvent::Failed {
                        topic: topic_name,
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
                Ok(LoaderEvent::FieldDefs(field_defs)) => {
                    app.set_message_field_defs(field_defs);
                    result.state_changed = true;
                    processed += 1;
                }
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

fn load_decoded_topic(
    reader: &McapReader,
    input: &Path,
    topic_name: &str,
    field_defs: FieldDefs,
    sender: &mpsc::Sender<LoaderEvent>,
    cancel: &AtomicBool,
) -> Result<()> {
    if sender.send(LoaderEvent::FieldDefs(field_defs)).is_err() {
        return Ok(());
    }

    let mut pending = Vec::new();
    let mut last_flush = Instant::now();
    let mut next_index = 0usize;
    let load_result = reader.for_each_decoded_message(input, topic_name, |message| {
        if cancel.load(Ordering::Relaxed) {
            return Err(CANCELLED_ERROR.into());
        }

        pending.push(to_loaded_message(next_index, message));
        next_index += 1;

        flush_pending_messages(sender, &mut pending, &mut last_flush)?;

        Ok(())
    });

    if !pending.is_empty() {
        let _ = sender.send(LoaderEvent::Messages(pending));
    }

    load_result.map_err(anyhow::Error::from)
}

fn load_raw_topic(
    reader: &McapReader,
    input: &Path,
    topic_name: &str,
    sender: &mpsc::Sender<LoaderEvent>,
    cancel: &AtomicBool,
) -> Result<()> {
    if sender
        .send(LoaderEvent::FieldDefs(raw_payload_field_defs()))
        .is_err()
    {
        return Ok(());
    }

    let mut pending = Vec::new();
    let mut last_flush = Instant::now();
    let mut next_index = 0usize;
    let load_result = reader.for_each_raw_message(input, topic_name, |message| {
        if cancel.load(Ordering::Relaxed) {
            return Err(CANCELLED_ERROR.into());
        }

        pending.push(to_loaded_raw_message(next_index, message));
        next_index += 1;

        flush_pending_messages(sender, &mut pending, &mut last_flush)?;

        Ok(())
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

fn raw_payload_field_defs() -> FieldDefs {
    vec![FieldDef::new("payload", DataTypeDef::Bytes, false)].into()
}

fn supports_raw_fallback(error: &McapReaderError) -> bool {
    matches!(
        error,
        McapReaderError::NoDecoder { .. }
            | McapReaderError::SchemaNotAvailable { .. }
            | McapReaderError::SchemaDerivationFailed { .. }
    )
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
