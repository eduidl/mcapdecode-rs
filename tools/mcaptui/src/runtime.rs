use std::{
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{Context, Result, bail};
use clap::Parser;
use crossterm::event::{self, Event};
use mcapdecode::McapReader;

use crate::{
    app::{self, App, AppRequest, AppUpdate},
    loader::{self, TopicLoader},
    schema::{self, SchemaCache},
    terminal, ui,
};

const EVENT_POLL_TIMEOUT: Duration = Duration::from_millis(50);
const MAX_INPUT_EVENTS_PER_FRAME: usize = 256;

#[derive(Debug, Parser)]
#[command(
    name = "mcaptui",
    about = "Browse MCAP topics and decoded messages in a TUI"
)]
struct Cli {
    /// Path to the MCAP file
    input: PathBuf,

    /// Open a specific topic directly
    #[arg(long)]
    topic: Option<String>,

    /// Enable parallel chunk decompression and decoding
    #[arg(long)]
    parallel: bool,
}

#[derive(Debug, Default)]
struct FrameInputBatch {
    events: Vec<Event>,
    hit_limit: bool,
}

#[derive(Debug, Default)]
struct FrameProcessingResult {
    state_changed: bool,
    should_quit: bool,
}

struct RuntimeContext<'a> {
    loader: &'a mut Option<TopicLoader>,
    reader: &'a McapReader,
    input: &'a Path,
    schema_cache: &'a mut SchemaCache,
    parallel: bool,
}

pub(crate) fn main() -> Result<()> {
    run(Cli::parse())
}

fn run(cli: Cli) -> Result<()> {
    let reader = McapReader::builder()
        .with_default_decoders()
        .with_parallel(cli.parallel)
        .build();
    let topics = reader
        .list_topics(&cli.input)
        .with_context(|| format!("failed to read topics from {}", cli.input.display()))?;
    let mut app = App::new(topics);
    let mut loader = None;
    let mut schema_cache = SchemaCache::default();

    if let Some(topic) = &cli.topic {
        let Some(update) = app.select_topic_by_name(topic) else {
            bail!("topic '{topic}' not found");
        };
        RuntimeContext {
            loader: &mut loader,
            reader: &reader,
            input: &cli.input,
            schema_cache: &mut schema_cache,
            parallel: cli.parallel,
        }
        .apply_update(update, &mut app, &mut FrameProcessingResult::default())?;
        let selected = app.selected_topic().context("selected topic missing")?;
        if let Some(reason) = selected.message_list_block_reason() {
            bail!(
                "topic '{}' cannot open messages: {reason}",
                selected.topic()
            );
        }
    }

    let mut terminal = terminal::init_terminal()?;

    if cli.topic.is_some() {
        let selected = app
            .selected_topic()
            .map(|row| (row.topic().to_string(), row.info.message_count))
            .context("selected topic missing")?;
        app.start_loading(selected.0, selected.1);
        start_topic_loader(&mut app, &mut loader, &cli.input, cli.parallel)?;
    }

    let mut needs_redraw = true;
    let mut fast_poll = false;

    loop {
        let loader_result = loader::drain_loader_events(&mut app, &mut loader);
        needs_redraw |= loader_result.state_changed;

        if needs_redraw {
            terminal.draw(|frame| ui::render(frame, &mut app))?;
            needs_redraw = false;
        }

        let input_batch = collect_input_events(if fast_poll || loader_result.hit_limit {
            Duration::ZERO
        } else {
            EVENT_POLL_TIMEOUT
        })?;
        fast_poll = input_batch.hit_limit || loader_result.hit_limit;

        if input_batch.events.is_empty() {
            continue;
        }

        let input_result = process_input_events(
            &mut app,
            &mut loader,
            &reader,
            &cli.input,
            &mut schema_cache,
            cli.parallel,
            input_batch.events,
        )?;

        needs_redraw |= input_result.state_changed;
        if input_result.should_quit {
            break;
        }
    }

    Ok(())
}

fn collect_input_events(timeout: Duration) -> Result<FrameInputBatch> {
    if !event::poll(timeout)? {
        return Ok(FrameInputBatch::default());
    }

    let mut events = Vec::with_capacity(MAX_INPUT_EVENTS_PER_FRAME.min(32));
    loop {
        events.push(event::read()?);

        if events.len() >= MAX_INPUT_EVENTS_PER_FRAME {
            return Ok(FrameInputBatch {
                events,
                hit_limit: event::poll(Duration::ZERO)?,
            });
        }

        if !event::poll(Duration::ZERO)? {
            return Ok(FrameInputBatch {
                events,
                hit_limit: false,
            });
        }
    }
}

fn process_input_events(
    app: &mut App,
    loader: &mut Option<TopicLoader>,
    reader: &McapReader,
    input: &Path,
    schema_cache: &mut SchemaCache,
    parallel: bool,
    events: Vec<Event>,
) -> Result<FrameProcessingResult> {
    let mut result = FrameProcessingResult::default();
    let mut pending_navigation = None;
    let mut runtime = RuntimeContext {
        loader,
        reader,
        input,
        schema_cache,
        parallel,
    };

    for event in events {
        if let Some(navigation) = classify_navigation_event(app, &event) {
            if runtime.queue_navigation_command(
                app,
                &mut pending_navigation,
                navigation,
                &mut result,
            )? {
                result.should_quit = true;
                return Ok(result);
            }
            continue;
        }

        if runtime.flush_navigation_command(app, &mut pending_navigation, &mut result)? {
            result.should_quit = true;
            return Ok(result);
        }

        match event {
            Event::Key(key)
                if should_process_key_event(&key)
                    && runtime.apply_update(app.handle_key(key), app, &mut result)? =>
            {
                result.should_quit = true;
                return Ok(result);
            }
            Event::Resize(_, _) => {
                result.state_changed = true;
            }
            _ => {}
        }
    }

    if runtime.flush_navigation_command(app, &mut pending_navigation, &mut result)? {
        result.should_quit = true;
    }

    Ok(result)
}

fn classify_navigation_event(app: &App, event: &Event) -> Option<app::NavigationCommand> {
    match event {
        Event::Key(key) => app.navigation_for_key(*key),
        Event::Mouse(mouse) => app.navigation_for_mouse(*mouse),
        _ => None,
    }
}

fn should_process_key_event(key: &crossterm::event::KeyEvent) -> bool {
    matches!(
        key.kind,
        crossterm::event::KeyEventKind::Press | crossterm::event::KeyEventKind::Repeat
    )
}

fn can_merge_navigation(current: app::NavigationCommand, next: app::NavigationCommand) -> bool {
    match (current, next) {
        (
            app::NavigationCommand::Relative {
                target: current_target,
                ..
            },
            app::NavigationCommand::Relative {
                target: next_target,
                ..
            },
        ) => current_target == next_target,
        (
            app::NavigationCommand::Page {
                target: current_target,
                ..
            },
            app::NavigationCommand::Page {
                target: next_target,
                ..
            },
        ) => current_target == next_target,
        (
            app::NavigationCommand::Absolute {
                target: current_target,
                ..
            },
            app::NavigationCommand::Absolute {
                target: next_target,
                ..
            },
        ) => current_target == next_target,
        _ => false,
    }
}

fn merge_navigation(
    current: app::NavigationCommand,
    next: app::NavigationCommand,
) -> app::NavigationCommand {
    match (current, next) {
        (
            app::NavigationCommand::Relative { target, delta },
            app::NavigationCommand::Relative {
                delta: next_delta, ..
            },
        ) => app::NavigationCommand::Relative {
            target,
            delta: delta.saturating_add(next_delta),
        },
        (
            app::NavigationCommand::Page { target, delta },
            app::NavigationCommand::Page {
                delta: next_delta, ..
            },
        ) => app::NavigationCommand::Page {
            target,
            delta: delta.saturating_add(next_delta),
        },
        (
            app::NavigationCommand::Absolute { target, .. },
            app::NavigationCommand::Absolute {
                endpoint: next_endpoint,
                ..
            },
        ) => app::NavigationCommand::Absolute {
            target,
            endpoint: next_endpoint,
        },
        _ => unreachable!("merge_navigation called on incompatible commands"),
    }
}

fn start_topic_loader(
    app: &mut App,
    loader: &mut Option<TopicLoader>,
    input: &Path,
    parallel: bool,
) -> Result<()> {
    loader::cancel_loader(loader);

    let topic = app
        .selected_topic()
        .map(|row| row.info.clone())
        .context("no topic selected")?;
    *loader = Some(TopicLoader::spawn(input.to_path_buf(), topic, parallel));
    Ok(())
}

impl RuntimeContext<'_> {
    fn queue_navigation_command(
        &mut self,
        app: &mut App,
        pending_navigation: &mut Option<app::NavigationCommand>,
        next_navigation: app::NavigationCommand,
        result: &mut FrameProcessingResult,
    ) -> Result<bool> {
        match pending_navigation.take() {
            Some(current) if can_merge_navigation(current, next_navigation) => {
                *pending_navigation = Some(merge_navigation(current, next_navigation));
                Ok(false)
            }
            Some(current) => {
                let should_quit = self.apply_navigation_command(app, current, result)?;
                *pending_navigation = Some(next_navigation);
                Ok(should_quit)
            }
            None => {
                *pending_navigation = Some(next_navigation);
                Ok(false)
            }
        }
    }

    fn flush_navigation_command(
        &mut self,
        app: &mut App,
        pending_navigation: &mut Option<app::NavigationCommand>,
        result: &mut FrameProcessingResult,
    ) -> Result<bool> {
        if let Some(navigation) = pending_navigation.take() {
            return self.apply_navigation_command(app, navigation, result);
        }

        Ok(false)
    }

    fn apply_navigation_command(
        &mut self,
        app: &mut App,
        navigation: app::NavigationCommand,
        result: &mut FrameProcessingResult,
    ) -> Result<bool> {
        self.apply_update(app.apply_navigation(navigation), app, result)
    }

    fn apply_update(
        &mut self,
        update: AppUpdate,
        app: &mut App,
        result: &mut FrameProcessingResult,
    ) -> Result<bool> {
        result.state_changed |= update.state_changed;

        match update.request {
            None => Ok(false),
            Some(AppRequest::Quit) => {
                loader::cancel_loader(self.loader);
                Ok(true)
            }
            Some(AppRequest::CancelLoader) => {
                loader::cancel_loader(self.loader);
                Ok(false)
            }
            Some(AppRequest::StartTopicLoad) => {
                if let Err(error) = start_topic_loader(app, self.loader, self.input, self.parallel)
                {
                    app.set_status(error.to_string());
                    result.state_changed = true;
                }
                Ok(false)
            }
            Some(AppRequest::LoadSelectedSchema) => {
                if let Err(error) =
                    schema::open_selected_schema(app, self.reader, self.input, self.schema_cache)
                {
                    app.clear_schema_view();
                    app.set_status(error.to_string());
                }
                result.state_changed = true;
                Ok(false)
            }
        }
    }
}
