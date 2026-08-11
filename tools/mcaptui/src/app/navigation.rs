use crossterm::event::{KeyCode, KeyEvent, KeyEventKind, MouseEvent, MouseEventKind};

use super::{
    util::{contains_point, max_horizontal_scroll, max_vertical_scroll, move_index},
    *,
};

impl App {
    pub fn handle_key(&mut self, key: KeyEvent) -> AppUpdate {
        if !matches!(key.kind, KeyEventKind::Press | KeyEventKind::Repeat) {
            return AppUpdate::default();
        }

        if key.code == KeyCode::Char('q') {
            return AppUpdate::request(AppRequest::Quit);
        }

        if let Some(navigation) = self.navigation_for_key(key) {
            return self.apply_navigation(navigation);
        }

        match self.session.screen {
            Screen::Topics => self.handle_topics_key(key.code),
            Screen::Messages => self.handle_messages_key(key.code),
        }
    }

    pub fn handle_mouse(&mut self, mouse: MouseEvent) -> AppUpdate {
        self.navigation_for_mouse(mouse)
            .map(|navigation| self.apply_navigation(navigation))
            .unwrap_or_default()
    }

    pub(crate) fn navigation_for_key(&self, key: KeyEvent) -> Option<NavigationCommand> {
        if !matches!(key.kind, KeyEventKind::Press | KeyEventKind::Repeat) {
            return None;
        }

        navigation_for_code(self.focused_target(), key.code)
    }

    /// The pane keyboard navigation acts on for the current screen and focus.
    fn focused_target(&self) -> NavigationTarget {
        match self.session.screen {
            Screen::Topics
                if self.session.focus == MessageFocus::Schema && self.schema_visible() =>
            {
                NavigationTarget::Schema
            }
            Screen::Topics => NavigationTarget::Topics,
            Screen::Messages => match self.session.focus {
                MessageFocus::List => NavigationTarget::MessageList,
                MessageFocus::Detail => NavigationTarget::MessageDetail,
                MessageFocus::Schema if self.schema_visible() => NavigationTarget::Schema,
                MessageFocus::Schema => NavigationTarget::MessageList,
            },
        }
    }

    pub(crate) fn navigation_for_mouse(&self, mouse: MouseEvent) -> Option<NavigationCommand> {
        let delta = match mouse.kind {
            MouseEventKind::ScrollUp => -1,
            MouseEventKind::ScrollDown => 1,
            _ => return None,
        };

        let target = match self.session.screen {
            Screen::Topics => {
                if contains_point(self.layout.schema_area, mouse.column, mouse.row) {
                    Some(NavigationTarget::Schema)
                } else if contains_point(self.layout.topics_area, mouse.column, mouse.row) {
                    Some(NavigationTarget::Topics)
                } else {
                    None
                }
            }
            Screen::Messages => {
                if contains_point(self.layout.schema_area, mouse.column, mouse.row) {
                    Some(NavigationTarget::Schema)
                } else if contains_point(self.layout.message_detail_area, mouse.column, mouse.row) {
                    Some(NavigationTarget::MessageDetail)
                } else if contains_point(self.layout.message_list_area, mouse.column, mouse.row) {
                    Some(NavigationTarget::MessageList)
                } else {
                    None
                }
            }
        }?;

        Some(NavigationCommand::Relative { target, delta })
    }

    pub(crate) fn apply_navigation(&mut self, command: NavigationCommand) -> AppUpdate {
        let update = match command {
            NavigationCommand::Relative { target, delta } => match target {
                NavigationTarget::Topics => self.move_topic_selection(delta as isize),
                NavigationTarget::MessageList => self.move_message_selection(delta as isize),
                NavigationTarget::Schema => {
                    let (pane, limits) = self.schema_pane();
                    pane.scroll_by(delta, limits);
                    AppUpdate::changed()
                }
                NavigationTarget::MessageDetail => {
                    let (pane, limits) = self.detail_pane();
                    pane.scroll_by(delta, limits);
                    AppUpdate::changed()
                }
            },
            NavigationCommand::Page { target, delta } => match target {
                NavigationTarget::Topics => self.page_topic_selection_by(delta),
                NavigationTarget::MessageList => self.page_message_selection_by(delta),
                NavigationTarget::Schema => {
                    let (pane, limits) = self.schema_pane();
                    pane.page_by(delta, limits);
                    AppUpdate::changed()
                }
                NavigationTarget::MessageDetail => {
                    let (pane, limits) = self.detail_pane();
                    pane.page_by(delta, limits);
                    AppUpdate::changed()
                }
            },
            NavigationCommand::Absolute { target, endpoint } => match target {
                NavigationTarget::Topics => match endpoint.index(self.topics.rows.len()) {
                    Some(index) => self.set_topic_selection(index),
                    None => AppUpdate::default(),
                },
                NavigationTarget::MessageList => match endpoint.index(self.messages.items.len()) {
                    Some(index) => self.set_message_selection(index),
                    None => AppUpdate::default(),
                },
                NavigationTarget::Schema => {
                    let limits = self.schema_limits();
                    self.schema.pane.scroll = endpoint.scroll(limits);
                    AppUpdate::changed()
                }
                NavigationTarget::MessageDetail => {
                    let limits = self.detail_limits();
                    self.detail.scroll = endpoint.scroll(limits);
                    AppUpdate::changed()
                }
            },
        };

        self.clamp_schema();
        self.clamp_detail();
        update
    }

    /// Keys that are not navigation: everything moving a selection or scrolling a
    /// pane is routed through [`App::navigation_for_key`] before reaching here.
    fn handle_topics_key(&mut self, code: KeyCode) -> AppUpdate {
        match code {
            KeyCode::Tab => {
                self.toggle_focus();
                AppUpdate::changed()
            }
            KeyCode::Char('s') => self.toggle_schema(),
            KeyCode::Left | KeyCode::Char('h') if self.schema_visible() => {
                self.scroll_schema_horizontal(-1)
            }
            KeyCode::Right | KeyCode::Char('l') if self.schema_visible() => {
                self.scroll_schema_horizontal(1)
            }
            KeyCode::Enter => self.open_selected_topic(),
            _ => AppUpdate::default(),
        }
    }

    /// See [`App::handle_topics_key`] for what does *not* reach this function.
    fn handle_messages_key(&mut self, code: KeyCode) -> AppUpdate {
        match code {
            KeyCode::Esc => {
                self.back_to_topics();
                AppUpdate::changed_with_request(AppRequest::CancelLoader)
            }
            KeyCode::Char('s') => self.toggle_schema(),
            KeyCode::Tab => {
                self.toggle_focus();
                AppUpdate::changed()
            }
            KeyCode::Left | KeyCode::Char('h') => self.scroll_focused_horizontal(-1),
            KeyCode::Right | KeyCode::Char('l') => self.scroll_focused_horizontal(1),
            _ => AppUpdate::default(),
        }
    }

    fn scroll_schema_horizontal(&mut self, delta: i32) -> AppUpdate {
        let (pane, limits) = self.schema_pane();
        pane.scroll_horizontal_by(delta, limits);
        AppUpdate::changed()
    }

    fn scroll_focused_horizontal(&mut self, delta: i32) -> AppUpdate {
        match self.session.focus {
            MessageFocus::Detail => {
                let (pane, limits) = self.detail_pane();
                pane.scroll_horizontal_by(delta, limits);
                AppUpdate::changed()
            }
            MessageFocus::Schema if self.schema_visible() => self.scroll_schema_horizontal(delta),
            MessageFocus::List | MessageFocus::Schema => AppUpdate::default(),
        }
    }

    pub(super) fn cycle_topics_focus(&mut self) {
        self.session.focus = if self.schema_visible() && self.session.focus != MessageFocus::Schema
        {
            self.schema.return_focus = self.session.focus;
            MessageFocus::Schema
        } else {
            MessageFocus::List
        };
    }

    pub(super) fn cycle_messages_focus(&mut self) {
        self.session.focus = match (self.session.focus, self.schema_visible()) {
            (MessageFocus::List, _) => MessageFocus::Detail,
            (MessageFocus::Detail, true) => {
                self.schema.return_focus = MessageFocus::Detail;
                MessageFocus::Schema
            }
            (MessageFocus::Detail, false) => MessageFocus::List,
            (MessageFocus::Schema, _) => MessageFocus::List,
        };
    }

    fn move_topic_selection(&mut self, delta: isize) -> AppUpdate {
        let mut next = self.topics.selected;
        move_index(&mut next, self.topics.rows.len(), delta);
        self.set_topic_selection(next)
    }

    fn page_topic_selection_by(&mut self, delta: i32) -> AppUpdate {
        let step = self.topics.page_step as isize;
        self.move_topic_selection(step.saturating_mul(delta as isize))
    }

    fn move_message_selection(&mut self, delta: isize) -> AppUpdate {
        if self.messages.items.is_empty() {
            self.messages.selected = 0;
            self.detail.scroll = 0;
            return AppUpdate::changed();
        }

        let mut next = self.messages.selected;
        move_index(&mut next, self.messages.items.len(), delta);
        self.set_message_selection(next)
    }

    fn page_message_selection_by(&mut self, delta: i32) -> AppUpdate {
        let step = self.messages.page_step as isize;
        self.move_message_selection(step.saturating_mul(delta as isize))
    }

    /// The detail pane together with the limits of the selected message's rows.
    fn detail_pane(&mut self) -> (&mut ScrollPane, ScrollLimits) {
        let limits = self.detail_limits();
        (&mut self.detail, limits)
    }

    /// The schema pane together with the limits of the loaded schema text.
    fn schema_pane(&mut self) -> (&mut ScrollPane, ScrollLimits) {
        let limits = self.schema_limits();
        (&mut self.schema.pane, limits)
    }

    fn detail_limits(&self) -> ScrollLimits {
        let Some(detail_rows) = self
            .messages
            .items
            .get(self.messages.selected)
            .and_then(|message| message.detail_rows.as_ref())
        else {
            return ScrollLimits::NONE;
        };
        ScrollLimits {
            vertical: max_vertical_scroll(detail_rows.len(), self.detail.view_height),
            horizontal: max_horizontal_scroll(
                detail_rows.iter().map(|row| row.text.as_str()),
                self.detail.view_width,
            ),
        }
    }

    fn schema_limits(&self) -> ScrollLimits {
        let Some(schema) = self.schema_view() else {
            return ScrollLimits::NONE;
        };
        ScrollLimits {
            vertical: max_vertical_scroll(schema.line_count, self.schema.pane.view_height),
            horizontal: max_horizontal_scroll(schema.text.lines(), self.schema.pane.view_width),
        }
    }

    pub(super) fn clamp_detail(&mut self) {
        let (pane, limits) = self.detail_pane();
        pane.clamp(limits);
    }

    pub(super) fn clamp_schema(&mut self) {
        let (pane, limits) = self.schema_pane();
        pane.clamp(limits);
    }

    fn set_message_selection(&mut self, next: usize) -> AppUpdate {
        if self.messages.items.is_empty() {
            self.messages.selected = 0;
            self.detail.scroll = 0;
            return AppUpdate::changed();
        }

        let next = next.min(self.messages.items.len() - 1);
        if next == self.messages.selected {
            return AppUpdate::default();
        }

        let anchor = self.current_detail_scroll_anchor();
        self.messages.selected = next;
        self.restore_detail_scroll(anchor.as_ref());
        self.clamp_detail();
        AppUpdate::changed()
    }

    pub(super) fn set_topic_selection(&mut self, next: usize) -> AppUpdate {
        if self.topics.rows.is_empty() {
            self.topics.selected = 0;
            return AppUpdate::default();
        }

        let next = next.min(self.topics.rows.len() - 1);
        if next == self.topics.selected {
            return AppUpdate::default();
        }

        self.topics.selected = next;
        self.topic_selection_changed()
    }

    fn topic_selection_changed(&mut self) -> AppUpdate {
        if !self.schema_visible() {
            return AppUpdate::changed();
        }

        if let Some((topic, reason)) = self.selected_topic().and_then(|row| {
            row.unsupported_reason
                .as_deref()
                .map(|reason| (row.topic().to_string(), reason.to_string()))
        }) {
            self.clear_schema_view();
            self.set_status(format!("Cannot show schema for '{topic}': {reason}"));
            return AppUpdate::changed();
        }

        self.begin_schema_view();
        AppUpdate::changed_with_request(AppRequest::LoadSelectedSchema)
    }

    fn open_selected_topic(&mut self) -> AppUpdate {
        if let Some(row) = self.selected_topic() {
            if let Some(reason) = row.message_list_block_reason() {
                self.set_status(format!("Cannot open '{}': {reason}", row.topic()));
                return AppUpdate::changed();
            }

            self.start_loading(row.topic().to_string(), row.info.message_count);
            return AppUpdate::changed_with_request(AppRequest::StartTopicLoad);
        }

        AppUpdate::default()
    }

    fn toggle_schema(&mut self) -> AppUpdate {
        if let Some(row) = self.selected_topic()
            && let Some(reason) = &row.unsupported_reason
        {
            self.set_status(format!(
                "Cannot show schema for '{}': {reason}",
                row.topic()
            ));
            return AppUpdate::changed();
        }

        if self.schema_visible() {
            self.clear_schema_view();
            self.set_status("Schema hidden");
            return AppUpdate::changed();
        }

        self.begin_schema_view();
        AppUpdate::changed_with_request(AppRequest::LoadSelectedSchema)
    }
}

/// The one mapping from keys to navigation commands, shared by every screen and
/// pane: only the target a key applies to varies, never the key itself.
fn navigation_for_code(target: NavigationTarget, code: KeyCode) -> Option<NavigationCommand> {
    let command = match code {
        KeyCode::Up | KeyCode::Char('k') => NavigationCommand::Relative { target, delta: -1 },
        KeyCode::Down | KeyCode::Char('j') => NavigationCommand::Relative { target, delta: 1 },
        KeyCode::PageUp => NavigationCommand::Page { target, delta: -1 },
        KeyCode::PageDown => NavigationCommand::Page { target, delta: 1 },
        KeyCode::Home => NavigationCommand::Absolute {
            target,
            endpoint: NavigationEndpoint::Start,
        },
        KeyCode::End => NavigationCommand::Absolute {
            target,
            endpoint: NavigationEndpoint::End,
        },
        _ => return None,
    };
    Some(command)
}

impl NavigationEndpoint {
    /// The index this endpoint selects in a list of `len` items, or `None` when
    /// the list is too short to have one.
    fn index(self, len: usize) -> Option<usize> {
        match self {
            // An empty list still selects index 0, which the selection setters
            // treat as "nothing to select" rather than an out-of-range index.
            Self::Start => Some(0),
            Self::End => len.checked_sub(1),
        }
    }

    /// The scroll offset this endpoint puts a pane at.
    fn scroll(self, limits: ScrollLimits) -> u16 {
        match self {
            Self::Start => 0,
            Self::End => limits.vertical,
        }
    }
}
