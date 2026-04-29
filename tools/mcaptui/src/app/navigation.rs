use crossterm::event::{KeyCode, KeyEvent, KeyEventKind, MouseEvent, MouseEventKind};

use super::{
    util::{
        clamp_i32_to_i16, contains_point, max_horizontal_scroll, max_vertical_scroll, move_index,
    },
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

        match self.session.screen {
            Screen::Topics => {
                let target = if self.session.focus == MessageFocus::Schema && self.schema_visible()
                {
                    NavigationTarget::Schema
                } else {
                    NavigationTarget::Topics
                };

                match key.code {
                    KeyCode::Up | KeyCode::Char('k') => {
                        Some(NavigationCommand::Relative { target, delta: -1 })
                    }
                    KeyCode::Down | KeyCode::Char('j') => {
                        Some(NavigationCommand::Relative { target, delta: 1 })
                    }
                    KeyCode::PageUp => Some(NavigationCommand::Page { target, delta: -1 }),
                    KeyCode::PageDown => Some(NavigationCommand::Page { target, delta: 1 }),
                    KeyCode::Home => Some(NavigationCommand::Absolute {
                        target,
                        endpoint: NavigationEndpoint::Start,
                    }),
                    KeyCode::End => Some(NavigationCommand::Absolute {
                        target,
                        endpoint: NavigationEndpoint::End,
                    }),
                    _ => None,
                }
            }
            Screen::Messages => {
                let target = match self.session.focus {
                    MessageFocus::List => NavigationTarget::MessageList,
                    MessageFocus::Detail => NavigationTarget::MessageDetail,
                    MessageFocus::Schema if self.schema_visible() => NavigationTarget::Schema,
                    MessageFocus::Schema => NavigationTarget::MessageList,
                };

                match key.code {
                    KeyCode::Up | KeyCode::Char('k') => {
                        Some(NavigationCommand::Relative { target, delta: -1 })
                    }
                    KeyCode::Down | KeyCode::Char('j') => {
                        Some(NavigationCommand::Relative { target, delta: 1 })
                    }
                    KeyCode::PageUp => Some(NavigationCommand::Page { target, delta: -1 }),
                    KeyCode::PageDown => Some(NavigationCommand::Page { target, delta: 1 }),
                    KeyCode::Home => Some(NavigationCommand::Absolute {
                        target,
                        endpoint: NavigationEndpoint::Start,
                    }),
                    KeyCode::End => Some(NavigationCommand::Absolute {
                        target,
                        endpoint: NavigationEndpoint::End,
                    }),
                    _ => None,
                }
            }
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
                NavigationTarget::Schema => {
                    self.scroll_schema(clamp_i32_to_i16(delta));
                    AppUpdate::changed()
                }
                NavigationTarget::MessageList => self.move_message_selection(delta as isize),
                NavigationTarget::MessageDetail => {
                    self.scroll_detail(clamp_i32_to_i16(delta));
                    AppUpdate::changed()
                }
            },
            NavigationCommand::Page { target, delta } => match target {
                NavigationTarget::Topics => self.page_topic_selection_by(delta),
                NavigationTarget::Schema => {
                    self.page_schema_by(delta);
                    AppUpdate::changed()
                }
                NavigationTarget::MessageList => self.page_message_selection_by(delta),
                NavigationTarget::MessageDetail => {
                    self.page_detail_by(delta);
                    AppUpdate::changed()
                }
            },
            NavigationCommand::Absolute { target, endpoint } => match (target, endpoint) {
                (NavigationTarget::Topics, NavigationEndpoint::Start) => {
                    self.set_topic_selection(0)
                }
                (NavigationTarget::Topics, NavigationEndpoint::End) => {
                    if !self.topics.rows.is_empty() {
                        self.set_topic_selection(self.topics.rows.len() - 1)
                    } else {
                        AppUpdate::default()
                    }
                }
                (NavigationTarget::Schema, NavigationEndpoint::Start) => {
                    self.schema.scroll = 0;
                    AppUpdate::changed()
                }
                (NavigationTarget::Schema, NavigationEndpoint::End) => {
                    self.schema.scroll = self.max_schema_scroll();
                    AppUpdate::changed()
                }
                (NavigationTarget::MessageList, NavigationEndpoint::Start) => {
                    self.set_message_selection(0)
                }
                (NavigationTarget::MessageList, NavigationEndpoint::End) => {
                    if !self.messages.items.is_empty() {
                        self.set_message_selection(self.messages.items.len() - 1)
                    } else {
                        AppUpdate::default()
                    }
                }
                (NavigationTarget::MessageDetail, NavigationEndpoint::Start) => {
                    self.detail.scroll = 0;
                    AppUpdate::changed()
                }
                (NavigationTarget::MessageDetail, NavigationEndpoint::End) => {
                    self.detail.scroll = self.max_detail_scroll();
                    AppUpdate::changed()
                }
            },
        };

        self.clamp_schema_scroll();
        self.clamp_detail_scroll();
        update
    }

    fn handle_topics_key(&mut self, code: KeyCode) -> AppUpdate {
        match code {
            KeyCode::Tab => {
                self.toggle_focus();
                return AppUpdate::changed();
            }
            KeyCode::Up | KeyCode::Char('k') => return self.move_topic_selection(-1),
            KeyCode::Down | KeyCode::Char('j') => return self.move_topic_selection(1),
            KeyCode::PageUp => return self.page_topic_selection(false),
            KeyCode::PageDown => return self.page_topic_selection(true),
            KeyCode::Home => return self.set_topic_selection(0),
            KeyCode::End if !self.topics.rows.is_empty() => {
                return self.set_topic_selection(self.topics.rows.len() - 1);
            }
            KeyCode::Char('s') => return self.toggle_schema(),
            KeyCode::Left | KeyCode::Char('h') if self.schema_visible() => {
                self.scroll_schema_horizontal(-1);
                return AppUpdate::changed();
            }
            KeyCode::Right | KeyCode::Char('l') if self.schema_visible() => {
                self.scroll_schema_horizontal(1);
                return AppUpdate::changed();
            }
            KeyCode::Enter => return self.open_selected_topic(),
            _ => {}
        }

        AppUpdate::default()
    }

    fn handle_messages_key(&mut self, code: KeyCode) -> AppUpdate {
        match code {
            KeyCode::Esc => {
                self.back_to_topics();
                return AppUpdate::changed_with_request(AppRequest::CancelLoader);
            }
            KeyCode::Char('s') => return self.toggle_schema(),
            KeyCode::Tab => {
                self.toggle_focus();
                return AppUpdate::changed();
            }
            KeyCode::Up | KeyCode::Char('k') => match self.session.focus {
                MessageFocus::List => return self.move_message_selection(-1),
                MessageFocus::Detail => {
                    self.scroll_detail(-1);
                    return AppUpdate::changed();
                }
                MessageFocus::Schema => {
                    self.scroll_schema(-1);
                    return AppUpdate::changed();
                }
            },
            KeyCode::Down | KeyCode::Char('j') => match self.session.focus {
                MessageFocus::List => return self.move_message_selection(1),
                MessageFocus::Detail => {
                    self.scroll_detail(1);
                    return AppUpdate::changed();
                }
                MessageFocus::Schema => {
                    self.scroll_schema(1);
                    return AppUpdate::changed();
                }
            },
            KeyCode::Left | KeyCode::Char('h') => {
                if self.session.focus == MessageFocus::Detail {
                    self.scroll_detail_horizontal(-1);
                    return AppUpdate::changed();
                } else if self.session.focus == MessageFocus::Schema && self.schema_visible() {
                    self.scroll_schema_horizontal(-1);
                    return AppUpdate::changed();
                }
            }
            KeyCode::Right | KeyCode::Char('l') => {
                if self.session.focus == MessageFocus::Detail {
                    self.scroll_detail_horizontal(1);
                    return AppUpdate::changed();
                } else if self.session.focus == MessageFocus::Schema && self.schema_visible() {
                    self.scroll_schema_horizontal(1);
                    return AppUpdate::changed();
                }
            }
            KeyCode::PageUp => match self.session.focus {
                MessageFocus::List => return self.page_message_selection(false),
                MessageFocus::Detail => {
                    self.page_detail(false);
                    return AppUpdate::changed();
                }
                MessageFocus::Schema => {
                    self.page_schema_by(-1);
                    return AppUpdate::changed();
                }
            },
            KeyCode::PageDown => match self.session.focus {
                MessageFocus::List => return self.page_message_selection(true),
                MessageFocus::Detail => {
                    self.page_detail(true);
                    return AppUpdate::changed();
                }
                MessageFocus::Schema => {
                    self.page_schema_by(1);
                    return AppUpdate::changed();
                }
            },
            KeyCode::Home => match self.session.focus {
                MessageFocus::List => return self.set_message_selection(0),
                MessageFocus::Detail => {
                    self.detail.scroll = 0;
                    return AppUpdate::changed();
                }
                MessageFocus::Schema => {
                    self.schema.scroll = 0;
                    return AppUpdate::changed();
                }
            },
            KeyCode::End => match self.session.focus {
                MessageFocus::List => {
                    if !self.messages.items.is_empty() {
                        return self.set_message_selection(self.messages.items.len() - 1);
                    }
                }
                MessageFocus::Detail => {
                    self.detail.scroll = self.max_detail_scroll();
                    return AppUpdate::changed();
                }
                MessageFocus::Schema => {
                    self.schema.scroll = self.max_schema_scroll();
                    return AppUpdate::changed();
                }
            },
            _ => {}
        }

        self.clamp_detail_scroll();
        self.clamp_detail_hscroll();
        self.clamp_schema_scroll();
        self.clamp_schema_hscroll();
        AppUpdate::default()
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

    fn page_topic_selection(&mut self, forward: bool) -> AppUpdate {
        let step = self.topics.page_step as isize;
        self.move_topic_selection(if forward { step } else { -step })
    }

    fn page_topic_selection_by(&mut self, delta: i32) -> AppUpdate {
        let step = self.topics.page_step as isize;
        self.move_topic_selection(step.saturating_mul(delta as isize))
    }

    fn scroll_schema(&mut self, delta: i16) {
        let next = if delta.is_negative() {
            self.schema.scroll.saturating_sub(delta.unsigned_abs())
        } else {
            self.schema.scroll.saturating_add(delta as u16)
        };
        self.schema.scroll = next.min(self.max_schema_scroll());
    }

    fn scroll_schema_horizontal(&mut self, delta: i16) {
        let next = if delta.is_negative() {
            self.schema.hscroll.saturating_sub(delta.unsigned_abs())
        } else {
            self.schema.hscroll.saturating_add(delta as u16)
        };
        self.schema.hscroll = next.min(self.max_schema_hscroll());
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

    fn page_message_selection(&mut self, forward: bool) -> AppUpdate {
        let step = self.messages.page_step as isize;
        self.move_message_selection(if forward { step } else { -step })
    }

    fn page_message_selection_by(&mut self, delta: i32) -> AppUpdate {
        let step = self.messages.page_step as isize;
        self.move_message_selection(step.saturating_mul(delta as isize))
    }

    fn scroll_detail(&mut self, delta: i16) {
        let next = if delta.is_negative() {
            self.detail.scroll.saturating_sub(delta.unsigned_abs())
        } else {
            self.detail.scroll.saturating_add(delta as u16)
        };
        self.detail.scroll = next.min(self.max_detail_scroll());
    }

    fn scroll_detail_horizontal(&mut self, delta: i16) {
        let step = DEFAULT_HORIZONTAL_STEP.max(1);
        let offset = step.saturating_mul(delta.unsigned_abs());
        self.detail.hscroll = if delta.is_negative() {
            self.detail.hscroll.saturating_sub(offset)
        } else {
            self.detail.hscroll.saturating_add(offset)
        };
        self.clamp_detail_hscroll();
    }

    fn page_detail(&mut self, forward: bool) {
        let step = self.detail.page_step.max(1);
        self.detail.scroll = if forward {
            self.detail.scroll.saturating_add(step)
        } else {
            self.detail.scroll.saturating_sub(step)
        };
        self.clamp_detail_scroll();
    }

    fn page_detail_by(&mut self, delta: i32) {
        let step = self.detail.page_step.max(1);
        let offset = step.saturating_mul(delta.unsigned_abs() as u16);
        self.detail.scroll = if delta.is_negative() {
            self.detail.scroll.saturating_sub(offset)
        } else {
            self.detail.scroll.saturating_add(offset)
        };
        self.clamp_detail_scroll();
    }

    fn max_detail_scroll(&self) -> u16 {
        let Some(message) = self.messages.items.get(self.messages.selected) else {
            return 0;
        };
        let Some(detail_rows) = message.detail_rows.as_ref() else {
            return 0;
        };
        max_vertical_scroll(detail_rows.len(), self.detail.view_height)
    }

    pub(super) fn clamp_detail_scroll(&mut self) {
        self.detail.scroll = self.detail.scroll.min(self.max_detail_scroll());
    }

    fn max_detail_hscroll(&self) -> u16 {
        let Some(message) = self.messages.items.get(self.messages.selected) else {
            return 0;
        };
        let Some(detail_rows) = message.detail_rows.as_ref() else {
            return 0;
        };
        max_horizontal_scroll(
            detail_rows.iter().map(|row| row.text.as_str()),
            self.detail.view_width,
        )
    }

    pub(super) fn clamp_detail_hscroll(&mut self) {
        self.detail.hscroll = self.detail.hscroll.min(self.max_detail_hscroll());
    }

    fn max_schema_scroll(&self) -> u16 {
        let Some(schema) = self.schema_view() else {
            return 0;
        };
        max_vertical_scroll(schema.line_count, self.schema.view_height)
    }

    pub(super) fn clamp_schema_scroll(&mut self) {
        self.schema.scroll = self.schema.scroll.min(self.max_schema_scroll());
    }

    fn max_schema_hscroll(&self) -> u16 {
        let Some(schema) = self.schema_view() else {
            return 0;
        };
        max_horizontal_scroll(schema.text.lines(), self.schema.view_width)
    }

    pub(super) fn clamp_schema_hscroll(&mut self) {
        self.schema.hscroll = self.schema.hscroll.min(self.max_schema_hscroll());
    }

    fn page_schema_by(&mut self, delta: i32) {
        let step = self.schema.page_step.max(1);
        let offset = step.saturating_mul(delta.unsigned_abs() as u16);
        self.schema.scroll = if delta.is_negative() {
            self.schema.scroll.saturating_sub(offset)
        } else {
            self.schema.scroll.saturating_add(offset)
        };
        self.clamp_schema_scroll();
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
        self.clamp_detail_hscroll();
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
