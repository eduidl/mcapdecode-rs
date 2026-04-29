use super::*;

impl App {
    pub fn new(topics: Vec<TopicInfo>) -> Self {
        let topic_rows = topics
            .into_iter()
            .map(TopicRow::from_info)
            .collect::<Vec<_>>();
        let mut app = Self {
            session: SessionState {
                screen: Screen::Topics,
                focus: MessageFocus::List,
                status: String::new(),
            },
            topics: TopicListState {
                rows: topic_rows,
                selected: 0,
                page_step: DEFAULT_PAGE_STEP,
            },
            messages: MessageState {
                items: Vec::new(),
                selected: 0,
                page_step: DEFAULT_PAGE_STEP,
                field_defs: None,
                loading: LoadingState::default(),
            },
            detail: DetailPaneState {
                scroll: 0,
                hscroll: 0,
                page_step: DEFAULT_DETAIL_STEP,
                view_height: 0,
                view_width: 0,
            },
            schema: SchemaPaneState {
                enabled: false,
                view: None,
                return_focus: MessageFocus::List,
                scroll: 0,
                hscroll: 0,
                page_step: DEFAULT_SCHEMA_STEP,
                view_height: 0,
                view_width: 0,
            },
            layout: LayoutState {
                topics_area: Rect::default(),
                schema_area: Rect::default(),
                message_list_area: Rect::default(),
                message_detail_area: Rect::default(),
            },
        };
        if app.topics.rows.is_empty() {
            app.session.status = "No topics found in MCAP summary".to_string();
        }
        app
    }

    pub fn screen(&self) -> Screen {
        self.session.screen
    }

    pub fn focus(&self) -> MessageFocus {
        self.session.focus
    }

    pub fn status(&self) -> &str {
        &self.session.status
    }

    pub fn set_status(&mut self, status: impl Into<String>) {
        self.session.status = status.into();
    }

    pub fn is_loading(&self) -> bool {
        self.messages.loading.label.is_some()
    }

    pub fn loading_text(&self) -> Option<String> {
        self.messages
            .loading
            .label
            .as_ref()
            .map(|label| match self.messages.loading.total {
                Some(total) => {
                    format!(
                        "Loading {label} ({}/{total})",
                        self.messages.loading.progress
                    )
                }
                None => format!("Loading {label} ({})", self.messages.loading.progress),
            })
    }

    pub fn topic_rows(&self) -> &[TopicRow] {
        &self.topics.rows
    }

    pub fn messages(&self) -> &[LoadedMessage] {
        &self.messages.items
    }

    pub fn topic_selected(&self) -> Option<usize> {
        (!self.topics.rows.is_empty()).then_some(self.topics.selected)
    }

    pub fn message_selected(&self) -> Option<usize> {
        (!self.messages.items.is_empty()).then_some(self.messages.selected)
    }

    pub fn selected_topic(&self) -> Option<&TopicRow> {
        self.topics.rows.get(self.topics.selected)
    }

    pub fn selected_message(&self) -> Option<&LoadedMessage> {
        self.messages.items.get(self.messages.selected)
    }

    pub fn schema_view(&self) -> Option<&SchemaView> {
        self.schema.view.as_ref()
    }

    pub fn schema_visible(&self) -> bool {
        self.schema.enabled
    }

    pub fn schema_scroll(&self) -> u16 {
        self.schema.scroll
    }

    pub fn schema_hscroll(&self) -> u16 {
        self.schema.hscroll
    }

    pub fn detail_scroll(&self) -> u16 {
        self.detail.scroll
    }

    pub fn detail_hscroll(&self) -> u16 {
        self.detail.hscroll
    }

    pub fn select_topic_by_name(&mut self, topic: &str) -> Option<AppUpdate> {
        self.topics
            .rows
            .iter()
            .position(|row| row.topic() == topic)
            .map(|index| self.set_topic_selection(index))
    }

    pub fn start_loading(&mut self, label: impl Into<String>, total: Option<u64>) {
        self.session.screen = Screen::Messages;
        self.session.focus = MessageFocus::List;
        self.messages.items.clear();
        self.messages.selected = 0;
        self.detail.scroll = 0;
        self.detail.hscroll = 0;
        self.messages.loading.label = Some(label.into());
        self.messages.loading.total = total;
        self.messages.loading.progress = 0;
        self.messages.field_defs = None;
        self.session.status = "Loading messages...".to_string();
    }

    pub fn update_loading(&mut self, progress: usize) {
        self.messages.loading.progress = progress;
        if let Some(topic) = &self.messages.loading.label {
            self.session.status = match self.messages.loading.total {
                Some(total) => format!("Loading {topic}: {progress}/{total}"),
                None => format!("Loading {topic}: {progress}"),
            };
        }
    }

    pub fn append_loaded_messages(&mut self, messages: Vec<LoadedMessage>) {
        let was_empty = self.messages.items.is_empty();
        self.messages.items.extend(messages);
        if was_empty && !self.messages.items.is_empty() {
            self.messages.selected = 0;
            self.detail.scroll = 0;
        }
        self.update_loading(self.messages.items.len());
    }

    pub fn set_message_field_defs(&mut self, field_defs: FieldDefs) {
        self.messages.field_defs = Some(field_defs);
    }

    pub fn finish_loading(&mut self, topic: &str) {
        let loaded_count = self.messages.items.len();
        self.messages.loading.label = None;
        self.messages.loading.total = None;
        self.messages.loading.progress = loaded_count;
        self.session.status = format!("Loaded {loaded_count} messages from {topic}");
    }

    pub fn fail_loading(&mut self, error: impl Into<String>) {
        self.messages.loading.label = None;
        self.messages.loading.total = None;
        self.session.status = error.into();
    }

    pub fn set_schema_view(
        &mut self,
        topic: impl Into<String>,
        title: impl Into<String>,
        text: impl Into<String>,
    ) {
        let text = text.into();
        let line_count = text.lines().count();
        if self.session.focus != MessageFocus::Schema {
            self.schema.return_focus = self.session.focus;
        }
        self.schema.enabled = true;
        self.schema.view = Some(SchemaView {
            topic: topic.into(),
            title: title.into(),
            text,
            line_count,
        });
        self.schema.scroll = 0;
        self.schema.hscroll = 0;
        self.clamp_schema_scroll();
        self.clamp_schema_hscroll();
    }

    pub fn clear_schema_view(&mut self) {
        self.schema.enabled = false;
        self.schema.view = None;
        self.schema.scroll = 0;
        self.schema.hscroll = 0;
        if self.session.focus == MessageFocus::Schema {
            self.session.focus = self.schema.return_focus;
        }
    }

    pub(crate) fn begin_schema_view(&mut self) {
        if self.session.focus != MessageFocus::Schema {
            self.schema.return_focus = self.session.focus;
        }
        self.schema.enabled = true;
        self.schema.view = None;
        self.schema.scroll = 0;
        self.schema.hscroll = 0;
    }

    pub fn back_to_topics(&mut self) {
        self.session.screen = Screen::Topics;
        self.session.focus = MessageFocus::List;
        self.detail.scroll = 0;
        self.detail.hscroll = 0;
        self.messages.loading.label = None;
        self.messages.loading.total = None;
        self.messages.field_defs = None;
    }

    pub fn toggle_focus(&mut self) {
        match self.session.screen {
            Screen::Topics => self.cycle_topics_focus(),
            Screen::Messages => self.cycle_messages_focus(),
        }
    }

    pub fn set_topic_page_step(&mut self, page_step: usize) {
        self.topics.page_step = page_step.max(1);
    }

    pub fn set_message_page_step(&mut self, page_step: usize) {
        self.messages.page_step = page_step.max(1);
    }

    pub fn set_detail_view_height(&mut self, height: u16) {
        self.detail.view_height = height;
        self.detail.page_step = height.max(1);
        self.clamp_detail_scroll();
    }

    pub fn set_topics_area(&mut self, area: Rect) {
        self.layout.topics_area = area;
    }

    pub fn set_schema_area(&mut self, area: Rect) {
        self.layout.schema_area = area;
        self.schema.view_height = area.height.saturating_sub(2);
        self.schema.view_width = area.width.saturating_sub(2);
        self.schema.page_step = self.schema.view_height.max(1);
        self.clamp_schema_scroll();
        self.clamp_schema_hscroll();
    }

    pub fn set_message_areas(&mut self, list_area: Rect, detail_area: Rect) {
        self.layout.message_list_area = list_area;
        self.layout.message_detail_area = detail_area;
        self.detail.view_width = detail_area.width.saturating_sub(2);
        self.clamp_detail_scroll();
        self.clamp_detail_hscroll();
    }
}
