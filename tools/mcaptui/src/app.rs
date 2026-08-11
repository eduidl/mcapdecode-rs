use mcapdecode::{
    TopicInfo,
    core::{FieldDefs, Value},
};
use ratatui::layout::Rect;

mod detail;
mod lifecycle;
mod navigation;
mod util;

use util::offset_scroll;

const DEFAULT_PAGE_STEP: usize = 10;
const DEFAULT_SCROLL_PAGE_STEP: u16 = 10;
const DETAIL_HORIZONTAL_STEP: u16 = 4;
const SCHEMA_HORIZONTAL_STEP: u16 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Screen {
    Topics,
    Messages,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageFocus {
    List,
    Detail,
    Schema,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicRow {
    pub info: TopicInfo,
    pub unsupported_reason: Option<String>,
}

impl TopicRow {
    pub fn from_info(info: TopicInfo) -> Self {
        let unsupported_reason =
            (info.channel_count > 1).then(|| "duplicate topic is not supported".to_string());
        Self {
            info,
            unsupported_reason,
        }
    }

    pub fn is_supported(&self) -> bool {
        self.unsupported_reason.is_none()
    }

    pub fn has_messages(&self) -> bool {
        self.info.message_count != Some(0)
    }

    pub fn message_list_block_reason(&self) -> Option<&str> {
        self.unsupported_reason
            .as_deref()
            .or_else(|| (!self.has_messages()).then_some("no messages"))
    }

    pub fn topic(&self) -> &str {
        &self.info.topic
    }
}

#[derive(Debug, Clone)]
pub struct LoadedMessage {
    pub index: usize,
    pub log_time: u64,
    pub publish_time: u64,
    pub log_time_display: String,
    pub publish_time_display: String,
    pub value: Value,
    pub detail_rows: Option<Vec<DetailRow>>,
    pub detail_text: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaView {
    pub topic: String,
    pub title: String,
    pub text: String,
    pub line_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DetailRow {
    pub text: String,
    pub field_path: Option<String>,
}

impl DetailRow {
    pub fn new(text: impl Into<String>, field_path: Option<String>) -> Self {
        Self {
            text: text.into(),
            field_path,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DetailScrollAnchor {
    field_path: Option<String>,
    occurrence: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppRequest {
    Quit,
    CancelLoader,
    StartTopicLoad,
    LoadSelectedSchema,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AppUpdate {
    pub state_changed: bool,
    pub request: Option<AppRequest>,
}

impl AppUpdate {
    pub fn changed() -> Self {
        Self {
            state_changed: true,
            request: None,
        }
    }

    pub fn request(request: AppRequest) -> Self {
        Self {
            state_changed: false,
            request: Some(request),
        }
    }

    pub fn changed_with_request(request: AppRequest) -> Self {
        Self {
            state_changed: true,
            request: Some(request),
        }
    }

    pub fn merge(self, other: Self) -> Self {
        Self {
            state_changed: self.state_changed || other.state_changed,
            request: other.request.or(self.request),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NavigationTarget {
    Topics,
    Schema,
    MessageList,
    MessageDetail,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NavigationEndpoint {
    Start,
    End,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NavigationCommand {
    Relative {
        target: NavigationTarget,
        delta: i32,
    },
    Page {
        target: NavigationTarget,
        delta: i32,
    },
    Absolute {
        target: NavigationTarget,
        endpoint: NavigationEndpoint,
    },
}

#[derive(Debug)]
pub struct App {
    session: SessionState,
    topics: TopicListState,
    messages: MessageState,
    detail: ScrollPane,
    schema: SchemaPaneState,
    layout: LayoutState,
}

#[derive(Debug)]
struct SessionState {
    screen: Screen,
    focus: MessageFocus,
    status: String,
}

#[derive(Debug)]
struct TopicListState {
    rows: Vec<TopicRow>,
    selected: usize,
    page_step: usize,
}

#[derive(Debug, Default)]
struct LoadingState {
    label: Option<String>,
    progress: usize,
    total: Option<u64>,
}

#[derive(Debug)]
struct MessageState {
    items: Vec<LoadedMessage>,
    selected: usize,
    page_step: usize,
    field_defs: Option<FieldDefs>,
    loading: LoadingState,
}

/// Scroll offsets and viewport of one scrollable pane.
///
/// The detail and schema panes scroll identically; only the content they
/// measure against differs, so every method takes the [`ScrollLimits`] derived
/// from that content instead of reading it back out of [`App`].
#[derive(Debug)]
struct ScrollPane {
    scroll: u16,
    hscroll: u16,
    page_step: u16,
    view_height: u16,
    view_width: u16,
    /// Columns moved per horizontal step. The detail pane advances by a whole
    /// indent level, the schema pane one column at a time.
    horizontal_step: u16,
}

/// How far a pane can scroll before running past the end of its content.
#[derive(Debug, Clone, Copy)]
struct ScrollLimits {
    vertical: u16,
    horizontal: u16,
}

impl ScrollLimits {
    /// The limits of a pane with nothing to show, which pins it to the origin.
    const NONE: Self = Self {
        vertical: 0,
        horizontal: 0,
    };
}

impl ScrollPane {
    fn new(horizontal_step: u16) -> Self {
        Self {
            scroll: 0,
            hscroll: 0,
            page_step: DEFAULT_SCROLL_PAGE_STEP,
            view_height: 0,
            view_width: 0,
            horizontal_step,
        }
    }

    fn reset(&mut self) {
        self.scroll = 0;
        self.hscroll = 0;
    }

    fn scroll_by(&mut self, delta: i32, limits: ScrollLimits) {
        self.scroll = offset_scroll(self.scroll, delta, 1).min(limits.vertical);
    }

    fn page_by(&mut self, delta: i32, limits: ScrollLimits) {
        self.scroll = offset_scroll(self.scroll, delta, self.page_step).min(limits.vertical);
    }

    fn scroll_horizontal_by(&mut self, delta: i32, limits: ScrollLimits) {
        self.hscroll =
            offset_scroll(self.hscroll, delta, self.horizontal_step).min(limits.horizontal);
    }

    fn clamp(&mut self, limits: ScrollLimits) {
        self.scroll = self.scroll.min(limits.vertical);
        self.hscroll = self.hscroll.min(limits.horizontal);
    }
}

#[derive(Debug)]
struct SchemaPaneState {
    enabled: bool,
    view: Option<SchemaView>,
    return_focus: MessageFocus,
    pane: ScrollPane,
}

#[derive(Debug)]
struct LayoutState {
    topics_area: Rect,
    schema_area: Rect,
    message_list_area: Rect,
    message_detail_area: Rect,
}
