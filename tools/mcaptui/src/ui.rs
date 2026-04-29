use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Cell, Clear, Paragraph, Row, Table, TableState},
};

use crate::app::{App, MessageFocus, Screen};

pub fn render(frame: &mut Frame<'_>, app: &mut App) {
    let area = frame.area();

    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Min(1),
            Constraint::Length(1),
            Constraint::Length(1),
        ])
        .split(area);

    match app.screen() {
        Screen::Topics => render_topics(frame, layout[0], app),
        Screen::Messages => render_messages(frame, layout[0], app),
    }

    render_status(frame, layout[1], app.status());
    render_help(frame, layout[2], app.screen(), app.focus());
}

fn render_topics(frame: &mut Frame<'_>, area: Rect, app: &mut App) {
    let table_area = area;
    let schema_area = app.schema_visible().then(|| schema_popup_area(area));

    app.set_topics_area(table_area);
    app.set_topic_page_step(visible_rows(table_area.height));
    app.set_schema_area(schema_area.unwrap_or_default());

    let header = Row::new(vec!["topic", "count", "schema", "message enc"])
        .style(Style::default().add_modifier(Modifier::BOLD));
    let rows = app.topic_rows().iter().map(|row| {
        let count = row
            .info
            .message_count
            .map(|count| count.to_string())
            .unwrap_or_else(|| "-".to_string());
        let schema = row
            .info
            .schema_name
            .clone()
            .unwrap_or_else(|| "-".to_string());
        let style = if !row.is_supported() {
            Style::default().fg(Color::Yellow)
        } else if !row.has_messages() {
            Style::default().add_modifier(Modifier::DIM)
        } else {
            Style::default()
        };

        Row::new(vec![
            Cell::from(row.info.topic.clone()),
            Cell::from(count),
            Cell::from(schema),
            Cell::from(row.info.message_encoding.clone()),
        ])
        .style(style)
    });

    let table_title = if app.focus() == MessageFocus::List {
        "Topics [focus]"
    } else {
        "Topics"
    };

    let table = Table::new(
        rows,
        [
            Constraint::Percentage(40),
            Constraint::Length(10),
            Constraint::Percentage(35),
            Constraint::Percentage(15),
        ],
    )
    .header(header)
    .block(Block::default().title(table_title).borders(Borders::ALL))
    .row_highlight_style(Style::default().bg(Color::Blue))
    .highlight_symbol(">> ");

    let mut state = TableState::default();
    state.select(app.topic_selected());
    frame.render_stateful_widget(table, table_area, &mut state);

    if let Some(schema_area) = schema_area {
        render_schema_widget(frame, schema_area, app);
    }
}

fn render_messages(frame: &mut Frame<'_>, area: Rect, app: &mut App) {
    let split = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Percentage(35), Constraint::Percentage(65)])
        .split(area);
    let (list_area, detail_area) = (split[0], split[1]);
    let schema_area = app.schema_visible().then(|| schema_popup_area(area));

    app.set_message_areas(list_area, detail_area);
    app.set_message_page_step(visible_rows(list_area.height));
    app.set_detail_view_height(detail_area.height.saturating_sub(2));
    app.set_schema_area(schema_area.unwrap_or_default());

    let list_title = if app.focus() == MessageFocus::List {
        "Messages [focus]"
    } else {
        "Messages"
    };
    let list_title = if let Some(loading_text) = app.loading_text() {
        format!("{list_title} [{loading_text}]")
    } else {
        list_title.to_string()
    };
    let detail_title = if app.focus() == MessageFocus::Detail {
        "Detail [focus]"
    } else {
        "Detail"
    };

    let header = Row::new(vec!["idx", "log_time", "publish_time"])
        .style(Style::default().add_modifier(Modifier::BOLD));
    let rows = app.messages().iter().map(|message| {
        Row::new(vec![
            Cell::from(message.index.to_string()),
            Cell::from(message.log_time_display.clone()),
            Cell::from(message.publish_time_display.clone()),
        ])
    });

    let table = Table::new(
        rows,
        [
            Constraint::Length(6),
            Constraint::Length(23),
            Constraint::Length(23),
        ],
    )
    .header(header)
    .block(Block::default().title(list_title).borders(Borders::ALL))
    .row_highlight_style(Style::default().bg(Color::Blue))
    .highlight_symbol(">> ");

    let mut state = TableState::default();
    state.select(app.message_selected());
    frame.render_stateful_widget(table, list_area, &mut state);

    let detail_text = app
        .selected_message_detail_text()
        .map(str::to_owned)
        .unwrap_or_else(|| {
            if app.is_loading() {
                "Loading messages...".to_string()
            } else {
                "No messages for the selected topic".to_string()
            }
        });
    let paragraph = Paragraph::new(detail_text)
        .block(Block::default().title(detail_title).borders(Borders::ALL))
        .scroll((app.detail_scroll(), app.detail_hscroll()));
    frame.render_widget(paragraph, detail_area);

    if let Some(schema_area) = schema_area {
        render_schema_widget(frame, schema_area, app);
    }
}

fn render_schema_widget(frame: &mut Frame<'_>, area: Rect, app: &mut App) {
    let (title, text) = app
        .schema_view()
        .map(|schema| (schema.title.as_str(), schema.text.as_str()))
        .unwrap_or(("Schema", "No schema loaded"));
    let title = if app.focus() == MessageFocus::Schema {
        format!("{title} [focus]")
    } else {
        title.to_string()
    };

    frame.render_widget(Clear, area);
    let paragraph = Paragraph::new(text)
        .block(Block::default().title(title).borders(Borders::ALL))
        .scroll((app.schema_scroll(), app.schema_hscroll()));
    frame.render_widget(paragraph, area);
}

fn schema_popup_area(area: Rect) -> Rect {
    let width_percent = match area.width {
        0..=79 => 94,
        80..=119 => 88,
        120..=159 => 72,
        _ => 60,
    };
    let height_percent = match area.height {
        0..=15 => 90,
        16..=29 => 80,
        _ => 70,
    };
    centered_rect(width_percent, height_percent, area)
}

fn centered_rect(width_percent: u16, height_percent: u16, area: Rect) -> Rect {
    let vertical = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - height_percent) / 2),
            Constraint::Percentage(height_percent),
            Constraint::Percentage(100 - height_percent - ((100 - height_percent) / 2)),
        ])
        .split(area);
    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - width_percent) / 2),
            Constraint::Percentage(width_percent),
            Constraint::Percentage(100 - width_percent - ((100 - width_percent) / 2)),
        ])
        .split(vertical[1])[1]
}

fn render_status(frame: &mut Frame<'_>, area: Rect, status: &str) {
    let paragraph = Paragraph::new(status.to_string()).style(Style::default().fg(Color::Cyan));
    frame.render_widget(paragraph, area);
}

fn render_help(frame: &mut Frame<'_>, area: Rect, screen: Screen, focus: MessageFocus) {
    let text = match screen {
        Screen::Topics => Line::from(vec![
            Span::raw(format!(
                "Focus: {}  ",
                if focus == MessageFocus::Schema {
                    "schema"
                } else {
                    "topics"
                }
            )),
            Span::raw("Tab switch  "),
            Span::raw("Up/Down j/k move  "),
            Span::raw("Left/Right h/l schema x-scroll  "),
            Span::raw("PageUp/PageDown page  "),
            Span::raw("Home/End jump  "),
            Span::raw("s toggle schema  "),
            Span::raw("Enter open  "),
            Span::raw("q quit"),
        ]),
        Screen::Messages => {
            let focus_text = match focus {
                MessageFocus::List => "list",
                MessageFocus::Detail => "detail",
                MessageFocus::Schema => "schema",
            };
            Line::from(vec![
                Span::raw(format!("Focus: {focus_text}  ")),
                Span::raw("s toggle schema  "),
                Span::raw("Tab switch  "),
                Span::raw("Up/Down move/scroll  "),
                Span::raw("Left/Right h/l x-scroll  "),
                Span::raw("PageUp/PageDown page  "),
                Span::raw("Home/End jump  "),
                Span::raw("Esc back  "),
                Span::raw("q quit"),
            ])
        }
    };
    frame.render_widget(Paragraph::new(text), area);
}

fn visible_rows(area_height: u16) -> usize {
    area_height.saturating_sub(3).max(1) as usize
}
