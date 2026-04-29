use crossterm::event::{KeyCode, KeyModifiers, MouseEvent, MouseEventKind};
use mcapdecode::{TopicInfo, core::Value};
use mcaptui::app::{App, AppRequest, AppUpdate, DetailRow, LoadedMessage, MessageFocus, Screen};
use ratatui::layout::Rect;

fn topic(name: &str, channel_count: usize) -> TopicInfo {
    TopicInfo {
        topic: name.to_string(),
        message_count: Some(3),
        schema_name: Some("test.Msg".to_string()),
        schema_encoding: "jsonschema".to_string(),
        message_encoding: "json".to_string(),
        channel_count,
    }
}

fn message(index: usize, lines: usize) -> LoadedMessage {
    let detail_rows: Vec<_> = (0..lines)
        .map(|line| DetailRow::new(format!("line-{line}"), None))
        .collect();
    let detail_text = detail_rows
        .iter()
        .map(|row| row.text.as_str())
        .collect::<Vec<_>>()
        .join("\n");

    LoadedMessage {
        index,
        log_time: index as u64,
        publish_time: index as u64,
        log_time_display: format!("log-{index}"),
        publish_time_display: format!("pub-{index}"),
        value: Value::Null,
        detail_rows: Some(detail_rows),
        detail_text: Some(detail_text),
    }
}

fn message_with_rows(index: usize, rows: Vec<(&str, Option<&str>)>) -> LoadedMessage {
    let detail_rows: Vec<_> = rows
        .into_iter()
        .map(|(text, field_path)| DetailRow::new(text, field_path.map(ToString::to_string)))
        .collect();
    let detail_text = detail_rows
        .iter()
        .map(|row| row.text.as_str())
        .collect::<Vec<_>>()
        .join("\n");

    LoadedMessage {
        index,
        log_time: index as u64,
        publish_time: index as u64,
        log_time_display: format!("log-{index}"),
        publish_time_display: format!("pub-{index}"),
        value: Value::Null,
        detail_rows: Some(detail_rows),
        detail_text: Some(detail_text),
    }
}

#[test]
fn duplicate_topic_rows_are_unsupported() {
    let app = App::new(vec![topic("/ok", 1), topic("/dup", 2)]);

    assert!(app.topic_rows()[0].is_supported());
    assert_eq!(
        app.topic_rows()[1].unsupported_reason.as_deref(),
        Some("duplicate topic is not supported")
    );
}

#[test]
fn topic_selection_moves_with_page_actions() {
    let mut app = App::new(vec![
        topic("/a", 1),
        topic("/b", 1),
        topic("/c", 1),
        topic("/d", 1),
    ]);
    app.set_topic_page_step(2);

    app.handle_key(KeyCode::PageDown.into());
    assert_eq!(app.topic_selected(), Some(2));

    app.handle_key(KeyCode::End.into());
    assert_eq!(app.topic_selected(), Some(3));

    app.handle_key(KeyCode::Home.into());
    assert_eq!(app.topic_selected(), Some(0));
}

#[test]
fn topic_screen_can_open_schema_action() {
    let mut app = App::new(vec![topic("/a", 1)]);

    let update = app.handle_key(KeyCode::Char('s').into());

    assert_eq!(
        update,
        AppUpdate::changed_with_request(AppRequest::LoadSelectedSchema)
    );
    assert!(app.schema_visible());
    assert!(app.schema_view().is_none());
}

#[test]
fn topic_screen_enter_transitions_to_messages_immediately() {
    let mut app = App::new(vec![topic("/a", 1)]);

    let update = app.handle_key(KeyCode::Enter.into());

    assert_eq!(app.screen(), Screen::Messages);
    assert_eq!(app.focus(), MessageFocus::List);
    assert!(app.is_loading());
    assert_eq!(
        update,
        AppUpdate::changed_with_request(AppRequest::StartTopicLoad)
    );
}

#[test]
fn schema_widget_scrolls_via_mouse() {
    let mut app = App::new(vec![topic("/a", 1)]);
    app.set_schema_view(
        "/a",
        "Schema: /a",
        "line-0\nline-1\nline-2\nline-3\nline-4\nline-5",
    );
    app.set_schema_area(Rect::new(0, 0, 80, 5));
    app.set_topics_area(Rect::new(80, 0, 40, 5));

    app.handle_mouse(MouseEvent {
        kind: MouseEventKind::ScrollDown,
        column: 2,
        row: 2,
        modifiers: KeyModifiers::empty(),
    });

    assert_eq!(app.schema_scroll(), 1);
    assert_eq!(app.topic_selected(), Some(0));
}

#[test]
fn message_screen_can_open_schema_action() {
    let mut app = App::new(vec![topic("/a", 1)]);
    app.start_loading("/a", Some(1));

    let update = app.handle_key(KeyCode::Char('s').into());

    assert_eq!(
        update,
        AppUpdate::changed_with_request(AppRequest::LoadSelectedSchema)
    );
    assert!(app.schema_visible());
    assert!(app.schema_view().is_none());
}

#[test]
fn topics_tab_cycles_focus_into_schema_popup() {
    let mut app = App::new(vec![topic("/a", 1)]);
    app.set_schema_view("/a", "Schema: /a", "line-0\nline-1\nline-2\nline-3");

    app.handle_key(KeyCode::Tab.into());
    assert_eq!(app.focus(), MessageFocus::Schema);

    app.handle_key(KeyCode::Tab.into());
    assert_eq!(app.focus(), MessageFocus::List);
}

#[test]
fn topics_schema_focus_routes_handle_key_to_schema_scroll() {
    let mut app = App::new(vec![topic("/a", 1), topic("/b", 1)]);
    app.set_schema_view("/a", "Schema: /a", "line-0\nline-1\nline-2\nline-3");
    app.set_schema_area(Rect::new(0, 0, 20, 4));

    app.handle_key(KeyCode::Tab.into());
    assert_eq!(app.focus(), MessageFocus::Schema);

    app.handle_key(KeyCode::PageDown.into());

    assert_eq!(app.schema_scroll(), 2);
    assert_eq!(app.topic_selected(), Some(0));
}

#[test]
fn topic_navigation_requests_schema_reload() {
    let mut app = App::new(vec![topic("/a", 1), topic("/b", 1)]);
    app.set_schema_view("/a", "Schema: /a", "field: string");

    let update = app.handle_key(KeyCode::End.into());

    assert_eq!(app.topic_selected(), Some(1));
    assert!(app.schema_visible());
    assert!(app.schema_view().is_none());
    assert_eq!(update.request, Some(AppRequest::LoadSelectedSchema));
}

#[test]
fn message_tab_cycles_focus_into_schema_popup() {
    let mut app = App::new(vec![topic("/a", 1)]);
    app.start_loading("/a", Some(1));
    app.finish_loading("/a");
    app.set_schema_view("/a", "Schema: /a", "line-0\nline-1\nline-2\nline-3");
    app.set_schema_area(Rect::new(0, 0, 20, 4));

    app.handle_key(KeyCode::Tab.into());
    assert_eq!(app.focus(), MessageFocus::Detail);

    app.handle_key(KeyCode::Tab.into());
    assert_eq!(app.focus(), MessageFocus::Schema);

    app.handle_key(KeyCode::PageDown.into());
    assert_eq!(app.schema_scroll(), 2);

    app.handle_key(KeyCode::Tab.into());
    assert_eq!(app.focus(), MessageFocus::List);
}

#[test]
fn clearing_schema_restores_previous_focus() {
    let mut app = App::new(vec![topic("/a", 1)]);
    app.start_loading("/a", Some(1));
    app.finish_loading("/a");
    app.set_schema_view("/a", "Schema: /a", "line-0\nline-1\nline-2\nline-3");

    app.handle_key(KeyCode::Tab.into());
    assert_eq!(app.focus(), MessageFocus::Detail);

    app.handle_key(KeyCode::Tab.into());
    assert_eq!(app.focus(), MessageFocus::Schema);

    app.clear_schema_view();
    assert_eq!(app.focus(), MessageFocus::Detail);
}

#[test]
fn selecting_topic_by_name_requests_schema_reload() {
    let mut app = App::new(vec![topic("/a", 1), topic("/b", 1)]);
    app.set_schema_view("/a", "Schema: /a", "field: string");

    let update = app.select_topic_by_name("/b").expect("topic should exist");

    assert_eq!(app.topic_selected(), Some(1));
    assert!(app.schema_visible());
    assert!(app.schema_view().is_none());
    assert_eq!(app.focus(), MessageFocus::List);
    assert_eq!(update.request, Some(AppRequest::LoadSelectedSchema));
}

#[test]
fn message_focus_toggle_and_detail_scroll_reset() {
    let mut app = App::new(vec![topic("/a", 1)]);
    app.start_loading("/a", Some(2));
    app.append_loaded_messages(vec![message(0, 20), message(1, 5)]);
    app.finish_loading("/a");
    app.set_detail_view_height(4);

    app.handle_key(KeyCode::Tab.into());
    assert_eq!(app.focus(), MessageFocus::Detail);

    app.handle_key(KeyCode::PageDown.into());
    assert_eq!(app.detail_scroll(), 4);

    app.handle_key(KeyCode::Tab.into());
    app.handle_key(KeyCode::Down.into());
    assert_eq!(app.message_selected(), Some(1));
    assert_eq!(app.detail_scroll(), 0);
}

#[test]
fn message_selection_keeps_detail_scroll_at_field_level() {
    let mut app = App::new(vec![topic("/a", 1)]);
    app.start_loading("/a", Some(2));
    app.append_loaded_messages(vec![
        message_with_rows(
            0,
            vec![
                ("@log_time: 0", None),
                ("@publish_time: 0", None),
                ("payload:", None),
                ("  alpha:", Some("alpha")),
                ("    alpha-child: 1", Some("alpha.child")),
                ("  beta: [2 items]", Some("beta")),
                ("    [0]: 10", Some("beta")),
                ("    [1]: 20", Some("beta")),
            ],
        ),
        message_with_rows(
            1,
            vec![
                ("@log_time: 1", None),
                ("@publish_time: 1", None),
                ("payload:", None),
                ("  alpha:", Some("alpha")),
                ("    alpha-child: 1", Some("alpha.child")),
                ("    alpha-child-2: 2", Some("alpha.child_2")),
                ("  beta: [1 items]", Some("beta")),
                ("    [0]: 30", Some("beta")),
            ],
        ),
    ]);
    app.finish_loading("/a");
    app.set_detail_view_height(1);

    app.handle_key(KeyCode::Tab.into());
    for _ in 0..5 {
        app.handle_key(KeyCode::Down.into());
    }
    assert_eq!(app.detail_scroll(), 5);

    app.handle_key(KeyCode::Tab.into());
    app.handle_key(KeyCode::Down.into());

    assert_eq!(app.message_selected(), Some(1));
    assert_eq!(app.detail_scroll(), 6);
}

#[test]
fn append_loaded_messages_keeps_loading_state_incremental() {
    let mut app = App::new(vec![topic("/a", 1)]);
    app.start_loading("/a", Some(3));

    app.append_loaded_messages(vec![message(0, 3)]);
    assert_eq!(app.screen(), Screen::Messages);
    assert_eq!(app.message_selected(), Some(0));
    assert_eq!(app.status(), "Loading /a: 1/3");

    app.append_loaded_messages(vec![message(1, 2), message(2, 1)]);
    app.finish_loading("/a");
    assert_eq!(app.status(), "Loaded 3 messages from /a");
}

#[test]
fn mouse_scroll_uses_detail_pane_without_focus() {
    let mut app = App::new(vec![topic("/a", 1)]);
    app.start_loading("/a", Some(2));
    app.append_loaded_messages(vec![message(0, 20), message(1, 5)]);
    app.finish_loading("/a");
    app.set_detail_view_height(4);
    app.set_message_areas(Rect::new(0, 0, 40, 10), Rect::new(40, 0, 40, 10));

    assert_eq!(app.focus(), MessageFocus::List);
    app.handle_mouse(MouseEvent {
        kind: MouseEventKind::ScrollDown,
        column: 50,
        row: 3,
        modifiers: KeyModifiers::empty(),
    });

    assert_eq!(app.detail_scroll(), 1);
    assert_eq!(app.message_selected(), Some(0));
}

#[test]
fn mouse_scroll_uses_list_pane_without_focus() {
    let mut app = App::new(vec![topic("/a", 1)]);
    app.start_loading("/a", Some(3));
    app.append_loaded_messages(vec![message(0, 20), message(1, 5), message(2, 5)]);
    app.finish_loading("/a");
    app.set_detail_view_height(4);
    app.set_message_areas(Rect::new(0, 0, 40, 10), Rect::new(40, 0, 40, 10));
    app.toggle_focus();

    assert_eq!(app.focus(), MessageFocus::Detail);
    app.handle_mouse(MouseEvent {
        kind: MouseEventKind::ScrollDown,
        column: 10,
        row: 3,
        modifiers: KeyModifiers::empty(),
    });

    assert_eq!(app.message_selected(), Some(1));
    assert_eq!(app.detail_scroll(), 0);
}

#[test]
fn detail_horizontal_scroll_clamps_to_longest_line() {
    let mut app = App::new(vec![topic("/a", 1)]);
    app.start_loading("/a", Some(1));
    app.append_loaded_messages(vec![message_with_rows(
        0,
        vec![("01234567890123456789", Some("payload"))],
    )]);
    app.finish_loading("/a");
    app.set_message_areas(Rect::new(0, 0, 20, 4), Rect::new(20, 0, 10, 3));
    app.set_detail_view_height(1);
    app.handle_key(KeyCode::Tab.into());

    for _ in 0..8 {
        app.handle_key(KeyCode::Right.into());
    }

    assert_eq!(app.detail_hscroll(), 12);
}

#[test]
fn schema_horizontal_scroll_clamps_to_longest_line() {
    let mut app = App::new(vec![topic("/a", 1)]);
    app.set_schema_view("/a", "Schema: /a", "01234567890123456789");
    app.set_schema_area(Rect::new(0, 0, 10, 3));
    app.set_topics_area(Rect::new(10, 0, 40, 5));

    for _ in 0..8 {
        app.handle_key(KeyCode::Right.into());
    }

    assert_eq!(app.schema_hscroll(), 8);
}
