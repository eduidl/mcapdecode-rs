use super::*;
use crate::format;

impl App {
    pub fn selected_message_detail_text(&mut self) -> Option<&str> {
        self.ensure_selected_message_materialized()?;
        self.messages
            .items
            .get(self.messages.selected)?
            .detail_text
            .as_deref()
    }

    pub(super) fn current_detail_scroll_anchor(&mut self) -> Option<DetailScrollAnchor> {
        self.ensure_selected_message_materialized()?;
        let message = self.messages.items.get(self.messages.selected)?;
        let detail_rows = message.detail_rows.as_ref()?;
        if detail_rows.is_empty() {
            return None;
        }
        let row_index = usize::min(
            self.detail.scroll as usize,
            detail_rows.len().saturating_sub(1),
        );
        let field_path = detail_rows.get(row_index)?.field_path.clone()?;
        let occurrence = detail_rows[..=row_index]
            .iter()
            .filter(|row| row.field_path.as_deref() == Some(field_path.as_str()))
            .count()
            .saturating_sub(1);
        Some(DetailScrollAnchor {
            field_path: Some(field_path),
            occurrence,
        })
    }

    pub(super) fn restore_detail_scroll(&mut self, anchor: Option<&DetailScrollAnchor>) {
        let Some(message) = self.materialize_selected_message() else {
            self.detail.scroll = 0;
            self.clamp_detail_scroll();
            return;
        };
        let detail_rows = message.detail_rows.clone();

        let next_scroll = match (detail_rows.as_ref(), anchor) {
            (Some(detail_rows), Some(anchor)) => self
                .find_detail_row_by_anchor(detail_rows, anchor)
                .unwrap_or(0),
            (Some(_), None) => 0,
            (None, _) => 0,
        };
        self.detail.scroll = next_scroll as u16;
        self.clamp_detail_scroll();
    }

    fn ensure_selected_message_materialized(&mut self) -> Option<()> {
        self.materialize_selected_message().map(|_| ())
    }

    fn materialize_selected_message(&mut self) -> Option<&LoadedMessage> {
        let selected_index = self.messages.selected;
        let message = self.messages.items.get_mut(selected_index)?;
        if message.detail_rows.is_none() {
            let field_defs = self.messages.field_defs.as_ref()?;
            let detail_rows = format::format_detail_rows(
                message.log_time,
                message.publish_time,
                &message.value,
                field_defs,
            );
            let detail_text = detail_rows
                .iter()
                .map(|row| row.text.as_str())
                .collect::<Vec<_>>()
                .join("\n");
            message.detail_rows = Some(detail_rows);
            message.detail_text = Some(detail_text);
        }
        self.messages.items.get(selected_index)
    }

    fn find_detail_row_by_anchor(
        &self,
        detail_rows: &[DetailRow],
        anchor: &DetailScrollAnchor,
    ) -> Option<usize> {
        detail_rows
            .iter()
            .enumerate()
            .filter(|(_, row)| row.field_path.as_ref() == anchor.field_path.as_ref())
            .nth(anchor.occurrence)
            .map(|(index, _)| index)
            .or_else(|| {
                detail_rows
                    .iter()
                    .position(|row| row.field_path.as_ref() == anchor.field_path.as_ref())
            })
    }
}
