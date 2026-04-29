use ratatui::layout::Rect;
use unicode_width::UnicodeWidthStr;

pub(super) fn move_index(index: &mut usize, len: usize, delta: isize) {
    if len == 0 {
        *index = 0;
        return;
    }

    let next = (*index as isize + delta).clamp(0, (len - 1) as isize);
    *index = next as usize;
}

pub(super) fn contains_point(area: Rect, x: u16, y: u16) -> bool {
    x >= area.x
        && x < area.x.saturating_add(area.width)
        && y >= area.y
        && y < area.y.saturating_add(area.height)
}

pub(super) fn clamp_i32_to_i16(delta: i32) -> i16 {
    delta.clamp(i16::MIN as i32, i16::MAX as i32) as i16
}

pub(super) fn max_vertical_scroll(total_lines: usize, height: u16) -> u16 {
    let visible = height as usize;
    total_lines.saturating_sub(visible).min(u16::MAX as usize) as u16
}

pub(super) fn max_horizontal_scroll<'a>(lines: impl Iterator<Item = &'a str>, width: u16) -> u16 {
    if width == 0 {
        return 0;
    }
    let max_width = lines.map(UnicodeWidthStr::width).max().unwrap_or(0);
    max_width
        .saturating_sub(width as usize)
        .min(u16::MAX as usize) as u16
}
