pub fn strip_comments(line: &str, in_block_comment: &mut bool) -> String {
    let mut out = String::with_capacity(line.len());
    let mut in_str = false;
    let mut escaped = false;
    let mut chars = line.char_indices().peekable();

    while let Some((_, ch)) = chars.next() {
        if *in_block_comment {
            if ch == '*' && chars.peek().is_some_and(|(_, next)| *next == '/') {
                *in_block_comment = false;
                chars.next();
            }
            continue;
        }

        if in_str {
            out.push(ch);
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                in_str = false;
            }
            continue;
        }

        if ch == '"' {
            in_str = true;
            out.push(ch);
            continue;
        }

        if ch == '/'
            && let Some((_, next)) = chars.peek()
        {
            if *next == '/' {
                break;
            }
            if *next == '*' {
                *in_block_comment = true;
                chars.next();
                continue;
            }
        }

        out.push(ch);
    }

    out
}

pub fn is_separator_line(line: &str) -> bool {
    let t = line.trim();
    !t.is_empty() && t.chars().all(|c| c == '=')
}

pub fn split_qual(name: &str, sep: &str) -> Vec<String> {
    name.split(sep)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(ToString::to_string)
        .collect()
}
