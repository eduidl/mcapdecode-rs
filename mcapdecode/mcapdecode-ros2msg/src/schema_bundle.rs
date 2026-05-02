use mcapdecode_ros2_common::Ros2Error;

/// One `.msg` section extracted from a schema bundle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MsgSection {
    pub msg_path: Vec<String>,
    pub body: String,
}

impl MsgSection {
    pub fn path(&self) -> String {
        self.msg_path.join("/")
    }

    pub fn schema_name(&self) -> String {
        self.path()
    }
}

/// A parsed collection of bundled `.msg` sections from one schema blob.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaBundle {
    pub sections: Vec<MsgSection>,
}

impl SchemaBundle {
    /// Parse a schema blob that may include `====`-separated `MSG:` sections.
    ///
    /// The root section may omit a `MSG:` header; in that case `schema_name`
    /// provides the top-level type path.
    pub fn parse(schema_name: &str, schema_text: &str) -> Result<Self, Ros2Error> {
        let root_path = parse_schema_name(schema_name)?;
        let mut sections = Vec::new();
        let mut buf = Vec::new();
        let mut first_block = true;

        for line in schema_text.lines() {
            if is_separator_line(line) {
                if has_meaningful_lines(&buf) {
                    sections.push(parse_section(
                        &buf,
                        first_block.then_some(root_path.clone()),
                    )?);
                    first_block = false;
                }
                buf.clear();
                continue;
            }
            buf.push(line.to_string());
        }

        if has_meaningful_lines(&buf) {
            sections.push(parse_section(&buf, first_block.then_some(root_path))?);
        }

        if sections.is_empty() {
            return Err(format!("no MSG sections found for schema '{schema_name}'").into());
        }

        Ok(Self { sections })
    }

    /// Return the qualified name of the section matching `schema_name`.
    pub fn main_type(&self, schema_name: &str) -> Option<Vec<String>> {
        let schema_key = parse_schema_name(schema_name).ok()?;
        self.sections
            .iter()
            .find(|section| section.msg_path == schema_key)
            .map(|section| section.msg_path.clone())
            .or_else(|| {
                self.sections
                    .first()
                    .map(|section| section.msg_path.clone())
            })
    }
}

fn parse_section(
    lines: &[String],
    fallback_path: Option<Vec<String>>,
) -> Result<MsgSection, Ros2Error> {
    let first_idx = lines
        .iter()
        .position(|line| !line.trim().is_empty())
        .ok_or_else(|| Ros2Error("empty MSG section".to_string()))?;
    let header = lines[first_idx].trim();

    let (msg_path, body_start) = if let Some(path) = header.strip_prefix("MSG:") {
        (parse_schema_name(path.trim())?, first_idx + 1)
    } else if let Some(path) = fallback_path {
        (path, first_idx)
    } else {
        return Err(format!("missing `MSG:` header: {header}").into());
    };

    Ok(MsgSection {
        msg_path,
        body: lines[body_start..].join("\n"),
    })
}

fn has_meaningful_lines(lines: &[String]) -> bool {
    lines.iter().any(|line| !line.trim().is_empty())
}

fn is_separator_line(line: &str) -> bool {
    let trimmed = line.trim();
    trimmed.len() >= 3 && trimmed.chars().all(|c| c == '=')
}

fn parse_schema_name(name: &str) -> Result<Vec<String>, Ros2Error> {
    let parts: Vec<&str> = name.split('/').collect();

    match parts.len() {
        3 => Ok(parts.into_iter().map(str::to_string).collect()),
        2 => Ok(vec![
            parts[0].to_string(),
            "msg".to_string(),
            parts[1].to_string(),
        ]),
        _ => Err(format!("invalid schema name format: {name}").into()),
    }
}
