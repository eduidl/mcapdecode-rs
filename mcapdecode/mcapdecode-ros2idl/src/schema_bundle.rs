//! Multi-section IDL schema bundle parsing.
//!
//! An MCAP ROS 2 IDL schema blob may contain multiple IDL files concatenated
//! with `====` separator lines.  Each section starts with an `IDL: <path>`
//! header line.  For example:
//!
//! ```text
//! ================================================================================
//! IDL: geometry_msgs/msg/Point
//! module geometry_msgs { module msg { struct Point { ... }; }; };
//! ================================================================================
//! IDL: std_msgs/msg/Header
//! module std_msgs { module msg { struct Header { ... }; }; };
//! ```
//!
//! [`SchemaBundle::parse`] splits such text into [`IdlSection`]s.
//! [`SchemaBundle::main_type`] then identifies which section corresponds to
//! the top-level message type named by `schema_name`.

use mcapdecode_ros2_common::Ros2Error;

/// One IDL section extracted from a schema bundle.
#[derive(Debug, Clone)]
pub struct IdlSection {
    /// Path components from the `IDL: pkg/msg/Type` header line.
    pub idl_path: Vec<String>,
    /// The raw IDL body text (everything after the header until the next separator).
    pub body: String,
}

/// A parsed collection of [`IdlSection`]s from a single schema blob.
#[derive(Debug, Clone)]
pub struct SchemaBundle {
    pub sections: Vec<IdlSection>,
}

impl SchemaBundle {
    /// Parse a schema blob that may contain one or more `====`-separated IDL sections.
    ///
    /// Returns an error if no sections are found or if any section is malformed.
    pub fn parse(schema_name: &str, schema_text: &str) -> Result<Self, Ros2Error> {
        let mut sections = Vec::new();
        let mut buf: Vec<String> = Vec::new();

        for line in schema_text.lines() {
            if is_separator_line(line) {
                if has_meaningful_lines(&buf) {
                    let sec = parse_section(&buf)?;
                    sections.push(sec);
                }
                buf.clear();
                continue;
            }
            buf.push(line.to_string());
        }
        if has_meaningful_lines(&buf) {
            let sec = parse_section(&buf)?;
            sections.push(sec);
        }

        if sections.is_empty() {
            return Err(format!("no IDL sections found for schema '{schema_name}'").into());
        }

        Ok(Self { sections })
    }

    /// Return the qualified name of the section that matches `schema_name`.
    ///
    /// Looks for a section whose `idl_path` equals the `/`-split components of
    /// `schema_name`.  Falls back to the first section if no exact match is found,
    /// which handles the common case of a single-section bundle.
    pub fn main_type(&self, schema_name: &str) -> Option<Vec<String>> {
        let schema_key = split_qual(schema_name, "/");
        if !schema_key.is_empty() {
            for s in &self.sections {
                if s.idl_path == schema_key {
                    return Some(s.idl_path.clone());
                }
            }
        }
        self.sections.first().map(|s| s.idl_path.clone())
    }
}

fn has_meaningful_lines(lines: &[String]) -> bool {
    lines.iter().any(|l| !l.trim().is_empty())
}

/// Parse one accumulated block of lines into an [`IdlSection`].
///
/// The first non-empty line must be an `IDL: <path>` header.
fn parse_section(lines: &[String]) -> Result<IdlSection, Ros2Error> {
    let mut it = lines.iter().map(|s| s.trim()).filter(|s| !s.is_empty());
    let header = it
        .next()
        .ok_or_else(|| Ros2Error("empty IDL section".to_string()))?;
    let path = header
        .strip_prefix("IDL:")
        .ok_or_else(|| Ros2Error(format!("missing `IDL:` header: {header}")))?
        .trim();
    if path.is_empty() {
        return Err("empty IDL path in section header".into());
    }

    let body = it.collect::<Vec<_>>().join("\n");
    Ok(IdlSection {
        idl_path: split_qual(path, "/"),
        body,
    })
}

fn is_separator_line(line: &str) -> bool {
    let trimmed = line.trim();
    !trimmed.is_empty() && trimmed.chars().all(|character| character == '=')
}

fn split_qual(name: &str, separator: &str) -> Vec<String> {
    name.split(separator)
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .map(ToString::to_string)
        .collect()
}
