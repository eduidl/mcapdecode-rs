use crate::Ros2Error;

/// Normalize a ROS 2 package resource name for schema-bundle matching.
///
/// Canonical resource names have three components, such as `pkg/msg/Type`.
/// Two-component message names (`pkg/Type`) remain accepted for compatibility
/// with ROS `.msg` type references and older schema bundles.
pub fn normalize_schema_name(name: &str) -> Result<String, Ros2Error> {
    let parts: Vec<&str> = name.split('/').collect();

    match parts.as_slice() {
        [package, resource] if !package.is_empty() && !resource.is_empty() => {
            Ok(format!("{package}/msg/{resource}"))
        }
        [package, interface, resource]
            if !package.is_empty() && !interface.is_empty() && !resource.is_empty() =>
        {
            Ok(name.to_string())
        }
        _ => Err(format!("invalid schema name format: {name}").into()),
    }
}

/// A schema section shared by ROS 2 IDL and MSG bundles.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaSection {
    /// Qualified package/resource path of the section.
    pub path: String,
    /// Schema text following the format-specific section header.
    pub body: String,
}

/// A parsed collection of ROS 2 schema sections.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaBundle {
    pub sections: Vec<SchemaSection>,
}

impl SchemaBundle {
    /// Return the section matching `schema_path`, or the first section.
    pub fn main_section(&self, schema_path: &str) -> Option<&SchemaSection> {
        self.sections
            .iter()
            .find(|section| section.path == schema_path)
            .or_else(|| self.sections.first())
    }
}

/// Split a ROS 2 schema bundle into its non-empty sections.
///
/// The ROS 2 message-definition encoding specifies an 80-character `=`
/// delimiter.  Readers also accept delimiters of three or more `=` characters
/// for compatibility with existing schema bundles.
pub fn split_schema_sections(schema_text: &str) -> Vec<Vec<&str>> {
    let mut sections = Vec::new();
    let mut current = Vec::new();

    for line in schema_text.lines() {
        if is_separator_line(line) {
            push_section(&mut sections, &mut current);
        } else {
            current.push(line);
        }
    }
    push_section(&mut sections, &mut current);

    sections
}

fn push_section<'a>(sections: &mut Vec<Vec<&'a str>>, current: &mut Vec<&'a str>) {
    if current.iter().any(|line| !line.trim().is_empty()) {
        sections.push(std::mem::take(current));
    } else {
        current.clear();
    }
}

fn is_separator_line(line: &str) -> bool {
    let trimmed = line.trim();
    trimmed.len() >= 3 && trimmed.chars().all(|character| character == '=')
}
