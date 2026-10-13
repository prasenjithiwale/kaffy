//! State of the Schema Registry screens, and schema diffs.

use std::collections::HashMap;

use kaffy_kafka::{SchemaType, SubjectSchema};
use ratatui::widgets::TableState;
use similar::{ChangeTag, TextDiff};

#[derive(Debug, Clone, Default)]
pub struct SchemasView {
    /// The latest version of each subject.
    pub list: Option<Vec<SubjectSchema>>,
    pub table: TableState,
    pub filter: String,
}

impl SchemasView {
    pub fn visible(&self) -> Vec<&SubjectSchema> {
        let needle = self.filter.to_lowercase();
        self.list
            .iter()
            .flatten()
            .filter(|s| s.subject.to_lowercase().contains(&needle))
            .collect()
    }

    pub fn selected(&self) -> Option<&SubjectSchema> {
        self.visible()
            .get(self.table.selected().unwrap_or(0))
            .copied()
    }

    pub(crate) fn clamp_selection(&mut self) {
        let len = self.visible().len();
        let selected = self.table.selected().unwrap_or(0);
        self.table.select(Some(selected.min(len.saturating_sub(1))));
    }
}

#[derive(Debug, Clone)]
pub struct SubjectView {
    pub subject: String,
    pub versions: Option<Vec<i32>>,
    pub compatibility: Option<String>,
    /// Fetched versions, by version number.
    pub schemas: HashMap<i32, SubjectSchema>,
    /// Selected row in `versions`.
    pub table: TableState,
    /// Show the change from the previous version instead of the schema.
    pub diff: bool,
    pub scroll: u16,
}

impl SubjectView {
    pub(crate) fn new(subject: String) -> Self {
        Self {
            subject,
            versions: None,
            compatibility: None,
            schemas: HashMap::new(),
            table: TableState::default(),
            diff: false,
            scroll: 0,
        }
    }

    pub fn selected_version(&self) -> Option<i32> {
        let versions = self.versions.as_ref()?;
        versions
            .get(
                self.table
                    .selected()
                    .unwrap_or(versions.len().saturating_sub(1)),
            )
            .copied()
    }

    pub fn previous_version(&self) -> Option<i32> {
        let versions = self.versions.as_ref()?;
        let i = versions
            .iter()
            .position(|v| Some(*v) == self.selected_version())?;
        i.checked_sub(1).map(|p| versions[p])
    }

    pub fn latest(&self) -> Option<&SubjectSchema> {
        self.versions
            .as_ref()?
            .last()
            .and_then(|v| self.schemas.get(v))
    }
}

/// One line of a diff: `-` removed, `+` added, ` ` unchanged.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiffLine {
    pub sign: char,
    pub text: String,
}

/// Line diff of two schemas, both pretty-printed first so formatting
/// differences don't show up as changes.
pub fn diff(schema_type: SchemaType, old: &str, new: &str) -> Vec<DiffLine> {
    let old = kaffy_schema::pretty_schema(schema_type, old);
    let new = kaffy_schema::pretty_schema(schema_type, new);
    TextDiff::from_lines(&old, &new)
        .iter_all_changes()
        .map(|change| DiffLine {
            sign: match change.tag() {
                ChangeTag::Delete => '-',
                ChangeTag::Insert => '+',
                ChangeTag::Equal => ' ',
            },
            text: change.value().trim_end_matches('\n').to_owned(),
        })
        .collect()
}

/// A starting point for a new subject in `$EDITOR`.
pub fn skeleton(schema_type: SchemaType, subject: &str) -> String {
    let name: String = subject
        .trim_end_matches("-value")
        .trim_end_matches("-key")
        .split(|c: char| !c.is_ascii_alphanumeric())
        .filter(|p| !p.is_empty())
        .map(|p| p[..1].to_uppercase() + &p[1..])
        .collect();
    let name = if name.is_empty() {
        "Record".to_owned()
    } else {
        name
    };
    match schema_type {
        SchemaType::Avro => format!(
            "{{\n  \"type\": \"record\",\n  \"name\": \"{name}\",\n  \"fields\": [\n    {{ \"name\": \"id\", \"type\": \"string\" }}\n  ]\n}}\n"
        ),
        SchemaType::Json => format!(
            "{{\n  \"$schema\": \"http://json-schema.org/draft-07/schema#\",\n  \"title\": \"{name}\",\n  \"type\": \"object\",\n  \"properties\": {{\n    \"id\": {{ \"type\": \"string\" }}\n  }},\n  \"required\": [\"id\"]\n}}\n"
        ),
        SchemaType::Protobuf => {
            format!("syntax = \"proto3\";\n\nmessage {name} {{\n  string id = 1;\n}}\n")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn diff_ignores_formatting_and_marks_changes() {
        let old = r#"{"type":"record","name":"A","fields":[{"name":"id","type":"string"}]}"#;
        let new = r#"{ "type": "record", "name": "A",
            "fields": [ {"name":"id","type":"string"}, {"name":"n","type":"int","default":0} ] }"#;
        let lines = diff(SchemaType::Avro, old, new);
        let added: Vec<_> = lines
            .iter()
            .filter(|l| l.sign == '+')
            .map(|l| l.text.trim())
            .collect();
        let removed: Vec<_> = lines
            .iter()
            .filter(|l| l.sign == '-')
            .map(|l| l.text.trim())
            .collect();
        assert!(added.contains(&"\"name\": \"n\","), "{lines:?}");
        // At most a brace moves; no field of the old schema is removed.
        assert!(
            removed.iter().all(|l| *l == "}" || *l == "},"),
            "{removed:?}"
        );
        assert!(
            diff(SchemaType::Avro, old, old)
                .iter()
                .all(|l| l.sign == ' ')
        );
    }

    #[test]
    fn skeletons_are_valid() {
        let avro = skeleton(SchemaType::Avro, "user-events-value");
        assert!(avro.contains("\"name\": \"UserEvents\""));
        kaffy_schema::AvroSchema::parse_str(&avro).unwrap();
        serde_json::from_str::<serde_json::Value>(&skeleton(SchemaType::Json, "x")).unwrap();
        assert!(skeleton(SchemaType::Protobuf, "orders-value").contains("message Orders {"));
    }
}
