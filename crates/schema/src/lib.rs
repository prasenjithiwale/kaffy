//! Payload decoding for display.
//!
//! Plain payloads are shown as JSON, UTF-8 text or a hex dump. Payloads in the
//! Confluent wire format (magic byte, schema id, body) are decoded through the
//! Schema Registry by [`Registry`]; the result travels with the record as a
//! [`Decoded`] and takes precedence when rendering.

mod registry;

use std::fmt::Write as _;

pub use apache_avro::Schema as AvroSchema;
pub use registry::{Registry, decode_avro, encode_avro};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    Json,
    Text,
    /// Confluent wire format: `0x00`, 4-byte big-endian schema id, encoded body.
    SchemaRegistry {
        schema_id: u32,
    },
    Binary,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaType {
    Avro,
    Json,
    Protobuf,
}

impl SchemaType {
    fn label(self) -> &'static str {
        match self {
            Self::Avro => "avro",
            Self::Json => "json schema",
            Self::Protobuf => "protobuf",
        }
    }
}

/// A Schema Registry payload after decoding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Decoded {
    pub schema_id: u32,
    /// `None` when the schema could not be fetched.
    pub schema_type: Option<SchemaType>,
    pub result: Result<serde_json::Value, String>,
}

/// A payload rendered for the detail view.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rendered {
    /// Short description such as `json`, `text` or `avro · schema 7`.
    pub label: String,
    /// `text` is pretty-printed JSON and can be syntax highlighted.
    pub json: bool,
    pub text: String,
}

/// Splits a Confluent wire-format payload into schema id and body.
pub fn wire_header(bytes: &[u8]) -> Option<(u32, &[u8])> {
    match bytes {
        [0, a, b, c, d, body @ ..] => Some((u32::from_be_bytes([*a, *b, *c, *d]), body)),
        _ => None,
    }
}

pub fn detect(bytes: &[u8]) -> Format {
    if let Ok(text) = std::str::from_utf8(bytes) {
        let trimmed = text.trim_start();
        if (trimmed.starts_with('{') || trimmed.starts_with('['))
            && serde_json::from_str::<serde_json::Value>(text).is_ok()
        {
            return Format::Json;
        }
        if !text.chars().any(|c| c.is_control() && !c.is_whitespace()) {
            return Format::Text;
        }
    }
    match wire_header(bytes) {
        Some((schema_id, _)) => Format::SchemaRegistry { schema_id },
        None => Format::Binary,
    }
}

/// Full, multi-line rendering for the detail view.
pub fn render(bytes: &[u8], decoded: Option<&Decoded>) -> Rendered {
    if let Some(decoded) = decoded {
        let kind = decoded.schema_type.map_or("schema", SchemaType::label);
        let label = format!("{kind} · schema {}", decoded.schema_id);
        return match &decoded.result {
            Ok(value) => Rendered {
                label,
                json: true,
                text: serde_json::to_string_pretty(value).expect("a Value always serializes"),
            },
            Err(error) => Rendered {
                label: format!("{label} · not decoded"),
                json: false,
                text: format!("{error}\n\n{}", hex_dump(bytes)),
            },
        };
    }
    match detect(bytes) {
        Format::Json => Rendered {
            label: "json".to_owned(),
            json: true,
            text: serde_json::to_string_pretty(&parse_json(bytes))
                .expect("a Value always serializes"),
        },
        Format::Text => Rendered {
            label: "text".to_owned(),
            json: false,
            text: String::from_utf8_lossy(bytes).into_owned(),
        },
        Format::SchemaRegistry { schema_id } => Rendered {
            label: format!("schema {schema_id} · not decoded"),
            json: false,
            text: format!(
                "No Schema Registry is configured for this profile.\n\n{}",
                hex_dump(bytes)
            ),
        },
        Format::Binary => Rendered {
            label: "binary".to_owned(),
            json: false,
            text: hex_dump(bytes),
        },
    }
}

/// Single-line rendering for table cells, at most `max_chars` characters.
pub fn preview(bytes: &[u8], decoded: Option<&Decoded>, max_chars: usize) -> String {
    let line = match (decoded, detect(bytes)) {
        (
            Some(Decoded {
                result: Ok(value), ..
            }),
            _,
        ) => value.to_string(),
        (Some(Decoded { schema_id, .. }), _) => {
            format!("<schema {schema_id}: not decoded, {} bytes>", bytes.len())
        }
        (None, Format::Json) => parse_json(bytes).to_string(),
        (None, Format::Text) => String::from_utf8_lossy(bytes)
            .chars()
            .map(|c| if c.is_whitespace() { ' ' } else { c })
            .collect(),
        (None, Format::SchemaRegistry { schema_id }) => {
            format!("<schema {schema_id}, {} bytes>", bytes.len())
        }
        (None, Format::Binary) => {
            let mut hex = String::new();
            for byte in bytes.iter().take(max_chars / 3 + 1) {
                let _ = write!(hex, "{byte:02x} ");
            }
            hex
        }
    };
    truncate(&line, max_chars)
}

/// The payload as text for searching and copying: decoded JSON when available,
/// otherwise the bytes as (lossy) UTF-8.
pub fn plain_text(bytes: &[u8], decoded: Option<&Decoded>) -> String {
    match decoded {
        Some(Decoded {
            result: Ok(value), ..
        }) => value.to_string(),
        _ => String::from_utf8_lossy(bytes).into_owned(),
    }
}

fn parse_json(bytes: &[u8]) -> serde_json::Value {
    serde_json::from_slice(bytes).expect("detect() validated the JSON")
}

fn truncate(s: &str, max_chars: usize) -> String {
    match s.char_indices().nth(max_chars) {
        Some((idx, _)) => {
            let mut out = s[..idx].trim_end().to_owned();
            out.push('…');
            out
        }
        None => s.to_owned(),
    }
}

/// `xxd`-style dump: offset, 16 hex bytes, printable ASCII.
pub fn hex_dump(bytes: &[u8]) -> String {
    let mut out = String::new();
    for (i, chunk) in bytes.chunks(16).enumerate() {
        let _ = write!(out, "{:08x}  ", i * 16);
        for j in 0..16 {
            match chunk.get(j) {
                Some(b) => {
                    let _ = write!(out, "{b:02x} ");
                }
                None => out.push_str("   "),
            }
            if j == 7 {
                out.push(' ');
            }
        }
        out.push(' ');
        out.extend(chunk.iter().map(|&b| {
            if b.is_ascii_graphic() || b == b' ' {
                b as char
            } else {
                '.'
            }
        }));
        out.push('\n');
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_formats() {
        assert_eq!(detect(br#"{"a":1}"#), Format::Json);
        assert_eq!(detect(b"[1,2]"), Format::Json);
        assert_eq!(detect(b"{not json"), Format::Text);
        assert_eq!(detect(b"hello world\n"), Format::Text);
        assert_eq!(
            detect(&[0, 0, 0, 0, 42, 0x02, 0x06]),
            Format::SchemaRegistry { schema_id: 42 }
        );
        assert_eq!(detect(&[0xff, 0xfe, 0x01]), Format::Binary);
    }

    #[test]
    fn json_preserves_key_order() {
        let pretty = render(br#"{"z":1,"a":2}"#, None).text;
        assert!(pretty.find("\"z\"").unwrap() < pretty.find("\"a\"").unwrap());
        assert_eq!(
            preview(br#"{ "z": 1, "a": 2 }"#, None, 100),
            r#"{"z":1,"a":2}"#
        );
    }

    #[test]
    fn preview_truncates_on_char_boundary() {
        assert_eq!(preview("héllo wörld".as_bytes(), None, 5), "héllo…");
        assert_eq!(preview(b"line1\nline2", None, 100), "line1 line2");
    }

    #[test]
    fn decoded_values_take_precedence() {
        let bytes = [0, 0, 0, 0, 7, 2, 4];
        let ok = Decoded {
            schema_id: 7,
            schema_type: Some(SchemaType::Avro),
            result: Ok(serde_json::json!({"id": 1})),
        };
        let rendered = render(&bytes, Some(&ok));
        assert_eq!(rendered.label, "avro · schema 7");
        assert!(rendered.json);
        assert_eq!(preview(&bytes, Some(&ok), 50), r#"{"id":1}"#);
        assert_eq!(plain_text(&bytes, Some(&ok)), r#"{"id":1}"#);

        let failed = Decoded {
            schema_id: 7,
            schema_type: None,
            result: Err("schema registry: 404 Not Found".into()),
        };
        let rendered = render(&bytes, Some(&failed));
        assert_eq!(rendered.label, "schema · schema 7 · not decoded");
        assert!(
            rendered
                .text
                .starts_with("schema registry: 404 Not Found\n\n00000000")
        );

        let undecoded = render(&bytes, None);
        assert_eq!(undecoded.label, "schema 7 · not decoded");
        assert!(undecoded.text.contains("No Schema Registry"));
    }

    #[test]
    fn wire_header_needs_magic_byte_and_id() {
        assert_eq!(wire_header(&[0, 0, 0, 1, 0, 9]), Some((256, &[9][..])));
        assert_eq!(wire_header(&[0, 0, 0, 0, 1]), Some((1, &[][..])));
        assert_eq!(wire_header(&[0, 0, 0, 1]), None);
        assert_eq!(wire_header(&[1, 0, 0, 0, 1]), None);
    }

    #[test]
    fn hex_dump_layout() {
        let dump = hex_dump(b"ABC");
        assert!(dump.starts_with("00000000  41 42 43 "));
        assert!(dump.trim_end().ends_with("ABC"));
    }
}
