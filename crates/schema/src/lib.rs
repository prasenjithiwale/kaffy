//! Payload decoding for display.
//!
//! Phase 1 handles JSON, UTF-8 text and raw bytes. Payloads in the Confluent wire
//! format (magic byte + schema id) are detected; Avro/Protobuf decoding through
//! the Schema Registry plugs in here later.

use std::fmt::Write as _;

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

impl Format {
    pub fn label(self) -> String {
        match self {
            Self::Json => "json".to_owned(),
            Self::Text => "text".to_owned(),
            Self::SchemaRegistry { schema_id } => format!("schema id {schema_id}"),
            Self::Binary => "binary".to_owned(),
        }
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
    if bytes.len() > 5 && bytes[0] == 0 {
        let schema_id = u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
        return Format::SchemaRegistry { schema_id };
    }
    Format::Binary
}

/// Full, multi-line rendering for the detail view.
pub fn render(bytes: &[u8]) -> (Format, String) {
    let format = detect(bytes);
    let text = match format {
        Format::Json => {
            let value: serde_json::Value =
                serde_json::from_slice(bytes).expect("detect() validated the JSON");
            serde_json::to_string_pretty(&value).expect("a Value always serializes")
        }
        Format::Text => String::from_utf8_lossy(bytes).into_owned(),
        Format::SchemaRegistry { .. } => format!(
            "(Schema Registry decoding is not implemented yet)\n\n{}",
            hex_dump(bytes)
        ),
        Format::Binary => hex_dump(bytes),
    };
    (format, text)
}

/// Single-line rendering for table cells, at most `max_chars` characters.
pub fn preview(bytes: &[u8], max_chars: usize) -> String {
    let line = match detect(bytes) {
        Format::Json => {
            let value: serde_json::Value =
                serde_json::from_slice(bytes).expect("detect() validated the JSON");
            value.to_string()
        }
        Format::Text => String::from_utf8_lossy(bytes)
            .chars()
            .map(|c| if c.is_whitespace() { ' ' } else { c })
            .collect(),
        Format::SchemaRegistry { schema_id } => {
            format!("<schema {schema_id}, {} bytes>", bytes.len())
        }
        Format::Binary => {
            let mut hex = String::new();
            for byte in bytes.iter().take(max_chars / 3 + 1) {
                let _ = write!(hex, "{byte:02x} ");
            }
            hex
        }
    };
    truncate(&line, max_chars)
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
        let (_, pretty) = render(br#"{"z":1,"a":2}"#);
        assert!(pretty.find("\"z\"").unwrap() < pretty.find("\"a\"").unwrap());
        assert_eq!(preview(br#"{ "z": 1, "a": 2 }"#, 100), r#"{"z":1,"a":2}"#);
    }

    #[test]
    fn preview_truncates_on_char_boundary() {
        assert_eq!(preview("héllo wörld".as_bytes(), 5), "héllo…");
        assert_eq!(preview(b"line1\nline2", 100), "line1 line2");
    }

    #[test]
    fn hex_dump_layout() {
        let dump = hex_dump(b"ABC");
        assert!(dump.starts_with("00000000  41 42 43 "));
        assert!(dump.trim_end().ends_with("ABC"));
    }
}
