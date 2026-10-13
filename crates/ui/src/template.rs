//! The text a user edits in `$EDITOR` to produce a message.
//!
//! ```text
//! # Produce a message to "orders". Save and quit to review it before sending.
//! # Delete everything to cancel.
//! topic: orders
//! partition: auto
//! key: cust-042
//! header: trace-id=abc123
//! value-format: avro 7
//! ---
//! {"order_id": "ord-1", ...}
//! ```
//!
//! Everything after the `---` line is the value, byte for byte (minus one
//! trailing newline). A value of exactly `null` produces a tombstone.

use kaffy_kafka::{Header, Payload, ProduceRequest, Record};

const SEPARATOR: &str = "---";

/// How the value text is turned into bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
enum ValueFormat {
    Text,
    /// JSON encoded as Avro with this Schema Registry schema id.
    Avro(u32),
    /// JSON encoded as this Protobuf message, with a Schema Registry schema or
    /// (`None`) the topic's local `.proto` files.
    Protobuf {
        schema_id: Option<u32>,
        message: String,
    },
}

/// A blank template for a new message on `topic`.
pub fn new_message(topic: &str) -> String {
    render(topic, None, None, &[], ValueFormat::Text, "")
}

/// A template pre-filled from `record`, for edit and re-produce. Avro values are
/// shown as JSON and encoded again with the same schema.
pub fn from_record(record: &Record) -> String {
    let key = record
        .key
        .as_deref()
        .map(|k| String::from_utf8_lossy(k).into_owned());
    let (format, value) = match (&record.value_decoded, &record.value) {
        (
            Some(kaffy_schema::Decoded {
                schema_id,
                schema_type: Some(kaffy_schema::SchemaType::Avro),
                message: None,
                result: Ok(json),
            }),
            _,
        ) => (
            ValueFormat::Avro(*schema_id),
            serde_json::to_string_pretty(json).expect("a Value always serializes"),
        ),
        (
            Some(kaffy_schema::Decoded {
                schema_id,
                schema_type: Some(kaffy_schema::SchemaType::Protobuf),
                message: Some(message),
                result: Ok(json),
            }),
            _,
        ) => (
            ValueFormat::Protobuf {
                schema_id: (*schema_id != 0).then_some(*schema_id),
                message: message.clone(),
            },
            serde_json::to_string_pretty(json).expect("a Value always serializes"),
        ),
        (_, None) => (ValueFormat::Text, "null".to_owned()),
        (_, Some(bytes)) => {
            let text = String::from_utf8_lossy(bytes);
            // Pretty-print JSON for editing; it is compacted again on parse.
            let text = match serde_json::from_str::<serde_json::Value>(&text) {
                Ok(json) if text.trim_start().starts_with(['{', '[']) => {
                    serde_json::to_string_pretty(&json).expect("a Value always serializes")
                }
                _ => text.into_owned(),
            };
            (ValueFormat::Text, text)
        }
    };
    render(
        &record.topic,
        Some(record.partition),
        key.as_deref(),
        &record.headers,
        format,
        &value,
    )
}

fn render(
    topic: &str,
    partition: Option<i32>,
    key: Option<&str>,
    headers: &[Header],
    format: ValueFormat,
    value: &str,
) -> String {
    let mut out = format!(
        "# Produce a message to \"{topic}\". Save and quit to review it before it is sent.\n\
         # Delete everything to cancel. Lines starting with # are ignored above the --- line.\n\
         #\n\
         # partition: a number, or auto to let the key decide (as Java producers do)\n\
         # key:       leave empty for no key\n\
         # header:    one line per header, name=value\n\
         # value-format: text (sent as written), avro <schema id>, protobuf <schema id> <message>,\n\
         #               or protobuf local <message> (JSON encoded with the schema)\n\
         # The value is everything below ---. Write exactly null for a tombstone.\n\
         topic: {topic}\n\
         partition: {}\n\
         key: {}\n",
        partition.map_or_else(|| "auto".to_owned(), |p| p.to_string()),
        key.unwrap_or(""),
    );
    for h in headers {
        let value = h
            .value
            .as_deref()
            .map(String::from_utf8_lossy)
            .unwrap_or_default();
        out.push_str(&format!("header: {}={value}\n", h.key));
    }
    out.push_str(&match format {
        ValueFormat::Text => "value-format: text\n".to_owned(),
        ValueFormat::Avro(id) => format!("value-format: avro {id}\n"),
        ValueFormat::Protobuf {
            schema_id: Some(id),
            message,
        } => format!("value-format: protobuf {id} {message}\n"),
        ValueFormat::Protobuf {
            schema_id: None,
            message,
        } => format!("value-format: protobuf local {message}\n"),
    });
    out.push_str(SEPARATOR);
    out.push('\n');
    out.push_str(value);
    out.push('\n');
    out
}

/// Parses an edited template. `Ok(None)` means the user cancelled (empty file).
pub fn parse(text: &str, id: u64) -> Result<Option<ProduceRequest>, String> {
    if text.trim().is_empty() {
        return Ok(None);
    }
    let Some((head, value)) = split_value(text) else {
        return Err(format!(
            "missing the {SEPARATOR} line that starts the value"
        ));
    };

    let mut topic = None;
    let mut partition = None;
    let mut key = None;
    let mut headers = Vec::new();
    let mut format = ValueFormat::Text;
    for (number, line) in head.lines().enumerate() {
        let line = line.trim_end();
        if line.trim().is_empty() || line.trim_start().starts_with('#') {
            continue;
        }
        let error = |message: &str| format!("line {}: {message}", number + 1);
        let (field, rest) = line
            .split_once(':')
            .ok_or_else(|| error("expected `field: value`"))?;
        let rest = rest.strip_prefix(' ').unwrap_or(rest);
        match field.trim() {
            "topic" => {
                let t = rest.trim();
                if t.is_empty() {
                    return Err(error("topic is empty"));
                }
                topic = Some(t.to_owned());
            }
            "partition" => {
                partition = match rest.trim() {
                    "" | "auto" => None,
                    n => Some(
                        n.parse::<i32>()
                            .ok()
                            .filter(|p| *p >= 0)
                            .ok_or_else(|| error("partition must be a number or auto"))?,
                    ),
                }
            }
            "key" => key = (!rest.is_empty()).then(|| rest.as_bytes().to_vec()),
            "header" => {
                let (name, value) = rest
                    .split_once('=')
                    .ok_or_else(|| error("header must be name=value"))?;
                if name.trim().is_empty() {
                    return Err(error("header name is empty"));
                }
                headers.push(Header {
                    key: name.trim().to_owned(),
                    value: Some(value.as_bytes().to_vec()),
                });
            }
            "value-format" => format = match rest.split_whitespace().collect::<Vec<_>>().as_slice()
            {
                ["text"] => ValueFormat::Text,
                ["avro", id] => ValueFormat::Avro(
                    id.parse()
                        .map_err(|_| error("avro needs a schema id, e.g. avro 7"))?,
                ),
                ["protobuf", "local", message] => ValueFormat::Protobuf {
                    schema_id: None,
                    message: message.to_string(),
                },
                ["protobuf", id, message] => ValueFormat::Protobuf {
                    schema_id: Some(id.parse().map_err(|_| {
                        error(
                            "protobuf needs a schema id and a message, e.g. protobuf 12 acme.Order",
                        )
                    })?),
                    message: message.to_string(),
                },
                _ => {
                    return Err(error(
                        "value-format must be text, avro <schema id>, protobuf <schema id> <message> or protobuf local <message>",
                    ));
                }
            },
            other => return Err(error(&format!("unknown field `{other}`"))),
        }
    }

    let value = value.strip_suffix('\n').unwrap_or(value);
    let value = value.strip_suffix('\r').unwrap_or(value);
    let payload = if value == "null" {
        None
    } else {
        Some(match format {
            ValueFormat::Text => {
                // Compact JSON back to a single line, as it was before editing.
                let compact = serde_json::from_str::<serde_json::Value>(value)
                    .ok()
                    .filter(|_| value.trim_start().starts_with(['{', '[']))
                    .map(|json| json.to_string());
                Payload::Bytes(compact.unwrap_or_else(|| value.to_owned()).into_bytes())
            }
            ValueFormat::Avro(schema_id) => Payload::Avro {
                schema_id,
                json: serde_json::from_str(value)
                    .map_err(|e| format!("the value is not valid JSON for Avro: {e}"))?,
            },
            ValueFormat::Protobuf { schema_id, message } => Payload::Protobuf {
                schema_id,
                message,
                json: serde_json::from_str(value)
                    .map_err(|e| format!("the value is not valid JSON for Protobuf: {e}"))?,
            },
        })
    };
    Ok(Some(ProduceRequest {
        id,
        topic: topic.ok_or("missing `topic:`")?,
        partition,
        key,
        value: payload,
        headers,
    }))
}

/// Splits at the first line that is exactly the separator.
fn split_value(text: &str) -> Option<(&str, &str)> {
    let mut offset = 0;
    for line in text.split_inclusive('\n') {
        if line.trim_end_matches(['\n', '\r']) == SEPARATOR {
            return Some((&text[..offset], &text[offset + line.len()..]));
        }
        offset += line.len();
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blank_template_needs_a_value_but_parses() {
        let request = parse(&new_message("orders"), 1).unwrap().unwrap();
        assert_eq!(request.topic, "orders");
        assert_eq!(request.partition, None);
        assert_eq!(request.key, None);
        assert_eq!(request.value, Some(Payload::Bytes(Vec::new())));
    }

    #[test]
    fn record_round_trips() {
        let record = Record {
            topic: "orders".into(),
            partition: 2,
            offset: 9,
            key: Some(b"cust-1".to_vec()),
            value: Some(br#"{"id":1,"tags":["a"]}"#.to_vec()),
            headers: vec![Header {
                key: "trace".into(),
                value: Some(b"x=y".to_vec()),
            }],
            ..Default::default()
        };
        let text = from_record(&record);
        assert!(
            text.contains("\n{\n  \"id\": 1,"),
            "JSON is pretty for editing:\n{text}"
        );
        let request = parse(&text, 3).unwrap().unwrap();
        assert_eq!(request.id, 3);
        assert_eq!(request.partition, Some(2));
        assert_eq!(request.key.as_deref(), Some(&b"cust-1"[..]));
        assert_eq!(
            request.headers, record.headers,
            "= inside header values survives"
        );
        assert_eq!(
            request.value,
            Some(Payload::Bytes(br#"{"id":1,"tags":["a"]}"#.to_vec()))
        );
    }

    #[test]
    fn avro_records_reencode_with_their_schema() {
        let record = Record {
            topic: "sensor-readings".into(),
            value: Some(vec![0, 0, 0, 0, 7, 1]),
            value_decoded: Some(kaffy_schema::Decoded {
                schema_id: 7,
                schema_type: Some(kaffy_schema::SchemaType::Avro),
                message: None,
                result: Ok(serde_json::json!({"sensor": "s1"})),
            }),
            ..Default::default()
        };
        let request = parse(&from_record(&record), 1).unwrap().unwrap();
        assert_eq!(
            request.value,
            Some(Payload::Avro {
                schema_id: 7,
                json: serde_json::json!({"sensor": "s1"})
            })
        );
    }

    #[test]
    fn protobuf_records_reencode_as_protobuf() {
        let decoded = |schema_id| kaffy_schema::Decoded {
            schema_id,
            schema_type: Some(kaffy_schema::SchemaType::Protobuf),
            message: Some("acme.Order".into()),
            result: Ok(serde_json::json!({"order_id": "o-1"})),
        };
        let record = |schema_id| Record {
            topic: "orders".into(),
            value: Some(vec![0, 1]),
            value_decoded: Some(decoded(schema_id)),
            ..Default::default()
        };
        let text = from_record(&record(12));
        assert!(
            text.contains("value-format: protobuf 12 acme.Order"),
            "{text}"
        );
        assert_eq!(
            parse(&text, 1).unwrap().unwrap().value,
            Some(Payload::Protobuf {
                schema_id: Some(12),
                message: "acme.Order".into(),
                json: serde_json::json!({"order_id": "o-1"}),
            })
        );
        let local = from_record(&record(0));
        assert!(
            local.contains("value-format: protobuf local acme.Order"),
            "{local}"
        );
        assert!(matches!(
            parse(&local, 1).unwrap().unwrap().value,
            Some(Payload::Protobuf {
                schema_id: None,
                ..
            })
        ));
        assert!(parse("topic: t\nvalue-format: protobuf x acme.A\n---\n{}", 1).is_err());
    }

    #[test]
    fn tombstones_text_and_cancel() {
        let tombstone = Record {
            topic: "t".into(),
            value: None,
            ..Default::default()
        };
        assert_eq!(
            parse(&from_record(&tombstone), 1).unwrap().unwrap().value,
            None
        );

        let text = "topic: t\nkey:\n---\nline one\n  indented --- not a separator\n";
        let request = parse(text, 1).unwrap().unwrap();
        assert_eq!(
            request.value,
            Some(Payload::Bytes(
                b"line one\n  indented --- not a separator".to_vec()
            ))
        );
        assert_eq!(request.key, None);

        assert_eq!(parse("  \n\n", 1).unwrap(), None);
    }

    #[test]
    fn errors_name_the_line() {
        assert_eq!(
            parse("topic: t\npartition: two\n---\nx", 1).unwrap_err(),
            "line 2: partition must be a number or auto"
        );
        assert_eq!(
            parse("topic: t\ncolour: red\n---\nx", 1).unwrap_err(),
            "line 2: unknown field `colour`"
        );
        assert_eq!(
            parse("topic: t\nkey: k\n", 1).unwrap_err(),
            "missing the --- line that starts the value"
        );
        assert_eq!(
            parse("partition: 1\n---\nx", 1).unwrap_err(),
            "missing `topic:`"
        );
        assert!(
            parse("topic: t\nvalue-format: avro 7\n---\n{bad", 1)
                .unwrap_err()
                .starts_with("the value is not valid JSON")
        );
    }
}
