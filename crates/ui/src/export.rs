//! Exporting messages to JSON, NDJSON or CSV.
//!
//! Each payload is written in its most useful form: decoded Avro and JSON text
//! as JSON, other UTF-8 as a string, and binary as `{"base64": "..."}` (CSV:
//! `base64:...`).

use chrono::{TimeZone, Utc};
use kaffy_kafka::Record;
use serde_json::{Value, json};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExportFormat {
    Json,
    Ndjson,
    Csv,
}

impl ExportFormat {
    pub fn extension(self) -> &'static str {
        match self {
            Self::Json => "json",
            Self::Ndjson => "ndjson",
            Self::Csv => "csv",
        }
    }
}

pub fn export(records: &[&Record], format: ExportFormat) -> String {
    match format {
        ExportFormat::Json => {
            let all: Vec<Value> = records.iter().map(|r| to_json(r)).collect();
            let mut out = serde_json::to_string_pretty(&all).expect("a Value always serializes");
            out.push('\n');
            out
        }
        ExportFormat::Ndjson => records
            .iter()
            .map(|r| to_json(r).to_string() + "\n")
            .collect(),
        ExportFormat::Csv => {
            let mut out = String::from("topic,partition,offset,timestamp,key,value,headers\n");
            for r in records {
                let headers = Value::Array(r.headers.iter().map(header_json).collect());
                let fields = [
                    r.topic.clone(),
                    r.partition.to_string(),
                    r.offset.to_string(),
                    r.timestamp_ms.map(iso).unwrap_or_default(),
                    csv_payload(r.key.as_deref(), r.key_decoded.as_ref()),
                    csv_payload(r.value.as_deref(), r.value_decoded.as_ref()),
                    if r.headers.is_empty() {
                        String::new()
                    } else {
                        headers.to_string()
                    },
                ];
                let line: Vec<String> = fields.iter().map(|f| csv_field(f)).collect();
                out.push_str(&line.join(","));
                out.push('\n');
            }
            out
        }
    }
}

fn to_json(r: &Record) -> Value {
    let mut object = json!({
        "topic": r.topic,
        "partition": r.partition,
        "offset": r.offset,
        "timestamp": r.timestamp_ms.map(iso),
        "key": json_payload(r.key.as_deref(), r.key_decoded.as_ref()),
        "value": json_payload(r.value.as_deref(), r.value_decoded.as_ref()),
        "headers": r.headers.iter().map(header_json).collect::<Vec<_>>(),
    });
    if let Some(decoded) = &r.value_decoded {
        object["value_schema_id"] = json!(decoded.schema_id);
    }
    object
}

fn header_json(h: &kaffy_kafka::Header) -> Value {
    json!({ "key": h.key, "value": json_payload(h.value.as_deref(), None) })
}

fn json_payload(bytes: Option<&[u8]>, decoded: Option<&kaffy_schema::Decoded>) -> Value {
    let Some(bytes) = bytes else {
        return Value::Null;
    };
    if let Some(kaffy_schema::Decoded {
        result: Ok(value), ..
    }) = decoded
    {
        return value.clone();
    }
    match kaffy_schema::detect(bytes) {
        kaffy_schema::Format::Json => {
            serde_json::from_slice(bytes).expect("detect() validated the JSON")
        }
        kaffy_schema::Format::Text => Value::String(String::from_utf8_lossy(bytes).into_owned()),
        _ => json!({ "base64": base64(bytes) }),
    }
}

fn csv_payload(bytes: Option<&[u8]>, decoded: Option<&kaffy_schema::Decoded>) -> String {
    match json_payload(bytes, decoded) {
        Value::Null => String::new(),
        Value::String(s) => s,
        Value::Object(o) if o.len() == 1 && o.contains_key("base64") && decoded.is_none() => {
            format!("base64:{}", o["base64"].as_str().unwrap_or_default())
        }
        other => other.to_string(),
    }
}

/// RFC 4180: quote fields with commas, quotes or line breaks; double the quotes.
fn csv_field(field: &str) -> String {
    if field.contains([',', '"', '\n', '\r']) {
        format!("\"{}\"", field.replace('"', "\"\""))
    } else {
        field.to_owned()
    }
}

fn iso(ms: i64) -> String {
    Utc.timestamp_millis_opt(ms)
        .single()
        .map(|t| t.to_rfc3339_opts(chrono::SecondsFormat::Millis, true))
        .unwrap_or_else(|| ms.to_string())
}

fn base64(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let n = (chunk[0] as u32) << 16
            | (*chunk.get(1).unwrap_or(&0) as u32) << 8
            | *chunk.get(2).unwrap_or(&0) as u32;
        for i in 0..4 {
            if i <= chunk.len() {
                out.push(ALPHABET[(n >> (18 - 6 * i) & 63) as usize] as char);
            } else {
                out.push('=');
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use kaffy_kafka::Header;

    fn records() -> Vec<Record> {
        vec![
            Record {
                topic: "orders".into(),
                partition: 1,
                offset: 7,
                timestamp_ms: Some(1_700_000_000_123),
                key: Some(b"cust-1".to_vec()),
                value: Some(br#"{"status":"PAID","note":"a, \"b\""}"#.to_vec()),
                headers: vec![Header {
                    key: "trace".into(),
                    value: Some(b"t1".to_vec()),
                }],
                ..Default::default()
            },
            Record {
                topic: "orders".into(),
                offset: 8,
                key: Some(vec![0xff, 0x00, 0x10]),
                value: None,
                ..Default::default()
            },
        ]
    }

    #[test]
    fn ndjson_keeps_json_values_as_json() {
        let records = records();
        let refs: Vec<&Record> = records.iter().collect();
        let out = export(&refs, ExportFormat::Ndjson);
        let lines: Vec<Value> = out
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[0]["value"]["status"], "PAID");
        assert_eq!(lines[0]["timestamp"], "2023-11-14T22:13:20.123Z");
        assert_eq!(
            lines[0]["headers"][0],
            json!({"key": "trace", "value": "t1"})
        );
        assert_eq!(lines[1]["key"], json!({"base64": "/wAQ"}));
        assert_eq!(lines[1]["value"], Value::Null);
    }

    #[test]
    fn json_is_an_array() {
        let records = records();
        let refs: Vec<&Record> = records.iter().collect();
        let all: Vec<Value> = serde_json::from_str(&export(&refs, ExportFormat::Json)).unwrap();
        assert_eq!(all.len(), 2);
        assert_eq!(all[1]["offset"], 8);
    }

    #[test]
    fn csv_quotes_and_encodes() {
        let records = records();
        let refs: Vec<&Record> = records.iter().collect();
        let out = export(&refs, ExportFormat::Csv);
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(
            lines[0],
            "topic,partition,offset,timestamp,key,value,headers"
        );
        assert_eq!(
            lines[1],
            r#"orders,1,7,2023-11-14T22:13:20.123Z,cust-1,"{""status"":""PAID"",""note"":""a, \""b\"""" }","[{""key"":""trace"",""value"":""t1""}]""#
                .replace(r#"""" }"#, r#""""}"#)
        );
        assert_eq!(lines[2], "orders,0,8,,base64:/wAQ,,");
    }

    #[test]
    fn base64_padding() {
        assert_eq!(base64(b""), "");
        assert_eq!(base64(b"f"), "Zg==");
        assert_eq!(base64(b"fo"), "Zm8=");
        assert_eq!(base64(b"foo"), "Zm9v");
        assert_eq!(base64(b"foobar"), "Zm9vYmFy");
    }
}
