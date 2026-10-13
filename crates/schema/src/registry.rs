//! Confluent-compatible Schema Registry client and payload decoder.
//!
//! Schemas are fetched by id on first use and cached for the life of the client.
//! Failed lookups are cached too, and retried after [`RETRY_AFTER`], so a registry
//! outage costs one request per schema id instead of one per message.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use apache_avro::Schema as AvroSchema;
use apache_avro::reader::datum::GenericDatumReader;
use reqwest::Url;
use reqwest::header::ACCEPT;
use serde::Deserialize;
use serde::de::DeserializeOwned;
use tokio::sync::Mutex;

use crate::{Decoded, SchemaType, wire_header};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const RETRY_AFTER: Duration = Duration::from_secs(30);
/// Guards against reference cycles and runaway schemas.
const MAX_REFERENCES: usize = 100;

pub struct Registry {
    base: Url,
    basic_auth: Option<(String, String)>,
    http: reqwest::Client,
    cache: Mutex<HashMap<u32, Cached>>,
}

enum Cached {
    Ready(Arc<Parsed>),
    Failed { error: String, at: Instant },
}

enum Parsed {
    Avro {
        schema: AvroSchema,
        /// Named types from referenced subjects.
        references: Vec<AvroSchema>,
    },
    Json,
    Protobuf,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SchemaResponse {
    schema: String,
    /// Absent for Avro, the original and default type.
    #[serde(default)]
    schema_type: Option<String>,
    #[serde(default)]
    references: Vec<Reference>,
}

#[derive(Deserialize)]
struct Reference {
    subject: String,
    version: i32,
}

#[derive(Deserialize)]
struct ErrorResponse {
    message: String,
}

impl Registry {
    pub fn new(url: &str, basic_auth: Option<(String, String)>) -> Result<Self, String> {
        let base = Url::parse(url).map_err(|e| format!("schema registry url {url:?}: {e}"))?;
        if base.cannot_be_a_base() {
            return Err(format!("schema registry url {url:?} is not an http(s) url"));
        }
        let http = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .user_agent(concat!("kaffy/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(|e| format!("schema registry client: {e}"))?;
        Ok(Self {
            base,
            basic_auth,
            http,
            cache: Mutex::new(HashMap::new()),
        })
    }

    /// Decodes a payload in the Confluent wire format. Returns `None` for payloads
    /// that are not in that format, so they can be shown as JSON, text or bytes.
    pub async fn decode(&self, bytes: &[u8]) -> Option<Decoded> {
        let (schema_id, _) = wire_header(bytes)?;
        Some(match self.schema(schema_id).await {
            Ok(parsed) => decode_with(&parsed, schema_id, bytes),
            Err(error) => Decoded {
                schema_id,
                schema_type: None,
                result: Err(error),
            },
        })
    }

    /// Encodes `json` with Avro schema `schema_id`, in the Confluent wire format.
    /// The JSON is matched to the schema the way the decoder prints it: unions
    /// unwrapped, enums as their symbol.
    pub async fn encode_avro(
        &self,
        schema_id: u32,
        json: &serde_json::Value,
    ) -> Result<Vec<u8>, String> {
        let parsed = self.schema(schema_id).await?;
        let Parsed::Avro { schema, references } = parsed.as_ref() else {
            return Err(format!("schema {schema_id} is not an Avro schema"));
        };
        encode_avro_body(schema, references, json).map(|body| wire(schema_id, &body))
    }

    async fn schema(&self, id: u32) -> Result<Arc<Parsed>, String> {
        let mut cache = self.cache.lock().await;
        match cache.get(&id) {
            Some(Cached::Ready(parsed)) => return Ok(parsed.clone()),
            Some(Cached::Failed { error, at }) if at.elapsed() < RETRY_AFTER => {
                return Err(error.clone());
            }
            _ => {}
        }
        let result = self.fetch(id).await.map(Arc::new);
        let entry = match &result {
            Ok(parsed) => Cached::Ready(parsed.clone()),
            Err(error) => {
                tracing::warn!(schema_id = id, %error, "schema lookup failed");
                Cached::Failed {
                    error: error.clone(),
                    at: Instant::now(),
                }
            }
        };
        cache.insert(id, entry);
        result
    }

    async fn fetch(&self, id: u32) -> Result<Parsed, String> {
        let response: SchemaResponse = self.get(&["schemas", "ids", &id.to_string()]).await?;
        match response.schema_type.as_deref().unwrap_or("AVRO") {
            "AVRO" => {
                let references = self.fetch_references(response.references).await?;
                parse_avro(&response.schema, &references)
            }
            "JSON" => Ok(Parsed::Json),
            "PROTOBUF" => Ok(Parsed::Protobuf),
            other => Err(format!("schema {id} has unknown type {other:?}")),
        }
    }

    /// Fetches the schemas behind `references`, and theirs, transitively.
    async fn fetch_references(&self, references: Vec<Reference>) -> Result<Vec<String>, String> {
        let mut pending = references;
        let mut seen = HashSet::new();
        let mut schemas = Vec::new();
        while let Some(reference) = pending.pop() {
            if !seen.insert((reference.subject.clone(), reference.version)) {
                continue;
            }
            if seen.len() > MAX_REFERENCES {
                return Err(format!("more than {MAX_REFERENCES} schema references"));
            }
            let response: SchemaResponse = self
                .get(&[
                    "subjects",
                    &reference.subject,
                    "versions",
                    &reference.version.to_string(),
                ])
                .await?;
            pending.extend(response.references);
            schemas.push(response.schema);
        }
        Ok(schemas)
    }

    async fn get<T: DeserializeOwned>(&self, segments: &[&str]) -> Result<T, String> {
        let mut url = self.base.clone();
        url.path_segments_mut()
            .expect("checked in Registry::new")
            .pop_if_empty()
            .extend(segments);

        let mut request = self.http.get(url.clone()).header(
            ACCEPT,
            "application/vnd.schemaregistry.v1+json, application/json",
        );
        if let Some((user, password)) = &self.basic_auth {
            request = request.basic_auth(user, Some(password));
        }
        let response = request
            .send()
            .await
            .map_err(|e| format!("schema registry: {}", error_chain(&e)))?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            let detail = serde_json::from_str::<ErrorResponse>(&body)
                .map(|e| e.message)
                .unwrap_or(body);
            return Err(format!("schema registry: {status}: {}", detail.trim()));
        }
        response
            .json()
            .await
            .map_err(|e| format!("schema registry: unexpected response from {url}: {e}"))
    }
}

/// reqwest's top-level message ("error sending request") hides the cause.
fn error_chain(error: &dyn std::error::Error) -> String {
    let mut message = error.to_string();
    let mut source = error.source();
    while let Some(cause) = source {
        message.push_str(": ");
        message.push_str(&cause.to_string());
        source = cause.source();
    }
    message
}

fn parse_avro(schema: &str, references: &[String]) -> Result<Parsed, String> {
    let error = |e: apache_avro::Error| format!("invalid Avro schema: {e}");
    if references.is_empty() {
        return Ok(Parsed::Avro {
            schema: AvroSchema::parse_str(schema).map_err(error)?,
            references: Vec::new(),
        });
    }
    let (schema, references) =
        AvroSchema::parse_str_with_list(schema, references).map_err(error)?;
    Ok(Parsed::Avro { schema, references })
}

fn decode_with(parsed: &Parsed, schema_id: u32, bytes: &[u8]) -> Decoded {
    let (_, body) = wire_header(bytes).expect("caller checked the wire format");
    let (schema_type, result) = match parsed {
        Parsed::Avro { schema, references } => {
            (SchemaType::Avro, decode_avro_body(schema, references, body))
        }
        Parsed::Json => (
            SchemaType::Json,
            serde_json::from_slice(body).map_err(|e| format!("invalid JSON payload: {e}")),
        ),
        Parsed::Protobuf => (
            SchemaType::Protobuf,
            Err("Protobuf decoding is not supported yet".to_owned()),
        ),
    };
    Decoded {
        schema_id,
        schema_type: Some(schema_type),
        result,
    }
}

/// Encodes JSON as Avro with a known schema, in the Confluent wire format.
pub fn encode_avro(
    schema: &AvroSchema,
    schema_id: u32,
    json: &serde_json::Value,
) -> Result<Vec<u8>, String> {
    encode_avro_body(schema, &[], json).map(|body| wire(schema_id, &body))
}

/// Decodes an Avro payload in the Confluent wire format with a known schema.
pub fn decode_avro(schema: &AvroSchema, bytes: &[u8]) -> Option<Decoded> {
    let (schema_id, body) = wire_header(bytes)?;
    Some(Decoded {
        schema_id,
        schema_type: Some(SchemaType::Avro),
        result: decode_avro_body(schema, &[], body),
    })
}

fn wire(schema_id: u32, body: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(body.len() + 5);
    bytes.push(0);
    bytes.extend_from_slice(&schema_id.to_be_bytes());
    bytes.extend_from_slice(body);
    bytes
}

fn encode_avro_body(
    schema: &AvroSchema,
    references: &[AvroSchema],
    json: &serde_json::Value,
) -> Result<Vec<u8>, String> {
    let error = |e: apache_avro::Error| format!("does not match the Avro schema: {e}");
    let value = apache_avro::types::Value::try_from(json.clone()).map_err(error)?;
    let value = value
        .resolve_schemata(schema, references.iter().collect())
        .map_err(error)?;
    let builder = apache_avro::writer::datum::GenericDatumWriter::builder(schema);
    let writer = if references.is_empty() {
        builder.build()
    } else {
        builder
            .schemata(references.iter().collect())
            .map_err(error)?
            .build()
    }
    .map_err(error)?;
    writer.write_value_to_vec(value).map_err(error)
}

fn decode_avro_body(
    schema: &AvroSchema,
    references: &[AvroSchema],
    mut body: &[u8],
) -> Result<serde_json::Value, String> {
    let error = |e: apache_avro::Error| format!("Avro decoding failed: {e}");
    let value = if references.is_empty() {
        GenericDatumReader::builder(schema)
            .build()
            .map_err(error)?
            .read_value(&mut body)
    } else {
        GenericDatumReader::builder(schema)
            .writer_schemata(references.iter().collect())
            .map_err(error)?
            .build()
            .map_err(error)?
            .read_value(&mut body)
    }
    .map_err(error)?;
    serde_json::Value::try_from(value).map_err(error)
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex as StdMutex;

    use apache_avro::types::Value as AvroValue;
    use apache_avro::writer::datum::GenericDatumWriter;
    use serde_json::json;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use super::*;

    /// A tiny HTTP server answering GETs from a fixed route table and recording requests.
    struct MockRegistry {
        url: String,
        requests: Arc<StdMutex<Vec<String>>>,
    }

    impl MockRegistry {
        async fn start(routes: Vec<(&'static str, u16, String)>) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}/", listener.local_addr().unwrap());
            let requests = Arc::new(StdMutex::new(Vec::new()));
            let log = requests.clone();
            tokio::spawn(async move {
                loop {
                    let (mut socket, _) = listener.accept().await.unwrap();
                    let mut buf = vec![0; 8192];
                    let n = socket.read(&mut buf).await.unwrap();
                    let request = String::from_utf8_lossy(&buf[..n]).into_owned();
                    let path = request.split_whitespace().nth(1).unwrap_or("").to_owned();
                    log.lock().unwrap().push(request);
                    let (status, body) = routes
                        .iter()
                        .find(|(p, _, _)| *p == path)
                        .map(|(_, s, b)| (*s, b.clone()))
                        .unwrap_or((404, r#"{"error_code":404,"message":"no route"}"#.into()));
                    let response = format!(
                        "HTTP/1.1 {status} X\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    socket.write_all(response.as_bytes()).await.unwrap();
                }
            });
            Self { url, requests }
        }

        fn hits(&self, path: &str) -> usize {
            let needle = format!("GET {path} ");
            self.requests
                .lock()
                .unwrap()
                .iter()
                .filter(|r| r.starts_with(&needle))
                .count()
        }
    }

    const ORDER: &str = r#"{
        "type": "record", "name": "Order", "namespace": "com.acme",
        "fields": [
            {"name": "id", "type": "string"},
            {"name": "amount", "type": "double"},
            {"name": "status", "type": {"type": "enum", "name": "Status", "symbols": ["NEW", "PAID"]}},
            {"name": "note", "type": ["null", "string"], "default": null}
        ]
    }"#;

    fn wire(schema_id: u32, body: &[u8]) -> Vec<u8> {
        let mut bytes = vec![0];
        bytes.extend_from_slice(&schema_id.to_be_bytes());
        bytes.extend_from_slice(body);
        bytes
    }

    fn schema_body(schema: &str) -> String {
        json!({ "schema": schema }).to_string()
    }

    fn encode(schema: &AvroSchema, schemata: &[&AvroSchema], value: AvroValue) -> Vec<u8> {
        let builder = GenericDatumWriter::builder(schema);
        let writer = if schemata.is_empty() {
            builder.build().unwrap()
        } else {
            builder
                .schemata(schemata.to_vec())
                .unwrap()
                .build()
                .unwrap()
        };
        writer.write_value_to_vec(value).unwrap()
    }

    fn order(note: Option<&str>) -> AvroValue {
        AvroValue::Record(vec![
            ("id".into(), AvroValue::String("ord-1".into())),
            ("amount".into(), AvroValue::Double(12.5)),
            ("status".into(), AvroValue::Enum(1, "PAID".into())),
            (
                "note".into(),
                match note {
                    Some(n) => AvroValue::Union(1, Box::new(AvroValue::String(n.into()))),
                    None => AvroValue::Union(0, Box::new(AvroValue::Null)),
                },
            ),
        ])
    }

    #[tokio::test]
    async fn decodes_avro_and_caches_schema() {
        let mock = MockRegistry::start(vec![("/schemas/ids/7", 200, schema_body(ORDER))]).await;
        let registry = Registry::new(&mock.url, None).unwrap();
        let schema = AvroSchema::parse_str(ORDER).unwrap();

        let first = registry
            .decode(&wire(7, &encode(&schema, &[], order(Some("gift")))))
            .await
            .unwrap();
        assert_eq!(first.schema_id, 7);
        assert_eq!(first.schema_type, Some(SchemaType::Avro));
        assert_eq!(
            first.result.unwrap(),
            json!({"id": "ord-1", "amount": 12.5, "status": "PAID", "note": "gift"})
        );

        let second = registry
            .decode(&wire(7, &encode(&schema, &[], order(None))))
            .await
            .unwrap();
        assert_eq!(second.result.unwrap()["note"], serde_json::Value::Null);
        assert_eq!(mock.hits("/schemas/ids/7"), 1, "schema is cached");
    }

    #[tokio::test]
    async fn encodes_json_back_to_avro() {
        let mock = MockRegistry::start(vec![("/schemas/ids/7", 200, schema_body(ORDER))]).await;
        let registry = Registry::new(&mock.url, None).unwrap();
        let json = json!({"id": "ord-9", "amount": 3, "status": "NEW", "note": "fragile"});
        let bytes = registry.encode_avro(7, &json).await.unwrap();
        assert_eq!(&bytes[..5], &[0, 0, 0, 0, 7]);
        // Round trip: integers widen to the schema's double.
        let decoded = registry.decode(&bytes).await.unwrap().result.unwrap();
        assert_eq!(
            decoded,
            json!({"id": "ord-9", "amount": 3.0, "status": "NEW", "note": "fragile"})
        );

        let null_note = json!({"id": "ord-9", "amount": 1.5, "status": "PAID", "note": null});
        assert!(registry.encode_avro(7, &null_note).await.is_ok());

        let error = registry
            .encode_avro(7, &json!({"id": "ord-9", "status": "SHIPPED"}))
            .await
            .unwrap_err();
        assert!(
            error.starts_with("does not match the Avro schema"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn resolves_schema_references() {
        let address = r#"{"type": "record", "name": "Address", "namespace": "com.acme",
            "fields": [{"name": "city", "type": "string"}]}"#;
        let customer = r#"{"type": "record", "name": "Customer", "namespace": "com.acme",
            "fields": [{"name": "name", "type": "string"}, {"name": "home", "type": "com.acme.Address"}]}"#;
        let customer_response = json!({
            "schema": customer,
            "references": [{"name": "com.acme.Address", "subject": "address-value", "version": 3}]
        });
        let mock = MockRegistry::start(vec![
            ("/schemas/ids/9", 200, customer_response.to_string()),
            (
                "/subjects/address-value/versions/3",
                200,
                schema_body(address),
            ),
        ])
        .await;
        let registry = Registry::new(&mock.url, None).unwrap();

        let (schema, deps) = AvroSchema::parse_str_with_list(customer, [address]).unwrap();
        let value = AvroValue::Record(vec![
            ("name".into(), AvroValue::String("Ada".into())),
            (
                "home".into(),
                AvroValue::Record(vec![("city".into(), AvroValue::String("Pune".into()))]),
            ),
        ]);
        let bytes = wire(9, &encode(&schema, &deps.iter().collect::<Vec<_>>(), value));
        let decoded = registry.decode(&bytes).await.unwrap();
        assert_eq!(
            decoded.result.unwrap(),
            json!({"name": "Ada", "home": {"city": "Pune"}})
        );
    }

    #[tokio::test]
    async fn reports_and_caches_lookup_errors() {
        let mock = MockRegistry::start(vec![(
            "/schemas/ids/5",
            404,
            r#"{"error_code":40403,"message":"Schema 5 not found"}"#.to_owned(),
        )])
        .await;
        let registry = Registry::new(&mock.url, None).unwrap();
        for _ in 0..3 {
            let decoded = registry.decode(&wire(5, b"\x02")).await.unwrap();
            assert_eq!(decoded.schema_type, None);
            assert_eq!(
                decoded.result.unwrap_err(),
                "schema registry: 404 Not Found: Schema 5 not found"
            );
        }
        assert_eq!(
            mock.hits("/schemas/ids/5"),
            1,
            "failures are not retried immediately"
        );
    }

    #[tokio::test]
    async fn json_schema_payloads_and_basic_auth() {
        let body = json!({"schemaType": "JSON", "schema": "{\"type\":\"object\"}"}).to_string();
        let mock = MockRegistry::start(vec![("/sr/schemas/ids/3", 200, body)]).await;
        let url = format!("{}sr", mock.url);
        let registry = Registry::new(&url, Some(("key".into(), "secret".into()))).unwrap();

        let decoded = registry.decode(&wire(3, br#"{"a":1}"#)).await.unwrap();
        assert_eq!(decoded.schema_type, Some(SchemaType::Json));
        assert_eq!(decoded.result.unwrap(), json!({"a": 1}));

        let requests = mock.requests.lock().unwrap();
        // base64("key:secret")
        assert!(
            requests[0]
                .to_lowercase()
                .contains("authorization: basic a2v5onnly3jlda==")
        );
    }

    #[tokio::test]
    async fn ignores_payloads_without_wire_header() {
        let registry = Registry::new("http://127.0.0.1:1/", None).unwrap();
        assert!(registry.decode(br#"{"plain":"json"}"#).await.is_none());
        assert!(registry.decode(b"\x00\x00").await.is_none());
    }

    #[tokio::test]
    async fn unreachable_registry_is_an_error_not_a_hang() {
        let registry = Registry::new("http://127.0.0.1:1/", None).unwrap();
        let decoded = registry.decode(&wire(1, b"\x02")).await.unwrap();
        let error = decoded.result.unwrap_err();
        assert!(error.starts_with("schema registry: "), "{error}");
    }
}
