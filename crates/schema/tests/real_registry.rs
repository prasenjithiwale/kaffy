//! Tests against a real Confluent Schema Registry. Ignored by default:
//!
//! ```sh
//! docker compose -f dev/docker-compose.yml up -d --wait
//! cargo test -p kaffy-schema --test real_registry -- --ignored
//! ```
//!
//! `KAFFY_TEST_SR` overrides the registry URL (default http://localhost:8081).

use std::time::{SystemTime, UNIX_EPOCH};

use kaffy_schema::{Registry, SchemaType};
use serde_json::json;

fn url() -> String {
    std::env::var("KAFFY_TEST_SR").unwrap_or_else(|_| "http://localhost:8081".to_owned())
}

/// A unique subject; call `delete` at the end of the test.
struct TestSubject(String);

impl TestSubject {
    fn new() -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        Self(format!("kaffy-test-{nanos}-value"))
    }

    /// Soft delete, then permanent delete, as the registry requires.
    async fn delete(self) {
        let client = reqwest::Client::new();
        let base = format!("{}/subjects/{}", url(), self.0);
        let _ = client.delete(&base).send().await;
        let _ = client.delete(format!("{base}?permanent=true")).send().await;
    }
}

const V1: &str = r#"{"type":"record","name":"Order","namespace":"test","fields":[{"name":"id","type":"string"}]}"#;
// Adds a field with a default: BACKWARD compatible.
const V2: &str = r#"{"type":"record","name":"Order","namespace":"test","fields":[{"name":"id","type":"string"},{"name":"amount","type":"double","default":0.0}]}"#;
// Adds a field without a default: not BACKWARD compatible.
const BAD: &str = r#"{"type":"record","name":"Order","namespace":"test","fields":[{"name":"id","type":"string"},{"name":"currency","type":"string"}]}"#;

#[tokio::test]
#[ignore = "needs a Schema Registry"]
async fn browse_check_and_register() {
    let subject = TestSubject::new();
    let registry = Registry::new(&url(), None).unwrap();

    let fresh = registry
        .check_compatibility(&subject.0, SchemaType::Avro, V1)
        .await
        .unwrap();
    assert!(fresh.compatible, "a new subject accepts any valid schema");

    let (id1, v1) = registry
        .register(&subject.0, SchemaType::Avro, V1)
        .await
        .unwrap();
    assert_eq!(v1, 1);
    assert_eq!(
        registry
            .register(&subject.0, SchemaType::Avro, V1)
            .await
            .unwrap(),
        (id1, 1),
        "idempotent"
    );

    let good = registry
        .check_compatibility(&subject.0, SchemaType::Avro, V2)
        .await
        .unwrap();
    assert!(good.compatible, "{good:?}");
    let bad = registry
        .check_compatibility(&subject.0, SchemaType::Avro, BAD)
        .await
        .unwrap();
    assert!(!bad.compatible);
    assert!(
        bad.messages.iter().any(|m| m.contains("currency")),
        "the registry explains why: {:?}",
        bad.messages
    );

    let (_, v2) = registry
        .register(&subject.0, SchemaType::Avro, V2)
        .await
        .unwrap();
    assert_eq!(v2, 2);
    assert_eq!(
        registry.subject_versions(&subject.0).await.unwrap(),
        vec![1, 2]
    );
    let first = registry.schema_version(&subject.0, Some(1)).await.unwrap();
    assert_eq!(
        (first.version, first.id, first.schema_type),
        (1, id1, SchemaType::Avro)
    );
    assert_eq!(
        registry.compatibility_level(&subject.0).await.unwrap(),
        "BACKWARD"
    );

    let latest = registry.latest_schemas().await.unwrap();
    let ours = latest
        .iter()
        .find(|s| s.subject == subject.0)
        .expect("listed");
    assert_eq!(ours.version, 2);

    // Avro through a real registry: encode by id, decode by id.
    let bytes = registry
        .encode_avro(ours.id, &json!({"id": "o-1", "amount": 9.5}))
        .await
        .unwrap();
    let decoded = registry.decode(&bytes).await.unwrap();
    assert_eq!(decoded.result.unwrap(), json!({"id": "o-1", "amount": 9.5}));

    // In verbose mode the registry reports invalid schemas as incompatible, with the reason.
    let invalid = registry
        .check_compatibility(&subject.0, SchemaType::Avro, "{not avro")
        .await
        .unwrap();
    assert!(!invalid.compatible);
    assert!(
        invalid.messages[0].starts_with("Invalid schema"),
        "{:?}",
        invalid.messages
    );

    subject.delete().await;
}
