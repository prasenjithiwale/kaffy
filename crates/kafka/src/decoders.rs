//! Everything that turns record bytes into JSON: the Schema Registry, and
//! local `.proto` files for topics without one.

use std::collections::HashMap;
use std::sync::Arc;

use kaffy_config::ProtobufTopic;
use kaffy_schema::protobuf::{self, ProtoSchema};
use kaffy_schema::{Decoded, Registry, SchemaType};
use prost_reflect::MessageDescriptor;

use crate::Record;

/// A topic's compiled `.proto` files and its key and value message types.
#[derive(Debug)]
pub(crate) struct LocalProto {
    pub schema: ProtoSchema,
    pub key: Option<MessageDescriptor>,
    pub value: Option<MessageDescriptor>,
}

#[derive(Clone, Default)]
pub(crate) struct Decoders {
    pub registry: Option<Arc<Registry>>,
    /// By topic.
    pub local: Arc<HashMap<String, LocalProto>>,
}

impl Decoders {
    /// Compiles every mapping up front, so mistakes show at startup.
    pub fn new(
        registry: Option<Arc<Registry>>,
        mappings: &[ProtobufTopic],
    ) -> Result<Self, String> {
        let mut local = HashMap::new();
        for m in mappings {
            let schema = ProtoSchema::compile_files(&m.files, &m.include)
                .map_err(|e| format!("protobuf for topic {}: {e}", m.topic))?;
            let find = |name: &Option<String>| {
                name.as_deref()
                    .map(|n| schema.message(n))
                    .transpose()
                    .map_err(|e| format!("protobuf for topic {}: {e}", m.topic))
            };
            let (key, value) = (find(&m.key)?, find(&m.value)?);
            local.insert(m.topic.clone(), LocalProto { schema, key, value });
        }
        Ok(Self {
            registry,
            local: Arc::new(local),
        })
    }

    pub fn is_active(&self) -> bool {
        self.registry.is_some() || !self.local.is_empty()
    }

    /// Fills in `key_decoded` and `value_decoded`: the registry handles its wire
    /// format; plain payloads on a mapped topic use the local `.proto` files.
    pub async fn decode(&self, record: &mut Record) {
        if let Some(registry) = &self.registry {
            if let Some(key) = &record.key {
                record.key_decoded = registry.decode(key).await;
            }
            if let Some(value) = &record.value {
                record.value_decoded = registry.decode(value).await;
            }
        }
        let Some(local) = self.local.get(&record.topic) else {
            return;
        };
        if record.key_decoded.is_none() {
            record.key_decoded = decode_local(local.key.as_ref(), record.key.as_deref());
        }
        if record.value_decoded.is_none() {
            record.value_decoded = decode_local(local.value.as_ref(), record.value.as_deref());
        }
    }

    /// Encodes JSON for `topic` with a local message type.
    pub fn encode_local(
        &self,
        topic: &str,
        message: &str,
        json: &serde_json::Value,
    ) -> Result<Vec<u8>, String> {
        let local = self
            .local
            .get(topic)
            .ok_or_else(|| format!("no local .proto files are configured for topic {topic}"))?;
        protobuf::encode(&local.schema.message(message)?, json)
    }
}

fn decode_local(message: Option<&MessageDescriptor>, bytes: Option<&[u8]>) -> Option<Decoded> {
    let (message, bytes) = (message?, bytes?);
    Some(Decoded {
        schema_id: 0,
        schema_type: Some(SchemaType::Protobuf),
        message: Some(message.full_name().to_owned()),
        result: protobuf::decode(message, bytes),
    })
}
