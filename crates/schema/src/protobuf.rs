//! Protobuf: compiling `.proto` sources and converting messages to and from JSON.
//!
//! Sources are compiled in-process with protox, so no `protoc` is needed.
//! Google's well-known types (`google/protobuf/timestamp.proto`, ...) are
//! always available as imports.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use prost_reflect::{
    DescriptorPool, DeserializeOptions, DynamicMessage, MessageDescriptor, SerializeOptions,
};
use protox::file::{
    ChainFileResolver, File, FileResolver, GoogleFileResolver, IncludeFileResolver,
};

/// Name given to a registry schema's own source when compiling it.
const MAIN_FILE: &str = "kaffy-registry-schema.proto";

/// A compiled set of `.proto` files.
#[derive(Debug, Clone)]
pub struct ProtoSchema {
    pool: DescriptorPool,
    /// The file whose messages message-index paths refer to.
    main: Option<String>,
}

/// Serves sources held in memory, by import name.
struct Sources(HashMap<String, String>);

impl FileResolver for Sources {
    fn open_file(&self, name: &str) -> Result<File, protox::Error> {
        match self.0.get(name) {
            Some(source) => File::from_source(name, source),
            None => Err(protox::Error::file_not_found(name)),
        }
    }
}

impl ProtoSchema {
    /// Compiles a registry schema: `main` is its source, `imports` the sources
    /// of the schemas it references, by the name it imports them with.
    pub fn compile(main: &str, imports: &[(String, String)]) -> Result<Self, String> {
        let mut sources: HashMap<String, String> = imports.iter().cloned().collect();
        sources.insert(MAIN_FILE.to_owned(), main.to_owned());
        let mut resolver = ChainFileResolver::new();
        resolver.add(Sources(sources));
        resolver.add(GoogleFileResolver::new());
        let mut compiler = protox::Compiler::with_file_resolver(resolver);
        compiler.include_imports(true);
        compiler
            .open_file(MAIN_FILE)
            .map_err(|e| format!("invalid Protobuf schema: {e}"))?;
        Ok(Self {
            pool: compiler.descriptor_pool(),
            main: Some(MAIN_FILE.to_owned()),
        })
    }

    /// Compiles `.proto` files from disk; imports are looked up in `includes`
    /// (and next to each file).
    pub fn compile_files(files: &[PathBuf], includes: &[PathBuf]) -> Result<Self, String> {
        let mut resolver = ChainFileResolver::new();
        let mut dirs: Vec<&Path> = includes.iter().map(PathBuf::as_path).collect();
        for file in files {
            if let Some(parent) = file.parent() {
                dirs.push(parent);
            }
        }
        dirs.dedup();
        for dir in &dirs {
            resolver.add(IncludeFileResolver::new(dir.to_path_buf()));
        }
        resolver.add(GoogleFileResolver::new());
        let mut compiler = protox::Compiler::with_file_resolver(resolver);
        compiler.include_imports(true);
        for file in files {
            compiler
                .open_file(file)
                .map_err(|e| format!("{}: {e}", file.display()))?;
        }
        Ok(Self {
            pool: compiler.descriptor_pool(),
            main: None,
        })
    }

    /// A message by its full name, e.g. `acme.Order` or `acme.Order.Item`.
    pub fn message(&self, name: &str) -> Result<MessageDescriptor, String> {
        self.pool.get_message_by_name(name).ok_or_else(|| {
            let known: Vec<String> = self
                .pool
                .all_messages()
                .filter(|m| !m.full_name().starts_with("google.protobuf."))
                .map(|m| m.full_name().to_owned())
                .take(8)
                .collect();
            format!(
                "no message {name} in the schema (it has: {})",
                known.join(", ")
            )
        })
    }

    /// The message a Confluent message-index path points to: the first index
    /// selects a top-level message of the main file, the rest nested ones.
    pub fn message_at(&self, indexes: &[i32]) -> Result<MessageDescriptor, String> {
        let main = self
            .main
            .as_deref()
            .ok_or("message indexes need a registry schema")?;
        let file = self
            .pool
            .get_file_by_name(main)
            .ok_or("schema file missing")?;
        let not_found = || format!("the schema has no message at index path {indexes:?}");
        let (&first, rest) = indexes.split_first().ok_or_else(not_found)?;
        let mut message = file.messages().nth(first as usize).ok_or_else(not_found)?;
        for &i in rest {
            let child = message.child_messages().nth(i as usize);
            message = child.ok_or_else(not_found)?;
        }
        Ok(message)
    }

    /// The message-index path of a message in the main file (inverse of `message_at`).
    pub fn indexes_of(&self, message: &MessageDescriptor) -> Result<Vec<i32>, String> {
        let main = self
            .main
            .as_deref()
            .ok_or("message indexes need a registry schema")?;
        if message.parent_file().name() != main {
            return Err(format!(
                "{} is not defined in this schema",
                message.full_name()
            ));
        }
        let mut path = Vec::new();
        let mut current = message.clone();
        loop {
            match current.parent_message() {
                Some(parent) => {
                    let i = parent
                        .child_messages()
                        .position(|m| m == current)
                        .expect("child of its parent");
                    path.push(i as i32);
                    current = parent;
                }
                None => {
                    let i = current
                        .parent_file()
                        .messages()
                        .position(|m| m == current)
                        .expect("message of its file");
                    path.push(i as i32);
                    break;
                }
            }
        }
        path.reverse();
        Ok(path)
    }
}

/// Decodes a message to JSON, with the field names used in the `.proto` file.
pub fn decode(message: &MessageDescriptor, bytes: &[u8]) -> Result<serde_json::Value, String> {
    let decoded = DynamicMessage::decode(message.clone(), bytes)
        .map_err(|e| format!("Protobuf decoding failed ({}): {e}", message.full_name()))?;
    let options = SerializeOptions::new().use_proto_field_name(true);
    decoded
        .serialize_with_options(serde_json::value::Serializer, &options)
        .map_err(|e| format!("Protobuf to JSON failed: {e}"))
}

/// Encodes JSON (proto field names or JSON names) as a message.
pub fn encode(message: &MessageDescriptor, json: &serde_json::Value) -> Result<Vec<u8>, String> {
    let options = DeserializeOptions::new().deny_unknown_fields(true);
    let decoded = DynamicMessage::deserialize_with_options(message.clone(), json, &options)
        .map_err(|e| format!("does not match {}: {e}", message.full_name()))?;
    Ok(protox::prost::Message::encode_to_vec(&decoded))
}

/// Reads the message-index array of the Confluent Protobuf wire format and
/// returns it with the rest of the payload. `[0]` is written as a single 0.
pub fn read_indexes(body: &[u8]) -> Result<(Vec<i32>, &[u8]), String> {
    let mut rest = body;
    let count = read_zigzag(&mut rest)?;
    if count == 0 {
        return Ok((vec![0], rest));
    }
    if !(1..=64).contains(&count) {
        return Err(format!("invalid message index count {count}"));
    }
    let mut indexes = Vec::with_capacity(count as usize);
    for _ in 0..count {
        indexes.push(read_zigzag(&mut rest)? as i32);
    }
    Ok((indexes, rest))
}

pub fn write_indexes(indexes: &[i32]) -> Vec<u8> {
    if indexes == [0] {
        return vec![0];
    }
    let mut out = Vec::new();
    write_zigzag(&mut out, indexes.len() as i64);
    for &i in indexes {
        write_zigzag(&mut out, i as i64);
    }
    out
}

fn read_zigzag(buf: &mut &[u8]) -> Result<i64, String> {
    let mut value: u64 = 0;
    for shift in (0..64).step_by(7) {
        let (&byte, rest) = buf.split_first().ok_or("truncated message indexes")?;
        *buf = rest;
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok((value >> 1) as i64 ^ -((value & 1) as i64));
        }
    }
    Err("invalid varint in message indexes".to_owned())
}

fn write_zigzag(out: &mut Vec<u8>, value: i64) {
    let mut v = ((value << 1) ^ (value >> 63)) as u64;
    loop {
        let byte = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const ORDERS: &str = r#"
        syntax = "proto3";
        package acme;
        import "google/protobuf/timestamp.proto";
        import "common/money.proto";

        message Order {
          string order_id = 1;
          common.Money total = 2;
          repeated Item items = 3;
          google.protobuf.Timestamp placed_at = 4;
          message Item { string sku = 1; int32 qty = 2; }
        }
        message Refund { string order_id = 1; }
    "#;
    const MONEY: &str = r#"
        syntax = "proto3";
        package common;
        message Money { string currency = 1; int64 units = 2; }
    "#;

    fn schema() -> ProtoSchema {
        ProtoSchema::compile(
            ORDERS,
            &[("common/money.proto".to_owned(), MONEY.to_owned())],
        )
        .unwrap()
    }

    #[test]
    fn index_paths_select_nested_messages() {
        let s = schema();
        assert_eq!(s.message_at(&[0]).unwrap().full_name(), "acme.Order");
        assert_eq!(s.message_at(&[1]).unwrap().full_name(), "acme.Refund");
        assert_eq!(
            s.message_at(&[0, 0]).unwrap().full_name(),
            "acme.Order.Item"
        );
        assert!(s.message_at(&[5]).is_err());
        let item = s.message("acme.Order.Item").unwrap();
        assert_eq!(s.indexes_of(&item).unwrap(), vec![0, 0]);
        assert_eq!(
            s.indexes_of(&s.message("acme.Refund").unwrap()).unwrap(),
            vec![1]
        );
        assert!(
            s.message("acme.Nope").unwrap_err().contains("acme.Order"),
            "lists what exists"
        );
    }

    #[test]
    fn json_round_trip_with_imports_and_well_known_types() {
        let s = schema();
        let order = s.message("acme.Order").unwrap();
        let json = json!({
            "order_id": "o-1",
            "total": {"currency": "EUR", "units": "12"},
            "items": [{"sku": "A", "qty": 2}],
            "placed_at": "2026-10-13T09:30:00Z"
        });
        let bytes = encode(&order, &json).unwrap();
        assert_eq!(decode(&order, &bytes).unwrap(), json);
        assert!(
            encode(&order, &json!({"nope": 1}))
                .unwrap_err()
                .contains("acme.Order")
        );
    }

    #[test]
    fn message_indexes_wire_format() {
        assert_eq!(write_indexes(&[0]), vec![0]);
        assert_eq!(read_indexes(&[0, 9]).unwrap(), (vec![0], &[9][..]));
        // [1, 0]: count 2, then 1 and 0, all zigzag.
        assert_eq!(write_indexes(&[1, 0]), vec![4, 2, 0]);
        assert_eq!(read_indexes(&[4, 2, 0, 7]).unwrap(), (vec![1, 0], &[7][..]));
        let long = write_indexes(&[300]);
        assert_eq!(read_indexes(&long).unwrap().0, vec![300]);
        assert!(read_indexes(&[]).is_err());
    }

    #[test]
    fn invalid_sources_report_the_problem() {
        let err = ProtoSchema::compile("syntax = \"proto3\"; message A { strin x = 1; }", &[])
            .unwrap_err();
        assert!(err.starts_with("invalid Protobuf schema"), "{err}");
        let missing =
            ProtoSchema::compile("syntax = \"proto3\"; import \"other.proto\";", &[]).unwrap_err();
        assert!(missing.contains("other.proto"), "{missing}");
    }
}
