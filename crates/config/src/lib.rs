//! Cluster profiles loaded from a TOML config file.
//!
//! ```toml
//! default_profile = "local"
//!
//! [profiles.local]
//! bootstrap_servers = "localhost:9092"
//!
//! [profiles.staging]
//! bootstrap_servers = "broker-1:9094,broker-2:9094"
//! security_protocol = "sasl_ssl"
//! sasl = { mechanism = "SCRAM-SHA-512", username = "svc-kaffy", password_env = "KAFFY_STAGING_PASSWORD" }
//! tls = { ca_location = "/etc/ssl/staging-ca.pem" }
//! ```

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::Deserialize;

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("could not read config file {path}: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("invalid config file {path}: {source}")]
    Parse {
        path: PathBuf,
        #[source]
        source: toml::de::Error,
    },
    #[error("no profile named `{0}` in config")]
    UnknownProfile(String),
    #[error("config has no profiles")]
    NoProfiles,
    #[error("profile `{profile}`: {message}")]
    Invalid { profile: String, message: String },
    #[error("profile `{profile}`: environment variable `{var}` (sasl.password_env) is not set")]
    MissingPasswordEnv { profile: String, var: String },
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub default_profile: Option<String>,
    /// Color theme: `dark`, `light` or `terminal` (16 colors). Detected when unset.
    pub theme: Option<String>,
    /// Command name to key or keys, replacing that command's default keys.
    #[serde(default, deserialize_with = "keys_table")]
    pub keys: BTreeMap<String, Vec<String>>,
    #[serde(default)]
    pub profiles: BTreeMap<String, Profile>,
}

/// `pause = "p"` or `pause = ["p", "space"]`.
fn keys_table<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<BTreeMap<String, Vec<String>>, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Keys {
        One(String),
        Many(Vec<String>),
    }
    let table = BTreeMap::<String, Keys>::deserialize(deserializer)?;
    Ok(table
        .into_iter()
        .map(|(command, keys)| {
            let keys = match keys {
                Keys::One(key) => vec![key],
                Keys::Many(keys) => keys,
            };
            (command, keys)
        })
        .collect())
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Profile {
    /// Filled in from the `[profiles.<name>]` key.
    #[serde(skip)]
    pub name: String,
    pub bootstrap_servers: String,
    #[serde(default)]
    pub security_protocol: SecurityProtocol,
    pub sasl: Option<Sasl>,
    /// Amazon MSK IAM authentication, instead of `[sasl]`.
    pub aws_msk_iam: Option<AwsMskIam>,
    pub tls: Option<Tls>,
    /// Blocks every write operation (produce, offset reset, delete, ...).
    #[serde(default = "default_true")]
    pub read_only: bool,
    /// Marks a production cluster: destructive operations need typed confirmation.
    #[serde(default)]
    pub prod: bool,
    pub schema_registry: Option<SchemaRegistry>,
    /// Raw librdkafka properties, applied last so they can override anything above.
    #[serde(default)]
    pub librdkafka: BTreeMap<String, String>,
    /// Topics whose messages are plain Protobuf (no Schema Registry), with the
    /// `.proto` files to decode them with.
    #[serde(default)]
    pub protobuf: Vec<ProtobufTopic>,
    /// Shows only the topics matching these patterns (see [`NameFilter`]).
    #[serde(default, deserialize_with = "name_filter")]
    pub topics: NameFilter,
    /// Shows only the consumer groups matching these patterns.
    #[serde(default, deserialize_with = "name_filter")]
    pub groups: NameFilter,
}

fn name_filter<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<NameFilter, D::Error> {
    let patterns = Vec::<String>::deserialize(deserializer)?;
    NameFilter::new(&patterns).map_err(serde::de::Error::custom)
}

/// Which topic or group names to show: glob patterns where `*` matches any
/// run of characters and `?` one character. A leading `!` excludes. With no
/// plain patterns everything is shown except the excluded names.
///
/// This narrows what kaffy shows; it is not access control (use Kafka ACLs).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NameFilter {
    include: Vec<String>,
    exclude: Vec<String>,
}

impl NameFilter {
    pub fn new(patterns: &[impl AsRef<str>]) -> Result<Self, String> {
        let mut filter = Self::default();
        for pattern in patterns {
            let pattern = pattern.as_ref().trim();
            match pattern.strip_prefix('!') {
                Some("") => return Err("`!` needs a pattern after it".to_owned()),
                Some(excluded) => filter.exclude.push(excluded.to_owned()),
                None if pattern.is_empty() => return Err("empty pattern".to_owned()),
                None => filter.include.push(pattern.to_owned()),
            }
        }
        Ok(filter)
    }

    /// `orders-*, payments, !*.dlq`, as `--topics` takes it.
    pub fn parse_list(list: &str) -> Result<Self, String> {
        let patterns: Vec<&str> = list.split(',').map(str::trim).collect();
        Self::new(&patterns)
    }

    /// True when the filter lets every name through.
    pub fn is_empty(&self) -> bool {
        self.include.is_empty() && self.exclude.is_empty()
    }

    pub fn matches(&self, name: &str) -> bool {
        (self.include.is_empty() || self.include.iter().any(|p| glob(p, name)))
            && !self.exclude.iter().any(|p| glob(p, name))
    }
}

impl std::fmt::Display for NameFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let excluded = self.exclude.iter().map(|p| format!("!{p}"));
        let all: Vec<String> = self.include.iter().cloned().chain(excluded).collect();
        f.write_str(&all.join(", "))
    }
}

/// Case-sensitive, like Kafka names. `*` backtracks to the last star only,
/// which is enough for globs and stays linear in practice.
fn glob(pattern: &str, name: &str) -> bool {
    let p: Vec<char> = pattern.chars().collect();
    let n: Vec<char> = name.chars().collect();
    let (mut pi, mut ni) = (0, 0);
    let mut star: Option<(usize, usize)> = None;
    while ni < n.len() {
        if pi < p.len() && (p[pi] == '?' || p[pi] == n[ni]) {
            pi += 1;
            ni += 1;
        } else if pi < p.len() && p[pi] == '*' {
            star = Some((pi, ni));
            pi += 1;
        } else if let Some((sp, sn)) = star {
            pi = sp + 1;
            ni = sn + 1;
            star = Some((sp, sn + 1));
        } else {
            return false;
        }
    }
    p[pi..].iter().all(|&c| c == '*')
}

/// Amazon MSK IAM: SASL/OAUTHBEARER with tokens signed by AWS credentials.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AwsMskIam {
    /// The cluster's AWS region, e.g. `eu-west-1`.
    pub region: String,
    /// A profile from `~/.aws/config`; otherwise the environment's credentials
    /// or the `default` profile.
    pub profile: Option<String>,
}

/// How to decode one topic's Protobuf messages from local `.proto` files.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProtobufTopic {
    pub topic: String,
    /// `.proto` files to compile; relative paths are resolved against the
    /// config file's directory.
    pub files: Vec<PathBuf>,
    /// Directories to look for imports in.
    #[serde(default)]
    pub include: Vec<PathBuf>,
    /// Full name of the value message, e.g. `acme.Order`.
    pub value: Option<String>,
    /// Full name of the key message, if keys are Protobuf too.
    pub key: Option<String>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SecurityProtocol {
    #[default]
    Plaintext,
    Ssl,
    SaslPlaintext,
    SaslSsl,
}

impl SecurityProtocol {
    fn as_librdkafka(self) -> &'static str {
        match self {
            Self::Plaintext => "plaintext",
            Self::Ssl => "ssl",
            Self::SaslPlaintext => "sasl_plaintext",
            Self::SaslSsl => "sasl_ssl",
        }
    }

    fn uses_sasl(self) -> bool {
        matches!(self, Self::SaslPlaintext | Self::SaslSsl)
    }

    fn uses_tls(self) -> bool {
        matches!(self, Self::Ssl | Self::SaslSsl)
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Sasl {
    pub mechanism: SaslMechanism,
    pub username: String,
    /// Name of the environment variable that holds the password.
    pub password_env: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub enum SaslMechanism {
    #[serde(rename = "PLAIN")]
    Plain,
    #[serde(rename = "SCRAM-SHA-256")]
    ScramSha256,
    #[serde(rename = "SCRAM-SHA-512")]
    ScramSha512,
}

impl SaslMechanism {
    fn as_librdkafka(self) -> &'static str {
        match self {
            Self::Plain => "PLAIN",
            Self::ScramSha256 => "SCRAM-SHA-256",
            Self::ScramSha512 => "SCRAM-SHA-512",
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Tls {
    pub ca_location: Option<PathBuf>,
    pub certificate_location: Option<PathBuf>,
    pub key_location: Option<PathBuf>,
    pub key_password_env: Option<String>,
}

/// Confluent-compatible Schema Registry, used to decode Avro payloads.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaRegistry {
    pub url: String,
    /// Basic auth (Confluent Cloud: API key and secret).
    pub username: Option<String>,
    pub password_env: Option<String>,
}

/// A Schema Registry with its secret resolved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedSchemaRegistry {
    pub url: String,
    pub basic_auth: Option<(String, String)>,
}

fn default_true() -> bool {
    true
}

impl Config {
    /// `$KAFFY_CONFIG`, else `~/.config/kaffy/config.toml`.
    pub fn default_path() -> Option<PathBuf> {
        std::env::var_os("KAFFY_CONFIG")
            .map(PathBuf::from)
            .or_else(|| dirs::home_dir().map(|home| home.join(".config/kaffy/config.toml")))
    }

    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        let text = std::fs::read_to_string(path).map_err(|source| ConfigError::Read {
            path: path.to_owned(),
            source,
        })?;
        let mut config = Self::from_toml_str(&text).map_err(|source| ConfigError::Parse {
            path: path.to_owned(),
            source,
        })?;
        let base = path.parent().unwrap_or(Path::new("."));
        for profile in config.profiles.values_mut() {
            for mapping in &mut profile.protobuf {
                for p in mapping.files.iter_mut().chain(mapping.include.iter_mut()) {
                    if p.is_relative() {
                        *p = base.join(&*p);
                    }
                }
            }
        }
        for profile in config.profiles.values() {
            profile.validate()?;
        }
        Ok(config)
    }

    pub fn from_toml_str(text: &str) -> Result<Self, toml::de::Error> {
        let mut config: Self = toml::from_str(text)?;
        for (name, profile) in &mut config.profiles {
            profile.name = name.clone();
        }
        Ok(config)
    }

    /// Picks `name`, else `default_profile`, else the first profile.
    pub fn profile(&self, name: Option<&str>) -> Result<&Profile, ConfigError> {
        match name.or(self.default_profile.as_deref()) {
            Some(name) => self
                .profiles
                .get(name)
                .ok_or_else(|| ConfigError::UnknownProfile(name.to_owned())),
            None => self.profiles.values().next().ok_or(ConfigError::NoProfiles),
        }
    }
}

impl Profile {
    /// A plaintext, read-only profile for `--bootstrap` on the command line.
    pub fn adhoc(bootstrap_servers: impl Into<String>) -> Self {
        Self {
            name: "adhoc".to_owned(),
            bootstrap_servers: bootstrap_servers.into(),
            security_protocol: SecurityProtocol::Plaintext,
            sasl: None,
            aws_msk_iam: None,
            tls: None,
            read_only: true,
            prod: false,
            schema_registry: None,
            librdkafka: BTreeMap::new(),
            protobuf: Vec::new(),
            topics: NameFilter::default(),
            groups: NameFilter::default(),
        }
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        let invalid = |message: &str| ConfigError::Invalid {
            profile: self.name.clone(),
            message: message.to_owned(),
        };
        if self.bootstrap_servers.trim().is_empty() {
            return Err(invalid("bootstrap_servers is empty"));
        }
        if let Some(iam) = &self.aws_msk_iam {
            if self.sasl.is_some() {
                return Err(invalid("use either [sasl] or aws_msk_iam, not both"));
            }
            if self.security_protocol != SecurityProtocol::SaslSsl {
                return Err(invalid(
                    "aws_msk_iam needs security_protocol = \"sasl_ssl\" (MSK's IAM listener, port 9098)",
                ));
            }
            if iam.region.trim().is_empty() {
                return Err(invalid("aws_msk_iam needs a region"));
            }
        } else {
            match (self.security_protocol.uses_sasl(), self.sasl.is_some()) {
                (true, false) => {
                    return Err(invalid(
                        "security_protocol needs a [sasl] section (or aws_msk_iam)",
                    ));
                }
                (false, true) => {
                    return Err(invalid("[sasl] is set but security_protocol is not sasl_*"));
                }
                _ => {}
            }
        }
        for mapping in &self.protobuf {
            if mapping.files.is_empty() || (mapping.value.is_none() && mapping.key.is_none()) {
                return Err(invalid(&format!(
                    "protobuf mapping for topic {} needs files and a value or key message",
                    mapping.topic
                )));
            }
        }
        if self.tls.is_some() && !self.security_protocol.uses_tls() {
            return Err(invalid(
                "[tls] is set but security_protocol is not ssl or sasl_ssl",
            ));
        }
        if let Some(sr) = &self.schema_registry {
            if sr.username.is_some() != sr.password_env.is_some() {
                return Err(invalid(
                    "schema_registry needs both username and password_env, or neither",
                ));
            }
        }
        Ok(())
    }

    /// Resolves secrets from the environment and returns the librdkafka client properties.
    pub fn librdkafka_properties(&self) -> Result<Vec<(String, String)>, ConfigError> {
        self.validate()?;
        let mut props = vec![
            (
                "bootstrap.servers".to_owned(),
                self.bootstrap_servers.clone(),
            ),
            (
                "security.protocol".to_owned(),
                self.security_protocol.as_librdkafka().to_owned(),
            ),
        ];
        let mut push = |key: &str, value: String| props.push((key.to_owned(), value));

        if self.aws_msk_iam.is_some() {
            // kaffy signs the tokens and hands them to each client.
            push("sasl.mechanism", "OAUTHBEARER".to_owned());
        }
        if let Some(sasl) = &self.sasl {
            push("sasl.mechanism", sasl.mechanism.as_librdkafka().to_owned());
            push("sasl.username", sasl.username.clone());
            push("sasl.password", self.env_secret(&sasl.password_env)?);
        }
        if let Some(tls) = &self.tls {
            let path = |p: &PathBuf| p.to_string_lossy().into_owned();
            if let Some(ca) = &tls.ca_location {
                push("ssl.ca.location", path(ca));
            }
            if let Some(cert) = &tls.certificate_location {
                push("ssl.certificate.location", path(cert));
            }
            if let Some(key) = &tls.key_location {
                push("ssl.key.location", path(key));
            }
            if let Some(var) = &tls.key_password_env {
                push("ssl.key.password", self.env_secret(var)?);
            }
        }
        for (key, value) in &self.librdkafka {
            push(key, value.clone());
        }
        Ok(props)
    }

    /// The Schema Registry settings with the password read from the environment.
    pub fn resolved_schema_registry(&self) -> Result<Option<ResolvedSchemaRegistry>, ConfigError> {
        let Some(sr) = &self.schema_registry else {
            return Ok(None);
        };
        let basic_auth = match (&sr.username, &sr.password_env) {
            (Some(user), Some(var)) => Some((user.clone(), self.env_secret(var)?)),
            _ => None,
        };
        Ok(Some(ResolvedSchemaRegistry {
            url: sr.url.trim_end_matches('/').to_owned(),
            basic_auth,
        }))
    }

    fn env_secret(&self, var: &str) -> Result<String, ConfigError> {
        std::env::var(var).map_err(|_| ConfigError::MissingPasswordEnv {
            profile: self.name.clone(),
            var: var.to_owned(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = r#"
default_profile = "local"

[profiles.local]
bootstrap_servers = "localhost:9092"

[profiles.staging]
bootstrap_servers = "b1:9094"
security_protocol = "sasl_ssl"
read_only = false
prod = true
sasl = { mechanism = "SCRAM-SHA-512", username = "svc", password_env = "KAFFY_TEST_STAGING_PW" }
tls = { ca_location = "/tmp/ca.pem" }
librdkafka = { "socket.timeout.ms" = "5000" }
schema_registry = { url = "https://sr:8081/", username = "key", password_env = "KAFFY_TEST_SR_PW" }
"#;

    fn get<'a>(props: &'a [(String, String)], key: &str) -> Option<&'a str> {
        props
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }

    #[test]
    fn parses_profiles_and_defaults() {
        let config = Config::from_toml_str(SAMPLE).unwrap();
        let local = config.profile(None).unwrap();
        assert_eq!(local.name, "local");
        assert!(local.read_only, "read_only defaults to true");
        assert_eq!(local.security_protocol, SecurityProtocol::Plaintext);

        let staging = config.profile(Some("staging")).unwrap();
        assert!(!staging.read_only);
        assert!(staging.prod);
        assert!(matches!(
            config.profile(Some("nope")),
            Err(ConfigError::UnknownProfile(_))
        ));
    }

    #[test]
    fn builds_sasl_ssl_properties() {
        let config = Config::from_toml_str(SAMPLE).unwrap();
        let staging = config.profile(Some("staging")).unwrap();

        // SAFETY: this is the only test that touches this variable.
        unsafe { std::env::remove_var("KAFFY_TEST_STAGING_PW") };
        assert!(matches!(
            staging.librdkafka_properties(),
            Err(ConfigError::MissingPasswordEnv { .. })
        ));

        unsafe { std::env::set_var("KAFFY_TEST_STAGING_PW", "s3cret") };
        let props = staging.librdkafka_properties().unwrap();
        assert_eq!(get(&props, "security.protocol"), Some("sasl_ssl"));
        assert_eq!(get(&props, "sasl.mechanism"), Some("SCRAM-SHA-512"));
        assert_eq!(get(&props, "sasl.password"), Some("s3cret"));
        assert_eq!(get(&props, "ssl.ca.location"), Some("/tmp/ca.pem"));
        assert_eq!(get(&props, "socket.timeout.ms"), Some("5000"));

        unsafe { std::env::set_var("KAFFY_TEST_SR_PW", "sr-secret") };
        let sr = staging.resolved_schema_registry().unwrap().unwrap();
        assert_eq!(sr.url, "https://sr:8081");
        assert_eq!(
            sr.basic_auth,
            Some(("key".to_owned(), "sr-secret".to_owned()))
        );
    }

    #[test]
    fn msk_iam_uses_oauthbearer() {
        let config = Config::from_toml_str(
            r#"
[profiles.msk]
bootstrap_servers = "b-1.msk.eu-west-1.amazonaws.com:9098"
security_protocol = "sasl_ssl"
aws_msk_iam = { region = "eu-west-1", profile = "dev" }

[profiles.both]
bootstrap_servers = "b:9098"
security_protocol = "sasl_ssl"
aws_msk_iam = { region = "eu-west-1" }
sasl = { mechanism = "PLAIN", username = "u", password_env = "X" }

[profiles.plaintext]
bootstrap_servers = "b:9098"
aws_msk_iam = { region = "eu-west-1" }
"#,
        )
        .unwrap();
        let msk = config.profile(Some("msk")).unwrap();
        let props = msk.librdkafka_properties().unwrap();
        assert_eq!(get(&props, "sasl.mechanism"), Some("OAUTHBEARER"));
        assert_eq!(get(&props, "security.protocol"), Some("sasl_ssl"));
        assert_eq!(
            msk.aws_msk_iam.as_ref().unwrap().profile.as_deref(),
            Some("dev")
        );
        for name in ["both", "plaintext"] {
            assert!(
                config.profile(Some(name)).unwrap().validate().is_err(),
                "{name} is inconsistent"
            );
        }
    }

    #[test]
    fn rejects_inconsistent_security() {
        let config = Config::from_toml_str(
            r#"
[profiles.bad]
bootstrap_servers = "b:9092"
security_protocol = "sasl_plaintext"
"#,
        )
        .unwrap();
        assert!(matches!(
            config.profile(None).unwrap().validate(),
            Err(ConfigError::Invalid { .. })
        ));
    }

    #[test]
    fn example_config_is_valid() {
        let config = Config::from_toml_str(include_str!("../../../config.example.toml")).unwrap();
        assert_eq!(config.profiles.len(), 5);
        for profile in config.profiles.values() {
            profile.validate().unwrap();
        }
        assert_eq!(config.profile(None).unwrap().name, "local");
    }

    #[test]
    fn protobuf_paths_are_relative_to_the_config_file() {
        let dir = std::env::temp_dir().join(format!("kaffy-config-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("config.toml");
        std::fs::write(
            &path,
            r#"
[profiles.local]
bootstrap_servers = "localhost:9092"

[[profiles.local.protobuf]]
topic = "orders"
files = ["proto/orders.proto", "/abs/other.proto"]
include = ["proto"]
value = "acme.Order"
"#,
        )
        .unwrap();
        let config = Config::load(&path).unwrap();
        let mapping = &config.profile(None).unwrap().protobuf[0];
        assert_eq!(mapping.files[0], dir.join("proto/orders.proto"));
        assert_eq!(mapping.files[1], PathBuf::from("/abs/other.proto"));
        assert_eq!(mapping.include[0], dir.join("proto"));
        std::fs::remove_dir_all(&dir).unwrap();

        let missing_message = Config::from_toml_str(
            "[profiles.x]\nbootstrap_servers = \"b\"\n[[profiles.x.protobuf]]\ntopic = \"t\"\nfiles = [\"a.proto\"]\n",
        )
        .unwrap();
        assert!(missing_message.profile(None).unwrap().validate().is_err());
    }

    #[test]
    fn rejects_unknown_keys() {
        let err = Config::from_toml_str("[profiles.x]\nbootstrap_servers = \"b\"\ntypo = 1\n");
        assert!(err.is_err());
    }

    #[test]
    fn globs() {
        assert!(glob("orders", "orders"));
        assert!(!glob("orders", "orders-v2"));
        assert!(glob("orders*", "orders-v2"));
        assert!(glob("*.dlq", "payments.dlq"));
        assert!(!glob("*.dlq", "payments.dlq.old"));
        assert!(glob("a*b*c", "a-x-b-y-b-c"));
        assert!(glob("team-?-*", "team-a-orders"));
        assert!(!glob("team-?-*", "team-ab-orders"));
        assert!(glob("*", ""));
        assert!(!glob("Orders", "orders"), "case-sensitive like Kafka");
    }

    #[test]
    fn name_filter_includes_then_excludes() {
        let filter = NameFilter::parse_list("orders-*, payments, !*.dlq").unwrap();
        assert!(filter.matches("orders-eu"));
        assert!(filter.matches("payments"));
        assert!(!filter.matches("orders-eu.dlq"));
        assert!(!filter.matches("users"));
        assert_eq!(filter.to_string(), "orders-*, payments, !*.dlq");

        let only_excludes = NameFilter::parse_list("!__*").unwrap();
        assert!(only_excludes.matches("orders"));
        assert!(!only_excludes.matches("__consumer_offsets"));

        assert!(NameFilter::default().matches("anything"));
        assert!(NameFilter::default().is_empty());
        assert!(NameFilter::parse_list("orders,").is_err());
        assert!(NameFilter::parse_list("!").is_err());
    }

    #[test]
    fn profile_filters_from_toml() {
        let config = Config::from_toml_str(
            "[profiles.x]\nbootstrap_servers = \"b\"\n\
             topics = [\"orders-*\", \"!*.dlq\"]\ngroups = [\"checkout-*\"]\n",
        )
        .unwrap();
        let profile = config.profile(None).unwrap();
        assert!(profile.topics.matches("orders-eu"));
        assert!(!profile.topics.matches("orders-eu.dlq"));
        assert!(profile.groups.matches("checkout-api"));
        assert!(!profile.groups.matches("billing"));

        let bad =
            Config::from_toml_str("[profiles.x]\nbootstrap_servers = \"b\"\ntopics = [\"\"]\n");
        assert!(bad.is_err());
    }
}
