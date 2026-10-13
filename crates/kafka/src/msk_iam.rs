//! Amazon MSK IAM authentication.
//!
//! MSK's IAM listener speaks SASL/OAUTHBEARER. The token is a SigV4-presigned
//! `kafka-cluster:Connect` URL, base64url-encoded, as AWS's own signer libraries
//! build it. kaffy signs one per client and hands it to librdkafka directly
//! (`rd_kafka_oauthbearer_set_token`), then re-signs on a timer while the client
//! lives, so it works whether or not the client's queue is polled.
//!
//! Credentials come from, in order: the profile named in the config (through
//! the AWS CLI, which handles SSO and assumed roles, else `~/.aws/credentials`);
//! otherwise the `AWS_ACCESS_KEY_ID` environment variables, then the CLI and
//! the credentials file for `$AWS_PROFILE` or `default`.

use std::ffi::{CString, c_char};
use std::process::Command;
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use base64::Engine as _;
use chrono::{DateTime, Utc};
use rdkafka::admin::AdminClient;
use rdkafka::client::{Client, ClientContext};
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext, StreamConsumer};
use rdkafka::producer::{FutureProducer, Producer};
use rdkafka::types::RDKafka;
use sha2::{Digest, Sha256};

/// How long a signed token is valid; AWS's signers use the same.
const TOKEN_LIFETIME: Duration = Duration::from_secs(900);
/// Re-sign well before expiry.
const REFRESH_EVERY: Duration = Duration::from_secs(300);
const SERVICE: &str = "kafka-cluster";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Credentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
    /// When temporary credentials run out.
    pub expires: Option<DateTime<Utc>>,
}

impl Credentials {
    fn fresh(&self) -> bool {
        self.expires
            .is_none_or(|at| at - chrono::Duration::minutes(5) > Utc::now())
    }
}

/// Signs MSK IAM tokens for one profile.
pub struct MskIam {
    region: String,
    profile: Option<String>,
    cached: Mutex<Option<Credentials>>,
}

impl MskIam {
    pub fn new(region: &str, profile: Option<&str>) -> Arc<Self> {
        Arc::new(Self {
            region: region.to_owned(),
            profile: profile.map(str::to_owned),
            cached: Mutex::new(None),
        })
    }

    fn credentials(&self) -> Result<Credentials, String> {
        let mut cached = self.cached.lock().expect("credentials lock");
        if let Some(creds) = cached.as_ref().filter(|c| c.fresh()) {
            return Ok(creds.clone());
        }
        let creds = resolve_credentials(self.profile.as_deref())?;
        *cached = Some(creds.clone());
        Ok(creds)
    }

    /// A token valid from now, and when it expires (epoch ms).
    pub fn token(&self) -> Result<(String, i64), String> {
        let creds = self.credentials()?;
        let now = Utc::now();
        let token = token(&creds, &self.region, now);
        let lifetime = now + chrono::Duration::from_std(TOKEN_LIFETIME).expect("short duration");
        Ok((token, lifetime.timestamp_millis()))
    }

    /// Gives `client` a token now and keeps it fresh until the client is dropped.
    pub fn attach<T: Native + Send + Sync + 'static>(
        self: &Arc<Self>,
        client: &Arc<T>,
    ) -> Result<(), String> {
        self.set_token(client.native())?;
        let signer = Arc::downgrade(self);
        let client: Weak<T> = Arc::downgrade(client);
        std::thread::Builder::new()
            .name("kaffy-msk-iam".into())
            .spawn(move || refresh_loop(signer, client))
            .map_err(|e| format!("starting the token refresher: {e}"))?;
        Ok(())
    }

    fn set_token(&self, client: *mut RDKafka) -> Result<(), String> {
        let (token, lifetime_ms) = match self.token() {
            Ok(token) => token,
            Err(e) => {
                let message = CString::new(e.replace('\0', " ")).unwrap_or_default();
                // SAFETY: `client` is live (the caller holds it) and `message` outlives the call.
                unsafe {
                    rdkafka::bindings::rd_kafka_oauthbearer_set_token_failure(
                        client,
                        message.as_ptr(),
                    )
                };
                return Err(e);
            }
        };
        let token = CString::new(token).expect("base64 has no NUL");
        let principal = CString::new("kaffy").expect("no NUL");
        let mut error = [0 as c_char; 512];
        // SAFETY: `client` is live, the strings outlive the call, and `error`
        // is a writable buffer of the size we pass. No extensions are passed.
        let code = unsafe {
            rdkafka::bindings::rd_kafka_oauthbearer_set_token(
                client,
                token.as_ptr(),
                lifetime_ms,
                principal.as_ptr(),
                std::ptr::null_mut(),
                0,
                error.as_mut_ptr(),
                error.len(),
            )
        };
        if code == rdkafka::bindings::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR {
            Ok(())
        } else {
            // SAFETY: librdkafka NUL-terminates the message within the buffer.
            let message = unsafe { std::ffi::CStr::from_ptr(error.as_ptr()) };
            Err(format!(
                "setting the MSK IAM token: {}",
                message.to_string_lossy()
            ))
        }
    }
}

fn refresh_loop<T: Native>(signer: Weak<MskIam>, client: Weak<T>) {
    let mut last = Instant::now();
    loop {
        // Wake often enough to notice a dropped client quickly.
        std::thread::sleep(Duration::from_secs(1));
        let (Some(signer), Some(client)) = (signer.upgrade(), client.upgrade()) else {
            return;
        };
        if last.elapsed() < REFRESH_EVERY {
            continue;
        }
        last = Instant::now();
        if let Err(e) = signer.set_token(client.native()) {
            tracing::warn!("MSK IAM token refresh failed: {e}");
        }
    }
}

/// Signs tokens for a profile's clients when it uses MSK IAM; does nothing otherwise.
#[derive(Clone, Default)]
pub struct Auth(Option<Arc<MskIam>>);

impl Auth {
    pub fn for_profile(profile: &kaffy_config::Profile) -> Self {
        Self(
            profile
                .aws_msk_iam
                .as_ref()
                .map(|iam| MskIam::new(&iam.region, iam.profile.as_deref())),
        )
    }

    /// Call right after creating a client, before it talks to the cluster.
    pub fn attach<T: Native + Send + Sync + 'static>(&self, client: &Arc<T>) -> Result<(), String> {
        match &self.0 {
            Some(iam) => iam.attach(client),
            None => Ok(()),
        }
    }
}

/// A librdkafka handle kaffy can give tokens to.
pub trait Native {
    fn native(&self) -> *mut RDKafka;
}

impl<C: ConsumerContext> Native for BaseConsumer<C> {
    fn native(&self) -> *mut RDKafka {
        self.client().native_ptr()
    }
}

impl<C: ConsumerContext + 'static> Native for StreamConsumer<C> {
    fn native(&self) -> *mut RDKafka {
        self.client().native_ptr()
    }
}

impl<C: ClientContext + 'static> Native for FutureProducer<C> {
    fn native(&self) -> *mut RDKafka {
        self.client().native_ptr()
    }
}

impl<C: ClientContext> Native for AdminClient<C> {
    fn native(&self) -> *mut RDKafka {
        let client: &Client<C> = self.inner();
        client.native_ptr()
    }
}

/// The MSK IAM token: a presigned `kafka-cluster:Connect` URL, base64url.
pub fn token(creds: &Credentials, region: &str, now: DateTime<Utc>) -> String {
    let host = format!("kafka.{region}.amazonaws.com");
    let mut url = presign(
        creds,
        &Presign {
            region,
            service: SERVICE,
            host: &host,
            params: vec![("Action".into(), "kafka-cluster:Connect".into())],
            expires: TOKEN_LIFETIME.as_secs(),
            payload_hash: &hex(&Sha256::digest(b"")),
            now,
        },
    );
    // Added after signing, as AWS's signers do.
    url.push_str("&User-Agent=");
    url.push_str(&uri_encode(concat!("kaffy/", env!("CARGO_PKG_VERSION"))));
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(url)
}

pub struct Presign<'a> {
    pub region: &'a str,
    pub service: &'a str,
    pub host: &'a str,
    pub params: Vec<(String, String)>,
    pub expires: u64,
    pub payload_hash: &'a str,
    pub now: DateTime<Utc>,
}

/// A SigV4 presigned `GET https://{host}/?…` URL.
pub fn presign(creds: &Credentials, request: &Presign) -> String {
    let amz_date = request.now.format("%Y%m%dT%H%M%SZ").to_string();
    let date = request.now.format("%Y%m%d").to_string();
    let scope = format!("{date}/{}/{}/aws4_request", request.region, request.service);
    let mut params = request.params.clone();
    params.extend([
        ("X-Amz-Algorithm".into(), "AWS4-HMAC-SHA256".into()),
        (
            "X-Amz-Credential".into(),
            format!("{}/{scope}", creds.access_key_id),
        ),
        ("X-Amz-Date".into(), amz_date.clone()),
        ("X-Amz-Expires".into(), request.expires.to_string()),
        ("X-Amz-SignedHeaders".into(), "host".into()),
    ]);
    if let Some(token) = &creds.session_token {
        params.push(("X-Amz-Security-Token".into(), token.clone()));
    }
    let mut encoded: Vec<(String, String)> = params
        .iter()
        .map(|(k, v)| (uri_encode(k), uri_encode(v)))
        .collect();
    encoded.sort();
    let query = encoded
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&");

    let canonical = format!(
        "GET\n/\n{query}\nhost:{}\n\nhost\n{}",
        request.host, request.payload_hash
    );
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
        hex(&Sha256::digest(canonical.as_bytes()))
    );
    let mut key = hmac_sha256(
        format!("AWS4{}", creds.secret_access_key).as_bytes(),
        date.as_bytes(),
    );
    for part in [request.region, request.service, "aws4_request"] {
        key = hmac_sha256(&key, part.as_bytes());
    }
    let signature = hex(&hmac_sha256(&key, string_to_sign.as_bytes()));
    format!(
        "https://{}/?{query}&X-Amz-Signature={signature}",
        request.host
    )
}

/// RFC 3986 percent-encoding, as SigV4 wants it: everything but A-Z a-z 0-9 - _ . ~
fn uri_encode(text: &str) -> String {
    let mut out = String::new();
    for byte in text.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char)
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    const BLOCK: usize = 64;
    let mut block = [0u8; BLOCK];
    if key.len() > BLOCK {
        block[..32].copy_from_slice(&Sha256::digest(key));
    } else {
        block[..key.len()].copy_from_slice(key);
    }
    let pad = |byte: u8| block.iter().map(|b| b ^ byte).collect::<Vec<u8>>();
    let inner = Sha256::new()
        .chain_update(pad(0x36))
        .chain_update(data)
        .finalize();
    Sha256::new()
        .chain_update(pad(0x5c))
        .chain_update(inner)
        .finalize()
        .to_vec()
}

fn resolve_credentials(profile: Option<&str>) -> Result<Credentials, String> {
    if profile.is_none() {
        if let Some(creds) = from_env() {
            return Ok(creds);
        }
    }
    let name = profile
        .map(str::to_owned)
        .or_else(|| std::env::var("AWS_PROFILE").ok())
        .unwrap_or_else(|| "default".to_owned());
    let cli = from_cli(&name);
    if let Ok(creds) = cli {
        return Ok(creds);
    }
    if let Some(creds) = from_credentials_file(&name) {
        return Ok(creds);
    }
    Err(format!(
        "no AWS credentials for profile `{name}`: set AWS_ACCESS_KEY_ID and \
         AWS_SECRET_ACCESS_KEY, or sign in (aws sso login --profile {name}); \
         the AWS CLI said: {}",
        cli.unwrap_err()
    ))
}

fn from_env() -> Option<Credentials> {
    Some(Credentials {
        access_key_id: std::env::var("AWS_ACCESS_KEY_ID").ok()?,
        secret_access_key: std::env::var("AWS_SECRET_ACCESS_KEY").ok()?,
        session_token: std::env::var("AWS_SESSION_TOKEN").ok(),
        expires: None,
    })
}

/// `aws configure export-credentials`, which resolves SSO, assumed roles and
/// instance profiles the way the CLI does.
fn from_cli(profile: &str) -> Result<Credentials, String> {
    let output = Command::new("aws")
        .args(["configure", "export-credentials", "--format", "process"])
        .args(["--profile", profile])
        .output()
        .map_err(|e| format!("could not run `aws`: {e}"))?;
    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr).trim().to_owned());
    }
    parse_process_credentials(&output.stdout)
}

fn parse_process_credentials(json: &[u8]) -> Result<Credentials, String> {
    let value: serde_json::Value =
        serde_json::from_slice(json).map_err(|e| format!("unexpected aws output: {e}"))?;
    let field = |name: &str| value.get(name).and_then(|v| v.as_str()).map(str::to_owned);
    Ok(Credentials {
        access_key_id: field("AccessKeyId").ok_or("aws output has no AccessKeyId")?,
        secret_access_key: field("SecretAccessKey").ok_or("aws output has no SecretAccessKey")?,
        session_token: field("SessionToken"),
        expires: field("Expiration")
            .and_then(|e| DateTime::parse_from_rfc3339(&e).ok())
            .map(|e| e.with_timezone(&Utc)),
    })
}

/// Static keys from `~/.aws/credentials` (or `$AWS_SHARED_CREDENTIALS_FILE`).
fn from_credentials_file(profile: &str) -> Option<Credentials> {
    let path = std::env::var_os("AWS_SHARED_CREDENTIALS_FILE")
        .map(Into::into)
        .or_else(|| dirs::home_dir().map(|h| h.join(".aws/credentials")))?;
    parse_credentials_file(&std::fs::read_to_string(path).ok()?, profile)
}

fn parse_credentials_file(text: &str, profile: &str) -> Option<Credentials> {
    let mut in_profile = false;
    let (mut key, mut secret, mut token) = (None, None, None);
    for line in text.lines().map(str::trim) {
        if line.starts_with('#') || line.starts_with(';') || line.is_empty() {
            continue;
        }
        if let Some(name) = line.strip_prefix('[').and_then(|l| l.strip_suffix(']')) {
            in_profile = name.trim() == profile;
            continue;
        }
        let Some((k, v)) = line.split_once('=') else {
            continue;
        };
        if in_profile {
            let v = Some(v.trim().to_owned());
            match k.trim() {
                "aws_access_key_id" => key = v,
                "aws_secret_access_key" => secret = v,
                "aws_session_token" => token = v,
                _ => {}
            }
        }
    }
    Some(Credentials {
        access_key_id: key?,
        secret_access_key: secret?,
        session_token: token,
        expires: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn example_creds() -> Credentials {
        Credentials {
            access_key_id: "AKIAIOSFODNN7EXAMPLE".into(),
            secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into(),
            session_token: None,
            expires: None,
        }
    }

    #[test]
    fn hmac_matches_rfc_4231() {
        // Test case 2.
        assert_eq!(
            hex(&hmac_sha256(b"Jefe", b"what do ya want for nothing?")),
            "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );
        // Test case 6: a key longer than the block size.
        assert_eq!(
            hex(&hmac_sha256(
                &[0xaa; 131],
                b"Test Using Larger Than Block-Size Key - Hash Key First"
            )),
            "60e431591ee0b67f0d8a26aacbf5b77f8e0bc6213728c5140546040f0ee37f54"
        );
    }

    /// AWS's documented presigned-URL example ("Authenticating Requests: Using
    /// Query Parameters", S3 GET of examplebucket/test.txt).
    #[test]
    fn presign_matches_the_aws_example() {
        let request = Presign {
            region: "us-east-1",
            service: "s3",
            host: "examplebucket.s3.amazonaws.com",
            params: vec![],
            expires: 86400,
            payload_hash: "UNSIGNED-PAYLOAD",
            now: Utc.with_ymd_and_hms(2013, 5, 24, 0, 0, 0).unwrap(),
        };
        // The example signs the path /test.txt; ours is always /, so check the
        // pieces and then the signature with the path swapped in.
        let url = presign(&example_creds(), &request);
        assert!(url.starts_with("https://examplebucket.s3.amazonaws.com/?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20130524%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20130524T000000Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host&X-Amz-Signature="));

        let canonical = "GET\n/test.txt\nX-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20130524%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20130524T000000Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host\nhost:examplebucket.s3.amazonaws.com\n\nhost\nUNSIGNED-PAYLOAD";
        let to_sign = format!(
            "AWS4-HMAC-SHA256\n20130524T000000Z\n20130524/us-east-1/s3/aws4_request\n{}",
            hex(&Sha256::digest(canonical.as_bytes()))
        );
        let mut key = hmac_sha256(b"AWS4wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", b"20130524");
        for part in ["us-east-1", "s3", "aws4_request"] {
            key = hmac_sha256(&key, part.as_bytes());
        }
        assert_eq!(
            hex(&hmac_sha256(&key, to_sign.as_bytes())),
            "aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404"
        );
    }

    #[test]
    fn msk_token_is_a_presigned_connect_url() {
        let mut creds = example_creds();
        creds.session_token = Some("tok/en+=".into());
        let now = Utc.with_ymd_and_hms(2026, 10, 13, 9, 30, 0).unwrap();
        let token = token(&creds, "eu-west-1", now);
        assert!(!token.contains('='), "base64url without padding");
        let url = String::from_utf8(
            base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode(&token)
                .unwrap(),
        )
        .unwrap();
        assert!(
            url.starts_with(
                "https://kafka.eu-west-1.amazonaws.com/?Action=kafka-cluster%3AConnect&"
            )
        );
        for part in [
            "X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20261013%2Feu-west-1%2Fkafka-cluster%2Faws4_request",
            "X-Amz-Date=20261013T093000Z",
            "X-Amz-Expires=900",
            "X-Amz-Security-Token=tok%2Fen%2B%3D",
            "&X-Amz-Signature=",
            "&User-Agent=kaffy%2F",
        ] {
            assert!(url.contains(part), "{part} in {url}");
        }
        // Signed query parameters are sorted, and the user agent comes last.
        assert!(
            url.find("X-Amz-Security-Token").unwrap() < url.find("X-Amz-SignedHeaders").unwrap()
        );
        assert!(url.find("X-Amz-Signature").unwrap() < url.find("User-Agent").unwrap());
    }

    #[test]
    fn librdkafka_accepts_the_token() {
        let iam = MskIam::new("eu-west-1", None);
        let mut creds = example_creds();
        creds.expires = Some(Utc::now() + chrono::Duration::hours(1));
        *iam.cached.lock().unwrap() = Some(creds);
        // Nothing listens here; setting a token needs no connection.
        let consumer: Arc<BaseConsumer> = Arc::new(
            rdkafka::ClientConfig::new()
                .set("bootstrap.servers", "127.0.0.1:1")
                .set("security.protocol", "sasl_ssl")
                .set("sasl.mechanism", "OAUTHBEARER")
                .set("group.id", "kaffy-test")
                .create()
                .unwrap(),
        );
        iam.attach(&consumer).unwrap();
        let weak = Arc::downgrade(&consumer);
        drop(consumer);
        assert!(
            weak.upgrade().is_none(),
            "the refresher holds no strong reference"
        );
    }

    #[test]
    fn reads_credentials_sources() {
        let file = "[default]\naws_access_key_id = AK1\naws_secret_access_key = S1\n\n[dev]\n# comment\naws_access_key_id=AK2\naws_secret_access_key=S2\naws_session_token=T2\n";
        let dev = parse_credentials_file(file, "dev").unwrap();
        assert_eq!(
            (dev.access_key_id.as_str(), dev.session_token.as_deref()),
            ("AK2", Some("T2"))
        );
        assert_eq!(
            parse_credentials_file(file, "default")
                .unwrap()
                .access_key_id,
            "AK1"
        );
        assert!(parse_credentials_file(file, "prod").is_none());

        let cli = br#"{"Version": 1, "AccessKeyId": "ASIA1", "SecretAccessKey": "S", "SessionToken": "T", "Expiration": "2026-10-13T10:00:00+00:00"}"#;
        let creds = parse_process_credentials(cli).unwrap();
        assert_eq!(creds.access_key_id, "ASIA1");
        assert_eq!(
            creds.expires,
            Some(Utc.with_ymd_and_hms(2026, 10, 13, 10, 0, 0).unwrap())
        );
        assert!(!creds.fresh(), "expired credentials are fetched again");
    }
}
