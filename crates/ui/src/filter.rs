//! The message filter language. Terms are separated by spaces and must all match.
//!
//! | Term | Matches |
//! |---|---|
//! | `word` | key or value contains `word` (case-insensitive) |
//! | `~regex` | key or value matches `regex` |
//! | `key:x` `value:x` `header:x` (or `k:` `v:` `h:`) | that field contains `x` |
//! | `key~re` `value~re` `header~re` | that field matches `re` |
//! | `p:0,2` / `partition:0,2` | partition is 0 or 2 |
//! | `after:T` / `before:T` | timestamp is at or after / before `T` (`-15m`, `2026-10-13T09:30`, RFC 3339, epoch ms) |
//! | `!term` | negates any term |
//!
//! To include spaces, wrap the whole term in `"..."` or `'...'`: `"key:a b"`, `'"status":"PAID"'`.
//! Quotes anywhere else are literal, so `~"status":"(PAID|SHIPPED)"` works as written.
//! Headers are matched as `name=value` lines.

use regex::{Regex, RegexBuilder};

use crate::fmt;

#[derive(Debug, Clone, Default)]
pub struct Filter {
    terms: Vec<(bool, Term)>,
}

#[derive(Debug, Clone)]
enum Term {
    Text { field: Field, pattern: Regex },
    Partitions(Vec<i32>),
    After(i64),
    Before(i64),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Field {
    KeyOrValue,
    Key,
    Value,
    Headers,
}

/// What a filter looks at in a record.
pub struct Subject<'a> {
    pub key: &'a str,
    pub value: &'a str,
    pub headers: &'a str,
    pub partition: i32,
    pub timestamp_ms: Option<i64>,
}

impl Filter {
    pub fn parse(input: &str, now_ms: i64) -> Result<Self, String> {
        let terms = tokenize(input)?
            .into_iter()
            .map(|token| parse_term(&token, now_ms))
            .collect::<Result<_, _>>()?;
        Ok(Self { terms })
    }

    pub fn is_empty(&self) -> bool {
        self.terms.is_empty()
    }

    pub fn matches(&self, subject: &Subject) -> bool {
        self.terms
            .iter()
            .all(|(negated, term)| term.matches(subject) != *negated)
    }
}

impl Term {
    fn matches(&self, s: &Subject) -> bool {
        match self {
            Self::Text { field, pattern } => match field {
                Field::KeyOrValue => pattern.is_match(s.key) || pattern.is_match(s.value),
                Field::Key => pattern.is_match(s.key),
                Field::Value => pattern.is_match(s.value),
                Field::Headers => pattern.is_match(s.headers),
            },
            Self::Partitions(partitions) => partitions.contains(&s.partition),
            Self::After(ms) => s.timestamp_ms.is_some_and(|ts| ts >= *ms),
            Self::Before(ms) => s.timestamp_ms.is_some_and(|ts| ts < *ms),
        }
    }
}

fn parse_term(token: &str, now_ms: i64) -> Result<(bool, Term), String> {
    let (negated, token) = match token.strip_prefix('!') {
        Some(rest) if !rest.is_empty() => (true, rest),
        _ => (false, token),
    };
    if let Some(re) = token.strip_prefix('~') {
        return Ok((negated, text(Field::KeyOrValue, re, true)?));
    }

    let Some(split) = token.find([':', '~']) else {
        return Ok((negated, text(Field::KeyOrValue, token, false)?));
    };
    let (name, op, arg) = (&token[..split], &token[split..=split], &token[split + 1..]);
    let regex = op == "~";
    let term = match name {
        "k" | "key" => text(Field::Key, arg, regex)?,
        "v" | "value" => text(Field::Value, arg, regex)?,
        "h" | "header" | "headers" => text(Field::Headers, arg, regex)?,
        "p" | "partition" if !regex => Term::Partitions(
            arg.split(',')
                .map(|p| p.trim().parse::<i32>())
                .collect::<Result<_, _>>()
                .map_err(|_| format!("bad partition list {arg:?}"))?,
        ),
        "after" | "since" if !regex => Term::After(time(arg, now_ms)?),
        "before" | "until" if !regex => Term::Before(time(arg, now_ms)?),
        // Not a known prefix: search for the whole token, e.g. `http://host`.
        _ => text(Field::KeyOrValue, token, false)?,
    };
    Ok((negated, term))
}

fn text(field: Field, pattern: &str, regex: bool) -> Result<Term, String> {
    if pattern.is_empty() {
        return Err("empty search term".to_owned());
    }
    let pattern = if regex {
        Regex::new(pattern).map_err(|e| regex_error(&e))?
    } else {
        RegexBuilder::new(&regex::escape(pattern))
            .case_insensitive(true)
            .build()
            .map_err(|e| regex_error(&e))?
    };
    Ok(Term::Text { field, pattern })
}

fn regex_error(e: &regex::Error) -> String {
    // The full message spans several lines with a caret diagram; keep the last line.
    let message = e.to_string();
    let last = message
        .lines()
        .last()
        .unwrap_or("")
        .trim_start_matches("error: ");
    format!("bad regex: {last}")
}

fn time(arg: &str, now_ms: i64) -> Result<i64, String> {
    fmt::parse_timestamp(arg, now_ms).ok_or_else(|| format!("not a time: {arg:?}"))
}

/// Splits on whitespace. A quote at the start of a token (after an optional `!`) groups
/// until the matching quote; other quotes are kept as literal characters.
fn tokenize(input: &str) -> Result<Vec<String>, String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut quote: Option<char> = None;
    let mut in_token = false;
    for c in input.chars() {
        match (quote, c) {
            (Some(q), c) if c == q => quote = None,
            (Some(_), c) => current.push(c),
            (None, '"' | '\'') if current.is_empty() || current == "!" => {
                quote = Some(c);
                in_token = true;
            }
            (None, c) if c.is_whitespace() => {
                if in_token {
                    tokens.push(std::mem::take(&mut current));
                    in_token = false;
                }
            }
            (None, c) => {
                current.push(c);
                in_token = true;
            }
        }
    }
    if quote.is_some() {
        return Err("unclosed quote".to_owned());
    }
    if in_token {
        tokens.push(current);
    }
    Ok(tokens)
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOW: i64 = 1_760_000_000_000;

    fn subject<'a>(key: &'a str, value: &'a str) -> Subject<'a> {
        Subject {
            key,
            value,
            headers: "source=seed\ntrace-id=abc",
            partition: 1,
            timestamp_ms: Some(NOW - 60_000),
        }
    }

    fn matches(filter: &str, s: &Subject) -> bool {
        Filter::parse(filter, NOW).unwrap().matches(s)
    }

    #[test]
    fn plain_words_are_case_insensitive_and_anded() {
        let s = subject("cust-001", r#"{"status":"PAID"}"#);
        assert!(matches("paid", &s));
        assert!(matches("paid CUST", &s));
        assert!(!matches("paid shipped", &s));
        assert!(matches("", &s));
    }

    #[test]
    fn field_prefixes() {
        let s = subject("cust-001", r#"{"customer":"cust-002"}"#);
        assert!(matches("key:cust-001", &s));
        assert!(!matches("key:cust-002", &s));
        assert!(matches("v:cust-002", &s));
        assert!(matches("h:trace-id=abc", &s));
        assert!(!matches("h:missing", &s));
    }

    #[test]
    fn regexes_and_negation() {
        let s = subject("cust-001", r#"{"status":"PAID","amount":12.5}"#);
        assert!(matches(r#"~"status":"(PAID|SHIPPED)""#, &s));
        assert!(matches(r"key~^cust-0\d\d$", &s));
        assert!(!matches("~paid", &s), "regexes are case-sensitive");
        assert!(matches("~(?i)paid", &s));
        assert!(matches("!cancelled", &s));
        assert!(!matches("!paid", &s));
    }

    #[test]
    fn quoting() {
        let s = subject("a b", r#"{"status":"PAID"}"#);
        assert!(matches(r#"'"status":"PAID"'"#, &s));
        assert!(matches(r#""key:a b""#, &s));
        assert!(matches(r#"!"key:x y""#, &s));
        assert!(!matches(r#""a c""#, &s));
        assert!(Filter::parse(r#""key:open"#, NOW).is_err());
    }

    #[test]
    fn partitions_and_time() {
        let s = subject("k", "v");
        assert!(matches("p:1", &s));
        assert!(matches("partition:0,1", &s));
        assert!(!matches("p:0,2", &s));
        assert!(matches("after:-5m", &s));
        assert!(!matches("after:-30s", &s));
        assert!(matches("before:-30s", &s));
        assert!(!matches("before:-5m", &s));
    }

    #[test]
    fn unknown_prefix_is_plain_text() {
        let s = subject("k", "see http://example.com");
        assert!(matches("http://example.com", &s));
    }

    #[test]
    fn errors_are_short() {
        let err = |f: &str| Filter::parse(f, NOW).unwrap_err();
        assert!(err("~(unclosed").starts_with("bad regex: "));
        assert!(!err("~(unclosed").contains('\n'));
        assert_eq!(err("p:x"), r#"bad partition list "x""#);
        assert_eq!(err("after:soon"), r#"not a time: "soon""#);
        assert_eq!(err("key:"), "empty search term");
    }
}
