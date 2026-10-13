//! The command palette: fuzzy search over this screen's commands and every
//! topic, consumer group and subject kaffy has loaded.

use crate::commands::{self, CommandDef};
use crate::state::AppState;

/// Rows the palette shows at most.
pub const MAX_RESULTS: usize = 12;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Palette {
    pub query: String,
    pub selected: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PaletteKey {
    Char(char),
    Backspace,
    Up,
    Down,
    Submit,
    Cancel,
}

/// Somewhere the palette can jump to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Target {
    Topic(String),
    Group(String),
    Subject(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Entry {
    Command(&'static str),
    Go(Target),
}

impl Entry {
    pub fn text(&self) -> &str {
        match self {
            Entry::Command(id) => commands::find(id).map_or(id, |c| c.title),
            Entry::Go(Target::Topic(name) | Target::Group(name) | Target::Subject(name)) => name,
        }
    }

    /// What kind of entry this is, shown beside it.
    pub fn kind(&self) -> &'static str {
        match self {
            Entry::Command(_) => "command",
            Entry::Go(Target::Topic(_)) => "topic",
            Entry::Go(Target::Group(_)) => "group",
            Entry::Go(Target::Subject(_)) => "subject",
        }
    }

    pub fn command(&self) -> Option<&'static CommandDef> {
        match self {
            Entry::Command(id) => commands::find(id),
            Entry::Go(_) => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Match {
    pub entry: Entry,
    /// Character positions in `entry.text()` that matched, for highlighting.
    pub positions: Vec<usize>,
}

/// Everything the palette can offer right now. Without a query only commands
/// are listed; topics, groups and subjects join once you type.
fn entries(state: &AppState, with_targets: bool) -> Vec<Entry> {
    let mut entries: Vec<Entry> = commands::on(state.screen)
        .filter(|c| c.palette)
        .map(|c| Entry::Command(c.id))
        .collect();
    if with_targets {
        let topics = state.topics.iter().flatten().map(|t| t.name.clone());
        entries.extend(topics.map(|n| Entry::Go(Target::Topic(n))));
        let groups = state.groups.list.iter().flatten().map(|g| g.name.clone());
        entries.extend(groups.map(|n| Entry::Go(Target::Group(n))));
        let subjects = state
            .schemas
            .list
            .iter()
            .flatten()
            .map(|s| s.subject.clone());
        entries.extend(subjects.map(|n| Entry::Go(Target::Subject(n))));
    }
    entries
}

/// The entries matching `query`, best first.
pub fn matches(state: &AppState, query: &str) -> Vec<Match> {
    let query = query.trim();
    let mut scored: Vec<(i64, usize, Match)> = entries(state, !query.is_empty())
        .into_iter()
        .enumerate()
        .filter_map(|(order, entry)| {
            let (score, positions) = fuzzy(query, entry.text())?;
            Some((score, order, Match { entry, positions }))
        })
        .collect();
    // Best score first; ties keep the listing order (commands before names).
    scored.sort_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));
    scored.into_iter().map(|(_, _, m)| m).collect()
}

/// Scores `candidate` against `query` when every query character appears in
/// order (ignoring case). Matches at word starts and runs of consecutive
/// characters score higher; gaps and long candidates score lower.
pub fn fuzzy(query: &str, candidate: &str) -> Option<(i64, Vec<usize>)> {
    // Taking each character as early as possible can miss a better word start
    // later on ("sr" in "sensor-readings"), so also try preferring word starts.
    let chars: Vec<char> = candidate.chars().collect();
    let greedy = score_match(query, &chars, false);
    let starts = score_match(query, &chars, true);
    match (greedy, starts) {
        (Some(a), Some(b)) => Some(if b.0 > a.0 { b } else { a }),
        (a, b) => a.or(b),
    }
}

fn is_word_start(chars: &[char], i: usize) -> bool {
    i == 0 || matches!(chars[i - 1], ' ' | '-' | '_' | '.' | '/')
}

fn score_match(query: &str, chars: &[char], prefer_starts: bool) -> Option<(i64, Vec<usize>)> {
    let same = |i: usize, q: char| chars[i].to_lowercase().next() == Some(q);
    let mut positions: Vec<usize> = Vec::new();
    let mut score = 0i64;
    let mut next = 0;
    for q in query.chars().filter(|c| !c.is_whitespace()) {
        let q = q.to_lowercase().next().unwrap_or(q);
        let first = (next..chars.len()).find(|&i| same(i, q))?;
        let consecutive_here = positions.last().is_some_and(|&last| last + 1 == first);
        let found = if prefer_starts && !consecutive_here && !is_word_start(chars, first) {
            (first..chars.len())
                .find(|&i| same(i, q) && is_word_start(chars, i))
                .unwrap_or(first)
        } else {
            first
        };
        let consecutive = positions.last().is_some_and(|&last| last + 1 == found);
        score += 1;
        if is_word_start(chars, found) {
            score += 8;
        }
        if consecutive {
            score += 5;
        }
        score -= (found - next) as i64 / 2;
        positions.push(found);
        next = found + 1;
    }
    score -= chars.len() as i64 / 8;
    Some((score, positions))
}

#[cfg(test)]
mod tests {
    use super::*;
    use kaffy_kafka::{Event, PartitionInfo, TopicInfo};

    #[test]
    fn fuzzy_prefers_word_starts_and_runs() {
        assert!(fuzzy("xyz", "orders").is_none());
        let (_, positions) = fuzzy("ord", "orders").unwrap();
        assert_eq!(positions, vec![0, 1, 2]);
        let start = fuzzy("ue", "user-events").unwrap().0;
        let middle = fuzzy("ue", "queue").unwrap().0;
        assert!(start > middle, "u at a word start, e after a dash");
        let (_, positions) = fuzzy("SR", "sensor-readings").unwrap();
        assert_eq!(positions, vec![0, 7], "case-insensitive, word starts");
    }

    #[test]
    fn lists_commands_then_finds_topics() {
        let mut state = AppState::new("t", false, false);
        let topic = |name: &str| TopicInfo {
            name: name.into(),
            partitions: vec![PartitionInfo {
                id: 0,
                leader: 1,
                replicas: vec![1],
                isr: vec![1],
            }],
        };
        state.on_event(Event::Metadata {
            brokers: 1,
            topics: vec![topic("orders"), topic("payments")],
        });
        let all = matches(&state, "");
        assert!(all.iter().all(|m| matches!(m.entry, Entry::Command(_))));
        assert!(all.iter().any(|m| m.entry == Entry::Command("new-topic")));
        assert!(
            !all.iter().any(|m| m.entry == Entry::Command("up")),
            "movement stays off the palette"
        );

        let found = matches(&state, "pay");
        assert_eq!(found[0].entry, Entry::Go(Target::Topic("payments".into())));
    }
}
