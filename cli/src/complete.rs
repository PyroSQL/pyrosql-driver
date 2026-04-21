//! Tab-completion + rustyline Helper glue.
//!
//! Scope is deliberately small:
//! - SQL keyword completion from a static list (no catalog query on Tab).
//! - Meta-command completion when the word-prefix begins with `\`.
//!
//! Catalog-backed completion (table names from `pg_tables`, column names
//! from `information_schema.columns`) is a worthy follow-up but needs
//! async plumbing into a context the blocking rustyline callback can
//! reach; the wire is simple (SQL → metadata), the lifetime dance is
//! what makes it non-trivial. Deferred until users ask.
//!
//! Case policy: keywords upper-case in the candidate list; matching is
//! case-insensitive. When the user typed lower-case the expansion keeps
//! lower-case so `select ` → `select * from…` still feels native.

use rustyline::completion::{Candidate, Completer, Pair};
use rustyline::highlight::Highlighter;
use rustyline::hint::Hinter;
use rustyline::validate::Validator;
use rustyline::Context;
use rustyline::Helper;

/// Static SQL keyword set covering the subset the pyrosql server
/// speaks day-to-day. Kept sorted for binary-search-friendly diffs.
const SQL_KEYWORDS: &[&str] = &[
    "ABORT", "ADD", "ALL", "ALTER", "AND", "AS", "ASC",
    "BEGIN", "BETWEEN", "BIGINT", "BIGSERIAL", "BOOLEAN", "BY",
    "CASE", "CAST", "CHAR", "CHECK", "COMMIT", "CONSTRAINT", "CREATE",
    "CROSS", "CURRENT_DATE", "CURRENT_TIMESTAMP",
    "DATABASE", "DATE", "DECIMAL", "DEFAULT", "DELETE", "DESC",
    "DISTINCT", "DOUBLE", "DROP",
    "ELSE", "END", "EXCEPT", "EXISTS", "EXPLAIN",
    "FALSE", "FLOAT", "FOREIGN", "FROM", "FULL",
    "GRANT", "GROUP",
    "HAVING",
    "IF", "ILIKE", "IN", "INDEX", "INNER", "INSERT", "INT", "INTEGER",
    "INTERSECT", "INTO", "IS",
    "JOIN",
    "KEY",
    "LEFT", "LIKE", "LIMIT",
    "NOT", "NULL", "NUMERIC",
    "OFFSET", "ON", "OR", "ORDER", "OUTER",
    "PRECISION", "PRIMARY",
    "REAL", "REFERENCES", "RETURNING", "REVOKE", "RIGHT", "ROLLBACK",
    "SAVEPOINT", "SELECT", "SERIAL", "SET", "SMALLINT",
    "TABLE", "TEXT", "THEN", "TIMESTAMP", "TO", "TRANSACTION", "TRUE", "TRUNCATE",
    "UNION", "UNIQUE", "UPDATE", "USING",
    "VALUES", "VARCHAR", "VIEW",
    "WHEN", "WHERE", "WITH",
];

/// Backslash meta commands the dispatcher recognises. One per alias —
/// the completer offers whichever alias starts with the user's prefix.
const META_COMMANDS: &[&str] = &[
    r"\c", r"\connect",
    r"\conninfo",
    r"\copy",
    r"\d", r"\dt", r"\du", r"\dn", r"\df",
    r"\e", r"\edit",
    r"\echo",
    r"\f",
    r"\gexec",
    r"\h", r"\help", r"\?",
    r"\i", r"\include",
    r"\l", r"\list",
    r"\pager",
    r"\password",
    r"\q", r"\quit", r"\exit",
    r"\set",
    r"\timing",
    r"\unset",
    r"\watch",
    r"\x",
    r"\!",
];

/// The rustyline Helper for `pyrosql`.  Combines the SQL/meta completer
/// with no-op Hinter / Validator / Highlighter so `Editor<SqlHelper, _>`
/// type-checks. Keeping the impls local (not the `#[derive]` macros)
/// avoids pulling in `rustyline-derive` as an extra dep.
pub struct SqlHelper;

impl SqlHelper {
    pub fn new() -> Self {
        Self
    }
}

impl Default for SqlHelper {
    fn default() -> Self {
        Self::new()
    }
}

impl Helper for SqlHelper {}
impl Hinter for SqlHelper {
    type Hint = String;
}
impl Highlighter for SqlHelper {}
impl Validator for SqlHelper {}

impl Completer for SqlHelper {
    type Candidate = Pair;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        _ctx: &Context<'_>,
    ) -> rustyline::Result<(usize, Vec<Self::Candidate>)> {
        let (start, word) = word_at(line, pos);
        if word.is_empty() {
            return Ok((start, Vec::new()));
        }
        let keep_case = word.chars().any(|c| c.is_ascii_lowercase());

        // Meta command completion — first word AND starts with backslash.
        if word.starts_with('\\') && start == line_start_non_whitespace(line) {
            let wlower = word.to_ascii_lowercase();
            let mut out: Vec<Pair> = META_COMMANDS
                .iter()
                .filter(|k| k.to_ascii_lowercase().starts_with(&wlower))
                .map(|k| Pair {
                    display: (*k).to_owned(),
                    replacement: (*k).to_owned(),
                })
                .collect();
            out.sort_by(|a, b| a.display().cmp(b.display()));
            return Ok((start, out));
        }

        // SQL keyword completion — case-insensitive prefix match.
        let wupper = word.to_ascii_uppercase();
        let mut out: Vec<Pair> = SQL_KEYWORDS
            .iter()
            .filter(|k| k.starts_with(&wupper))
            .map(|k| {
                let replacement = if keep_case {
                    k.to_ascii_lowercase()
                } else {
                    (*k).to_owned()
                };
                Pair {
                    display: (*k).to_owned(),
                    replacement,
                }
            })
            .collect();
        out.sort_by(|a, b| a.display().cmp(b.display()));
        Ok((start, out))
    }
}

/// Return `(start, word)` where `word` is the current token at `pos`
/// and `start` is its byte offset in `line`. Splits on whitespace,
/// `;`, `,`, `(`, `)`, `=` — the usual SQL-ish word boundaries. The
/// backslash stays PART of the word so `\c<Tab>` completes to `\c`.
fn word_at(line: &str, pos: usize) -> (usize, &str) {
    let bytes = line.as_bytes();
    let pos = pos.min(bytes.len());
    let mut start = pos;
    while start > 0 {
        let b = bytes[start - 1];
        if is_word_break(b) {
            break;
        }
        start -= 1;
    }
    (start, &line[start..pos])
}

fn is_word_break(b: u8) -> bool {
    matches!(
        b,
        b' ' | b'\t' | b'\n' | b'\r' | b';' | b',' | b'(' | b')' | b'='
    )
}

/// First non-whitespace byte offset in `line`. Used so the meta-command
/// path only fires when the backslash is the first real token on the
/// line (stops `select \d` typos from triggering meta completion).
fn line_start_non_whitespace(line: &str) -> usize {
    line.bytes().position(|b| !b.is_ascii_whitespace()).unwrap_or(line.len())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustyline::history::DefaultHistory;

    fn ctx<'a>(h: &'a DefaultHistory) -> Context<'a> {
        Context::new(h)
    }

    #[test]
    fn completes_sql_keyword_uppercase_prefix() {
        let helper = SqlHelper::new();
        let hist = DefaultHistory::new();
        let (start, cands) = helper.complete("SEL", 3, &ctx(&hist)).unwrap();
        assert_eq!(start, 0);
        assert!(cands.iter().any(|c| c.replacement == "SELECT"),
                "SELECT must be suggested, got: {:?}",
                cands.iter().map(|c| c.replacement.clone()).collect::<Vec<_>>());
    }

    #[test]
    fn completes_sql_keyword_preserves_case_when_user_lowercase() {
        let helper = SqlHelper::new();
        let hist = DefaultHistory::new();
        let (_, cands) = helper.complete("sel", 3, &ctx(&hist)).unwrap();
        assert!(cands.iter().any(|c| c.replacement == "select"),
                "lowercase prefix must expand to lowercase replacement");
    }

    #[test]
    fn completes_meta_command_backslash() {
        let helper = SqlHelper::new();
        let hist = DefaultHistory::new();
        let (start, cands) = helper.complete(r"\d", 2, &ctx(&hist)).unwrap();
        assert_eq!(start, 0);
        let reps: Vec<String> = cands.iter().map(|c| c.replacement.clone()).collect();
        assert!(reps.contains(&r"\d".to_owned()));
        assert!(reps.contains(&r"\dt".to_owned()));
        assert!(reps.contains(&r"\du".to_owned()));
        assert!(reps.contains(&r"\dn".to_owned()));
        assert!(reps.contains(&r"\df".to_owned()));
    }

    #[test]
    fn meta_only_at_line_start() {
        // A `\d` that appears mid-line (e.g. after typed text) must NOT
        // trigger meta-command completion — it's just a word fragment.
        let helper = SqlHelper::new();
        let hist = DefaultHistory::new();
        let line = "select \\d";
        let (_, cands) = helper.complete(line, line.len(), &ctx(&hist)).unwrap();
        let reps: Vec<String> = cands.iter().map(|c| c.replacement.clone()).collect();
        assert!(
            !reps.iter().any(|r| r.starts_with('\\')),
            "meta candidates must not leak into mid-line position, got: {reps:?}",
        );
    }

    #[test]
    fn empty_word_returns_no_candidates() {
        let helper = SqlHelper::new();
        let hist = DefaultHistory::new();
        let (_, cands) = helper.complete("SELECT * ", 9, &ctx(&hist)).unwrap();
        assert!(cands.is_empty(), "empty-word position must not explode the suggestion list");
    }

    #[test]
    fn unknown_prefix_returns_empty() {
        let helper = SqlHelper::new();
        let hist = DefaultHistory::new();
        let (_, cands) = helper.complete("XYZZY", 5, &ctx(&hist)).unwrap();
        assert!(cands.is_empty());
    }
}
