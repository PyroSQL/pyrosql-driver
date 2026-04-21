//! Minimal SQL-aware tokenizer — just enough to know where statements
//! end inside a script or a REPL buffer.
//!
//! Handles (in strict order so we don't misclassify a `--` inside a
//! string):
//!
//! * `'single-quoted'` — escaped by doubling the quote (`''`).
//! * `"double-quoted" identifiers` — also doubled-quote escape.
//! * `$tag$ ... $tag$` dollar-quoted blocks, including the default
//!   untagged `$$ ... $$` form. Tag is a valid identifier or empty.
//! * `-- line comment` through end of line.
//! * `/* block comment */` — nested per SQL:2003.
//!
//! The public API is two helpers: [`is_complete`] for the REPL's
//! line-at-a-time loop, and [`split_statements`] for batch scripts.
//! Both walk the same state machine but collect slightly different
//! summaries.

#![forbid(unsafe_code)]

#[derive(Debug, Clone, PartialEq, Eq)]
enum State {
    Normal,
    SingleQuote,
    DoubleQuote,
    /// Inside `$tag$ ... $tag$`. Tag stored as an owned `String` so we
    /// can pattern-match against the closing sequence without juggling
    /// borrows from the input buffer.
    DollarQuote(String),
    LineComment,
    /// Inside `/* ... */`. `u8` is nesting depth (≥1).
    BlockComment(u8),
}

/// Returns `true` if `buf` ends with a `;` that is outside every quote
/// and comment. Whitespace and comments after the semicolon are allowed.
pub fn is_complete(buf: &str) -> bool {
    let mut state = State::Normal;
    let mut last_meaningful: Option<char> = None;
    let bytes = buf.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i] as char;
        match &state {
            State::Normal => {
                if c == '\'' {
                    state = State::SingleQuote;
                    last_meaningful = Some('\'');
                    i += 1;
                    continue;
                }
                if c == '"' {
                    state = State::DoubleQuote;
                    last_meaningful = Some('"');
                    i += 1;
                    continue;
                }
                if c == '-' && bytes.get(i + 1).copied() == Some(b'-') {
                    state = State::LineComment;
                    i += 2;
                    continue;
                }
                if c == '/' && bytes.get(i + 1).copied() == Some(b'*') {
                    state = State::BlockComment(1);
                    i += 2;
                    continue;
                }
                if c == '$' {
                    if let Some(tag_len) = find_dollar_close(&bytes[i + 1..]) {
                        let tag = std::str::from_utf8(&bytes[i + 1..i + 1 + tag_len])
                            .unwrap_or("")
                            .to_owned();
                        state = State::DollarQuote(tag);
                        i += 1 + tag_len + 1; // skip `$tag$`
                        continue;
                    }
                }
                if !c.is_whitespace() {
                    last_meaningful = Some(c);
                }
            }
            State::SingleQuote => {
                if c == '\'' {
                    if bytes.get(i + 1).copied() == Some(b'\'') {
                        i += 2;
                        continue;
                    }
                    state = State::Normal;
                }
            }
            State::DoubleQuote => {
                if c == '"' {
                    if bytes.get(i + 1).copied() == Some(b'"') {
                        i += 2;
                        continue;
                    }
                    state = State::Normal;
                }
            }
            State::DollarQuote(tag) => {
                if c == '$' {
                    let want = tag.len() + 2; // `$tag$`
                    if i + want <= bytes.len()
                        && bytes[i + 1 + tag.len()] == b'$'
                        && &bytes[i + 1..i + 1 + tag.len()] == tag.as_bytes()
                    {
                        state = State::Normal;
                        i += want;
                        continue;
                    }
                }
            }
            State::LineComment => {
                if c == '\n' {
                    state = State::Normal;
                }
            }
            State::BlockComment(depth) => {
                let depth = *depth;
                if c == '*' && bytes.get(i + 1).copied() == Some(b'/') {
                    let d = depth - 1;
                    state = if d == 0 { State::Normal } else { State::BlockComment(d) };
                    i += 2;
                    continue;
                }
                if c == '/' && bytes.get(i + 1).copied() == Some(b'*') {
                    state = State::BlockComment(depth.saturating_add(1));
                    i += 2;
                    continue;
                }
            }
        }
        i += 1;
    }
    last_meaningful == Some(';') && matches!(state, State::Normal)
}

/// Split a SQL script on statement boundaries. Returns trimmed
/// statements; empties are dropped. The terminating semicolon is
/// stripped so the server doesn't receive an empty second statement.
pub fn split_statements(src: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut state = State::Normal;
    let mut start = 0usize;
    let bytes = src.as_bytes();
    let mut i = 0;

    while i < bytes.len() {
        let c = bytes[i] as char;
        match &state {
            State::Normal => {
                match c {
                    '\'' => state = State::SingleQuote,
                    '"' => state = State::DoubleQuote,
                    ';' => {
                        let s = src[start..i].trim();
                        if !s.is_empty() {
                            out.push(s.to_owned());
                        }
                        start = i + 1;
                    }
                    '-' if bytes.get(i + 1).copied() == Some(b'-') => {
                        state = State::LineComment;
                        i += 2;
                        continue;
                    }
                    '/' if bytes.get(i + 1).copied() == Some(b'*') => {
                        state = State::BlockComment(1);
                        i += 2;
                        continue;
                    }
                    '$' => {
                        if let Some(tag_len) = find_dollar_close(&bytes[i + 1..]) {
                            let tag = std::str::from_utf8(&bytes[i + 1..i + 1 + tag_len])
                                .unwrap_or("")
                                .to_owned();
                            state = State::DollarQuote(tag);
                            i += 1 + tag_len + 1;
                            continue;
                        }
                    }
                    _ => {}
                }
            }
            State::SingleQuote => {
                if c == '\'' {
                    if bytes.get(i + 1).copied() == Some(b'\'') {
                        i += 2;
                        continue;
                    }
                    state = State::Normal;
                }
            }
            State::DoubleQuote => {
                if c == '"' {
                    if bytes.get(i + 1).copied() == Some(b'"') {
                        i += 2;
                        continue;
                    }
                    state = State::Normal;
                }
            }
            State::DollarQuote(tag) => {
                if c == '$' {
                    let want = tag.len() + 2;
                    if i + want <= bytes.len()
                        && bytes[i + 1 + tag.len()] == b'$'
                        && &bytes[i + 1..i + 1 + tag.len()] == tag.as_bytes()
                    {
                        state = State::Normal;
                        i += want;
                        continue;
                    }
                }
            }
            State::LineComment => {
                if c == '\n' {
                    state = State::Normal;
                }
            }
            State::BlockComment(depth) => {
                let depth = *depth;
                if c == '*' && bytes.get(i + 1).copied() == Some(b'/') {
                    let d = depth - 1;
                    state = if d == 0 { State::Normal } else { State::BlockComment(d) };
                    i += 2;
                    continue;
                }
                if c == '/' && bytes.get(i + 1).copied() == Some(b'*') {
                    state = State::BlockComment(depth.saturating_add(1));
                    i += 2;
                    continue;
                }
            }
        }
        i += 1;
    }
    // Tail: whatever's between the last `;` and end-of-input.
    let s = src[start..].trim();
    if !s.is_empty() {
        out.push(s.to_owned());
    }
    out
}

/// Given the bytes AFTER an opening `$`, locate the matching close `$`
/// of the tag (empty tag is legal: `$$`). Returns the position of that
/// closing `$` within the slice, or `None` if the tag was invalid (not
/// ASCII-identifier characters) — matching PostgreSQL's behaviour of
/// treating a standalone `$` as ordinary text in that case.
fn find_dollar_close(after_open: &[u8]) -> Option<usize> {
    for (j, &b) in after_open.iter().enumerate() {
        if b == b'$' {
            return Some(j);
        }
        let ok = b.is_ascii_alphanumeric() || b == b'_';
        if !ok {
            return None;
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn complete_simple() {
        assert!(is_complete("SELECT 1;"));
        assert!(!is_complete("SELECT 1"));
        assert!(!is_complete(""));
    }

    #[test]
    fn complete_with_trailing_comment() {
        assert!(is_complete("SELECT 1; -- done\n"));
        assert!(is_complete("SELECT 1; /* bye */"));
    }

    #[test]
    fn semicolon_inside_single_quote_ignored() {
        assert!(!is_complete("INSERT INTO t VALUES ('a;b')"));
        assert!(is_complete("INSERT INTO t VALUES ('a;b');"));
    }

    #[test]
    fn semicolon_inside_double_quote_ignored() {
        assert!(!is_complete("SELECT \"weird;col\" FROM t"));
        assert!(is_complete("SELECT \"weird;col\" FROM t;"));
    }

    #[test]
    fn doubled_quote_is_escape() {
        assert!(!is_complete("SELECT 'it''s ok"));
        assert!(is_complete("SELECT 'it''s ok';"));
    }

    #[test]
    fn dollar_quote_default_tag() {
        assert!(!is_complete("CREATE FUNCTION f() RETURNS int AS $$ SELECT 1; $$"));
        assert!(is_complete(
            "CREATE FUNCTION f() RETURNS int AS $$ SELECT 1; $$ LANGUAGE sql;"
        ));
    }

    #[test]
    fn dollar_quote_tagged() {
        assert!(is_complete("DO $body$ BEGIN PERFORM 1; END $body$;"));
    }

    #[test]
    fn block_comment_nested() {
        assert!(!is_complete("/* outer /* inner */ still inside */"));
        assert!(is_complete("/* outer /* inner */ still */ SELECT 1;"));
    }

    #[test]
    fn split_simple() {
        assert_eq!(
            split_statements("SELECT 1; SELECT 2;"),
            vec!["SELECT 1", "SELECT 2"]
        );
    }

    #[test]
    fn split_no_trailing_semicolon() {
        assert_eq!(split_statements("SELECT 1"), vec!["SELECT 1"]);
    }

    #[test]
    fn split_dollar_quote_preserves_semicolons() {
        let sql = "CREATE FUNCTION f() RETURNS int AS $$ SELECT 1; SELECT 2; $$ LANGUAGE sql; \
                   SELECT 3;";
        let v = split_statements(sql);
        assert_eq!(v.len(), 2, "got {v:?}");
        assert!(v[0].contains("$$ SELECT 1; SELECT 2; $$"));
        assert_eq!(v[1], "SELECT 3");
    }

    #[test]
    fn split_block_comment_with_semicolon() {
        let v = split_statements("/* a;b;c */ SELECT 1; SELECT 2");
        assert_eq!(v.len(), 2);
        assert_eq!(v[1], "SELECT 2");
    }

    #[test]
    fn split_empty_whitespace() {
        assert!(split_statements("").is_empty());
        assert!(split_statements("\n\n;;  ;").is_empty());
    }
}
