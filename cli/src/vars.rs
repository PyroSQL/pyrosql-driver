//! psql-style client-side variable expansion.
//!
//! Three interpolation shapes, matching psql:
//!
//! * `:name`       → raw value, verbatim. The caller is responsible
//!                   for SQL safety — e.g. `\set x 5; SELECT :x;`
//!                   substitutes the literal `5`. Use this for numeric
//!                   literals or identifier-like text you control.
//! * `:'name'`     → value wrapped as a SQL single-quoted string, with
//!                   embedded single quotes doubled per PG escape rules.
//! * `:"name"`     → value wrapped as a SQL double-quoted identifier,
//!                   with embedded double quotes doubled.
//!
//! The scanner is SQL-aware enough to skip over real string literals
//! (`'…'` with `''` as escape), dollar-quoted bodies (`$tag$…$tag$`),
//! line comments (`--…\n`), and block comments (`/* … */`, nested per
//! SQL:2003).  `:name` occurrences inside those spans are left alone.
//!
//! Unknown variable names are left intact (emit the literal `:name`
//! back out) — no error.  psql does the same; it keeps diagnostics
//! where they belong (the server's parser) instead of second-guessing
//! what the user meant.

use std::collections::HashMap;

/// Expand `:name`, `:'name'`, `:"name"` occurrences in `sql` using the
/// supplied variable table. Returns a new owned string whenever at
/// least one substitution happened; otherwise returns `None` so the
/// caller can keep the input reference.
pub fn expand(sql: &str, vars: &HashMap<String, String>) -> Option<String> {
    if vars.is_empty() || !sql.contains(':') {
        return None;
    }
    let mut out = String::with_capacity(sql.len());
    let bytes = sql.as_bytes();
    let mut i = 0;
    let mut changed = false;

    while i < bytes.len() {
        let c = bytes[i];
        // ── Skip string literal ('…') including doubled-quote escape ─
        if c == b'\'' {
            let end = skip_single_quoted(bytes, i);
            out.push_str(&sql[i..end]);
            i = end;
            continue;
        }
        // ── Skip identifier ("…") including doubled-quote escape ──
        if c == b'"' {
            let end = skip_double_quoted(bytes, i);
            out.push_str(&sql[i..end]);
            i = end;
            continue;
        }
        // ── Skip line comment (-- …\n) ─────────────────────────────
        if c == b'-' && bytes.get(i + 1).copied() == Some(b'-') {
            let nl = sql[i..].find('\n').map(|off| i + off + 1).unwrap_or(bytes.len());
            out.push_str(&sql[i..nl]);
            i = nl;
            continue;
        }
        // ── Skip block comment (/* … */), SQL:2003-nested ─────────
        if c == b'/' && bytes.get(i + 1).copied() == Some(b'*') {
            let end = skip_block_comment(bytes, i);
            out.push_str(&sql[i..end]);
            i = end;
            continue;
        }
        // ── Skip dollar-quoted ($tag$ … $tag$) ────────────────────
        if c == b'$' {
            if let Some(tag_len) = find_dollar_close(&bytes[i + 1..]) {
                let tag_bytes = &bytes[i + 1..i + 1 + tag_len];
                let open_end = i + 1 + tag_len + 1;
                let closing_pattern_len = tag_len + 2; // $tag$
                let rest = &bytes[open_end..];
                let close_rel = rest.windows(closing_pattern_len).position(|w| {
                    w.first() == Some(&b'$')
                        && w.last() == Some(&b'$')
                        && &w[1..1 + tag_len] == tag_bytes
                });
                let end = match close_rel {
                    Some(p) => open_end + p + closing_pattern_len,
                    None => bytes.len(),
                };
                out.push_str(&sql[i..end]);
                i = end;
                continue;
            }
            // Not a dollar-quote opener: treat as literal char.
        }
        // ── Variable expansion ─────────────────────────────────────
        if c == b':' {
            // :'name' — string-literal form
            if bytes.get(i + 1).copied() == Some(b'\'') {
                if let Some((name, consumed)) = read_quoted_name(&bytes[i + 2..], b'\'') {
                    if let Some(v) = vars.get(name) {
                        out.push_str(&quote_sql_string(v));
                        i += 2 + consumed;
                        changed = true;
                        continue;
                    }
                }
            }
            // :"name" — identifier form
            if bytes.get(i + 1).copied() == Some(b'"') {
                if let Some((name, consumed)) = read_quoted_name(&bytes[i + 2..], b'"') {
                    if let Some(v) = vars.get(name) {
                        out.push_str(&quote_sql_ident(v));
                        i += 2 + consumed;
                        changed = true;
                        continue;
                    }
                }
            }
            // :name — bare form
            if let Some(name_len) = read_bare_name(&bytes[i + 1..]) {
                let name = &sql[i + 1..i + 1 + name_len];
                if let Some(v) = vars.get(name) {
                    out.push_str(v);
                    i += 1 + name_len;
                    changed = true;
                    continue;
                }
            }
            // Fall through: `:` not followed by a known variable.
        }
        // ── Default: copy one byte, advance ──────────────────────
        out.push(c as char);
        i += 1;
    }

    if changed { Some(out) } else { None }
}

/// Variant of [`expand`] that always returns an owned `String` — useful
/// for call-sites that need `String` regardless of whether substitution
/// happened.
pub fn expand_owned(sql: &str, vars: &HashMap<String, String>) -> String {
    expand(sql, vars).unwrap_or_else(|| sql.to_owned())
}

fn skip_single_quoted(bytes: &[u8], start: usize) -> usize {
    debug_assert_eq!(bytes[start], b'\'');
    let mut i = start + 1;
    while i < bytes.len() {
        if bytes[i] == b'\'' {
            if bytes.get(i + 1).copied() == Some(b'\'') {
                i += 2;
                continue;
            }
            return i + 1;
        }
        i += 1;
    }
    bytes.len()
}

fn skip_double_quoted(bytes: &[u8], start: usize) -> usize {
    debug_assert_eq!(bytes[start], b'"');
    let mut i = start + 1;
    while i < bytes.len() {
        if bytes[i] == b'"' {
            if bytes.get(i + 1).copied() == Some(b'"') {
                i += 2;
                continue;
            }
            return i + 1;
        }
        i += 1;
    }
    bytes.len()
}

fn skip_block_comment(bytes: &[u8], start: usize) -> usize {
    debug_assert_eq!(&bytes[start..start + 2], b"/*");
    let mut i = start + 2;
    let mut depth: u32 = 1;
    while i + 1 < bytes.len() {
        if bytes[i] == b'*' && bytes[i + 1] == b'/' {
            depth -= 1;
            i += 2;
            if depth == 0 {
                return i;
            }
            continue;
        }
        if bytes[i] == b'/' && bytes[i + 1] == b'*' {
            depth += 1;
            i += 2;
            continue;
        }
        i += 1;
    }
    bytes.len()
}

/// Tag locator for `$tag$…$tag$`. Given bytes after the opening `$`,
/// return the number of bytes forming the tag (before the closing `$`),
/// or `None` if the tag contains non-identifier characters.
fn find_dollar_close(after: &[u8]) -> Option<usize> {
    for (j, &b) in after.iter().enumerate() {
        if b == b'$' {
            return Some(j);
        }
        if !(b.is_ascii_alphanumeric() || b == b'_') {
            return None;
        }
    }
    None
}

/// Consume a bare identifier `name` starting at byte 0 of `after`.
/// Returns the length in bytes, or `None` if the first byte is not a
/// valid identifier start.
fn read_bare_name(after: &[u8]) -> Option<usize> {
    let first = *after.first()?;
    if !(first.is_ascii_alphabetic() || first == b'_') {
        return None;
    }
    let mut len = 1;
    while let Some(&b) = after.get(len) {
        if b.is_ascii_alphanumeric() || b == b'_' {
            len += 1;
        } else {
            break;
        }
    }
    Some(len)
}

/// Read a quoted name `name<delim>` from `after`. Returns `(name,
/// bytes_consumed_including_closing_delim)`. `delim` is the closing
/// character (`'` or `"`); the opening delim was already consumed by
/// the caller.
fn read_quoted_name(after: &[u8], delim: u8) -> Option<(&str, usize)> {
    let end = after.iter().position(|&b| b == delim)?;
    let name = std::str::from_utf8(&after[..end]).ok()?;
    // Accept anything printable — the contents are the var name, not a
    // SQL identifier. psql just reads between the delimiters.
    Some((name, end + 1))
}

/// Produce `'value'` with embedded `'` doubled.
fn quote_sql_string(v: &str) -> String {
    let escaped = v.replace('\'', "''");
    format!("'{escaped}'")
}

/// Produce `"value"` with embedded `"` doubled.
fn quote_sql_ident(v: &str) -> String {
    let escaped = v.replace('"', "\"\"");
    format!("\"{escaped}\"")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vars(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs.iter().map(|(k, v)| ((*k).to_owned(), (*v).to_owned())).collect()
    }

    #[test]
    fn bare_substitution() {
        let v = vars(&[("x", "42")]);
        assert_eq!(expand("SELECT :x", &v).as_deref(), Some("SELECT 42"));
    }

    #[test]
    fn string_literal_form_quotes_and_escapes() {
        let v = vars(&[("who", "O'Hara")]);
        assert_eq!(
            expand("SELECT :'who'", &v).as_deref(),
            Some("SELECT 'O''Hara'"),
        );
    }

    #[test]
    fn identifier_form_quotes_and_escapes() {
        let v = vars(&[("col", "weird\"name")]);
        assert_eq!(
            expand("SELECT :\"col\" FROM t", &v).as_deref(),
            Some("SELECT \"weird\"\"name\" FROM t"),
        );
    }

    #[test]
    fn unknown_variable_is_passthrough() {
        let v = vars(&[]);
        // No change → returns None so caller can keep original reference.
        assert_eq!(expand("SELECT :never_set + 1", &v), None);
    }

    #[test]
    fn ignored_inside_single_quoted_literal() {
        let v = vars(&[("x", "should_not_appear")]);
        assert_eq!(expand("SELECT 'time :x stays'", &v), None);
    }

    #[test]
    fn ignored_inside_double_quoted_identifier() {
        let v = vars(&[("x", "X")]);
        assert_eq!(expand("SELECT \"id :x also safe\"", &v), None);
    }

    #[test]
    fn ignored_inside_line_comment() {
        let v = vars(&[("x", "X")]);
        assert_eq!(expand("SELECT 1 -- not :x here\n", &v), None);
    }

    #[test]
    fn ignored_inside_block_comment() {
        let v = vars(&[("x", "X")]);
        assert_eq!(expand("/* block :x nested */ SELECT 1", &v), None);
    }

    #[test]
    fn nested_block_comment_respected() {
        let v = vars(&[("x", "X")]);
        // Closing `*/` inside the inner comment must not unnest us early.
        let q = "/* outer /* :x inner */ :x still inside */ :x";
        // Only the trailing :x outside all comments should substitute.
        assert_eq!(expand(q, &v).as_deref(), Some("/* outer /* :x inner */ :x still inside */ X"));
    }

    #[test]
    fn dollar_quote_skipped() {
        let v = vars(&[("x", "X")]);
        let q = "DO $body$ BEGIN SELECT :x; END $body$";
        assert_eq!(expand(q, &v), None);
    }

    #[test]
    fn colon_followed_by_non_name_is_passthrough() {
        let v = vars(&[("x", "X")]);
        assert_eq!(expand("SELECT ::int FROM t", &v), None); // PG cast syntax
        assert_eq!(expand("WHERE a = : and b = 1", &v), None);
    }

    #[test]
    fn adjacent_vars_expand_both() {
        let v = vars(&[("a", "1"), ("b", "2")]);
        assert_eq!(
            expand("VALUES (:a, :b)", &v).as_deref(),
            Some("VALUES (1, 2)"),
        );
    }

    #[test]
    fn trailing_only_colon_is_passthrough() {
        let v = vars(&[("x", "X")]);
        assert_eq!(expand("SELECT 1; -- :", &v), None);
    }
}
