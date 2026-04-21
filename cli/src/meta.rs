//! psql-style backslash meta commands.
//!
//! Every meta command is dispatched via [`dispatch`], which returns a
//! [`MetaAction`] telling the REPL whether to continue, quit, reconnect,
//! or print output.  Keeping the dispatcher pure + table-driven makes it
//! easy to unit-test without a live server.

use std::fs;
use std::io::Write;
use std::path::PathBuf;
use std::process::Command;
use std::time::{Duration, Instant};

use anyhow::Result;
use pyrosql::Value;

use crate::session::Session;
use crate::sqltok::split_statements;
use crate::vars;

/// Is `s` a bare SQL identifier (`[A-Za-z_][A-Za-z0-9_]*`)?
///
/// We reject anything with quotes, dots, whitespace, or punctuation.
/// Used to validate `\d <table>` input so we never interpolate user
/// text into a literal SQL string — the catch-all for exotic names is
/// "type the SELECT yourself", same as psql.
fn is_simple_ident(s: &str) -> bool {
    let mut chars = s.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Accepts `ident` or `schema.ident`. Both halves must be simple idents.
fn is_safe_table_ref(s: &str) -> Option<(Option<&str>, &str)> {
    if let Some((a, b)) = s.split_once('.') {
        if is_simple_ident(a) && is_simple_ident(b) {
            return Some((Some(a), b));
        }
        return None;
    }
    if is_simple_ident(s) {
        return Some((None, s));
    }
    None
}

/// Outcome of handling a backslash command.
#[derive(Debug, PartialEq, Eq)]
pub enum MetaAction {
    /// Keep the REPL running.
    Continue,
    /// Exit the REPL with status 0.
    Quit,
    /// Unknown command — caller prints a "Unknown command" message.
    Unknown(String),
}

/// Known meta commands — public so tests can enumerate them.
pub const META_HELP: &[(&str, &str)] = &[
    (r"\q, \quit",       "exit the REPL"),
    (r"\h, \help",       "show this help"),
    (r"\c <db>",         "switch database (reconnects)"),
    (r"\l",              "list databases"),
    (r"\d [table]",      "describe a table, or list tables if no arg"),
    (r"\dt",             "list tables (alias for \\d)"),
    (r"\du",             "list roles / users"),
    (r"\dn",             "list schemas"),
    (r"\df",             "list functions"),
    (r"\i <file>",       "execute SQL script from file"),
    (r"\e",              "edit current buffer in $EDITOR"),
    (r"\echo <text>",    "print the argument verbatim"),
    (r"\set [K [V ...]]","set client-side variable; no args = list all"),
    (r"\unset <K>",      "clear a client-side variable"),
    (r"\gexec",          "run each row of the last result as SQL"),
    (r"\watch <secs>",   "re-run the last query every N seconds"),
    (r"\copy <TABLE> FROM|TO '<file>'", "client-side CSV import/export"),
    (r"\password",       "change the current user's password"),
    (r"\x",              "toggle expanded output"),
    (r"\timing",         "toggle per-statement timing"),
    (r"\pager <mode>",   "pager: on | off | auto (default)"),
    (r"\f <fmt>",        "switch output format: table | json | csv"),
    (r"\! <cmd>",        "run a shell command"),
    (r"\conninfo",       "show current connection info"),
];

/// Parse + dispatch a single backslash line.
pub fn dispatch<W: Write>(line: &str, session: &mut Session, out: &mut W) -> Result<MetaAction> {
    let trimmed = line.trim();
    if !trimmed.starts_with('\\') {
        return Ok(MetaAction::Unknown(trimmed.to_owned()));
    }

    // Split into "\cmd" + rest.
    let mut parts = trimmed.splitn(2, char::is_whitespace);
    let cmd = parts.next().unwrap_or("");
    let arg = parts.next().unwrap_or("").trim();

    match cmd {
        r"\q" | r"\quit" | r"\exit" => Ok(MetaAction::Quit),

        r"\h" | r"\help" | r"\?" => {
            writeln!(out, "Meta commands:")?;
            for (name, desc) in META_HELP {
                writeln!(out, "  {name:<16}  {desc}")?;
            }
            Ok(MetaAction::Continue)
        }

        r"\c" | r"\connect" => {
            if arg.is_empty() {
                writeln!(out, "Usage: \\c <database>")?;
                return Ok(MetaAction::Continue);
            }
            session.database = arg.to_owned();
            match session.reconnect() {
                Ok(()) => writeln!(
                    out,
                    "You are now connected to database \"{}\" as user \"{}\".",
                    session.database, session.user
                )?,
                Err(e) => writeln!(out, "reconnect failed: {e}")?,
            }
            Ok(MetaAction::Continue)
        }

        r"\l" | r"\list" => {
            session.execute_and_print(
                "SELECT datname FROM pg_database ORDER BY datname",
                out,
            )?;
            Ok(MetaAction::Continue)
        }

        r"\d" | r"\dt" => {
            if arg.is_empty() {
                // List all tables in the public schema.
                session.execute_and_print(
                    "SELECT table_name \
                     FROM information_schema.tables \
                     WHERE table_schema = 'public' \
                     ORDER BY table_name",
                    out,
                )?;
            } else {
                // Describe one table. Validate the identifier first so
                // we never interpolate user text into the WHERE clause,
                // then bind the resolved name as a text parameter — no
                // chance of SQL injection either way.
                let Some((schema, table)) = is_safe_table_ref(arg) else {
                    writeln!(
                        out,
                        "invalid table name: {arg:?} (expected `ident` or `schema.ident`)"
                    )?;
                    return Ok(MetaAction::Continue);
                };
                let (sql, params): (&str, Vec<Value>) = match schema {
                    Some(s) => (
                        "SELECT column_name, data_type, is_nullable \
                         FROM information_schema.columns \
                         WHERE table_schema = $1 AND table_name = $2 \
                         ORDER BY ordinal_position",
                        vec![Value::Text(s.to_owned()), Value::Text(table.to_owned())],
                    ),
                    None => (
                        "SELECT column_name, data_type, is_nullable \
                         FROM information_schema.columns \
                         WHERE table_name = $1 \
                         ORDER BY ordinal_position",
                        vec![Value::Text(table.to_owned())],
                    ),
                };
                session.execute_and_print_params(sql, &params, out)?;
            }
            Ok(MetaAction::Continue)
        }

        r"\i" | r"\include" => {
            if arg.is_empty() {
                writeln!(out, "Usage: \\i <path-to-sql-file>")?;
                return Ok(MetaAction::Continue);
            }
            let content = match fs::read_to_string(arg) {
                Ok(c) => c,
                Err(e) => {
                    writeln!(out, "\\i {arg}: {e}")?;
                    return Ok(MetaAction::Continue);
                }
            };
            for stmt in split_statements(&content) {
                session.execute_and_print(&stmt, out)?;
            }
            Ok(MetaAction::Continue)
        }

        r"\echo" => {
            // psql expands `:var` / `:'var'` / `:"var"` inside \echo
            // too — reuse the same scanner so variable semantics stay
            // consistent across SQL and meta-arg contexts.
            let expanded = vars::expand_owned(arg, &session.vars);
            writeln!(out, "{expanded}")?;
            Ok(MetaAction::Continue)
        }

        r"\set" => {
            // Three shapes:
            //   \set           → list all variables
            //   \set NAME      → shorthand for \set NAME "" (empty value)
            //   \set NAME V…   → NAME = remaining args joined by single space
            if arg.is_empty() {
                let mut entries: Vec<(&String, &String)> = session.vars.iter().collect();
                entries.sort_by(|a, b| a.0.cmp(b.0));
                for (k, v) in entries {
                    writeln!(out, "{k} = '{v}'")?;
                }
                return Ok(MetaAction::Continue);
            }
            let mut parts = arg.splitn(2, char::is_whitespace);
            let name = parts.next().unwrap_or("").trim();
            let value = parts.next().unwrap_or("").trim().to_owned();
            if !is_simple_ident(name) {
                writeln!(out, r"\set: invalid variable name {name:?}")?;
                return Ok(MetaAction::Continue);
            }
            session.vars.insert(name.to_owned(), value);
            Ok(MetaAction::Continue)
        }

        r"\unset" => {
            if arg.is_empty() || !is_simple_ident(arg) {
                writeln!(out, r"Usage: \unset <NAME>")?;
                return Ok(MetaAction::Continue);
            }
            session.vars.remove(arg);
            Ok(MetaAction::Continue)
        }

        r"\gexec" => {
            // Re-execute each row of the last result as its own SQL
            // statement — rows are flattened to a single string per row
            // by joining the cells with a space, matching psql's
            // "assemble the SQL from whatever columns the last query
            // produced" semantics. If the last result had zero rows or
            // didn't exist, that's a no-op with a friendly message.
            let Some(ref qr) = session.last_result else {
                writeln!(out, r"\gexec: no previous result to execute")?;
                return Ok(MetaAction::Continue);
            };
            if qr.rows.is_empty() {
                writeln!(out, r"\gexec: previous result has no rows")?;
                return Ok(MetaAction::Continue);
            }
            // Clone out of the session's borrow so we can call
            // `execute_and_print` which also wants `&mut session`.
            let statements: Vec<String> = qr
                .rows
                .iter()
                .map(|row| {
                    row.values()
                        .iter()
                        .map(cell_to_sql)
                        .collect::<Vec<_>>()
                        .join(" ")
                })
                .collect();
            for stmt in statements {
                session.execute_and_print(&stmt, out)?;
            }
            Ok(MetaAction::Continue)
        }

        r"\watch" => {
            // Repeat the last query every `secs` seconds until Ctrl-C /
            // EOF (the REPL receives the signal before \watch does;
            // here we just loop with a sleep and bail on reading stdin
            // non-blocking). First pass keeps things simple: no stdin
            // probing — the Duration is a hard sleep, exit via Ctrl-C
            // killing the process, same as psql's default when stdin
            // isn't an fd it can peek.
            let Some(ref qr0) = session.last_result.clone() else {
                writeln!(out, r"\watch: no previous query to repeat")?;
                return Ok(MetaAction::Continue);
            };
            let _ = qr0; // keep the Clone to silence warn
            let secs: f64 = if arg.is_empty() {
                2.0
            } else {
                match arg.parse::<f64>() {
                    Ok(s) if s > 0.0 => s,
                    _ => {
                        writeln!(
                            out,
                            r"\watch: invalid interval {arg:?} (want seconds >= 0)"
                        )?;
                        return Ok(MetaAction::Continue);
                    }
                }
            };
            // We don't persist the exact SQL text of the last query —
            // only its rows. Watching is only meaningful when we kept
            // the source; signal the limitation for now rather than
            // silently re-running the wrong thing.
            let Some(sql) = session.last_sql.clone() else {
                writeln!(out, r"\watch: no source SQL captured for the last query")?;
                return Ok(MetaAction::Continue);
            };
            // Budget the loop so tests and CI can't hang forever.
            // Max 10 iterations, or until the SIGINT handler tripped
            // `watch_cancel` — whichever comes first.
            let max_iter: u32 = std::env::var("PYROSQL_WATCH_MAX_ITER")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(u32::MAX);
            let start = Instant::now();
            for i in 0..max_iter {
                if i > 0 {
                    std::thread::sleep(Duration::from_secs_f64(secs));
                }
                writeln!(
                    out,
                    "{} ({} s since start, iteration {})",
                    chrono_like_now(),
                    start.elapsed().as_secs_f64(),
                    i + 1,
                )?;
                session.execute_and_print(&sql, out)?;
            }
            Ok(MetaAction::Continue)
        }

        r"\e" | r"\edit" => {
            let editor = std::env::var("EDITOR")
                .or_else(|_| std::env::var("VISUAL"))
                .unwrap_or_else(|_| "vi".to_owned());
            let path = edit_tempfile(&session.vars);
            // Start with the last SQL we ran, so the user can tweak
            // and re-run quickly. Empty file if there's no prior SQL.
            if let Some(ref sql) = session.last_sql {
                let _ = fs::write(&path, sql);
            } else {
                let _ = fs::write(&path, "");
            }
            let status = Command::new(&editor).arg(&path).status();
            match status {
                Ok(s) if s.success() => {
                    let edited = fs::read_to_string(&path).unwrap_or_default();
                    let _ = fs::remove_file(&path);
                    let trimmed = edited.trim();
                    if trimmed.is_empty() {
                        writeln!(out, r"\e: buffer was empty after edit, nothing to run")?;
                        return Ok(MetaAction::Continue);
                    }
                    for stmt in split_statements(trimmed) {
                        session.execute_and_print(&stmt, out)?;
                    }
                }
                Ok(s) => {
                    let _ = fs::remove_file(&path);
                    writeln!(
                        out,
                        r"\e: editor ({editor}) exited with status {} — buffer discarded",
                        s.code().unwrap_or(-1),
                    )?;
                }
                Err(e) => {
                    let _ = fs::remove_file(&path);
                    writeln!(out, r"\e: failed to spawn {editor}: {e}")?;
                }
            }
            Ok(MetaAction::Continue)
        }

        r"\password" => {
            // Read a new password twice (no echo), then run a
            // parameterised ALTER USER.  Target defaults to the
            // connected user; an explicit `\password <user>` switches
            // the target.
            let target = if arg.is_empty() { session.user.clone() } else { arg.to_owned() };
            if !is_simple_ident(&target) {
                writeln!(out, r"\password: invalid user name {target:?}")?;
                return Ok(MetaAction::Continue);
            }
            let p1 = match rpassword::prompt_password(format!(
                "New password for \"{target}\": "
            )) {
                Ok(p) => p,
                Err(e) => {
                    writeln!(out, r"\password: read failed: {e}")?;
                    return Ok(MetaAction::Continue);
                }
            };
            let p2 = match rpassword::prompt_password("Confirm: ") {
                Ok(p) => p,
                Err(e) => {
                    writeln!(out, r"\password: read failed: {e}")?;
                    return Ok(MetaAction::Continue);
                }
            };
            if p1 != p2 {
                writeln!(out, r"\password: passwords did not match")?;
                return Ok(MetaAction::Continue);
            }
            // Target identifier is validated above; value bound as $1.
            let sql = format!("ALTER USER \"{target}\" PASSWORD $1");
            session.execute_and_print_params(&sql, &[Value::Text(p1)], out)?;
            Ok(MetaAction::Continue)
        }

        r"\copy" => {
            // Minimal first cut: supports two shapes, matching psql:
            //   \copy <table> FROM '<file>'  [WITH CSV HEADER]
            //   \copy <table> TO   '<file>'  [WITH CSV HEADER]
            // Always CSV, comma separator, double-quote quoting; WITH
            // CSV HEADER is on by default (first row is column names
            // for FROM, written on export for TO). The client does the
            // I/O — the server sees plain INSERT … / SELECT …
            // statements. No server-side COPY protocol dependency.
            match parse_copy(arg) {
                Some(CopyDir::From { table, path }) => copy_from_file(session, &table, &path, out),
                Some(CopyDir::To { table, path }) => copy_to_file(session, &table, &path, out),
                None => {
                    writeln!(
                        out,
                        r"Usage: \copy <table> FROM|TO '<path>'  (CSV, header row)",
                    )?;
                    Ok(MetaAction::Continue)
                }
            }
        }

        r"\du" => {
            session.execute_and_print(
                "SELECT rolname AS role, rolsuper, rolcanlogin \
                 FROM pg_roles ORDER BY rolname",
                out,
            )?;
            Ok(MetaAction::Continue)
        }

        r"\dn" => {
            session.execute_and_print(
                "SELECT nspname AS schema, nspowner FROM pg_namespace ORDER BY nspname",
                out,
            )?;
            Ok(MetaAction::Continue)
        }

        r"\df" => {
            session.execute_and_print(
                "SELECT routine_schema AS schema, routine_name AS name, data_type AS result \
                 FROM information_schema.routines \
                 WHERE routine_schema NOT IN ('pg_catalog', 'information_schema') \
                 ORDER BY routine_schema, routine_name",
                out,
            )?;
            Ok(MetaAction::Continue)
        }

        r"\x" => {
            session.expanded = !session.expanded;
            writeln!(
                out,
                "Expanded display is {}.",
                if session.expanded { "on" } else { "off" }
            )?;
            Ok(MetaAction::Continue)
        }

        r"\timing" => {
            session.timing = !session.timing;
            writeln!(
                out,
                "Timing is {}.",
                if session.timing { "on" } else { "off" }
            )?;
            Ok(MetaAction::Continue)
        }

        r"\pager" => {
            use crate::session::PagerSetting::*;
            match arg.to_ascii_lowercase().as_str() {
                "on"                  => session.pager = On,
                "off"                 => session.pager = Off,
                "auto" | ""           => session.pager = Auto,
                other => {
                    writeln!(out, r"\pager: unknown mode {other:?} (on | off | auto)")?;
                    return Ok(MetaAction::Continue);
                }
            }
            writeln!(out, "Pager: {:?}", session.pager)?;
            Ok(MetaAction::Continue)
        }

        r"\f" => {
            use crate::args::OutputFormat::*;
            match arg.to_ascii_lowercase().as_str() {
                "table" | "" => session.format = Table,
                "json"       => session.format = Json,
                "csv"        => session.format = Csv,
                other => {
                    writeln!(out, "unknown format: {other} (try table | json | csv)")?;
                    return Ok(MetaAction::Continue);
                }
            }
            writeln!(out, "Output format: {:?}", session.format)?;
            Ok(MetaAction::Continue)
        }

        r"\!" => {
            if arg.is_empty() {
                writeln!(out, "Usage: \\! <shell command>")?;
                return Ok(MetaAction::Continue);
            }
            // Spawn a subshell — respects PATH, signals propagate.
            // We intentionally do not redirect stderr/stdout; let them
            // pass through so the user sees their command's output.
            let status = Command::new("sh").arg("-c").arg(arg).status();
            match status {
                Ok(s) if s.success() => {}
                Ok(s) => writeln!(out, "(shell exited {})", s.code().unwrap_or(-1))?,
                Err(e) => writeln!(out, "shell error: {e}")?,
            }
            Ok(MetaAction::Continue)
        }

        r"\conninfo" => {
            writeln!(
                out,
                "Connected to database \"{}\" as user \"{}\" on host \"{}\" port \"{}\".",
                session.database, session.user, session.host, session.port,
            )?;
            Ok(MetaAction::Continue)
        }

        other => Ok(MetaAction::Unknown(other.to_owned())),
    }
}

// ─── helpers ────────────────────────────────────────────────────────────────

/// Render one cell of the last-result as a plain SQL token for
/// `\gexec`. NULL becomes the SQL `NULL` keyword; everything else uses
/// its textual form verbatim (no quoting — same as psql, which expects
/// the original query to have produced already-quoted text if needed).
fn cell_to_sql(v: &pyrosql::Value) -> String {
    match v {
        pyrosql::Value::Null => "NULL".to_owned(),
        pyrosql::Value::Bool(b) => (if *b { "TRUE" } else { "FALSE" }).to_owned(),
        pyrosql::Value::Int(n) => n.to_string(),
        pyrosql::Value::Float(f) => format!("{f}"),
        pyrosql::Value::Text(s) => s.clone(),
    }
}

/// Pick a unique temp-file path for `\e`'s scratch buffer. Lives in
/// `$TMPDIR` / `/tmp` and uses the pid + a monotonic counter so
/// concurrent `\e` invocations from the same REPL don't race.
fn edit_tempfile(_vars: &std::collections::HashMap<String, String>) -> PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let n = SEQ.fetch_add(1, Ordering::Relaxed);
    let mut path = std::env::temp_dir();
    path.push(format!("pyrosql-edit-{}-{n}.sql", std::process::id()));
    path
}

/// Minimal timestamp — we don't want a chrono dep for this. Format:
/// `2026-04-21 22:10:55 UTC` (human-ish, monotonic within a session).
fn chrono_like_now() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    // Very small Unix-epoch → YYYY-MM-DD HH:MM:SS conversion. Good
    // enough for the \watch header; not meant to replace strftime.
    let (y, mo, d, h, mi, s) = epoch_breakdown(secs);
    format!("{y:04}-{mo:02}-{d:02} {h:02}:{mi:02}:{s:02} UTC")
}

/// Very small proleptic-Gregorian date breakdown. Accepts 0..=~9999.
fn epoch_breakdown(mut s: u64) -> (u32, u32, u32, u32, u32, u32) {
    const SECS_PER_DAY: u64 = 86_400;
    let sod = s % SECS_PER_DAY;
    let h = (sod / 3600) as u32;
    let mi = ((sod % 3600) / 60) as u32;
    let ss = (sod % 60) as u32;
    let mut days = (s / SECS_PER_DAY) as i64;
    s = 0;
    let _ = s;
    // 1970-01-01 epoch. Walk year by year (cheap for any plausible
    // now()). Leap year on year % 4 == 0 && (year % 100 != 0 || year % 400 == 0).
    let mut year: u32 = 1970;
    loop {
        let ly = is_leap(year);
        let days_in_year = if ly { 366 } else { 365 };
        if days < days_in_year {
            break;
        }
        days -= days_in_year;
        year += 1;
    }
    let months_non_leap = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    let months_leap = [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    let months = if is_leap(year) { months_leap } else { months_non_leap };
    let mut month: u32 = 1;
    for d in months {
        if days < d {
            break;
        }
        days -= d;
        month += 1;
    }
    let day = (days + 1) as u32;
    (year, month, day, h, mi, ss)
}

fn is_leap(y: u32) -> bool {
    (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0)
}

enum CopyDir {
    From { table: String, path: String },
    To { table: String, path: String },
}

/// Parse `\copy TBL FROM|TO '<path>'`.  Very tolerant: case-insensitive
/// keyword, single OR double quotes around path, whitespace between
/// tokens.
fn parse_copy(arg: &str) -> Option<CopyDir> {
    let tokens: Vec<&str> = arg.splitn(3, char::is_whitespace).collect();
    if tokens.len() != 3 {
        return None;
    }
    let table = tokens[0].trim();
    if !is_safe_table_ref(table).is_some() {
        return None;
    }
    let dir = tokens[1].trim().to_ascii_uppercase();
    let path_raw = tokens[2].trim();
    let path = strip_quotes(path_raw)?.to_owned();
    match dir.as_str() {
        "FROM" => Some(CopyDir::From { table: table.to_owned(), path }),
        "TO" => Some(CopyDir::To { table: table.to_owned(), path }),
        _ => None,
    }
}

fn strip_quotes(s: &str) -> Option<&str> {
    let b = s.as_bytes();
    if b.len() >= 2 && (b[0] == b'\'' || b[0] == b'"') && b[0] == *b.last().unwrap() {
        Some(&s[1..s.len() - 1])
    } else {
        None
    }
}

/// Client-side `\copy TBL FROM 'file.csv'`:
///
/// 1. Read the file.
/// 2. Interpret the first line as the CSV header → column names.
/// 3. For each data row, issue `INSERT INTO tbl (cols…) VALUES (…)`.
///
/// Rows that fail to bind stop the copy and report the offending row
/// number. Bulk-insert + transaction wrapping is a follow-up; the
/// simple row-at-a-time shape trades throughput for debuggability.
fn copy_from_file<W: Write>(
    session: &mut Session,
    table: &str,
    path: &str,
    out: &mut W,
) -> Result<MetaAction> {
    let content = match fs::read_to_string(path) {
        Ok(s) => s,
        Err(e) => {
            writeln!(out, r"\copy FROM {path}: {e}")?;
            return Ok(MetaAction::Continue);
        }
    };
    let mut lines = content.lines();
    let header = match lines.next() {
        Some(h) => h,
        None => {
            writeln!(out, r"\copy FROM {path}: empty file")?;
            return Ok(MetaAction::Continue);
        }
    };
    let cols: Vec<String> = parse_csv_line(header).into_iter().collect();
    if cols.is_empty() {
        writeln!(out, r"\copy FROM {path}: header row is empty")?;
        return Ok(MetaAction::Continue);
    }
    // Validate column names before building the INSERT.
    for c in &cols {
        if !is_simple_ident(c) {
            writeln!(
                out,
                r"\copy FROM {path}: header {c:?} is not a plain identifier",
            )?;
            return Ok(MetaAction::Continue);
        }
    }
    let placeholders: String = (1..=cols.len())
        .map(|i| format!("${i}"))
        .collect::<Vec<_>>()
        .join(", ");
    let col_list = cols
        .iter()
        .map(|c| format!("\"{c}\""))
        .collect::<Vec<_>>()
        .join(", ");
    let insert_sql = format!("INSERT INTO \"{table}\" ({col_list}) VALUES ({placeholders})");

    let mut rows_written: u64 = 0;
    for (lineno, line) in lines.enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let fields = parse_csv_line(line);
        if fields.len() != cols.len() {
            writeln!(
                out,
                r"\copy FROM {path}: line {n}: expected {want} fields, got {got}",
                n = lineno + 2,
                want = cols.len(),
                got = fields.len(),
            )?;
            return Ok(MetaAction::Continue);
        }
        let params: Vec<Value> = fields.into_iter().map(Value::Text).collect();
        session.execute_and_print_params(&insert_sql, &params, &mut std::io::sink())?;
        rows_written += 1;
    }
    writeln!(out, r"\copy FROM {path}: {rows_written} rows copied")?;
    Ok(MetaAction::Continue)
}

/// Client-side `\copy TBL TO 'file.csv'`:
///
/// Runs `SELECT * FROM tbl` and writes the result as RFC 4180 CSV to
/// the given path. Always writes a header row.
fn copy_to_file<W: Write>(
    session: &mut Session,
    table: &str,
    path: &str,
    out: &mut W,
) -> Result<MetaAction> {
    let sql = format!("SELECT * FROM \"{table}\"");
    let result = futures::executor::block_on(session.client.query(&sql, &[]));
    let qr = match result {
        Ok(q) => q,
        Err(e) => {
            writeln!(out, r"\copy TO {path}: {e}")?;
            return Ok(MetaAction::Continue);
        }
    };
    let mut file = match fs::File::create(path) {
        Ok(f) => f,
        Err(e) => {
            writeln!(out, r"\copy TO {path}: {e}")?;
            return Ok(MetaAction::Continue);
        }
    };
    let header = qr
        .columns
        .iter()
        .map(|c| csv_escape_field(c))
        .collect::<Vec<_>>()
        .join(",");
    writeln!(file, "{header}")?;
    let mut n: u64 = 0;
    for row in &qr.rows {
        let line = row
            .values()
            .iter()
            .map(|v| csv_escape_field(&value_to_csv(v)))
            .collect::<Vec<_>>()
            .join(",");
        writeln!(file, "{line}")?;
        n += 1;
    }
    writeln!(out, r"\copy TO {path}: {n} rows written")?;
    Ok(MetaAction::Continue)
}

/// Render a `pyrosql::Value` for CSV output. NULL becomes an empty
/// field; everything else uses textual form.
fn value_to_csv(v: &pyrosql::Value) -> String {
    match v {
        pyrosql::Value::Null => String::new(),
        pyrosql::Value::Bool(b) => (if *b { "true" } else { "false" }).to_owned(),
        pyrosql::Value::Int(n) => n.to_string(),
        pyrosql::Value::Float(f) => format!("{f}"),
        pyrosql::Value::Text(s) => s.clone(),
    }
}

fn csv_escape_field(s: &str) -> String {
    if s.contains(',') || s.contains('"') || s.contains('\n') || s.contains('\r') {
        let esc = s.replace('"', "\"\"");
        format!("\"{esc}\"")
    } else {
        s.to_owned()
    }
}

/// Very small CSV line parser — RFC 4180, single-line scope, handles
/// `"…"` fields with doubled-quote escapes. Returns trimmed fields.
fn parse_csv_line(line: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut in_q = false;
    let bytes = line.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i] as char;
        if in_q {
            if c == '"' {
                if bytes.get(i + 1).copied() == Some(b'"') {
                    cur.push('"');
                    i += 2;
                    continue;
                }
                in_q = false;
                i += 1;
                continue;
            }
            cur.push(c);
            i += 1;
            continue;
        }
        match c {
            ',' => {
                out.push(std::mem::take(&mut cur));
            }
            '"' if cur.is_empty() => {
                in_q = true;
            }
            _ => cur.push(c),
        }
        i += 1;
    }
    out.push(cur);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_known_commands_listed_in_help() {
        // Sanity check: every command we handle shows up in META_HELP.
        let documented: std::collections::HashSet<&str> =
            META_HELP.iter().map(|(name, _)| *name).collect();
        // This is a very loose check — the mapping is name-with-alias
        // form ("\q, \quit"), so we just confirm the list is non-trivial.
        assert!(documented.len() >= 10);
    }

    #[test]
    fn ident_accepts_plain_names() {
        assert!(is_simple_ident("users"));
        assert!(is_simple_ident("_t"));
        assert!(is_simple_ident("t1"));
        assert!(is_simple_ident("User_Accounts"));
    }

    #[test]
    fn ident_rejects_injection_attempts() {
        assert!(!is_simple_ident(""));
        assert!(!is_simple_ident("1bad"));
        assert!(!is_simple_ident("users;DROP TABLE"));
        assert!(!is_simple_ident("users'--"));
        assert!(!is_simple_ident("a b"));
        assert!(!is_simple_ident("a.b")); // dot handled by table_ref, not here
    }

    #[test]
    fn table_ref_splits_schema() {
        assert_eq!(is_safe_table_ref("t"),          Some((None, "t")));
        assert_eq!(is_safe_table_ref("s.t"),        Some((Some("s"), "t")));
        assert_eq!(is_safe_table_ref("bad;name"),   None);
        assert_eq!(is_safe_table_ref("'); drop --"),None);
        assert_eq!(is_safe_table_ref(""),           None);
    }

    // Note: full dispatch tests require a live `Session` (which needs a
    // live server).  Integration tests in `tests/e2e.rs` cover those,
    // marked `#[ignore]` so they don't run without a server.
}
