//! Connection bringup + non-interactive execution paths (`-c` / `-f`).
//!
//! The `Session` struct wraps a [`pyrosql::Client`] plus the toggles that
//! live alongside the connection (expanded display, timing, output
//! format).  Those toggles are exposed so the REPL can mutate them from
//! meta commands (`\x`, `\timing`, etc.) without reopening a socket.

use std::collections::HashMap;
use std::fs;
use std::io::{self, IsTerminal, Read, Write};
use std::time::Instant;

use anyhow::{bail, Context, Result};
use futures::executor::block_on;
use pyrosql::{Client, ConnectConfig, QueryResult, Scheme, Value};

use crate::args::{Cli, OutputFormat};
use crate::format;

/// Runtime session state mutated by REPL meta commands.
pub struct Session {
    pub client: Client,
    pub host: String,
    pub port: u16,
    pub user: String,
    pub database: String,
    pub password: String,
    pub format: OutputFormat,
    pub expanded: bool,
    pub timing: bool,
    pub verbose: bool,
    pub in_transaction: bool,
    /// Dialect the server applies to SQL in this session. `None` means
    /// "use the server default" — kept instead of always collapsing to
    /// `SyntaxMode::PyroSQL` so `\c` reconnect preserves the exact
    /// handshake intent the user had at startup.
    pub dialect: Option<pyrosql::SyntaxMode>,
    /// Client-side variables set via `\set NAME value`.  Expanded in
    /// SQL and meta-arg strings as `:NAME` (bare), `:'NAME'` (single-
    /// quoted literal), and `:"NAME"` (double-quoted identifier),
    /// matching psql semantics. Lives alongside the connection and is
    /// preserved across `\c` reconnects — variables are a purely
    /// client-side concept.
    pub vars: HashMap<String, String>,
    /// Last QueryResult returned to the user — used by `\gexec` to
    /// iterate over the previous result set's rows.  Stored as owned
    /// data so it doesn't keep a borrow on the Client or its buffers.
    pub last_result: Option<QueryResult>,
    /// Exact SQL text of the last successful query — used by `\watch`
    /// to re-run the same statement and by `\e` to pre-populate the
    /// external editor with the previous buffer.
    pub last_sql: Option<String>,
    /// Pager setting — when Auto on an interactive stdout and the
    /// rendered result exceeds the terminal height, we pipe the output
    /// through `$PAGER` (default `less -FRSX`). `Off` always writes
    /// direct; `On` always pages even on non-TTY (rarely useful but
    /// lets a user force it).
    pub pager: PagerSetting,
}

/// Three-state pager switch. Default: `Auto`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PagerSetting {
    /// Never pipe through a pager.
    Off,
    /// Always pipe through `$PAGER`.
    On,
    /// Pipe only when stdout is a TTY and the output is long enough to
    /// benefit from paging.
    Auto,
}

impl Default for PagerSetting {
    fn default() -> Self {
        Self::Auto
    }
}

impl Session {
    /// Build a [`ConnectConfig`] from the current session fields.
    fn connect_config(&self) -> ConnectConfig {
        let mut cfg = ConnectConfig::new(&self.host, self.port)
            .user(&self.user)
            .password(&self.password)
            .database(&self.database)
            .scheme(Scheme::Wire);
        if let Some(mode) = self.dialect {
            cfg = cfg.dialect(mode);
        }
        cfg
    }

    /// Reconnect against (possibly new) session fields.  Used by `\c`.
    pub fn reconnect(&mut self) -> Result<()> {
        let cfg = self.connect_config();
        let client = block_on(Client::connect(cfg))
            .context("reconnect failed")?;
        self.client = client;
        self.in_transaction = false;
        Ok(())
    }

    /// Execute a single SQL statement and print its result.
    ///
    /// Tracks transaction state heuristically from the SQL head so the
    /// REPL can switch the prompt to `pyrosql*>` inside a tx.
    pub fn execute_and_print<W: Write>(&mut self, sql: &str, out: &mut W) -> Result<()> {
        self.execute_and_print_params(sql, &[], out)
    }

    /// Parameterized variant of [`execute_and_print`]. Used by meta
    /// commands like `\d <table>` where we want to bind the table name
    /// as a `$1` text parameter instead of formatting it into a literal.
    pub fn execute_and_print_params<W: Write>(
        &mut self,
        sql: &str,
        params: &[Value],
        out: &mut W,
    ) -> Result<()> {
        // Expand `:var` / `:'var'` / `:"var"` BEFORE sending. Parametrised
        // queries still get the substitution — `\d :tbl` is a valid
        // psql-ism — but server-side `$N` placeholders are untouched
        // since those use `$`, not `:`.
        let expanded = crate::vars::expand(sql, &self.vars);
        let sql: &str = expanded.as_deref().unwrap_or(sql);
        let started = Instant::now();
        let result = block_on(self.client.query(sql, params));
        let elapsed = started.elapsed();

        match result {
            Ok(qr) => {
                self.track_tx_state(sql, &qr);
                // Render into a buffer so we can decide pager-or-not
                // AFTER knowing the final line count. For short results
                // on a TTY we still write directly; only long ones go
                // through the pager.
                let mut buf: Vec<u8> = Vec::new();
                format::print_result(&qr, self.format, self.expanded, &mut buf)?;
                write_maybe_paged(self.pager, &buf, out)?;
                if self.timing {
                    writeln!(out, "{}", format_timing(elapsed))?;
                }
                // Remember the last result so `\gexec` can iterate it
                // and `\watch` / `\e` can re-run the same SQL.
                self.last_result = Some(qr);
                self.last_sql = Some(sql.to_owned());
                Ok(())
            }
            Err(e) => {
                let (red, reset) = error_color();
                if self.verbose {
                    writeln!(out, "{red}ERROR:{reset} {e:#}")?;
                } else {
                    writeln!(out, "{red}ERROR:{reset} {e}")?;
                }
                // Keep the session alive; caller decides whether to abort
                // (one-shot / -f abort; interactive continues).
                Ok(())
            }
        }
    }

    /// Heuristic: detect BEGIN / COMMIT / ROLLBACK at the start of the
    /// statement to flip the prompt indicator.  Cheap and mostly
    /// correct — server state is the source of truth if it matters.
    fn track_tx_state(&mut self, sql: &str, _qr: &QueryResult) {
        let head: String = sql
            .trim_start()
            .chars()
            .take_while(|c| !c.is_whitespace() && *c != ';')
            .map(|c| c.to_ascii_uppercase())
            .collect();
        match head.as_str() {
            "BEGIN" | "START" => self.in_transaction = true,
            "COMMIT" | "END" | "ROLLBACK" | "ABORT" => self.in_transaction = false,
            _ => {}
        }
    }
}

/// Open a [`Session`] using the CLI args (resolve URL vs flags, prompt
/// for password if requested).
pub fn open_session(cli: &Cli) -> Result<Session> {
    // If a URL was given, it wins over the individual flags.  When the
    // URL already carries `?dialect=…` we honour that (treat the URL as
    // fully specified); otherwise fall back to the `-D` flag so the CLI
    // still lets the user pick a dialect without hand-crafting the URL.
    let cfg = if let Some(url) = cli.url.as_deref() {
        warn_foreign_scheme(url);
        let mut cfg = ConnectConfig::from_url(url).context("parsing connection URL")?;
        if cfg.syntax_mode.is_none() {
            cfg = cfg.dialect(cli.dialect.to_syntax_mode());
        }
        cfg
    } else {
        ConnectConfig::new(&cli.host, cli.port)
            .user(&cli.user)
            .database(&cli.database)
            .scheme(Scheme::Wire)
            .dialect(cli.dialect.to_syntax_mode())
    };

    // Password precedence:  --password > -W prompt > PYROSQL_PASSWORD > URL > "".
    let mut password = cfg.password.clone();
    if let Some(p) = cli.password.as_deref() {
        password = p.to_owned();
    } else if cli.password_prompt {
        password = read_password_stdin()?;
    } else if password.is_empty() {
        if let Ok(p) = std::env::var("PYROSQL_PASSWORD") {
            password = p;
        }
    }

    // TLS is not yet wired through the PWire driver (see the TODO at the
    // bottom of rust/src/pwire.rs).  Be loud about it rather than silently
    // accepting the flag: a user who asked for TLS probably has a security
    // requirement that we cannot honour, so failing shut is the right call.
    if cli.tls {
        bail!(
            "--tls: TLS is not yet implemented in the PWire client. \
             Connections are plaintext-TCP only. Remove --tls or wait \
             for the TLS-capable driver release (tracked at \
             https://github.com/PyroSQL/pyrosql-driver/issues/tls)."
        );
    }
    // --no-tls is a no-op: plaintext is already the only mode. Silently
    // accepting it is fine because the user got what they asked for.
    let _ = cli.no_tls;

    // Rebuild the config with the resolved password so `reconnect()` can
    // reuse it verbatim.  Preserve the dialect selection from the
    // original cfg — the builder chain below drops every field that
    // isn't explicitly re-set, which silently defeated `--dialect` on
    // reconnect before this line existed.
    let preserved_dialect = cfg.syntax_mode;
    let mut cfg = ConnectConfig::new(&cfg.host, cfg.port)
        .user(&cfg.user)
        .password(&password)
        .database(&cfg.database)
        .scheme(cfg.scheme);
    if let Some(mode) = preserved_dialect {
        cfg = cfg.dialect(mode);
    }

    let host = cfg.host.clone();
    let port = cfg.port;
    let user = cfg.user.clone();
    let database = cfg.database.clone();
    let dialect = cfg.syntax_mode;

    let client = block_on(Client::connect(cfg)).context("connecting to PyroSQL server")?;

    // Suppress the banner in non-interactive contexts (piped stdout / stderr,
    // CI logs, `pyrosql -c`), not just when --quiet is explicit. Script
    // authors redirecting to a file don't want the banner polluting output.
    let quiet = cli.quiet
        || cli.command.is_some()
        || cli.file.is_some()
        || !io::stderr().is_terminal();
    if !quiet {
        let dialect_str = dialect
            .map(|m| m.as_set_value())
            .unwrap_or("pyro (default)");
        eprintln!(
            "pyrosql {} (PWire-only client, plaintext TCP) — connected to {}@{}:{}/{}  dialect={}",
            env!("CARGO_PKG_VERSION"),
            user,
            host,
            port,
            database,
            dialect_str,
        );
        eprintln!(r"Type \h for help, \q to quit.");
    }

    Ok(Session {
        client,
        host,
        port,
        user,
        database,
        password,
        format: cli.format,
        expanded: cli.expanded,
        timing: cli.timing,
        verbose: cli.verbose,
        in_transaction: false,
        dialect,
        vars: HashMap::new(),
        last_result: None,
        last_sql: None,
        pager: PagerSetting::default(),
    })
}

/// Write `buf` either directly to `out` or through `$PAGER`,
/// depending on the pager setting and TTY-state of stdout.
///
/// The pager decision is:
/// - `Off`                          → write direct.
/// - `On`                           → always pipe through `$PAGER`
///                                    (defaulting to `less -FRSX`).
/// - `Auto` + stdout is a TTY
///    + output ≥ terminal-height   → pipe through `$PAGER`.
/// - any other combination          → write direct.
///
/// When paging, the function spawns the pager with a piped stdin,
/// writes the buffer in one go, closes the pipe, and waits for the
/// pager to exit. Pager spawn failures fall back to direct write so
/// a missing `less` never drops output on the floor.
fn write_maybe_paged<W: Write>(
    setting: PagerSetting,
    buf: &[u8],
    out: &mut W,
) -> Result<()> {
    let should_page = match setting {
        PagerSetting::Off => false,
        PagerSetting::On => true,
        PagerSetting::Auto => {
            let lines = buf.iter().filter(|&&b| b == b'\n').count();
            io::stdout().is_terminal() && lines >= terminal_height().saturating_sub(1)
        }
    };
    if !should_page {
        out.write_all(buf)?;
        return Ok(());
    }
    let pager_cmd = std::env::var("PAGER").unwrap_or_else(|_| "less -FRSX".to_owned());
    let mut parts = pager_cmd.split_whitespace();
    let prog = parts.next().unwrap_or("less");
    let rest: Vec<&str> = parts.collect();
    let mut cmd = std::process::Command::new(prog);
    cmd.args(&rest);
    cmd.stdin(std::process::Stdio::piped());
    let spawn = cmd.spawn();
    match spawn {
        Ok(mut child) => {
            if let Some(stdin) = child.stdin.as_mut() {
                let _ = stdin.write_all(buf);
            }
            let _ = child.wait();
            Ok(())
        }
        Err(_) => {
            // Pager missing — fall back to direct write rather than
            // losing the result.
            out.write_all(buf)?;
            Ok(())
        }
    }
}

/// Rough terminal height in rows — used to decide when the Auto pager
/// should kick in. We keep it unsafe-free: read `$LINES` when set
/// (covers the common case where the shell exports it, plus tests
/// that need a deterministic value), otherwise default to 24 rows.
/// A more accurate answer would need `ioctl(TIOCGWINSZ)` — an
/// `unsafe` call — and the UX gain vs a conservative default is
/// small enough that the simpler path wins.
fn terminal_height() -> usize {
    std::env::var("LINES")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(24)
}

/// Decide whether error output should carry ANSI colour. Honours the
/// universal `NO_COLOR` convention and only emits escapes when stdout
/// is an interactive terminal — piping `pyrosql` to a file keeps the
/// output clean. Returns `(prefix, suffix)` strings so callers can
/// interpolate without branching at every write-site.
fn error_color() -> (&'static str, &'static str) {
    let no_color = std::env::var_os("NO_COLOR")
        .map(|v| !v.is_empty())
        .unwrap_or(false);
    if !no_color && io::stdout().is_terminal() {
        ("\x1b[1;31m", "\x1b[0m")
    } else {
        ("", "")
    }
}

/// Read a password from stdin without echoing via `rpassword`.  Pure
/// sync (termios on Unix, Console* on Windows); musl-compatible.
fn read_password_stdin() -> Result<String> {
    rpassword::prompt_password("Password: ")
        .map_err(|e| anyhow::anyhow!("reading password: {e}"))
}

/// Format a query duration in the largest unit that keeps at least one
/// integer digit, capped at three significant figures. Sub-millisecond
/// queries print in µs, millisecond-scale in ms, and multi-second
/// queries in s — matches the `time`/`hyperfine` convention and is
/// friendlier than psql's unconditional-millis default.
pub fn format_timing(d: std::time::Duration) -> String {
    let ns = d.as_nanos();
    if ns < 1_000 {
        format!("Time: {ns} ns")
    } else if ns < 1_000_000 {
        // µs range.
        format!("Time: {:.3} µs", ns as f64 / 1_000.0)
    } else if ns < 1_000_000_000 {
        // ms range.
        format!("Time: {:.3} ms", ns as f64 / 1_000_000.0)
    } else {
        // s range.
        format!("Time: {:.3} s", ns as f64 / 1_000_000_000.0)
    }
}

#[cfg(test)]
mod timing_tests {
    use super::format_timing;
    use std::time::Duration;

    #[test]
    fn nanoseconds_format() {
        assert_eq!(format_timing(Duration::from_nanos(500)), "Time: 500 ns");
    }

    #[test]
    fn microseconds_format() {
        let s = format_timing(Duration::from_nanos(12_345));
        assert!(s.contains("µs"), "got {s}");
        assert!(s.contains("12."), "got {s}");
    }

    #[test]
    fn milliseconds_format() {
        let s = format_timing(Duration::from_millis(7));
        assert!(s.contains("ms"), "got {s}");
        assert!(s.contains("7."), "got {s}");
    }

    #[test]
    fn seconds_format() {
        let s = format_timing(Duration::from_secs_f64(1.25));
        assert!(s.contains(" s"), "got {s}");
        assert!(s.contains("1.250"), "got {s}");
    }
}

/// Warn (but don't fail) when a user hands us a `postgres://` / `pg://` /
/// `mysql://` URL. `ConnectConfig::from_url` happily accepts those for
/// UX parity, but we'll still speak PWire at the socket — which means
/// the target server has to be PyroSQL. Giving a Postgres or MySQL URL
/// to this client will get cryptic handshake errors; a one-line warning
/// up front is much friendlier than debugging a mismatched protocol.
fn warn_foreign_scheme(url: &str) {
    let lower = url.trim_start().to_ascii_lowercase();
    let foreign = lower.starts_with("postgres://")
        || lower.starts_with("postgresql://")
        || lower.starts_with("pg://")
        || lower.starts_with("mysql://");
    if foreign && io::stderr().is_terminal() {
        eprintln!(
            "warning: this client only speaks the PWire protocol. \
             The URL scheme is accepted for convenience, but the target \
             server must be a PyroSQL instance — Postgres/MySQL servers \
             will fail the handshake."
        );
    }
}

// ── Non-interactive entry points ────────────────────────────────────────────

/// `pyrosql -c "SELECT 1"` — execute once and exit.
pub fn run_one_shot(cli: &Cli, sql: &str) -> Result<()> {
    let mut session = open_session(cli)?;
    let stdout = io::stdout();
    let mut out = stdout.lock();
    session.execute_and_print(sql, &mut out)
}

/// `pyrosql -f script.sql` — read a file, split on `;` boundaries, run
/// each statement.  Stops on the first error unless stdin is piped.
pub fn run_file(cli: &Cli, path: &str) -> Result<()> {
    let content = if path == "-" {
        let mut s = String::new();
        io::stdin().read_to_string(&mut s)?;
        s
    } else {
        fs::read_to_string(path)
            .with_context(|| format!("reading script file {path}"))?
    };

    let mut session = open_session(cli)?;
    let stdout = io::stdout();
    let mut out = stdout.lock();

    for stmt in crate::sqltok::split_statements(&content) {
        session.execute_and_print(&stmt, &mut out)?;
    }
    Ok(())
}

// Tests for statement splitting live next to the tokenizer in `sqltok.rs`.
