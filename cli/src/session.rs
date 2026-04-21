//! Connection bringup + non-interactive execution paths (`-c` / `-f`).
//!
//! The `Session` struct wraps a [`pyrosql::Client`] plus the toggles that
//! live alongside the connection (expanded display, timing, output
//! format).  Those toggles are exposed so the REPL can mutate them from
//! meta commands (`\x`, `\timing`, etc.) without reopening a socket.

use std::fs;
use std::io::{self, IsTerminal, Read, Write};
use std::time::Instant;

use anyhow::{bail, Context, Result};
use futures::executor::block_on;
use pyrosql::{Client, ConnectConfig, QueryResult, Scheme};

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
}

impl Session {
    /// Build a [`ConnectConfig`] from the current session fields.
    fn connect_config(&self) -> ConnectConfig {
        ConnectConfig::new(&self.host, self.port)
            .user(&self.user)
            .password(&self.password)
            .database(&self.database)
            .scheme(Scheme::Wire)
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
        let started = Instant::now();
        let result = block_on(self.client.query(sql, &[]));
        let elapsed = started.elapsed();

        match result {
            Ok(qr) => {
                self.track_tx_state(sql, &qr);
                format::print_result(&qr, self.format, self.expanded, out)?;
                if self.timing {
                    writeln!(out, "Time: {:.3} ms", elapsed.as_secs_f64() * 1_000.0)?;
                }
                Ok(())
            }
            Err(e) => {
                if self.verbose {
                    writeln!(out, "ERROR: {e:#}")?;
                } else {
                    writeln!(out, "ERROR: {e}")?;
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
    // If a URL was given, it wins over the individual flags.
    let cfg = if let Some(url) = cli.url.as_deref() {
        ConnectConfig::from_url(url).context("parsing connection URL")?
    } else {
        ConnectConfig::new(&cli.host, cli.port)
            .user(&cli.user)
            .database(&cli.database)
            .scheme(Scheme::Wire)
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
    // reuse it verbatim.
    let cfg = ConnectConfig::new(&cfg.host, cfg.port)
        .user(&cfg.user)
        .password(&password)
        .database(&cfg.database)
        .scheme(cfg.scheme);

    let host = cfg.host.clone();
    let port = cfg.port;
    let user = cfg.user.clone();
    let database = cfg.database.clone();

    let client = block_on(Client::connect(cfg)).context("connecting to PyroSQL server")?;

    // Suppress the banner in non-interactive contexts (piped stdout / stderr,
    // CI logs, `pyrosql -c`), not just when --quiet is explicit. Script
    // authors redirecting to a file don't want the banner polluting output.
    let quiet = cli.quiet
        || cli.command.is_some()
        || cli.file.is_some()
        || !io::stderr().is_terminal();
    if !quiet {
        eprintln!(
            "pyrosql {} (PWire-only client, plaintext TCP) — connected to {}@{}:{}/{}",
            env!("CARGO_PKG_VERSION"),
            user,
            host,
            port,
            database,
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
    })
}

/// Read a password from stdin without echoing via `rpassword`.  Pure
/// sync (termios on Unix, Console* on Windows); musl-compatible.
fn read_password_stdin() -> Result<String> {
    rpassword::prompt_password("Password: ")
        .map_err(|e| anyhow::anyhow!("reading password: {e}"))
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

    for stmt in split_statements(&content) {
        let trimmed = stmt.trim();
        if trimmed.is_empty() {
            continue;
        }
        session.execute_and_print(trimmed, &mut out)?;
    }
    Ok(())
}

/// Split a SQL script on `;` boundaries, ignoring semicolons inside
/// single-quoted strings and `--` line comments.  Good enough for psql-
/// style scripts; does NOT handle dollar-quoted blocks yet.
pub fn split_statements(src: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let mut cur = String::new();
    let mut in_str = false;
    let mut in_line_comment = false;
    for ch in src.chars() {
        if in_line_comment {
            cur.push(ch);
            if ch == '\n' {
                in_line_comment = false;
            }
            continue;
        }
        if in_str {
            cur.push(ch);
            if ch == '\'' {
                in_str = false;
            }
            continue;
        }
        match ch {
            '\'' => {
                in_str = true;
                cur.push(ch);
            }
            '-' if cur.ends_with('-') => {
                in_line_comment = true;
                cur.push(ch);
            }
            ';' => {
                if !cur.trim().is_empty() {
                    out.push(cur.clone());
                }
                cur.clear();
            }
            _ => cur.push(ch),
        }
    }
    if !cur.trim().is_empty() {
        out.push(cur);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::split_statements;

    #[test]
    fn split_empty() {
        assert!(split_statements("").is_empty());
        assert!(split_statements("   \n\n  ").is_empty());
    }

    #[test]
    fn split_simple() {
        let v = split_statements("SELECT 1; SELECT 2;");
        assert_eq!(v.len(), 2);
        assert!(v[0].contains("SELECT 1"));
        assert!(v[1].contains("SELECT 2"));
    }

    #[test]
    fn split_no_trailing_semicolon() {
        let v = split_statements("SELECT 1");
        assert_eq!(v.len(), 1);
    }

    #[test]
    fn split_string_contains_semicolon() {
        let v = split_statements("INSERT INTO t VALUES ('a;b'); SELECT 1;");
        assert_eq!(v.len(), 2);
        assert!(v[0].contains("'a;b'"));
    }

    #[test]
    fn split_line_comment() {
        let v = split_statements("-- first; stmt\nSELECT 1;");
        assert_eq!(v.len(), 1);
        assert!(v[0].contains("SELECT 1"));
    }
}
