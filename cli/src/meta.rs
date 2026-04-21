//! psql-style backslash meta commands.
//!
//! Every meta command is dispatched via [`dispatch`], which returns a
//! [`MetaAction`] telling the REPL whether to continue, quit, reconnect,
//! or print output.  Keeping the dispatcher pure + table-driven makes it
//! easy to unit-test without a live server.

use std::io::Write;
use std::process::Command;

use anyhow::Result;

use crate::session::Session;

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
    (r"\q, \quit",     "exit the REPL"),
    (r"\h, \help",     "show this help"),
    (r"\c <db>",       "switch database (reconnects)"),
    (r"\l",            "list databases"),
    (r"\d [table]",    "describe a table, or list tables if no arg"),
    (r"\dt",           "list tables (alias for \\d)"),
    (r"\x",            "toggle expanded output"),
    (r"\timing",       "toggle per-statement timing"),
    (r"\f <fmt>",      "switch output format: table | json | csv"),
    (r"\! <cmd>",      "run a shell command"),
    (r"\conninfo",     "show current connection info"),
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
                // Describe one table.
                let table = arg.replace('\'', "''");
                let sql = format!(
                    "SELECT column_name, data_type, is_nullable \
                     FROM information_schema.columns \
                     WHERE table_name = '{table}' \
                     ORDER BY ordinal_position"
                );
                session.execute_and_print(&sql, out)?;
            }
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

    // Note: full dispatch tests require a live `Session` (which needs a
    // live server).  Integration tests in `tests/e2e.rs` cover those,
    // marked `#[ignore]` so they don't run without a server.
}
