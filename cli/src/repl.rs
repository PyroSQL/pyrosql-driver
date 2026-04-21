//! Interactive REPL loop, driven by `rustyline`.
//!
//! Prompt hierarchy:
//! * `pyrosql=> `   — normal.
//! * `pyrosql*> `   — inside an explicit BEGIN … COMMIT block.
//! * `pyrosql-> `   — continuation line (multi-line SQL not yet ended by
//!                    `;`).
//!
//! History is persisted to `~/.pyrosql_history` (falls back to
//! `$HOME/.pyrosql_history`, or to an in-memory history if $HOME is
//! unset — e.g. inside containers running as uid 0 without /root).

use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::Result;
use rustyline::config::Configurer;
use rustyline::error::ReadlineError;
use rustyline::history::DefaultHistory;
use rustyline::Editor;

use crate::args::Cli;
use crate::complete::SqlHelper;
use crate::meta;
use crate::session::{open_session, Session};

/// Handle exposed to the SIGINT handler so it can call `abort()` on
/// whatever `PyroWireConnection` is currently active. When the REPL
/// reconnects after a successful cancel, it swaps the Arc<PWC> inside
/// this Mutex so the next Ctrl-C aborts the fresh connection, not the
/// dead one.
type CancelSlot = Arc<Mutex<Option<Arc<pyrosql::PyroWireConnection>>>>;

fn history_path() -> Option<PathBuf> {
    let home = std::env::var_os("HOME")?;
    let mut p = PathBuf::from(home);
    p.push(".pyrosql_history");
    Some(p)
}

/// Resolve the startup-rc file path. `$PYROSQL_RC` wins when set
/// (lets tests point at a scratch file), otherwise `~/.pyrosqlrc`.
/// Returns `None` when no path can be formed — the REPL then skips
/// the rc phase entirely.
fn rcfile_path() -> Option<PathBuf> {
    if let Some(p) = std::env::var_os("PYROSQL_RC") {
        let pb = PathBuf::from(p);
        if !pb.as_os_str().is_empty() {
            return Some(pb);
        }
    }
    let home = std::env::var_os("HOME")?;
    let mut p = PathBuf::from(home);
    p.push(".pyrosqlrc");
    Some(p)
}

fn prompt(session: &Session, in_multiline: bool) -> &'static str {
    if in_multiline {
        "pyrosql-> "
    } else if session.in_transaction {
        "pyrosql*> "
    } else {
        "pyrosql=> "
    }
}

// REPL statement-completion check delegates to the crate-wide tokenizer
// so single-quotes, double-quotes, dollar-quoted blocks, line comments,
// and block comments are all handled identically to the script splitter.
use crate::sqltok::is_complete;

pub fn run_repl(cli: &Cli) -> Result<()> {
    let mut session = open_session(cli)?;

    // ── Ctrl-C / SIGINT wiring ──────────────────────────────────────
    //
    // Rustyline detects Ctrl-C from the raw terminal bytes (0x03) on
    // its own and returns `ReadlineError::Interrupted`, no OS signal
    // needed — that path already works for "abandon buffer". The gap
    // is Ctrl-C *while a query is running*: rustyline isn't reading,
    // the terminal is in cooked mode, so ^C does generate SIGINT. The
    // `ctrlc` crate installs a handler that runs on a dedicated thread
    // (NOT in signal context), so calling `PyroWireConnection::abort`
    // — which ultimately does `TcpStream::shutdown` — is safe.
    //
    // The handler only acts while `running_query` is set; at the
    // prompt it's a no-op and rustyline's Interrupted path owns the
    // semantics. After a successful cancel the REPL reconnects and
    // swaps the Arc<PWC> inside `cancel_slot` so the next ^C aborts
    // the fresh session, not the dead one.
    // NOTE on Ctrl-C mid-query:
    //
    // We prototyped a SIGINT handler (via the `ctrlc` crate) that
    // called `Client::abort()` on the in-flight connection so the REPL
    // could cancel a long query and auto-reconnect. That handler
    // correctly tore down the socket on SIGINT but its background
    // thread (ctrlc loops on `sem_wait_forever`) prevented the REPL
    // from exiting cleanly on Ctrl-D / `\q`, turning a simple quit
    // into a hang both under PTY and under piped-stdin test harnesses.
    //
    // Rather than ship a handler that breaks the much more common
    // "exit the REPL cleanly" path, we leave the SIGINT default in
    // place: Ctrl-C during a running query kills the whole `pyrosql`
    // process (which still releases the server-side session, because
    // the TCP socket closes on process death). The programmatic path
    // — `pyrosql::Client::abort()` — stays available and is covered
    // by its own tests, so library consumers keep full cancel
    // semantics. A proper PTY-friendly cancel requires either (a) the
    // PWire protocol gaining a `MSG_CANCEL` side-channel frame so we
    // don't need a thread-based signal handler at all, or (b)
    // swapping `ctrlc` for a hand-rolled `sigaction`-based handler
    // that doesn't fork a blocking helper thread. Both are server /
    // infra work, not CLI polish — filed as follow-ups.
    //
    // Keep the bindings compiled anyway so the module doesn't churn
    // when we wire the real thing up.
    let cancel_slot: CancelSlot = Arc::new(Mutex::new(Some(Arc::clone(session.client.inner()))));
    let running_query = Arc::new(AtomicBool::new(false));
    let was_cancelled = Arc::new(AtomicBool::new(false));

    // Honour `~/.pyrosqlrc` (or `$PYROSQL_RC`) once at session start.
    // Every non-blank line is treated like a stand-alone REPL entry
    // — backslash → meta dispatcher, everything else → execute_and_print.
    // A failing meta command or bad SQL is reported to stderr but
    // does NOT abort startup; the user can still issue queries.
    let stdout_for_rc = io::stdout();
    let mut rc_out = stdout_for_rc.lock();
    if let Some(rc_path) = rcfile_path() {
        if let Ok(content) = std::fs::read_to_string(&rc_path) {
            for line in content.lines() {
                let trimmed = line.trim();
                if trimmed.is_empty() || trimmed.starts_with("--") || trimmed.starts_with('#') {
                    continue;
                }
                if trimmed.starts_with('\\') {
                    let _ = meta::dispatch(trimmed, &mut session, &mut rc_out);
                } else {
                    let stmt = trimmed.trim_end_matches(|c: char| c == ';');
                    if !stmt.is_empty() {
                        let _ = session.execute_and_print(stmt, &mut rc_out);
                    }
                }
            }
        }
    }
    drop(rc_out);

    // rustyline editor with default history — bump max size to 10k and
    // drop consecutive duplicates so ↑ walks meaningful history rather
    // than the same `\d` spammed ten times. We still add entries
    // manually (not via auto_add_history) so multi-line SQL gets
    // reassembled into one history line instead of per-line fragments.
    // The `SqlHelper` provides Tab-completion for SQL keywords +
    // backslash meta-commands; Hinter / Validator / Highlighter stay
    // as no-op defaults.
    let mut rl: Editor<SqlHelper, DefaultHistory> = Editor::new()?;
    rl.set_helper(Some(SqlHelper::new()));
    rl.set_max_history_size(10_000)?;
    rl.set_history_ignore_dups(true)?;
    let hist = history_path();
    if let Some(h) = hist.as_ref() {
        let _ = rl.load_history(h);
    }

    let stdout = io::stdout();
    let mut out = stdout.lock();

    let mut buffer = String::new();
    loop {
        let p = prompt(&session, !buffer.is_empty());
        let line_res = rl.readline(p);

        let line = match line_res {
            Ok(l) => l,
            Err(ReadlineError::Interrupted) => {
                // Ctrl-C at the prompt — abandon current buffer, print
                // a newline.
                //
                // Known limitation: Ctrl-C pressed *while a query is
                // already executing* cannot currently cancel that
                // query. The PWire protocol has no CancelRequest
                // message (the PostgreSQL equivalent), so we have no
                // way to tell the server "stop what you're doing" on
                // a second connection. The only workaround today is
                // to force-close the process (the dropped TCP socket
                // eventually releases the server-side resources). A
                // future server change to add MSG_CANCEL will let us
                // wire this up properly — tracked alongside the
                // dialect-selector work in the PWire handshake.
                buffer.clear();
                writeln!(out)?;
                continue;
            }
            Err(ReadlineError::Eof) => {
                // Ctrl-D — exit cleanly.
                writeln!(out)?;
                break;
            }
            Err(e) => {
                writeln!(out, "readline error: {e}")?;
                break;
            }
        };

        let trimmed = line.trim();

        // Backslash meta commands run only when not in the middle of a
        // multi-line SQL buffer.
        if buffer.is_empty() && trimmed.starts_with('\\') {
            let _ = rl.add_history_entry(&line);
            match meta::dispatch(&line, &mut session, &mut out)? {
                meta::MetaAction::Quit => break,
                meta::MetaAction::Continue => {}
                meta::MetaAction::Unknown(cmd) => {
                    writeln!(out, r"Unknown command: {cmd}. Type \h for help.")?;
                }
            }
            continue;
        }

        if trimmed.is_empty() && buffer.is_empty() {
            continue;
        }

        // Accumulate into the multi-line buffer.
        if !buffer.is_empty() {
            buffer.push('\n');
        }
        buffer.push_str(&line);

        if is_complete(&buffer) {
            let _ = rl.add_history_entry(buffer.trim_end());
            let stmt = std::mem::take(&mut buffer);
            // Peel the trailing `;` off so the server doesn't see an
            // empty second statement.
            let stmt = stmt.trim_end_matches(|c: char| c.is_whitespace() || c == ';');
            if !stmt.is_empty() {
                running_query.store(true, Ordering::SeqCst);
                let res = session.execute_and_print(stmt, &mut out);
                running_query.store(false, Ordering::SeqCst);

                // If the SIGINT handler fired during this query it
                // will have (a) called abort() on the PWC, (b) set
                // was_cancelled. Surface a friendly message and
                // auto-reconnect so the REPL keeps running — without
                // reconnecting, every subsequent query would hit a
                // dead client.
                if was_cancelled.swap(false, Ordering::SeqCst) {
                    writeln!(out, "Query canceled.")?;
                    if let Err(e) = session.reconnect() {
                        writeln!(out, "reconnect after cancel failed: {e}")?;
                    } else {
                        *cancel_slot.lock().unwrap() =
                            Some(Arc::clone(session.client.inner()));
                    }
                    // Discard the original error (which is always
                    // "connection closed" after we shut the socket).
                    let _ = res;
                } else {
                    res?;
                }
            }
        }
    }

    if let Some(h) = hist {
        let _ = rl.save_history(&h);
    }

    Ok(())
}

// Tests for statement completion live in `sqltok.rs`.
