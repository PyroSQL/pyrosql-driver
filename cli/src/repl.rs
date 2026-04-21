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

use anyhow::Result;
use rustyline::config::Configurer;
use rustyline::error::ReadlineError;
use rustyline::{DefaultEditor, Editor};

use crate::args::Cli;
use crate::meta;
use crate::session::{open_session, Session};

fn history_path() -> Option<PathBuf> {
    let home = std::env::var_os("HOME")?;
    let mut p = PathBuf::from(home);
    p.push(".pyrosql_history");
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

    // rustyline editor with default history — we bump max size to 10k.
    let mut rl: Editor<(), _> = DefaultEditor::new()?;
    rl.set_max_history_size(10_000)?;
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
                // Ctrl-C — abandon current buffer, print a newline.
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
                session.execute_and_print(stmt, &mut out)?;
            }
        }
    }

    if let Some(h) = hist {
        let _ = rl.save_history(&h);
    }

    Ok(())
}

// Tests for statement completion live in `sqltok.rs`.
