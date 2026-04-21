//! PTY-driven end-to-end tests for the `pyrosql` interactive REPL.
//!
//! The other e2e suites (`e2e.rs`, `e2e_full.rs`) cover the REPL's
//! dispatch surface by piping stdin to a child process. That misses
//! everything that rustyline only does when attached to a real
//! terminal: arrow-key history navigation, Ctrl-C mid-buffer abandon,
//! Ctrl-D EOF, the prompt switcher for multi-line SQL buffering, and
//! the banner emission gated on `stderr.is_terminal()`.
//!
//! This file uses `expectrl` to spawn `pyrosql` under a pseudo-tty and
//! drive it the same way a human does. All tests are `#[ignore]`d —
//! they need a live PyroSQL server at `127.0.0.1:12520`, same as the
//! other e2e suites.
//!
//!     cargo test -p pyrosql-cli --test e2e_pty -- --ignored
//!
//! PTY tests are inherently timing-sensitive. Each test carries a 5 s
//! expect timeout to avoid an infinite hang if rustyline's behaviour
//! changes, and asserts read from a rolling buffer so we accept any
//! surrounding noise (colour escapes, prompt redraws, etc.).

use std::time::Duration;

use expectrl::{ControlCode, Regex, Session, WaitStatus};

const HOST: &str = "127.0.0.1";
const PORT: &str = "12520";

/// Spawn the `pyrosql` binary under a PTY with a generous but bounded
/// expect timeout. `--quiet` is OMITTED so the banner is visible for
/// the banner test; individual tests may skip past it with `expect`.
fn spawn_repl() -> Session {
    let bin = env!("CARGO_BIN_EXE_pyrosql");
    let cmd = format!("{bin} -h {HOST} -p {PORT}");
    let mut s = expectrl::spawn(&cmd).expect("spawn pyrosql under PTY");
    s.set_expect_timeout(Some(Duration::from_secs(5)));
    s
}

fn spawn_repl_quiet() -> Session {
    let bin = env!("CARGO_BIN_EXE_pyrosql");
    let cmd = format!("{bin} -h {HOST} -p {PORT} --quiet");
    let mut s = expectrl::spawn(&cmd).expect("spawn pyrosql under PTY");
    s.set_expect_timeout(Some(Duration::from_secs(5)));
    s
}

// ─────────────────────────────────────────────────────────────────────────────
// Banner / TTY detection
// ─────────────────────────────────────────────────────────────────────────────

#[test]
#[ignore]
fn banner_shown_on_tty() {
    // Banner goes to stderr only when `stderr.is_terminal()`. Under a
    // PTY both fds are TTYs so the banner must fire.  It carries the
    // host/port we connected to + the version + the help hint.
    let mut s = spawn_repl();
    s.expect(Regex("connected to"))
        .expect("banner line with 'connected to' must appear on TTY");
    s.expect(Regex(r"Type \\h for help"))
        .expect("help hint must follow the banner");
    s.expect(Regex(r"pyrosql=> "))
        .expect("prompt must follow the banner");
    // Clean exit.
    let _ = s.send(ControlCode::EndOfTransmission);
}

// ─────────────────────────────────────────────────────────────────────────────
// Ctrl-D / Ctrl-C
// ─────────────────────────────────────────────────────────────────────────────

#[test]
#[ignore]
fn ctrl_d_exits_cleanly() {
    // With an empty buffer, Ctrl-D ends the session with exit status 0.
    let mut s = spawn_repl_quiet();
    s.expect(Regex(r"pyrosql=> ")).expect("initial prompt");
    s.send(ControlCode::EndOfTransmission).unwrap();
    let status = s.get_process().wait().expect("child wait");
    assert!(
        matches!(status, WaitStatus::Exited(_, 0)),
        "pyrosql must exit 0 on Ctrl-D, got: {status:?}",
    );
}

#[test]
#[ignore]
fn ctrl_c_abandons_buffer_and_returns_to_fresh_prompt() {
    // Type SQL WITHOUT terminating semicolon → rustyline stays on the
    // same line awaiting more input. Ctrl-C must wipe the buffer and
    // drop us at a fresh `pyrosql=>` prompt. A subsequent complete
    // query then executes as if nothing happened.
    let mut s = spawn_repl_quiet();
    s.expect(Regex(r"pyrosql=> ")).expect("initial prompt");
    // Unterminated input buffered but not yet executed.
    s.send("SELECT 9999").unwrap();
    s.send(ControlCode::EndOfText).unwrap();
    s.expect(Regex(r"pyrosql=> "))
        .expect("prompt must reappear after Ctrl-C");
    // Totally different query — if Ctrl-C failed to abandon the old
    // buffer we'd get a syntax error on the concatenated mess.
    s.send_line("SELECT 42 AS after_cancel;").unwrap();
    s.expect(Regex("42"))
        .expect("post-cancel query must execute and return 42");
    let _ = s.send(ControlCode::EndOfTransmission);
}

// ─────────────────────────────────────────────────────────────────────────────
// Prompt switching
// ─────────────────────────────────────────────────────────────────────────────

#[test]
#[ignore]
fn multiline_prompt_switches_on_unterminated_sql() {
    // SQL without `;` at end-of-line must switch the prompt to the
    // continuation marker `pyrosql-> ` until the user finishes with
    // a semicolon.
    let mut s = spawn_repl_quiet();
    s.expect(Regex(r"pyrosql=> ")).expect("initial prompt");
    s.send_line("SELECT 7").unwrap();
    s.expect(Regex(r"pyrosql-> "))
        .expect("continuation prompt must appear for unterminated SQL");
    // Finish the statement — result must land.
    s.send_line("AS v;").unwrap();
    s.expect(Regex("7"))
        .expect("multi-line SELECT must execute and return 7");
    let _ = s.send(ControlCode::EndOfTransmission);
}

#[test]
#[ignore]
fn tx_prompt_switches_on_begin_and_back_on_commit() {
    // Inside an explicit BEGIN … COMMIT block the prompt switches to
    // `pyrosql*> `. Exiting via COMMIT switches back to the idle
    // `pyrosql=> ` form.
    let mut s = spawn_repl_quiet();
    s.expect(Regex(r"pyrosql=> ")).expect("initial prompt");
    s.send_line("BEGIN;").unwrap();
    s.expect(Regex(r"pyrosql\*> "))
        .expect("tx prompt must appear after BEGIN");
    s.send_line("COMMIT;").unwrap();
    s.expect(Regex(r"pyrosql=> "))
        .expect("idle prompt must restore after COMMIT");
    let _ = s.send(ControlCode::EndOfTransmission);
}

// ─────────────────────────────────────────────────────────────────────────────
// History navigation
// ─────────────────────────────────────────────────────────────────────────────

// ─────────────────────────────────────────────────────────────────────────────
// Tab completion
// ─────────────────────────────────────────────────────────────────────────────

// Note: there is NO PTY test for "SIGINT cancels the in-flight query"
// because our PTY harness disables the ctrlc handler (see the comment
// on `spawn_repl_base`). The deterministic cancel semantics live in
// the driver-level tests (`abort_then_query_returns_connection_error`
// and `concurrent_abort_interrupts_query_loop` in
// `pyrosql-driver/rust/tests/e2e.rs`), which drive `Client::abort()`
// from a side thread and assert the resulting `ClientError::Connection`.

#[test]
#[ignore]
fn tab_completes_sql_keyword() {
    // Type a unique SQL keyword prefix and press Tab. Rustyline's
    // default CompletionType::Circular expands the line in place when
    // exactly one candidate matches. "SELE" → SELECT.
    let mut s = spawn_repl_quiet();
    s.expect(Regex(r"pyrosql=> ")).expect("initial prompt");
    s.send("SELE\t").unwrap();
    s.expect(Regex("SELECT"))
        .expect("Tab on SELE must expand to SELECT");
    // Finish the statement so the session shuts down cleanly.
    s.send_line(" 1;").unwrap();
    s.expect(Regex("1")).expect("expanded query must still run");
    let _ = s.send(ControlCode::EndOfTransmission);
}

#[test]
#[ignore]
fn tab_completes_meta_command() {
    let mut s = spawn_repl_quiet();
    s.expect(Regex(r"pyrosql=> ")).expect("initial prompt");
    // `\conni` is unique among meta commands → `\conninfo`.
    // (`\co` would branch between `\connect`, `\conninfo`, `\copy`.)
    s.send("\\conni\t").unwrap();
    s.expect(Regex(r"\\conninfo"))
        .expect("Tab on \\coni must expand to \\conninfo");
    s.send_line("").unwrap();
    s.expect(Regex(r"Connected to"))
        .expect("\\conninfo must run and print the connection summary");
    let _ = s.send(ControlCode::EndOfTransmission);
}

#[test]
#[ignore]
fn arrow_up_recalls_previous_query() {
    // rustyline's default Emacs-mode binds ↑ to previous-history.
    // After executing one query, ↑ at the fresh prompt must redisplay
    // it; pressing Enter re-executes.
    let mut s = spawn_repl_quiet();
    s.expect(Regex(r"pyrosql=> ")).expect("initial prompt");
    s.send_line("SELECT 314 AS first;").unwrap();
    s.expect(Regex("314")).expect("first query result");
    s.expect(Regex(r"pyrosql=> "))
        .expect("prompt must return after first query");
    // ANSI up-arrow escape: `ESC [ A`.
    s.send("\x1b[A").unwrap();
    // Rustyline echoes the recalled line back to the terminal — look
    // for the literal SQL on the line. We can't tell cursor-position
    // from the stream, but the text must be present.
    s.expect(Regex("SELECT 314"))
        .expect("↑ must re-display the previous query");
    s.send_line("").unwrap(); // Enter to execute
    s.expect(Regex("314"))
        .expect("recalled query must run again and return 314");
    let _ = s.send(ControlCode::EndOfTransmission);
}
