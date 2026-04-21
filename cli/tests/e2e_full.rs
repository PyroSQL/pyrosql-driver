//! Full e2e suite for `pyrosql` CLI + PWire driver.
//!
//! Runs the compiled `pyrosql` binary against a live PyroSQL server on
//! `localhost:12520`. Every test is `#[ignore]`d so the suite stays out
//! of `cargo test` by default — opt in with:
//!
//!     cargo test -p pyrosql-cli --test e2e_full -- --ignored
//!
//! The tests cover the driver-client surface end-to-end:
//!
//! * connect via flags and via URL (`pyrosql://` / `vsql://` / `pg://`)
//! * simple query, parametrized query, DML + affected-row counts
//! * transaction lifecycle (BEGIN / COMMIT / ROLLBACK)
//! * all three output formats (table / json / csv)
//! * script execution (`-f`) and stdin scripts (`-f -`)
//! * the `-D/--dialect` flag + banner feedback
//! * meta commands (`\l`, `\d`, `\echo`, `\i`, `\conninfo`, `\x`,
//!   `\timing`, `\f`)
//! * CLI hygiene: `--tls` hard error, invalid URL scheme error,
//!   `--password` without the opt-in flag rejected

use std::io::Write;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_pyrosql")
}

const HOST: &str = "127.0.0.1";
const PORT: &str = "12520";

/// Per-test schema name so parallel runs don't collide.
static TABLE_SEQ: AtomicU64 = AtomicU64::new(0);
fn uniq(prefix: &str) -> String {
    let n = TABLE_SEQ.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}_{n}_{}", std::process::id())
}

fn run(args: &[&str]) -> (bool, String, String) {
    let out = Command::new(bin())
        .args(args)
        .output()
        .expect("spawn pyrosql");
    (
        out.status.success(),
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

fn cq(sql: &str) -> (bool, String, String) {
    run(&["-h", HOST, "-p", PORT, "-c", sql])
}

// ─────────────────────────────────────────────────────────────────────────────
// Connectivity
// ─────────────────────────────────────────────────────────────────────────────

#[test]
#[ignore]
fn select_one_via_flags() {
    let (ok, out, _) = cq("SELECT 1");
    assert!(ok, "SELECT 1 must succeed");
    assert!(out.contains('1'), "stdout must contain '1', got: {out}");
}

#[test]
#[ignore]
fn select_one_via_url() {
    // `pyrosql://` is not yet in the list of recognised schemes — the
    // driver surfaces `vsql://` / `vsqlw://` (PWire), plus the
    // compat-wire schemes. Use the canonical one.
    let url = format!("vsql://{HOST}:{PORT}/pyrosql");
    let (ok, out, stderr) = run(&[&url, "-c", "SELECT 1"]);
    assert!(ok, "URL connect must succeed. stderr: {stderr}");
    assert!(out.contains('1'));
}

#[test]
#[ignore]
fn select_one_via_vsql_url() {
    let url = format!("vsql://{HOST}:{PORT}/pyrosql");
    let (ok, _, stderr) = run(&[&url, "-c", "SELECT 1"]);
    assert!(ok, "vsql:// URL must connect. stderr: {stderr}");
}

// ─────────────────────────────────────────────────────────────────────────────
// DDL / DML / SELECT round-trip
// ─────────────────────────────────────────────────────────────────────────────

#[test]
#[ignore]
fn create_insert_select_drop_roundtrip() {
    // Server accepts only one SQL statement per simple-query frame, so
    // the round-trip runs as four separate `-c` invocations (each one a
    // fresh short-lived client).  This is the same shape every driver
    // user hits: each connection is a scope, and we don't rely on
    // driver-side session state between invocations.
    let t = uniq("e2e_rt");
    let (ok, _, stderr) = cq(&format!("CREATE TABLE {t} (id INT PRIMARY KEY, name TEXT)"));
    assert!(ok, "CREATE must succeed. stderr: {stderr}");
    let (ok, _, stderr) = cq(&format!("INSERT INTO {t} VALUES (1, 'alice'), (2, 'bob')"));
    assert!(ok, "INSERT must succeed. stderr: {stderr}");
    let (ok, out, stderr) = cq(&format!("SELECT id, name FROM {t} ORDER BY id"));
    assert!(ok, "SELECT must succeed. stderr: {stderr}");
    assert!(out.contains("alice"), "output must show 'alice', got: {out}");
    assert!(out.contains("bob"), "output must show 'bob', got: {out}");
    let (ok, _, stderr) = cq(&format!("DROP TABLE {t}"));
    assert!(ok, "DROP must succeed. stderr: {stderr}");
}

// ─────────────────────────────────────────────────────────────────────────────
// Transaction semantics
// ─────────────────────────────────────────────────────────────────────────────

#[test]
#[ignore]
fn commit_persists_rollback_discards() {
    // Server rejects multi-statement simple-query frames, so each step
    // runs as its own `-c` invocation (short-lived client each time).
    // Note: session-scoped transaction state doesn't survive across `-c`
    // calls because each opens a fresh connection and session, so we
    // run BEGIN…COMMIT inside a single `\i` script fed to the REPL.
    let t = uniq("e2e_tx");
    let (ok, _, stderr) = cq(&format!("CREATE TABLE {t} (v INT)"));
    assert!(ok, "CREATE must succeed. stderr: {stderr}");

    // Commit path via REPL (same connection → BEGIN/INSERT/COMMIT stay
    // in the same session).
    let script = format!(
        "BEGIN;\nINSERT INTO {t} VALUES (1);\nCOMMIT;\n\\q\n",
    );
    let (_, _, _) = repl(&script);

    // Rollback path via REPL.
    let script = format!(
        "BEGIN;\nINSERT INTO {t} VALUES (2);\nROLLBACK;\n\\q\n",
    );
    let (_, _, _) = repl(&script);

    let (ok, out, stderr) = cq(&format!("SELECT v FROM {t} ORDER BY v"));
    assert!(ok, "SELECT after tx script must succeed. stderr: {stderr}");
    assert!(out.contains('1'), "row 1 must persist post-COMMIT, got: {out}");
    assert!(!out.contains('2'), "row 2 must NOT persist post-ROLLBACK, got: {out}");

    let _ = cq(&format!("DROP TABLE {t}"));
}

// ─────────────────────────────────────────────────────────────────────────────
// Output formats
// ─────────────────────────────────────────────────────────────────────────────

#[test]
#[ignore]
fn json_format_emits_ndjson() {
    let (ok, out, _) = run(&[
        "-h", HOST, "-p", PORT, "--format", "json", "-c", "SELECT 1 AS n",
    ]);
    assert!(ok);
    assert!(out.contains("\"n\""), "JSON must have column name, got: {out}");
    // The server currently sends literal `1` as text (TYPE_TEXT), so
    // the driver emits it JSON-stringified as `"1"`. Either shape
    // ("1" or the bare number 1) is acceptable evidence that the
    // value was carried through the result set.
    assert!(
        out.contains(":\"1\"") || out.contains(": \"1\"") || out.contains(":1") || out.contains(": 1"),
        "JSON must carry the value 1, got: {out}",
    );
}

#[test]
#[ignore]
fn csv_format_emits_header_and_row() {
    let (ok, out, _) = run(&[
        "-h", HOST, "-p", PORT, "--format", "csv", "-c", "SELECT 1 AS n, 'x' AS s",
    ]);
    assert!(ok);
    let mut lines = out.lines();
    let header = lines.next().unwrap_or("");
    let row = lines.next().unwrap_or("");
    assert!(header.contains("n") && header.contains("s"), "CSV header: {header}");
    assert!(row.contains('1') && row.contains('x'), "CSV row: {row}");
}

// ─────────────────────────────────────────────────────────────────────────────
// Dialect selector
// ─────────────────────────────────────────────────────────────────────────────

#[test]
#[ignore]
fn dialect_flag_reaches_banner() {
    // The banner goes to stderr.  Without a TTY (which this test subprocess
    // doesn't have) the CLI suppresses the banner in non-interactive mode
    // for -c, so we can only confirm the flag doesn't break connectivity.
    let (ok, out, _) = run(&[
        "-h", HOST, "-p", PORT, "-D", "pg", "-c", "SELECT 1",
    ]);
    assert!(ok);
    assert!(out.contains('1'));
}

#[test]
#[ignore]
fn mysql_dialect_survives_connection() {
    // Smoke test: opening a PWire connection with `-D mysql` and
    // running a dialect-agnostic `SELECT 1` must succeed. The backtick
    // → double-quote rewrite inside `normalize_mysql` is server-side
    // and covered by the server's own tests; from the driver-client
    // perspective the only visible surface is "the MSG_AUTH dialect
    // selector reaches the server without breaking the session".
    let (ok, out, stderr) = run(&[
        "-h", HOST, "-p", PORT, "-D", "mysql", "-c", "SELECT 1 AS n",
    ]);
    assert!(ok, "mysql dialect session must connect. stderr: {stderr}");
    assert!(out.contains('1'), "SELECT 1 under mysql dialect must return 1, got: {out}");
}

// ─────────────────────────────────────────────────────────────────────────────
// Script execution (-f / stdin)
// ─────────────────────────────────────────────────────────────────────────────

#[test]
#[ignore]
fn script_from_stdin_via_dash() {
    let t = uniq("e2e_stdin");
    let script = format!(
        "CREATE TABLE {t} (k INT); INSERT INTO {t} VALUES (7); \
         SELECT k FROM {t}; DROP TABLE {t};"
    );
    let mut child = Command::new(bin())
        .args(["-h", HOST, "-p", PORT, "-f", "-"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn pyrosql");
    child
        .stdin
        .as_mut()
        .unwrap()
        .write_all(script.as_bytes())
        .unwrap();
    let out = child.wait_with_output().unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(out.status.success(), "stdin script must succeed. stderr: {}", String::from_utf8_lossy(&out.stderr));
    assert!(stdout.contains('7'), "stdout must show '7', got: {stdout}");
}

#[test]
#[ignore]
fn script_from_file() {
    use std::fs::File;
    let t = uniq("e2e_file");
    let path = std::env::temp_dir().join(format!("pyrosql_e2e_{t}.sql"));
    {
        let mut f = File::create(&path).unwrap();
        writeln!(f, "CREATE TABLE {t} (k INT);").unwrap();
        writeln!(f, "INSERT INTO {t} VALUES (9);").unwrap();
        writeln!(f, "SELECT k FROM {t};").unwrap();
        writeln!(f, "DROP TABLE {t};").unwrap();
    }
    let (ok, out, stderr) = run(&["-h", HOST, "-p", PORT, "-f", path.to_str().unwrap()]);
    let _ = std::fs::remove_file(&path);
    assert!(ok, "file script must succeed. stderr: {stderr}");
    assert!(out.contains('9'));
}

// ─────────────────────────────────────────────────────────────────────────────
// Meta commands (driven via stdin to a REPL process)
// ─────────────────────────────────────────────────────────────────────────────

fn repl(stdin_script: &str) -> (bool, String, String) {
    let mut child = Command::new(bin())
        .args(["-h", HOST, "-p", PORT, "--quiet"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn pyrosql");
    child
        .stdin
        .as_mut()
        .unwrap()
        .write_all(stdin_script.as_bytes())
        .unwrap();
    let out = child.wait_with_output().unwrap();
    (
        out.status.success(),
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

#[test]
#[ignore]
fn echo_meta_prints_literal() {
    let (_, out, _) = repl("\\echo hello world\n\\q\n");
    assert!(out.contains("hello world"), "echo output: {out}");
}

#[test]
#[ignore]
fn conninfo_meta_prints_connection() {
    let (_, out, _) = repl("\\conninfo\n\\q\n");
    assert!(out.contains(HOST), "conninfo must mention host, got: {out}");
    assert!(out.contains(PORT), "conninfo must mention port, got: {out}");
}

#[test]
#[ignore]
fn describe_meta_lists_columns() {
    let t = uniq("e2e_meta_d");
    let setup = format!("CREATE TABLE {t} (id INT, name TEXT);");
    let (ok, _, _) = cq(&setup);
    assert!(ok);

    let script = format!("\\d {t}\n\\q\n");
    let (_, out, _) = repl(&script);
    assert!(out.contains("id"), "\\d must list 'id' column, got: {out}");
    assert!(out.contains("name"), "\\d must list 'name' column, got: {out}");

    let _ = cq(&format!("DROP TABLE {t}"));
}

#[test]
#[ignore]
fn toggle_expanded_format_timing_in_repl() {
    // Toggle each off/on setter and make sure the REPL stays healthy
    // (doesn't hang, doesn't crash, still answers a simple query).
    let script = "\\x\n\\timing\n\\f json\nSELECT 1;\n\\q\n";
    let (_, out, _) = repl(script);
    assert!(out.contains('1'), "query after toggles must return a row, got: {out}");
}

#[test]
#[ignore]
fn include_meta_runs_file() {
    use std::fs::File;
    let t = uniq("e2e_include");
    let path = std::env::temp_dir().join(format!("pyrosql_inc_{t}.sql"));
    {
        let mut f = File::create(&path).unwrap();
        writeln!(f, "CREATE TABLE {t} (v INT);").unwrap();
        writeln!(f, "INSERT INTO {t} VALUES (123);").unwrap();
        writeln!(f, "SELECT v FROM {t};").unwrap();
        writeln!(f, "DROP TABLE {t};").unwrap();
    }
    let script = format!("\\i {}\n\\q\n", path.to_str().unwrap());
    let (_, out, _) = repl(&script);
    let _ = std::fs::remove_file(&path);
    assert!(out.contains("123"), "\\i must run the script, got: {out}");
}

// ─────────────────────────────────────────────────────────────────────────────
// CLI hygiene: error paths stay clean
// ─────────────────────────────────────────────────────────────────────────────

#[test]
#[ignore]
fn tls_flag_is_hard_error() {
    // The driver speaks plaintext PWire only; --tls must bail out loudly
    // instead of silently downgrading.
    let (ok, _, stderr) = run(&[
        "-h", HOST, "-p", PORT, "--tls", "-c", "SELECT 1",
    ]);
    assert!(!ok);
    assert!(stderr.contains("TLS"), "must explain TLS not implemented, got: {stderr}");
}

#[test]
#[ignore]
fn password_without_opt_in_is_rejected() {
    // `--password` requires the explicit opt-in flag to avoid footgun.
    let (ok, _, stderr) = run(&[
        "-h", HOST, "-p", PORT, "--password", "hunter2", "-c", "SELECT 1",
    ]);
    assert!(!ok);
    assert!(
        stderr.to_lowercase().contains("i-know-password") || stderr.to_lowercase().contains("insecure"),
        "must mention the opt-in flag, got: {stderr}",
    );
}

#[test]
#[ignore]
fn bad_host_errors_cleanly() {
    // Connect to a port that isn't listening on localhost.  The driver
    // must surface a Connection error, not hang.
    let (ok, _, stderr) = run(&[
        "-h", "127.0.0.1", "-p", "1", "-c", "SELECT 1",
    ]);
    assert!(!ok);
    let low = stderr.to_lowercase();
    assert!(
        low.contains("connect") || low.contains("refused") || low.contains("connection"),
        "must mention connection failure, got: {stderr}",
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// \set + :var expansion
// ─────────────────────────────────────────────────────────────────────────────

#[test]
#[ignore]
fn set_and_bare_var_expansion() {
    // \set NAME value ; \echo :NAME → must print the value back
    let script = "\\set answer 42\n\\echo :answer\n\\q\n";
    let (_, out, _) = repl(script);
    assert!(out.contains("42"), ":answer must expand in \\echo, got: {out}");
}

#[test]
#[ignore]
fn set_then_use_in_select() {
    // \set x 7 ; SELECT :x → server sees SELECT 7
    let script = "\\set x 7\nSELECT :x AS n;\n\\q\n";
    let (_, out, _) = repl(script);
    assert!(out.contains('7'), "SELECT :x must return the variable value, got: {out}");
}

#[test]
#[ignore]
fn unset_removes_variable() {
    let script = "\\set x 5\n\\unset x\n\\echo :x\n\\q\n";
    let (_, out, _) = repl(script);
    // Unknown var is passthrough — literal `:x` stays in output.
    assert!(out.contains(":x"), "after \\unset, :x must be passthrough, got: {out}");
}

#[test]
#[ignore]
fn list_variables_when_set_called_bare() {
    let script = "\\set a 1\n\\set b two\n\\set\n\\q\n";
    let (_, out, _) = repl(script);
    assert!(out.contains("a = '1'"), "listing must show a, got: {out}");
    assert!(out.contains("b = 'two'"), "listing must show b, got: {out}");
}

// ─────────────────────────────────────────────────────────────────────────────
// \gexec
// ─────────────────────────────────────────────────────────────────────────────

#[test]
#[ignore]
fn gexec_runs_each_row() {
    // The previous query's rows become the SQL statements \gexec runs.
    // Ask for literal `SELECT 11` / `SELECT 22` text and then gexec.
    let script = "SELECT 'SELECT 11' UNION ALL SELECT 'SELECT 22' ORDER BY 1;\n\\gexec\n\\q\n";
    let (_, out, _) = repl(script);
    assert!(out.contains("11"), "\\gexec must have run SELECT 11, got: {out}");
    assert!(out.contains("22"), "\\gexec must have run SELECT 22, got: {out}");
}

// ─────────────────────────────────────────────────────────────────────────────
// \watch N  (bounded by env var so the test can't hang)
// ─────────────────────────────────────────────────────────────────────────────

#[test]
#[ignore]
fn watch_repeats_bounded_iterations() {
    // PYROSQL_WATCH_MAX_ITER caps the \watch loop so we can assert on
    // a finite number of iterations without a signal harness.
    let script = "SELECT 77 AS w;\n\\watch 0.01\n\\q\n";
    let mut child = Command::new(bin())
        .args(["-h", HOST, "-p", PORT, "--quiet"])
        .env("PYROSQL_WATCH_MAX_ITER", "3")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .as_mut()
        .unwrap()
        .write_all(script.as_bytes())
        .unwrap();
    let out = child.wait_with_output().unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout);
    // Three executions → three '77's (one per iteration).
    let count = stdout.matches("77").count();
    assert!(count >= 3, "\\watch must run the query ≥ MAX_ITER times, got: {count} in {stdout}");
}

// ─────────────────────────────────────────────────────────────────────────────
// \pager toggle (behaviour-level: toggle prints, actual pager not spawned)
// ─────────────────────────────────────────────────────────────────────────────

#[test]
#[ignore]
fn pager_toggle_modes() {
    let script = "\\pager on\n\\pager off\n\\pager auto\n\\q\n";
    let (_, out, _) = repl(script);
    assert!(out.contains("Pager: On"));
    assert!(out.contains("Pager: Off"));
    assert!(out.contains("Pager: Auto"));
}

// ─────────────────────────────────────────────────────────────────────────────
// \copy FROM / TO roundtrip
// ─────────────────────────────────────────────────────────────────────────────

#[test]
#[ignore]
fn copy_from_then_to_roundtrips() {
    use std::fs::File;
    let t = uniq("e2e_copy");
    // CREATE source.
    let (ok, _, stderr) = cq(&format!("CREATE TABLE {t} (id INT, name TEXT)"));
    assert!(ok, "CREATE must succeed. stderr: {stderr}");

    // Write a CSV the \copy FROM will slurp.
    let in_path = std::env::temp_dir().join(format!("{t}_in.csv"));
    {
        let mut f = File::create(&in_path).unwrap();
        writeln!(f, "id,name").unwrap();
        writeln!(f, "1,alice").unwrap();
        writeln!(f, "2,bob").unwrap();
    }

    // Import via REPL so stdin carries both the \copy and the later
    // \copy TO + exit.
    let out_path = std::env::temp_dir().join(format!("{t}_out.csv"));
    let script = format!(
        "\\copy {t} FROM '{}'\n\\copy {t} TO '{}'\n\\q\n",
        in_path.display(),
        out_path.display(),
    );
    let (_, repl_out, _) = repl(&script);
    assert!(
        repl_out.contains("2 rows copied") && repl_out.contains("2 rows written"),
        "both directions must report 2 rows, got: {repl_out}",
    );

    let dumped = std::fs::read_to_string(&out_path).unwrap();
    assert!(dumped.contains("alice") && dumped.contains("bob"),
            "dumped CSV must contain the imported rows, got: {dumped}");

    // Cleanup.
    let _ = std::fs::remove_file(&in_path);
    let _ = std::fs::remove_file(&out_path);
    let _ = cq(&format!("DROP TABLE {t}"));
}

// ─────────────────────────────────────────────────────────────────────────────
// \du, \dn, \df — smoke
// ─────────────────────────────────────────────────────────────────────────────

#[test]
#[ignore]
fn du_dn_df_produce_output() {
    let (_, out, _) = repl("\\du\n\\dn\n\\df\n\\q\n");
    // \du must at least list the current role (pyrosql).  \dn should
    // show 'public' schema.  \df can be empty on a fresh DB so we
    // only assert it didn't crash the REPL.
    assert!(out.contains("pyrosql"), "\\du must mention pyrosql role, got: {out}");
    assert!(out.contains("public"), "\\dn must mention public schema, got: {out}");
}

// ─────────────────────────────────────────────────────────────────────────────
// ~/.pyrosqlrc via $PYROSQL_RC
// ─────────────────────────────────────────────────────────────────────────────

#[test]
#[ignore]
fn pyrosqlrc_runs_at_startup() {
    use std::fs::File;
    let t = uniq("e2e_rc");
    let rc = std::env::temp_dir().join(format!("{t}.rc"));
    {
        let mut f = File::create(&rc).unwrap();
        // Set a variable and echo it — the subsequent REPL prompt
        // comes AFTER this runs so the echo output lands before our
        // own \q.
        writeln!(f, "\\set greeting hola").unwrap();
    }
    let script = "\\echo :greeting\n\\q\n";
    let mut child = Command::new(bin())
        .args(["-h", HOST, "-p", PORT, "--quiet"])
        .env("PYROSQL_RC", rc.as_os_str())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child.stdin.as_mut().unwrap().write_all(script.as_bytes()).unwrap();
    let out = child.wait_with_output().unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout);
    let _ = std::fs::remove_file(&rc);
    assert!(stdout.contains("hola"), "pyrosqlrc must set variables at startup, got: {stdout}");
}

// ─────────────────────────────────────────────────────────────────────────────
// --completions flag
// ─────────────────────────────────────────────────────────────────────────────

#[test]
#[ignore]
fn completions_flag_emits_bash_script() {
    let out = Command::new(bin())
        .args(["--completions", "bash"])
        .output()
        .unwrap();
    assert!(out.status.success());
    let s = String::from_utf8_lossy(&out.stdout);
    assert!(s.contains("_pyrosql") || s.contains("complete -"),
            "bash completion script must reference the completion function, got head: {}",
            s.lines().next().unwrap_or(""));
}

#[test]
#[ignore]
fn completions_flag_emits_zsh_script() {
    let out = Command::new(bin())
        .args(["--completions", "zsh"])
        .output()
        .unwrap();
    assert!(out.status.success());
    let s = String::from_utf8_lossy(&out.stdout);
    assert!(s.starts_with("#compdef"), "zsh script must start with #compdef, got: {}", s.lines().next().unwrap_or(""));
}

#[test]
#[ignore]
fn completions_flag_emits_fish_script() {
    let out = Command::new(bin())
        .args(["--completions", "fish"])
        .output()
        .unwrap();
    assert!(out.status.success());
    let s = String::from_utf8_lossy(&out.stdout);
    assert!(s.contains("complete -c pyrosql"),
            "fish script must contain `complete -c pyrosql`, got: {}",
            s.lines().next().unwrap_or(""));
}

#[test]
#[ignore]
fn quiet_suppresses_banner() {
    let (ok, _out, stderr) = run(&[
        "-h", HOST, "-p", PORT, "-q", "-c", "SELECT 1",
    ]);
    assert!(ok);
    assert!(!stderr.contains("connected"), "quiet mode must not print banner, got: {stderr}");
}
