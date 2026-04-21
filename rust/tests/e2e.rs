//! End-to-end tests for the `pyrosql` Rust driver crate.
//!
//! Exercise the public API (`Client::connect`, `ConnectConfig`, `query`,
//! `execute`, `begin`, `commit`, `rollback`, dialect selection) against
//! a live PyroSQL server on `127.0.0.1:12520`.  All tests are
//! `#[ignore]`d so `cargo test -p pyrosql` stays purely unit.
//!
//! Opt in with:
//!     cargo test -p pyrosql --test e2e -- --ignored
//!
//! Naming: each test generates a unique table name via `uniq(...)` so
//! they can run in parallel and against a shared data directory.

use std::sync::atomic::{AtomicU64, Ordering};

use futures::executor::block_on;
use pyrosql::{Client, ConnectConfig, Scheme, SyntaxMode, Value};

const ADDR: &str = "127.0.0.1";
const PORT: u16 = 12520;

static SEQ: AtomicU64 = AtomicU64::new(0);
fn uniq(prefix: &str) -> String {
    let n = SEQ.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}_{n}_{}", std::process::id())
}

fn connect() -> Client {
    let cfg = ConnectConfig::new(ADDR, PORT).scheme(Scheme::Wire);
    block_on(Client::connect(cfg)).expect("Client::connect")
}

fn connect_dialect(mode: SyntaxMode) -> Client {
    let cfg = ConnectConfig::new(ADDR, PORT)
        .scheme(Scheme::Wire)
        .dialect(mode);
    block_on(Client::connect(cfg)).expect("Client::connect with dialect")
}

// ─────────────────────────────────────────────────────────────────────────────
// Connect / handshake
// ─────────────────────────────────────────────────────────────────────────────

#[test]
#[ignore]
fn connect_and_select_one() {
    let c = connect();
    let r = block_on(c.query("SELECT 1", &[])).expect("SELECT 1");
    assert_eq!(r.rows.len(), 1);
}

#[test]
#[ignore]
fn connect_via_url() {
    let url = format!("vsql://{ADDR}:{PORT}/pyrosql");
    let c = block_on(Client::connect_url(&url)).expect("connect_url");
    let r = block_on(c.query("SELECT 1", &[])).expect("SELECT 1");
    assert_eq!(r.rows.len(), 1);
}

#[test]
#[ignore]
fn connect_rejects_bad_host() {
    let cfg = ConnectConfig::new("127.0.0.1", 1).scheme(Scheme::Wire);
    let r = block_on(Client::connect(cfg));
    assert!(r.is_err(), "connect to port 1 must fail");
}

// ─────────────────────────────────────────────────────────────────────────────
// Query path — simple + parametrized + DML
// ─────────────────────────────────────────────────────────────────────────────

#[test]
#[ignore]
fn simple_query_returns_rows() {
    let c = connect();
    let t = uniq("drv_simple");
    block_on(c.query(&format!("CREATE TABLE {t} (n INT)"), &[])).expect("CREATE");
    block_on(c.query(&format!("INSERT INTO {t} VALUES (1),(2),(3)"), &[])).expect("INSERT");
    let r = block_on(c.query(&format!("SELECT n FROM {t} ORDER BY n"), &[])).expect("SELECT");
    assert_eq!(r.rows.len(), 3);
    block_on(c.query(&format!("DROP TABLE {t}"), &[])).expect("DROP");
}

#[test]
#[ignore]
fn parametrized_query_binds_value() {
    let c = connect();
    let t = uniq("drv_param");
    block_on(c.query(&format!("CREATE TABLE {t} (id INT, name TEXT)"), &[])).expect("CREATE");
    block_on(c.query(
        &format!("INSERT INTO {t} VALUES ($1, $2)"),
        &[Value::Int(42), Value::Text("alice".into())],
    ))
    .expect("INSERT $1 $2");
    let r = block_on(c.query(
        &format!("SELECT name FROM {t} WHERE id = $1"),
        &[Value::Int(42)],
    ))
    .expect("SELECT $1");
    assert_eq!(r.rows.len(), 1);
    block_on(c.query(&format!("DROP TABLE {t}"), &[])).expect("DROP");
}

#[test]
#[ignore]
fn execute_returns_rows_affected() {
    let c = connect();
    let t = uniq("drv_dml");
    block_on(c.query(&format!("CREATE TABLE {t} (v INT)"), &[])).expect("CREATE");
    let n = block_on(c.execute(&format!("INSERT INTO {t} VALUES (1),(2),(3)"), &[]))
        .expect("INSERT");
    assert_eq!(n, 3, "INSERT must report 3 rows affected");
    let n = block_on(c.execute(
        &format!("DELETE FROM {t} WHERE v < $1"),
        &[Value::Int(3)],
    ))
    .expect("DELETE");
    assert_eq!(n, 2, "DELETE must report 2 rows affected");
    block_on(c.query(&format!("DROP TABLE {t}"), &[])).expect("DROP");
}

#[test]
#[ignore]
fn query_error_surfaces_as_client_error() {
    let c = connect();
    let r = block_on(c.query("SELECT * FROM this_table_does_not_exist_12345", &[]));
    assert!(r.is_err(), "missing table must surface a ClientError");
}

// ─────────────────────────────────────────────────────────────────────────────
// Transactions
// ─────────────────────────────────────────────────────────────────────────────

#[test]
#[ignore]
fn commit_persists_changes() {
    // Known server bug: an isolated COMMIT frame hangs ~10s waiting
    // for the response of the previous simple-query (memory entry
    // `project_pipeline_shift_2026_04_20.md`). The server rejects
    // multi-statement simple-query frames with "expected exactly one
    // SQL statement", so there is no client-side workaround. The
    // autocommit path (plain INSERT outside BEGIN…COMMIT) stays
    // covered by `execute_returns_rows_affected` above. This test
    // will start passing automatically once the pipeline-shift fix
    // lands on the server.
    let c = connect();
    let t = uniq("drv_tx_commit");
    block_on(c.query(&format!("CREATE TABLE {t} (v INT)"), &[])).expect("CREATE");
    block_on(c.query("BEGIN", &[])).expect("BEGIN");
    block_on(c.query(&format!("INSERT INTO {t} VALUES (10)"), &[])).expect("INSERT");
    block_on(c.query("COMMIT", &[])).expect("COMMIT");
    let r = block_on(c.query(&format!("SELECT v FROM {t}"), &[])).expect("SELECT");
    assert_eq!(r.rows.len(), 1);
    block_on(c.query(&format!("DROP TABLE {t}"), &[])).expect("DROP");
}

#[test]
#[ignore]
fn rollback_discards_changes() {
    let c = connect();
    let t = uniq("drv_tx_rb");
    block_on(c.query(&format!("CREATE TABLE {t} (v INT)"), &[])).expect("CREATE");
    block_on(c.query("BEGIN", &[])).expect("BEGIN");
    block_on(c.query(&format!("INSERT INTO {t} VALUES (99)"), &[])).expect("INSERT");
    block_on(c.query("ROLLBACK", &[])).expect("ROLLBACK");
    let r = block_on(c.query(&format!("SELECT v FROM {t}"), &[])).expect("SELECT");
    assert_eq!(r.rows.len(), 0, "ROLLBACK must discard the INSERT");
    block_on(c.query(&format!("DROP TABLE {t}"), &[])).expect("DROP");
}

// ─────────────────────────────────────────────────────────────────────────────
// Dialect selector (via MSG_AUTH)
// ─────────────────────────────────────────────────────────────────────────────

#[test]
#[ignore]
fn dialect_pyro_is_default_and_connects() {
    let c = connect_dialect(SyntaxMode::PyroSQL);
    let r = block_on(c.query("SELECT 1", &[])).expect("SELECT 1 under pyro dialect");
    assert_eq!(r.rows.len(), 1);
}

#[test]
#[ignore]
fn dialect_pg_roundtrips_simple_query() {
    let c = connect_dialect(SyntaxMode::PostgreSQL);
    let r = block_on(c.query("SELECT 1", &[])).expect("SELECT 1 under pg dialect");
    assert_eq!(r.rows.len(), 1);
}

#[test]
#[ignore]
fn dialect_mysql_roundtrips_simple_query() {
    let c = connect_dialect(SyntaxMode::MySQL);
    let r = block_on(c.query("SELECT 1", &[])).expect("SELECT 1 under mysql dialect");
    assert_eq!(r.rows.len(), 1);
}

// ─────────────────────────────────────────────────────────────────────────────
// URL parsing surface (does not need the server)
// ─────────────────────────────────────────────────────────────────────────────

// ─────────────────────────────────────────────────────────────────────────────
// Client::abort()
// ─────────────────────────────────────────────────────────────────────────────

#[test]
#[ignore]
fn abort_then_query_returns_connection_error() {
    // Calling `abort()` on a healthy Client must tear the underlying
    // TCP socket down — every subsequent query on THIS client must
    // fail with a `ClientError::Connection`. Proves the abort path
    // is actually wired, independent of the race-y "cancel mid-flight"
    // case.
    let c = connect();
    assert!(block_on(c.query("SELECT 1", &[])).is_ok(), "sanity: pre-abort query works");
    c.abort();
    let res = block_on(c.query("SELECT 1", &[]));
    assert!(res.is_err(), "post-abort query must fail, got: {res:?}");
}

#[test]
#[ignore]
fn concurrent_abort_interrupts_query_loop() {
    // Simulate "user hits Ctrl-C while queries are running" from the
    // driver level. Spawn a side thread that calls abort() after a
    // short delay; the main thread loops on SELECT 1 — within the
    // deadline (≤ 2 s) at least one iteration MUST surface an error,
    // proving the abort reaches an in-flight future.
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    let client = Arc::new(connect());
    let client_for_abort = Arc::clone(&client);
    let t = thread::spawn(move || {
        thread::sleep(Duration::from_millis(80));
        client_for_abort.abort();
    });

    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    let mut saw_error = false;
    while std::time::Instant::now() < deadline {
        if block_on(client.query("SELECT 1", &[])).is_err() {
            saw_error = true;
            break;
        }
    }
    t.join().unwrap();
    assert!(saw_error, "main-thread query loop must see an error within 2s of abort()");
}

#[test]
fn url_parser_accepts_dialect_alias() {
    // Pure unit — reads no socket.  Kept non-`#[ignore]` so it runs in
    // the normal `cargo test -p pyrosql` pass.
    let cfg = ConnectConfig::from_url("vsql://h/db?dialect=pg").unwrap();
    assert_eq!(cfg.syntax_mode, Some(SyntaxMode::PostgreSQL));
}

#[test]
fn url_parser_accepts_syntax_mode_spelling() {
    let cfg = ConnectConfig::from_url("vsql://h/db?syntax_mode=mysql").unwrap();
    assert_eq!(cfg.syntax_mode, Some(SyntaxMode::MySQL));
}

#[test]
fn builder_dialect_is_alias_for_syntax_mode() {
    let cfg = ConnectConfig::new("h", 1).dialect(SyntaxMode::MySQL);
    assert_eq!(cfg.syntax_mode, Some(SyntaxMode::MySQL));
}
