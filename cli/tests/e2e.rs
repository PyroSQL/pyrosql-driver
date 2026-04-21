//! End-to-end tests for the `pyrosql` CLI.
//!
//! The meaningful tests require a live PyroSQL server on `localhost:
//! 12520` with a writable `pyrosql` database — marked `#[ignore]` so
//! they do not run on CI / `cargo test --release` without opt-in.
//!
//! Run with:
//!     cargo test -p pyrosql-cli -- --ignored

use std::process::Command;

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_pyrosql")
}

#[test]
fn help_flag_prints_usage() {
    let out = Command::new(bin()).arg("--help").output().unwrap();
    assert!(out.status.success(), "--help must succeed");
    let s = String::from_utf8_lossy(&out.stdout);
    assert!(s.contains("--host"), "help text must mention --host");
    assert!(s.contains("--user"), "help text must mention --user");
    assert!(s.contains("--format"), "help text must mention --format");
}

#[test]
fn version_flag_prints_semver() {
    let out = Command::new(bin()).arg("--version").output().unwrap();
    assert!(out.status.success(), "--version must succeed");
    let s = String::from_utf8_lossy(&out.stdout);
    assert!(s.starts_with("pyrosql "));
}

#[test]
fn invalid_url_scheme_errors_cleanly() {
    let out = Command::new(bin())
        .arg("http://bad/scheme")
        .arg("-c")
        .arg("SELECT 1")
        .output()
        .unwrap();
    assert!(!out.status.success());
    let s = String::from_utf8_lossy(&out.stderr);
    assert!(
        s.to_lowercase().contains("url") || s.to_lowercase().contains("invalid"),
        "stderr must mention invalid url, got: {s}",
    );
}

#[test]
#[ignore]
fn one_shot_select_one() {
    let out = Command::new(bin())
        .args(["-h", "localhost", "-p", "12520", "-c", "SELECT 1"])
        .output()
        .unwrap();
    assert!(out.status.success(), "SELECT 1 must succeed against live server");
    let s = String::from_utf8_lossy(&out.stdout);
    assert!(s.contains('1'));
}
