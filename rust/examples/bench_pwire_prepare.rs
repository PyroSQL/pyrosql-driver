//! Quick micro-bench for PWire transport + client-side PREPARE cache.
//!
//! Uses the new `PyroWireConnection` (pyro-runtime-backed, zero tokio).
//! Drives the async API via `futures::executor::block_on`, because the
//! main thread has no async runtime of its own — `PyroWireConnection`
//! spawns its own pyro-runtime on a dedicated worker thread.
//!
//! Usage:
//!   cargo run --release --example bench_pwire_prepare -- <host:port> <seconds>

use std::time::{Duration, Instant};

use futures::executor::block_on;
use pyrosql::pwire::PyroWireConnection;
use pyrosql::row::Value;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let addr = args.get(1).cloned().unwrap_or_else(|| "127.0.0.1:12520".to_owned());
    let duration_secs: u64 = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(10);

    let conn = PyroWireConnection::connect(&addr).expect("PyroWireConnection::connect");
    println!("connected to {addr} via pyro-runtime PWire");

    // SELECT pk with a single $1 placeholder.  First call triggers MSG_PREPARE;
    // subsequent calls go through MSG_EXECUTE (server-side template cache hit).
    let sql = "SELECT abalance FROM pgbench_accounts WHERE aid = $1";

    // Warm-up.
    match block_on(conn.query(sql, &[Value::Int(1)])) {
        Ok(_) => println!("warm-up OK"),
        Err(e) => {
            eprintln!("warm-up failed: {e:?}");
            std::process::exit(1);
        }
    }

    let deadline = Instant::now() + Duration::from_secs(duration_secs);
    let mut ops: u64 = 0;
    let start = Instant::now();
    while Instant::now() < deadline {
        let id = (ops % 100_000 + 1) as i64;
        match block_on(conn.query(sql, &[Value::Int(id)])) {
            Ok(_) => ops += 1,
            Err(e) => {
                eprintln!("query error at op {ops}: {e:?}");
                break;
            }
        }
    }
    let elapsed = start.elapsed().as_secs_f64();
    let tps = ops as f64 / elapsed;
    println!("{ops} ops in {elapsed:.2}s — {tps:.0} tps (c=1 PWire prepared, pyro-runtime)");
}
