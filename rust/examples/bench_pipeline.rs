//! Pipelined PWire bench — ships 16 queries per round-trip to amortise the
//! network RTT.  Uses the new `PyroWireConnection::pipeline()` API.
//!
//! Expected uplift vs `bench_pwire_prepare`: if the non-pipelined path
//! reaches N tps at c=1, the 16-batch pipeline should reach ≈ 16 × N tps
//! on the same link (RTT is fully amortised).
//!
//! Usage:
//!   cargo run --release --example bench_pipeline -- <host:port> <seconds> [batch_size]

use std::time::{Duration, Instant};

use futures::executor::block_on;
use pyrosql::pwire::PyroWireConnection;
use pyrosql::row::Value;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let addr = args.get(1).cloned().unwrap_or_else(|| "127.0.0.1:12520".to_owned());
    let duration_secs: u64 = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(10);
    let batch_size: usize = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(16);

    if batch_size == 0 {
        eprintln!("batch_size must be > 0");
        std::process::exit(2);
    }

    let conn = PyroWireConnection::connect(&addr).expect("PyroWireConnection::connect");
    println!("connected to {addr} via pyro-runtime PWire (batch = {batch_size})");

    let sql = "SELECT abalance FROM pgbench_accounts WHERE aid = $1";

    // Warm-up round-trip — primes the server-side template cache AND our
    // client-side prepare map so that the timed loop is pure EXECUTE.
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
    let mut next_id: i64 = 1;

    while Instant::now() < deadline {
        let mut pl = conn.pipeline();
        for _ in 0..batch_size {
            pl = pl.query(sql, &[Value::Int(next_id)]);
            next_id = (next_id % 100_000) + 1;
        }
        match block_on(pl.send()) {
            Ok(responses) => {
                // Count every statement that returned Ok(...) as a successful op.
                for r in responses.into_results() {
                    match r {
                        Ok(_) => ops += 1,
                        Err(e) => {
                            eprintln!("batch sub-error: {e:?}");
                        }
                    }
                }
            }
            Err(e) => {
                eprintln!("pipeline error at {ops} ops: {e:?}");
                break;
            }
        }
    }

    let elapsed = start.elapsed().as_secs_f64();
    let tps = ops as f64 / elapsed;
    let batches = ops / batch_size as u64;
    println!(
        "{ops} ops ({batches} batches × {batch_size}) in {elapsed:.2}s — {tps:.0} tps (pipelined)"
    );
}
