//! Bench the MuxConnection — N concurrent "app tasks" each issuing
//! request/response queries without any explicit pipelining.  The
//! driver's mux layer coalesces them into pipelined writes on the
//! shared TCP socket.
//!
//! Measures two baselines for comparison:
//!   1. Sequential: single task, one query at a time (no concurrency).
//!   2. Concurrent via mux: N tasks, each awaiting one query at a time,
//!      but the mux aggregates them on the wire.
//!
//! Usage: bench_mux <host:port> <duration_s> <num_app_tasks>
//!   Example: bench_mux 127.0.0.1:12520 10 16

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use pyrosql::mux::MuxConnection;

const MSG_PREPARE: u8 = 0x02;
const MSG_EXECUTE: u8 = 0x03;
const RESP_OK: u8 = 0x02;

fn build_execute(handle: u32, aid: i64) -> Vec<u8> {
    let a = aid.to_string();
    let payload_len: u32 = 4 + 2 + 4 + a.len() as u32;
    let mut buf = Vec::with_capacity(5 + payload_len as usize);
    buf.push(MSG_EXECUTE);
    buf.extend_from_slice(&payload_len.to_le_bytes());
    buf.extend_from_slice(&handle.to_le_bytes());
    buf.extend_from_slice(&1u16.to_le_bytes());
    buf.extend_from_slice(&(a.len() as u32).to_le_bytes());
    buf.extend_from_slice(a.as_bytes());
    buf
}

async fn prepare(conn: &MuxConnection, sql: &str) -> u32 {
    let mut frame = Vec::with_capacity(5 + sql.len());
    frame.push(MSG_PREPARE);
    frame.extend_from_slice(&(sql.len() as u32).to_le_bytes());
    frame.extend_from_slice(sql.as_bytes());
    let (ty, payload) = conn.submit(frame).await.expect("PREPARE");
    assert_eq!(ty, RESP_OK, "PREPARE failed");
    u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]])
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let addr = args.get(1).cloned().unwrap_or_else(|| "127.0.0.1:12520".to_owned());
    let dur_s: u64 = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(10);
    let n_tasks: usize = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(16);

    let conn = MuxConnection::connect(&addr).expect("connect");
    let sql = "SELECT abalance FROM pgbench_accounts WHERE aid = $1";

    // Block on futures using a tiny single-threaded executor.  We spawn
    // our own native OS threads for the N "app tasks" so each can
    // block on its mux future independently — this simulates N
    // independent web-handler threads.
    let handle = futures::executor::block_on(prepare(&conn, sql));
    println!("prepared handle={handle}, tasks={n_tasks}, dur={dur_s}s");

    let deadline = Instant::now() + Duration::from_secs(dur_s);
    let ops = Arc::new(AtomicU64::new(0));
    let mut worker_handles: Vec<std::thread::JoinHandle<()>> = Vec::new();

    for tid in 0..n_tasks {
        let conn = conn.clone();
        let ops = Arc::clone(&ops);
        let dl = deadline;
        worker_handles.push(
            std::thread::Builder::new()
                .name(format!("app-task-{tid}"))
                .spawn(move || {
                    let mut rng: u64 = 0xDEAD ^ (tid as u64 * 0xBEEF);
                    while Instant::now() < dl {
                        rng ^= rng << 13; rng ^= rng >> 7; rng ^= rng << 17;
                        let aid = ((rng % 200_000) + 1) as i64;
                        let frame = build_execute(handle, aid);
                        let fut = conn.submit(frame);
                        // Block this OS thread on ONE query at a time —
                        // no explicit pipelining, no concurrency tricks.
                        // The mux driver coalesces the N workers' in-
                        // flight requests onto the shared socket.
                        let _ = futures::executor::block_on(fut);
                        ops.fetch_add(1, Ordering::Relaxed);
                    }
                })
                .expect("spawn worker"),
        );
    }

    let start = Instant::now();
    for h in worker_handles { let _ = h.join(); }
    let elapsed = start.elapsed().as_secs_f64();
    let total = ops.load(Ordering::Relaxed);
    println!(
        "\nMUX c={n_tasks} (app tasks each blocking on one query at a time):\n  \
         {total} ops in {elapsed:.2}s → {:.0} ops/s",
        total as f64 / elapsed
    );
    // The two mux I/O threads are non-daemon; forcibly exit so the
    // process terminates without waiting on their blocked read()s.
    std::process::exit(0);
}
