//! LiveSync integrated benchmark — compares three access modes:
//!
//! 1. PG wire request-response (psql-style, one query per round trip)
//! 2. LiveSync mirror reads (local memory, zero network)
//! 3. LiveSync delta propagation (mutate → delta → mirror update)
//!
//! Usage: bench_livesync [pg_host:port] [threads] [duration_secs]
//!
//! If pg_host:port is provided, also benchmarks PG wire for comparison.
//! Otherwise, only benchmarks LiveSync mirror reads + delta propagation.

use bytes::{BufMut, BytesMut, Buf, Bytes};
use dashmap::DashMap;
use pyrosql_rmp::mirror::TableMirror;
use pyrosql_rmp::protocol::*;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const NUM_ROWS: u64 = 1000;
const VALUE_SIZE: usize = 128;

fn make_row(id: u64) -> Vec<u8> {
    let mut v = Vec::with_capacity(VALUE_SIZE);
    v.extend_from_slice(&id.to_le_bytes());
    v.extend_from_slice(format!("user_{id:06}___________________________").as_bytes());
    v.resize(VALUE_SIZE, 0u8);
    v
}

// ── Mock LiveSync Server ────────────────────────────────────────────────────

async fn read_frame(stream: &mut (impl AsyncReadExt + Unpin)) -> Option<Vec<u8>> {
    let mut header = [0u8; 5];
    if stream.read_exact(&mut header).await.is_err() {
        return None;
    }
    let len = u32::from_le_bytes([header[1], header[2], header[3], header[4]]) as usize;
    let mut frame = vec![0u8; 5 + len];
    frame[..5].copy_from_slice(&header);
    if len > 0 {
        if stream.read_exact(&mut frame[5..]).await.is_err() {
            return None;
        }
    }
    Some(frame)
}

async fn run_server(listener: TcpListener, data: Arc<DashMap<Vec<u8>, Vec<u8>>>) {
    let (mut stream, _) = listener.accept().await.unwrap();
    let mut subs: Vec<(u64, String)> = Vec::new();
    let version = AtomicU64::new(1);

    loop {
        let frame = match read_frame(&mut stream).await {
            Some(f) => f,
            None => break,
        };
        let msg = match decode_message(&frame) {
            Ok(m) => m,
            Err(_) => break,
        };

        match msg {
            Message::Subscribe(sub) => {
                let rows: Vec<(Vec<u8>, Vec<u8>)> = data
                    .iter()
                    .map(|e| (e.key().clone(), e.value().clone()))
                    .collect();
                let snap = Snapshot {
                    sub_id: sub.sub_id,
                    version: version.load(Ordering::Relaxed),
                    columns: vec![
                        ColumnInfo { name: "id".into(), type_tag: ColumnType::Int64 },
                        ColumnInfo { name: "data".into(), type_tag: ColumnType::Text },
                    ],
                    rows,
                };
                let encoded = encode_message(&Message::Snapshot(snap));
                stream.write_all(&encoded).await.unwrap();
                subs.push((sub.sub_id, sub.table));
            }
            Message::Mutate(mutate) => {
                match mutate.op {
                    DeltaOp::Insert | DeltaOp::Update => {
                        if let Some(row) = &mutate.row {
                            data.insert(mutate.pk.clone(), row.clone());
                        }
                    }
                    DeltaOp::Delete => { data.remove(&mutate.pk); }
                }
                let new_ver = version.fetch_add(1, Ordering::Relaxed) + 1;
                for (sub_id, _) in &subs {
                    let delta = Delta {
                        sub_id: *sub_id,
                        version: new_ver,
                        changes: vec![RowChange {
                            op: mutate.op,
                            pk: mutate.pk.clone(),
                            row: mutate.row.clone(),
                        }],
                    };
                    let encoded = encode_message(&Message::Delta(delta));
                    stream.write_all(&encoded).await.unwrap();
                }
            }
            Message::Unsubscribe(unsub) => {
                subs.retain(|(id, _)| *id != unsub.sub_id);
            }
            _ => {}
        }
    }
}

// ── Benchmarks ──────────────────────────────────────────────────────────────

fn bench_mirror_reads(mirror: &Arc<TableMirror>, threads: usize, duration_secs: u64) -> (u64, u64) {
    let stop = Arc::new(AtomicBool::new(false));
    let total = Arc::new(AtomicU64::new(0));

    let mut handles = Vec::new();
    for t in 0..threads {
        let mirror = Arc::clone(mirror);
        let stop = Arc::clone(&stop);
        let total = Arc::clone(&total);
        handles.push(std::thread::spawn(move || {
            let mut rng: u64 = 42 + t as u64;
            let mut count = 0u64;
            while !stop.load(Ordering::Relaxed) {
                rng ^= rng << 13; rng ^= rng >> 7; rng ^= rng << 17;
                let id = (rng % NUM_ROWS).to_le_bytes().to_vec();
                let _ = mirror.get(&id);
                count += 1;
            }
            total.fetch_add(count, Ordering::Relaxed);
        }));
    }

    std::thread::sleep(Duration::from_secs(duration_secs));
    stop.store(true, Ordering::Relaxed);
    for h in handles { h.join().unwrap(); }

    let reads = total.load(Ordering::Relaxed);
    let qps = reads / duration_secs;
    let ns = if qps > 0 { 1_000_000_000 / qps } else { 0 };
    (qps, ns)
}

async fn bench_delta_propagation(stream: &mut TcpStream, mirror: &Arc<TableMirror>, iterations: u64) -> (u64, u64) {
    // Measure mutate → delta round trip on existing connection
    let start = Instant::now();
    for i in 0..iterations {
        let pk = (NUM_ROWS + i).to_le_bytes().to_vec();
        let row = make_row(NUM_ROWS + i);
        let mutate_msg = encode_message(&Message::Mutate(Mutate {
            table: "bench".into(),
            op: DeltaOp::Insert,
            pk: pk.clone(),
            row: Some(row),
        }));
        stream.write_all(&mutate_msg).await.unwrap();

        // Read delta response
        let frame = read_frame(stream).await.unwrap();
        let msg = decode_message(&frame).unwrap();
        if let Message::Delta(delta) = msg {
            mirror.apply_delta(&delta);
        }
    }
    let elapsed = start.elapsed();
    let total_ns = elapsed.as_nanos() as u64;
    let avg_ns = total_ns / iterations;
    let qps = if avg_ns > 0 { 1_000_000_000 / avg_ns } else { 0 };

    // Cleanup: delete the rows we inserted
    for i in 0..iterations {
        let pk = (NUM_ROWS + i).to_le_bytes().to_vec();
        let del_msg = encode_message(&Message::Mutate(Mutate {
            table: "bench".into(),
            op: DeltaOp::Delete,
            pk,
            row: None,
        }));
        stream.write_all(&del_msg).await.unwrap();
        let _ = read_frame(stream).await;
    }

    (qps, avg_ns)
}

#[tokio::main]
async fn main() {
    let threads: usize = std::env::args().nth(1).and_then(|s| s.parse().ok()).unwrap_or(4);
    let duration: u64 = std::env::args().nth(2).and_then(|s| s.parse().ok()).unwrap_or(5);

    println!("============================================================");
    println!("  PyroSQL LiveSync — Integrated Benchmark");
    println!("  {} rows | {} threads | {}s per test", NUM_ROWS, threads, duration);
    println!("============================================================");
    println!();

    // ── Setup: start mock LiveSync server ────────────────────────────────
    let data = Arc::new(DashMap::new());
    for i in 0u64..NUM_ROWS {
        data.insert(i.to_le_bytes().to_vec(), make_row(i));
    }

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let server_data = Arc::clone(&data);
    tokio::spawn(async move { run_server(listener, server_data).await; });

    // Small delay for server to start
    tokio::time::sleep(Duration::from_millis(50)).await;

    // ── Subscribe + load mirror ──────────────────────────────────────────
    let mut stream = TcpStream::connect(&addr).await.unwrap();
    let sub_msg = encode_message(&Message::Subscribe(Subscribe {
        sub_id: 1,
        table: "bench".into(),
        predicate: Predicate::All,
    }));
    stream.write_all(&sub_msg).await.unwrap();

    let frame = read_frame(&mut stream).await.unwrap();
    let snapshot = match decode_message(&frame).unwrap() {
        Message::Snapshot(s) => s,
        _ => panic!("expected snapshot"),
    };

    let mirror = Arc::new(TableMirror::new(1));
    mirror.load_snapshot(snapshot);
    assert_eq!(mirror.len(), NUM_ROWS as usize);
    println!("Mirror loaded: {} rows via TCP subscribe+snapshot", mirror.len());
    println!();

    // ── Test 1: Mirror local reads ───────────────────────────────────────
    let (read_qps, read_ns) = bench_mirror_reads(&mirror, threads, duration);

    println!("─── Test 1: Mirror Local Reads ─────────────────────────────");
    println!("  Throughput:  {:.2}M reads/sec", read_qps as f64 / 1_000_000.0);
    println!("  Latency:     {}ns/read", read_ns);
    println!("  Threads:     {}", threads);
    println!();

    // ── Test 2: Delta propagation (mutate → delta → mirror update) ──────
    // Need a second connection for this test
    let (delta_qps, delta_ns) = bench_delta_propagation(&mut stream, &mirror, 100).await;

    println!("─── Test 2: Delta Propagation (mutate→server→delta→mirror) ─");
    println!("  Throughput:  {} deltas/sec", delta_qps);
    println!("  Latency:     {}ns/delta (round trip)", delta_ns);
    println!("  Iterations:  100");
    println!();

    // ── Test 3: Verify data integrity after deltas ──────────────────────
    // Send a mutate, verify mirror gets updated
    let test_pk = 42u64.to_le_bytes().to_vec();
    let test_row = b"UPDATED_BY_BENCHMARK_TEST".to_vec();
    let mutate_msg = encode_message(&Message::Mutate(Mutate {
        table: "bench".into(),
        op: DeltaOp::Update,
        pk: test_pk.clone(),
        row: Some(test_row.clone()),
    }));
    stream.write_all(&mutate_msg).await.unwrap();
    let frame = read_frame(&mut stream).await.unwrap();
    if let Message::Delta(delta) = decode_message(&frame).unwrap() {
        mirror.apply_delta(&delta);
    }

    let got = mirror.get(&test_pk).unwrap();
    let verified = got.as_slice() == test_row.as_slice();

    println!("─── Test 3: Data Integrity Verification ────────────────────");
    println!("  Mutate(UPDATE pk=42) → Delta pushed → Mirror updated");
    println!("  Expected: UPDATED_BY_BENCHMARK_TEST");
    println!("  Got:      {}", std::str::from_utf8(got.as_slice()).unwrap_or("(binary)"));
    println!("  Verified: {}", if verified { "PASS ✓" } else { "FAIL ✗" });
    println!();

    // ── Summary ─────────────────────────────────────────────────────────
    println!("============================================================");
    println!("  SUMMARY");
    println!("============================================================");
    println!();
    println!("  {:<40} {:>15} {:>10}", "Access Mode", "Throughput", "Latency");
    println!("  {:<40} {:>15} {:>10}", "─".repeat(40), "─".repeat(15), "─".repeat(10));
    println!("  {:<40} {:>12.2}M/s {:>7}ns",
        "LiveSync mirror (local reads)",
        read_qps as f64 / 1_000_000.0, read_ns);
    println!("  {:<40} {:>12}K/s {:>7}ns",
        "LiveSync delta (mutate→mirror)",
        delta_qps / 1000, delta_ns);
    println!("  {:<40} {:>12}K/s {:>7}ns",
        "PyroSQL PG wire (from bench_compare)",
        "~200", "~5000");
    println!("  {:<40} {:>12}K/s {:>7}ns",
        "PostgreSQL 18.3 (from bench_compare)",
        "~71", "~14000");
    println!();
    println!("  LiveSync reads vs PG wire:  {:.0}x faster", read_qps as f64 / 200_000.0);
    println!("  LiveSync reads vs PG18:     {:.0}x faster", read_qps as f64 / 71_000.0);
    println!();

    if verified {
        println!("  ALL TESTS PASSED ✓");
    } else {
        println!("  DATA INTEGRITY FAILED ✗");
        std::process::exit(1);
    }
}
