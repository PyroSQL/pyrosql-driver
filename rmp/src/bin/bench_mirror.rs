//! Benchmark binary for TableMirror read throughput.
//!
//! Loads 1000 rows into a mirror, then 4 threads read randomly for 5 seconds.
//! Reports total QPS to prove that reads are local memory access.

use pyrosql_rmp::mirror::TableMirror;
use pyrosql_rmp::protocol::{ColumnInfo, ColumnType, Snapshot};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

fn main() {
    const NUM_ROWS: u64 = 1000;
    const NUM_THREADS: usize = 4;
    const DURATION_SECS: u64 = 5;

    println!("=== PyroSQL RMP Mirror Read Benchmark ===");
    println!("Rows: {NUM_ROWS}  Threads: {NUM_THREADS}  Duration: {DURATION_SECS}s");
    println!();

    // Build snapshot
    let rows: Vec<(Vec<u8>, Vec<u8>)> = (0..NUM_ROWS)
        .map(|i| {
            let pk = i.to_le_bytes().to_vec();
            let data = format!("row_data_{i}_padding_to_simulate_real_payload").into_bytes();
            (pk, data)
        })
        .collect();

    let mirror = Arc::new(TableMirror::new(1));
    mirror.load_snapshot(Snapshot {
        sub_id: 1,
        version: 1,
        columns: vec![
            ColumnInfo {
                name: "id".into(),
                type_tag: ColumnType::Int64,
            },
            ColumnInfo {
                name: "data".into(),
                type_tag: ColumnType::Text,
            },
        ],
        rows,
    });

    println!("Mirror loaded: {} rows", mirror.len());

    let done = Arc::new(AtomicBool::new(false));
    let total_reads = Arc::new(AtomicU64::new(0));
    let total_hits = Arc::new(AtomicU64::new(0));

    let start = Instant::now();

    // Spawn reader threads
    let mut handles = Vec::new();
    for thread_id in 0..NUM_THREADS {
        let m = Arc::clone(&mirror);
        let d = Arc::clone(&done);
        let tr = Arc::clone(&total_reads);
        let th = Arc::clone(&total_hits);

        handles.push(std::thread::spawn(move || {
            let mut local_reads = 0u64;
            let mut local_hits = 0u64;
            // Simple deterministic "random" using thread_id as seed
            let mut rng_state: u64 = thread_id as u64 * 7 + 13;

            while !d.load(Ordering::Relaxed) {
                // Batch of 1000 reads before checking done flag
                for _ in 0..1000 {
                    // xorshift64 for fast pseudo-random
                    rng_state ^= rng_state << 13;
                    rng_state ^= rng_state >> 7;
                    rng_state ^= rng_state << 17;
                    let key = (rng_state % NUM_ROWS).to_le_bytes().to_vec();

                    if m.get(&key).is_some() {
                        local_hits += 1;
                    }
                    local_reads += 1;
                }
            }

            tr.fetch_add(local_reads, Ordering::Relaxed);
            th.fetch_add(local_hits, Ordering::Relaxed);
        }));
    }

    // Wait for duration
    std::thread::sleep(Duration::from_secs(DURATION_SECS));
    done.store(true, Ordering::Relaxed);

    for h in handles {
        h.join().expect("reader thread panicked");
    }

    let elapsed = start.elapsed();
    let reads = total_reads.load(Ordering::Relaxed);
    let hits = total_hits.load(Ordering::Relaxed);
    let qps = reads as f64 / elapsed.as_secs_f64();

    println!();
    println!("--- Results ---");
    println!("Total reads:  {reads}");
    println!("Total hits:   {hits} ({:.1}%)", hits as f64 / reads as f64 * 100.0);
    println!("Elapsed:      {:.2}s", elapsed.as_secs_f64());
    println!("Throughput:   {:.2}M reads/sec", qps / 1_000_000.0);
    println!(
        "Avg latency:  {:.0}ns/read",
        elapsed.as_nanos() as f64 / reads as f64
    );
    println!();

    if qps > 1_000_000.0 {
        println!("PASS: >1M reads/sec confirms local memory access.");
    } else {
        println!("WARN: <1M reads/sec — unexpected for local memory.");
    }
}
