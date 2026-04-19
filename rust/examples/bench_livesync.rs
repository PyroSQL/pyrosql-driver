//! LiveSync / RMP bench — subscribe to a table, populate a local mirror,
//! then hammer mirror reads as a proxy for web-app read traffic.
//!
//! The mirror is a `DashMap<Vec<u8>, Vec<u8>>` — the same layout the
//! server+driver design documents call out. We build it from a single
//! SNAPSHOT frame (no deltas needed for a static dataset); that's the
//! apples-to-apples view of what a web handler would read from on every
//! request if the table was subscribed at boot.
//!
//! Usage:
//!   bench_livesync <host:port> <table> <duration_s> <threads>

use std::net::TcpStream;
use std::io::{Read, Write};
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;

const MSG_SUBSCRIBE: u8 = 0x20;
const MSG_SNAPSHOT:  u8 = 0x22;
const MSG_DELTA:     u8 = 0x23;

fn write_frame(s: &mut TcpStream, msg_type: u8, payload: &[u8]) -> std::io::Result<()> {
    let mut buf = Vec::with_capacity(5 + payload.len());
    buf.push(msg_type);
    buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    buf.extend_from_slice(payload);
    s.write_all(&buf)
}

fn read_frame(s: &mut TcpStream) -> std::io::Result<(u8, Vec<u8>)> {
    let mut hdr = [0u8; 5];
    s.read_exact(&mut hdr)?;
    let ty = hdr[0];
    let len = u32::from_le_bytes([hdr[1], hdr[2], hdr[3], hdr[4]]) as usize;
    let mut payload = vec![0u8; len];
    if len > 0 { s.read_exact(&mut payload)?; }
    Ok((ty, payload))
}

fn encode_subscribe_all(sub_id: u64, table: &str) -> Vec<u8> {
    // Payload: [sub_id: u64 LE][table_len: u32 LE][table][pred_tag: u8 = 0 (All)]
    let mut p = Vec::new();
    p.extend_from_slice(&sub_id.to_le_bytes());
    p.extend_from_slice(&(table.len() as u32).to_le_bytes());
    p.extend_from_slice(table.as_bytes());
    p.push(0); // Predicate::All
    p
}

fn read_le_u32(buf: &[u8], pos: &mut usize) -> u32 {
    let v = u32::from_le_bytes([buf[*pos], buf[*pos+1], buf[*pos+2], buf[*pos+3]]);
    *pos += 4;
    v
}
fn read_le_u64(buf: &[u8], pos: &mut usize) -> u64 {
    let v = u64::from_le_bytes([
        buf[*pos], buf[*pos+1], buf[*pos+2], buf[*pos+3],
        buf[*pos+4], buf[*pos+5], buf[*pos+6], buf[*pos+7],
    ]);
    *pos += 8;
    v
}
fn read_le_bytes<'a>(buf: &'a [u8], pos: &mut usize) -> &'a [u8] {
    let len = read_le_u32(buf, pos) as usize;
    let start = *pos;
    *pos += len;
    &buf[start..start+len]
}

/// Decode a SNAPSHOT payload → (Vec<pk_bytes>, Vec<row_bytes>).
fn parse_snapshot(payload: &[u8]) -> (Vec<Vec<u8>>, Vec<Vec<u8>>) {
    let mut pos = 0;
    let _sub_id = read_le_u64(payload, &mut pos);
    let _version = read_le_u64(payload, &mut pos);
    let col_count = read_le_u32(payload, &mut pos) as usize;
    for _ in 0..col_count {
        let _name = read_le_bytes(payload, &mut pos);
        let _tag = payload[pos];  pos += 1;
    }
    let row_count = read_le_u32(payload, &mut pos) as usize;
    let mut pks = Vec::with_capacity(row_count);
    let mut rows = Vec::with_capacity(row_count);
    for _ in 0..row_count {
        let pk = read_le_bytes(payload, &mut pos).to_vec();
        let row = read_le_bytes(payload, &mut pos).to_vec();
        pks.push(pk);
        rows.push(row);
    }
    (pks, rows)
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let addr    = args.get(1).cloned().unwrap_or_else(|| "127.0.0.1:12700".to_owned());
    let table   = args.get(2).cloned().unwrap_or_else(|| "pgbench_accounts".to_owned());
    let dur_s: u64 = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(10);
    let threads: usize = args.get(4).and_then(|s| s.parse().ok()).unwrap_or(1);

    println!("RMP bench: addr={addr} table={table} threads={threads} dur={dur_s}s");

    // Connect + subscribe.
    let mut s = TcpStream::connect(&addr).expect("connect");
    s.set_nodelay(true).ok();
    let sub_payload = encode_subscribe_all(1, &table);
    write_frame(&mut s, MSG_SUBSCRIBE, &sub_payload).expect("send SUBSCRIBE");

    // Read SNAPSHOT.
    let t_snap_start = Instant::now();
    let (ty, payload) = read_frame(&mut s).expect("recv SNAPSHOT");
    if ty != MSG_SNAPSHOT {
        eprintln!("expected SNAPSHOT (0x22), got 0x{ty:02x}");
        std::process::exit(1);
    }
    let (pks, rows) = parse_snapshot(&payload);
    let snap_ms = t_snap_start.elapsed().as_secs_f64() * 1000.0;
    println!("SNAPSHOT: {} rows, {} bytes payload, {:.1} ms wallclock",
        pks.len(), payload.len(), snap_ms);

    // Build the mirror — DashMap<pk_bytes, row_bytes>.
    let mirror: Arc<DashMap<Vec<u8>, Vec<u8>>> = Arc::new(DashMap::with_capacity(pks.len()));
    for (pk, row) in pks.iter().zip(rows.iter()) {
        mirror.insert(pk.clone(), row.clone());
    }
    let pk_count = pks.len();
    let pk_arr: Arc<Vec<Vec<u8>>> = Arc::new(pks);
    println!("mirror populated: {} entries", pk_count);

    // Spin N worker threads, each does random .get() in a tight loop.
    let deadline = Instant::now() + Duration::from_secs(dur_s);
    let barrier = Arc::new(std::sync::Barrier::new(threads));
    let mut handles: Vec<std::thread::JoinHandle<(u64, Vec<u128>)>> = Vec::with_capacity(threads);
    for tid in 0..threads {
        let mirror  = Arc::clone(&mirror);
        let pk_arr  = Arc::clone(&pk_arr);
        let barrier = Arc::clone(&barrier);
        handles.push(std::thread::spawn(move || {
            barrier.wait();
            let mut hits: u64 = 0;
            let mut rng: u64 = 0x9E3779B97F4A7C15 ^ (tid as u64 * 0xDEADBEEF);
            // Sample 1024 per-op latencies distributed through the run.
            let mut sample_every: u64 = 10_000;
            let mut samples: Vec<u128> = Vec::with_capacity(1024);
            let mut op: u64 = 0;
            while Instant::now() < deadline {
                // Run a batch of 10K ops without checking time, then re-check.
                for _ in 0..sample_every {
                    rng ^= rng << 13; rng ^= rng >> 7; rng ^= rng << 17;
                    let idx = (rng as usize) % pk_count;
                    let pk = &pk_arr[idx];
                    if samples.len() < 1024 && (op % 100 == 0) {
                        let t0 = Instant::now();
                        let hit = mirror.get(pk);
                        let ns = t0.elapsed().as_nanos();
                        samples.push(ns);
                        if hit.is_some() { hits += 1; }
                    } else if mirror.get(pk).is_some() {
                        hits += 1;
                    }
                    op += 1;
                }
            }
            if sample_every == 0 { sample_every = 1; }
            let _ = sample_every;
            (hits, samples)
        }));
    }

    let mut total_hits: u64 = 0;
    let mut all_samples: Vec<u128> = Vec::new();
    let t_start = Instant::now();
    for h in handles {
        let (hits, samples) = h.join().expect("thread");
        total_hits += hits;
        all_samples.extend(samples);
    }
    let elapsed = t_start.elapsed().as_secs_f64();
    let tps = total_hits as f64 / elapsed;

    all_samples.sort();
    let n = all_samples.len();
    let pct = |p: f64| -> u128 {
        if n == 0 { return 0; }
        all_samples[((n as f64 * p) as usize).min(n - 1)]
    };
    let p50 = pct(0.50); let p95 = pct(0.95);
    let p99 = pct(0.99); let p999 = pct(0.999);

    println!(
        "\nRESULT: {} hits in {:.2}s → {:.0} ops/sec (threads={})",
        total_hits, elapsed, tps, threads,
    );
    println!(
        "latency (ns, sampled every 100th op): p50={} p95={} p99={} p99.9={}",
        p50, p95, p99, p999,
    );
    println!(
        "                      in µs:         p50={:.2} p95={:.2} p99={:.2} p99.9={:.2}",
        p50 as f64 / 1000.0, p95 as f64 / 1000.0, p99 as f64 / 1000.0, p999 as f64 / 1000.0,
    );
}
