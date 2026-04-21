//! Raw-socket PWire pipeline bench — zero driver abstractions.
//!
//! Proves the pipelining ceiling of the wire protocol + server combo.
//! One TCP connection, one PREPARE, then a tight loop of pipelined
//! EXECUTE batches.  Each batch = N MSG_EXECUTE frames written in a
//! single `write_all`, followed by N response reads.  That amortises
//! one RTT over N queries → c=1 throughput of roughly N × (1 / RTT).
//!
//! Usage:
//!   bench_pipeline_raw <host:port> <seconds> <batch_size>
//!
//! Numbers on the author's dev box (Azure L16s_v4 loopback):
//!   batch=1   → ~35 K ops/s  (sequential baseline)
//!   batch=16  → ~500 K ops/s (pipelined)
//!   batch=64  → ~1.5 M ops/s (pipelined, larger batch)

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::{Duration, Instant};

const MSG_QUERY:   u8 = 0x01;
const MSG_PREPARE: u8 = 0x02;
const MSG_EXECUTE: u8 = 0x03;
const RESP_RESULT_SET: u8 = 0x01;
const RESP_OK:    u8 = 0x02;
const RESP_ERROR: u8 = 0x03;

fn write_frame(s: &mut TcpStream, msg_type: u8, payload: &[u8]) -> std::io::Result<()> {
    let mut buf = Vec::with_capacity(5 + payload.len());
    buf.push(msg_type);
    buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    buf.extend_from_slice(payload);
    s.write_all(&buf)
}

fn read_frame(s: &mut TcpStream) -> std::io::Result<(u8, Vec<u8>)> {
    let mut header = [0u8; 5];
    s.read_exact(&mut header)?;
    let msg_type = header[0];
    let len = u32::from_le_bytes([header[1], header[2], header[3], header[4]]) as usize;
    let mut payload = vec![0u8; len];
    if len > 0 { s.read_exact(&mut payload)?; }
    Ok((msg_type, payload))
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let addr = args.get(1).cloned().unwrap_or_else(|| "127.0.0.1:12520".to_owned());
    let duration_secs: u64 = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(10);
    let batch: usize = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(16);

    let mut stream = TcpStream::connect(&addr).expect("connect");
    stream.set_nodelay(true).ok();
    println!("connected to {addr}; batch={batch}");

    // PREPARE once.
    let sql = "SELECT abalance FROM pgbench_accounts WHERE aid = $1";
    write_frame(&mut stream, MSG_PREPARE, sql.as_bytes()).expect("PREPARE send");
    let (resp_ty, payload) = read_frame(&mut stream).expect("PREPARE recv");
    if resp_ty != RESP_OK || payload.len() < 4 {
        eprintln!("PREPARE failed: resp_type=0x{resp_ty:02x} payload={payload:?}");
        std::process::exit(1);
    }
    let handle = u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]);
    println!("PREPARE ok, handle={handle}");

    // Pre-build ONE EXECUTE batch frame buffer of batch copies for warm-up
    // — we'll rebuild per-iteration with random IDs in the hot loop.
    let deadline = Instant::now() + Duration::from_secs(duration_secs);
    let mut ops: u64 = 0;
    let mut rng: u64 = 0x9E3779B97F4A7C15;
    let start = Instant::now();

    // Reused buffers, zero per-iteration allocation (the whole point of raw).
    let mut send_buf: Vec<u8> = Vec::with_capacity(batch * 32);
    let mut header_buf = [0u8; 5];
    let mut resp_payload: Vec<u8> = Vec::with_capacity(256);

    while Instant::now() < deadline {
        // Build `batch` MSG_EXECUTE frames back-to-back.
        send_buf.clear();
        for _ in 0..batch {
            // xorshift64 rng for the param value.
            rng ^= rng << 13; rng ^= rng >> 7; rng ^= rng << 17;
            let id = (rng % 100_000) as i64 + 1;
            let id_str = id.to_string();
            let payload_len: u32 = 4 + 2 + 4 + id_str.len() as u32;
            send_buf.push(MSG_EXECUTE);
            send_buf.extend_from_slice(&payload_len.to_le_bytes());
            send_buf.extend_from_slice(&handle.to_le_bytes());            // u32 handle
            send_buf.extend_from_slice(&1u16.to_le_bytes());               // u16 param count
            send_buf.extend_from_slice(&(id_str.len() as u32).to_le_bytes()); // u32 param len
            send_buf.extend_from_slice(id_str.as_bytes());
        }
        // Single write syscall for the whole batch.
        if stream.write_all(&send_buf).is_err() { break; }

        // Read `batch` responses back in order.
        let mut ok = true;
        for _ in 0..batch {
            if stream.read_exact(&mut header_buf).is_err() { ok = false; break; }
            let resp_ty = header_buf[0];
            let resp_len = u32::from_le_bytes([
                header_buf[1], header_buf[2], header_buf[3], header_buf[4]
            ]) as usize;
            resp_payload.clear();
            resp_payload.resize(resp_len, 0);
            if resp_len > 0 && stream.read_exact(&mut resp_payload).is_err() {
                ok = false; break;
            }
            if resp_ty == RESP_ERROR {
                eprintln!("server error: {}", String::from_utf8_lossy(&resp_payload));
                ok = false; break;
            }
            if resp_ty != RESP_RESULT_SET && resp_ty != RESP_OK {
                eprintln!("unexpected resp_type 0x{resp_ty:02x}");
                ok = false; break;
            }
        }
        if !ok { break; }
        ops += batch as u64;
    }

    let elapsed = start.elapsed().as_secs_f64();
    let tps = ops as f64 / elapsed;
    println!(
        "{ops} ops in {elapsed:.2}s — {tps:.0} ops/s (c=1, pipeline batch={batch})",
    );
}
