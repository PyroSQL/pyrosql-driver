//! Pipelined INSERT via pwire (separate table to avoid PK collisions).
//!
//! Usage: bench_insert_raw <host:port> <seconds> <batch_size>

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::{Duration, Instant};

const MSG_QUERY: u8 = 0x01;
const MSG_PREPARE: u8 = 0x02;
const MSG_EXECUTE: u8 = 0x03;
const RESP_OK: u8 = 0x02;

fn write_frame(s: &mut TcpStream, ty: u8, payload: &[u8]) -> std::io::Result<()> {
    let mut buf = Vec::with_capacity(5 + payload.len());
    buf.push(ty);
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

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let addr = args.get(1).cloned().unwrap_or_else(|| "127.0.0.1:12520".to_owned());
    let dur_s: u64 = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(10);
    let batch: usize = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(16);

    let mut s = TcpStream::connect(&addr).expect("connect");
    s.set_nodelay(true).ok();

    // Fresh table per run so aid never collides.
    let setup = format!(
        "DROP TABLE IF EXISTS bi_{ts}",
        ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()
    );
    let _ = write_frame(&mut s, MSG_QUERY, setup.as_bytes());
    let _ = read_frame(&mut s);
    let table = format!("bi_{}",
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs());
    let create = format!("CREATE TABLE {table} (aid BIGINT PRIMARY KEY, val INT)");
    write_frame(&mut s, MSG_QUERY, create.as_bytes()).unwrap();
    let _ = read_frame(&mut s);

    let sql = format!("INSERT INTO {table} (aid, val) VALUES ($1, $2)");
    write_frame(&mut s, MSG_PREPARE, sql.as_bytes()).unwrap();
    let (ty, payload) = read_frame(&mut s).unwrap();
    if ty != RESP_OK { eprintln!("prepare fail"); std::process::exit(1); }
    let handle = u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]);
    println!("INSERT into {table}, handle={handle}, batch={batch}");

    let deadline = Instant::now() + Duration::from_secs(dur_s);
    let mut ops: u64 = 0;
    let mut aid: i64 = 1;
    let mut send_buf: Vec<u8> = Vec::with_capacity(batch * 64);
    let mut hdr_buf = [0u8; 5];
    let start = Instant::now();
    while Instant::now() < deadline {
        send_buf.clear();
        for _ in 0..batch {
            let a_str = aid.to_string();
            let v_str = (aid & 0xff).to_string();
            aid += 1;
            let payload_len: u32 = 4 + 2 + 4 + a_str.len() as u32 + 4 + v_str.len() as u32;
            send_buf.push(MSG_EXECUTE);
            send_buf.extend_from_slice(&payload_len.to_le_bytes());
            send_buf.extend_from_slice(&handle.to_le_bytes());
            send_buf.extend_from_slice(&2u16.to_le_bytes());
            send_buf.extend_from_slice(&(a_str.len() as u32).to_le_bytes());
            send_buf.extend_from_slice(a_str.as_bytes());
            send_buf.extend_from_slice(&(v_str.len() as u32).to_le_bytes());
            send_buf.extend_from_slice(v_str.as_bytes());
        }
        if s.write_all(&send_buf).is_err() { break; }
        let mut ok = true;
        for _ in 0..batch {
            if s.read_exact(&mut hdr_buf).is_err() { ok = false; break; }
            let len = u32::from_le_bytes([hdr_buf[1], hdr_buf[2], hdr_buf[3], hdr_buf[4]]) as usize;
            if len > 0 {
                let mut p = vec![0u8; len];
                if s.read_exact(&mut p).is_err() { ok = false; break; }
            }
        }
        if !ok { break; }
        ops += batch as u64;
    }
    let el = start.elapsed().as_secs_f64();
    println!("{ops} INSERTs in {el:.2}s — {:.0} ops/s (batch={batch})", ops as f64 / el);
}
