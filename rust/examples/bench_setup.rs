//! One-shot: create pgbench_accounts + seed 100K rows via raw PWire.
//!
//! Usage: cargo run --release --example bench_setup -- <host:port> [rows]

use std::io::{Read, Write};
use std::net::TcpStream;

const MSG_QUERY: u8 = 0x01;
const RESP_OK: u8 = 0x02;
const RESP_ERROR: u8 = 0x03;
const RESP_RESULT_SET: u8 = 0x01;

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

fn exec(stream: &mut TcpStream, sql: &str) {
    write_frame(stream, MSG_QUERY, sql.as_bytes()).expect("send");
    let (ty, payload) = read_frame(stream).expect("recv");
    match ty {
        RESP_OK | RESP_RESULT_SET => {
            println!("ok [{}] — {}", sql.get(..60).unwrap_or(sql), payload.len());
        }
        RESP_ERROR => {
            eprintln!("ERR [{}] — {}", sql.get(..60).unwrap_or(sql), String::from_utf8_lossy(&payload));
        }
        other => eprintln!("unexpected 0x{other:02x}"),
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let addr = args.get(1).cloned().unwrap_or_else(|| "127.0.0.1:12520".to_owned());
    let rows: i64 = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(100_000);

    let mut stream = TcpStream::connect(&addr).expect("connect");
    stream.set_nodelay(true).ok();
    println!("connected to {addr}");

    exec(&mut stream, "DROP TABLE IF EXISTS pgbench_accounts");
    exec(&mut stream,
        "CREATE TABLE pgbench_accounts (aid BIGINT PRIMARY KEY, bid INTEGER, abalance INTEGER, filler TEXT)");

    // Seed in batches of 1000 via multi-row INSERT.
    const BATCH: i64 = 1000;
    let mut inserted = 0i64;
    while inserted < rows {
        let end = (inserted + BATCH).min(rows);
        let mut sql = String::from("INSERT INTO pgbench_accounts (aid, bid, abalance, filler) VALUES ");
        for i in inserted..end {
            if i > inserted { sql.push(','); }
            sql.push_str(&format!("({}, 1, 0, 'x')", i + 1));
        }
        write_frame(&mut stream, MSG_QUERY, sql.as_bytes()).expect("send");
        let (ty, payload) = read_frame(&mut stream).expect("recv");
        if ty == RESP_ERROR {
            eprintln!("insert err at {}: {}", inserted, String::from_utf8_lossy(&payload));
            break;
        }
        inserted = end;
        if inserted % 10_000 == 0 { println!("  inserted {inserted}/{rows}"); }
    }
    println!("done: {inserted} rows");
}
