//! Profile a single multi-row INSERT via pg-wire protocol using raw TCP
//! (no driver abstractions).  Compares N-row INSERT latency for N in
//! [1, 10, 100, 500, 1000] to find the super-linear cliff in the
//! pg-wire INSERT path.
//!
//! Usage: cargo run --release --example bench_multi_insert_pg -- <host:port> <user> <pass> <db>

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Instant;

fn write_msg(s: &mut TcpStream, ty: u8, body: &[u8]) -> std::io::Result<()> {
    let mut buf = Vec::with_capacity(5 + body.len());
    buf.push(ty);
    buf.extend_from_slice(&((4 + body.len()) as u32).to_be_bytes());
    buf.extend_from_slice(body);
    s.write_all(&buf)
}

fn read_msg(s: &mut TcpStream) -> std::io::Result<(u8, Vec<u8>)> {
    let mut hdr = [0u8; 5];
    s.read_exact(&mut hdr)?;
    let ty = hdr[0];
    let len = u32::from_be_bytes([hdr[1], hdr[2], hdr[3], hdr[4]]) as usize;
    let mut body = vec![0u8; len - 4];
    if len > 4 { s.read_exact(&mut body)?; }
    Ok((ty, body))
}

fn startup(s: &mut TcpStream, user: &str, db: &str) -> std::io::Result<()> {
    // Startup message: version 196608 (3.0), then key-value pairs.
    let mut body = Vec::new();
    body.extend_from_slice(&196608u32.to_be_bytes());
    body.extend_from_slice(b"user\0");
    body.extend_from_slice(user.as_bytes());
    body.push(0);
    body.extend_from_slice(b"database\0");
    body.extend_from_slice(db.as_bytes());
    body.push(0);
    body.push(0); // trailing null
    let mut buf = Vec::new();
    buf.extend_from_slice(&((4 + body.len()) as u32).to_be_bytes());
    buf.extend_from_slice(&body);
    s.write_all(&buf)
}

fn main() -> std::io::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let addr = args.get(1).cloned().unwrap_or_else(|| "127.0.0.1:5433".into());
    let user = args.get(2).cloned().unwrap_or_else(|| "pyrosql".into());
    let pass = args.get(3).cloned().unwrap_or_else(|| "benchpass1".into());
    let db   = args.get(4).cloned().unwrap_or_else(|| "pyrosql".into());

    let mut s = TcpStream::connect(&addr)?;
    s.set_nodelay(true).ok();
    startup(&mut s, &user, &db)?;

    // Drain through auth.  Expect AuthenticationCleartextPassword (3) or similar.
    loop {
        let (ty, body) = read_msg(&mut s)?;
        match ty {
            b'R' => {
                let code = u32::from_be_bytes([body[0], body[1], body[2], body[3]]);
                if code == 0 { continue; } // AuthenticationOk
                if code == 3 {
                    // Cleartext password
                    let mut pw = pass.as_bytes().to_vec();
                    pw.push(0);
                    write_msg(&mut s, b'p', &pw)?;
                } else {
                    eprintln!("unsupported auth code {code} — try trust or SCRAM support");
                    std::process::exit(1);
                }
            }
            b'S' => { /* parameter */ }
            b'K' => { /* backend key */ }
            b'Z' => break, // ReadyForQuery
            b'E' => {
                eprintln!("startup err: {}", String::from_utf8_lossy(&body));
                std::process::exit(1);
            }
            _ => {}
        }
    }

    // Setup: drop + recreate table.
    let setup = "DROP TABLE IF EXISTS bench_mi; CREATE TABLE bench_mi (a BIGINT PRIMARY KEY, b INT, c INT, d TEXT)";
    for stmt in setup.split(';').filter(|s| !s.trim().is_empty()) {
        let mut q = stmt.as_bytes().to_vec();
        q.push(0);
        write_msg(&mut s, b'Q', &q)?;
        loop {
            let (ty, _body) = read_msg(&mut s)?;
            if ty == b'Z' { break; }
            if ty == b'E' { break; }
        }
    }

    // Two phases:
    //  (A) Isolated micro — one INSERT of N rows on a fresh table, per size.
    //  (B) Realistic loop — repeated 100-row batches accumulating 100K rows,
    //      showing how per-batch latency evolves as the table grows (PK
    //      index, WAL growth, DGVS pressure).
    let sizes = [1usize, 10, 50, 100, 200, 500, 1000];
    println!("--- (A) isolated: single N-row INSERT on fresh table ---");
    println!("batch_n  wallclock_ms  rows_per_sec");
    for &n in &sizes {
        let mut sql = String::with_capacity(64 + n * 20);
        sql.push_str("INSERT INTO bench_mi (a,b,c,d) VALUES ");
        for i in 0..n {
            if i > 0 { sql.push(','); }
            sql.push_str(&format!("({},1,0,'x')", 1_000_000 * (n as i64) + i as i64));
        }
        let mut q = sql.into_bytes();
        q.push(0);
        let t0 = Instant::now();
        write_msg(&mut s, b'Q', &q)?;
        let mut got_ready = false;
        while !got_ready {
            let (ty, body) = read_msg(&mut s)?;
            if ty == b'E' {
                eprintln!("INSERT err: {}", String::from_utf8_lossy(&body));
                break;
            }
            if ty == b'Z' { got_ready = true; }
        }
        let elapsed_ms = t0.elapsed().as_secs_f64() * 1000.0;
        let rps = n as f64 / (elapsed_ms / 1000.0);
        println!("{:>7}  {:>11.2}   {:>10.0}", n, elapsed_ms, rps);
    }

    // Fresh table for phase B.
    for stmt in ["DROP TABLE IF EXISTS bench_mi2", "CREATE TABLE bench_mi2 (a BIGINT PRIMARY KEY, b INT, c INT, d TEXT)"] {
        let mut q = stmt.as_bytes().to_vec();
        q.push(0);
        write_msg(&mut s, b'Q', &q)?;
        loop {
            let (ty, _body) = read_msg(&mut s)?;
            if ty == b'Z' || ty == b'E' { break; }
        }
    }

    println!("\n--- (B) realistic: 1000 × 100-row INSERT into same table ---");
    println!("batch_no  cum_rows  batch_ms  cum_ms  batch_rps");
    let n = 100usize;
    let total_batches = 1000usize;
    let t_start = Instant::now();
    let mut last_print = 0usize;
    for batch_i in 0..total_batches {
        let mut sql = String::with_capacity(64 + n * 20);
        sql.push_str("INSERT INTO bench_mi2 (a,b,c,d) VALUES ");
        for i in 0..n {
            if i > 0 { sql.push(','); }
            let id = (batch_i * n + i) as i64;
            sql.push_str(&format!("({},1,0,'x')", id));
        }
        let mut q = sql.into_bytes();
        q.push(0);
        let t_batch = Instant::now();
        write_msg(&mut s, b'Q', &q)?;
        let mut got_ready = false;
        while !got_ready {
            let (ty, body) = read_msg(&mut s)?;
            if ty == b'E' {
                eprintln!("INSERT err at batch {batch_i}: {}", String::from_utf8_lossy(&body));
                return Ok(());
            }
            if ty == b'Z' { got_ready = true; }
        }
        let elapsed_ms = t_batch.elapsed().as_secs_f64() * 1000.0;
        // Print every 50 batches, and first/last.
        if batch_i == 0 || batch_i - last_print >= 50 || batch_i == total_batches - 1 {
            let cum = (batch_i + 1) * n;
            let cum_ms = t_start.elapsed().as_secs_f64() * 1000.0;
            let rps = n as f64 / (elapsed_ms / 1000.0);
            println!("{:>8}  {:>8}  {:>8.1}  {:>6.0}  {:>8.0}",
                batch_i, cum, elapsed_ms, cum_ms, rps);
            last_print = batch_i;
        }
    }
    let total_ms = t_start.elapsed().as_secs_f64() * 1000.0;
    println!("TOTAL: {} rows in {:.1}s = {:.0} rows/sec",
        total_batches * n, total_ms / 1000.0,
        (total_batches * n) as f64 / (total_ms / 1000.0));
    Ok(())
}
