//! Minimal smoke test for MuxConnection: connect, PREPARE, one EXECUTE, exit.

use std::time::Instant;
use pyrosql::mux::MuxConnection;

const MSG_PREPARE: u8 = 0x02;
const MSG_EXECUTE: u8 = 0x03;

fn main() {
    eprintln!("[smoke] connecting...");
    let conn = MuxConnection::connect("127.0.0.1:12520").expect("connect");
    eprintln!("[smoke] connected, preparing...");
    let sql = "SELECT abalance FROM pgbench_accounts WHERE aid = $1";
    let mut frame = Vec::with_capacity(5 + sql.len());
    frame.push(MSG_PREPARE);
    frame.extend_from_slice(&(sql.len() as u32).to_le_bytes());
    frame.extend_from_slice(sql.as_bytes());

    let t0 = Instant::now();
    let fut = conn.submit(frame);
    eprintln!("[smoke] submitted PREPARE, awaiting...");
    let res = futures::executor::block_on(fut);
    eprintln!("[smoke] PREPARE result in {:?}: {:?}", t0.elapsed(), res.as_ref().map(|(t, p)| (t, p.len())));
    let (ty, payload) = res.expect("PREPARE");
    assert_eq!(ty, 0x02);
    let handle = u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]);
    eprintln!("[smoke] handle = {handle}");

    // One EXECUTE
    let a = "42".to_string();
    let pl: u32 = 4 + 2 + 4 + a.len() as u32;
    let mut exec = Vec::with_capacity(5 + pl as usize);
    exec.push(MSG_EXECUTE);
    exec.extend_from_slice(&pl.to_le_bytes());
    exec.extend_from_slice(&handle.to_le_bytes());
    exec.extend_from_slice(&1u16.to_le_bytes());
    exec.extend_from_slice(&(a.len() as u32).to_le_bytes());
    exec.extend_from_slice(a.as_bytes());
    let t1 = Instant::now();
    let r = futures::executor::block_on(conn.submit(exec)).expect("EXECUTE");
    eprintln!("[smoke] EXECUTE in {:?}: ty=0x{:02x} len={}", t1.elapsed(), r.0, r.1.len());
    eprintln!("[smoke] done.");
}
