use crate::config::PoolMode;
use crate::pool::{ConnectionPool, PooledConnection};
use crate::stats::PoolStats;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::{debug, error, warn};

/// PWire protocol constants.
const HEADER_SIZE: usize = 5;

// Request message types
const MSG_QUERY: u8 = 0x01;
const MSG_PREPARE: u8 = 0x02;
const MSG_EXECUTE: u8 = 0x03;
const MSG_CLOSE: u8 = 0x04;
const MSG_PING: u8 = 0x05;
const MSG_AUTH: u8 = 0x06;
const MSG_QUIT: u8 = 0xFF;

// Response types
const RESP_ERROR: u8 = 0x03;
const RESP_PONG: u8 = 0x04;

/// Handle a single client connection.
pub async fn handle_client(
    mut client: TcpStream,
    pool: Arc<ConnectionPool>,
    pool_mode: PoolMode,
    stats: Arc<PoolStats>,
) {
    let peer = client
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|_| "unknown".into());
    debug!("new client connection from {}", peer);

    let _ = client.set_nodelay(true);

    match pool_mode {
        PoolMode::Transaction => {
            handle_transaction_mode(&mut client, &pool, &stats).await;
        }
        PoolMode::Session => {
            handle_session_mode(&mut client, &pool, &stats).await;
        }
    }

    debug!("client {} disconnected", peer);
}

/// Transaction-level pooling: acquire a connection per request/transaction,
/// then return it after the response.
async fn handle_transaction_mode(
    client: &mut TcpStream,
    pool: &Arc<ConnectionPool>,
    stats: &Arc<PoolStats>,
) {
    let mut in_transaction = false;
    let mut held_conn: Option<PooledConnection> = None;

    loop {
        // Read the next frame header from the client.
        let mut hdr = [0u8; HEADER_SIZE];
        if let Err(_) = client.read_exact(&mut hdr).await {
            // Client disconnected.
            if let Some(conn) = held_conn.take() {
                conn.discard();
            }
            return;
        }

        let msg_type = hdr[0];
        let payload_len = u32::from_le_bytes([hdr[1], hdr[2], hdr[3], hdr[4]]) as usize;

        // Read the payload.
        let mut payload = vec![0u8; payload_len];
        if payload_len > 0 {
            if let Err(_) = client.read_exact(&mut payload).await {
                if let Some(conn) = held_conn.take() {
                    conn.discard();
                }
                return;
            }
        }

        // Handle QUIT: close the client connection.
        if msg_type == MSG_QUIT {
            if let Some(conn) = held_conn.take() {
                conn.release().await;
            }
            return;
        }

        // Handle PING locally (no need for upstream).
        if msg_type == MSG_PING {
            let pong: [u8; 5] = [RESP_PONG, 0x00, 0x00, 0x00, 0x00];
            let _ = client.write_all(&pong).await;
            let _ = client.flush().await;
            continue;
        }

        // Handle SHOW STATS as a special admin command for QUERY messages.
        if msg_type == MSG_QUERY {
            let sql_text = std::str::from_utf8(&payload).unwrap_or("");
            if sql_text.trim().eq_ignore_ascii_case("SHOW STATS") {
                send_stats_response(client, stats).await;
                continue;
            }
        }

        // Detect transaction boundaries from QUERY payloads.
        let starts_transaction = if msg_type == MSG_QUERY {
            let sql = std::str::from_utf8(&payload)
                .unwrap_or("")
                .trim()
                .to_uppercase();
            sql.starts_with("BEGIN") || sql.starts_with("START TRANSACTION")
        } else {
            false
        };

        let ends_transaction = if msg_type == MSG_QUERY {
            let sql = std::str::from_utf8(&payload)
                .unwrap_or("")
                .trim()
                .to_uppercase();
            sql.starts_with("COMMIT") || sql.starts_with("ROLLBACK")
        } else {
            false
        };

        // Acquire a connection if we don't have one.
        if held_conn.is_none() {
            match pool.acquire().await {
                Ok(conn) => {
                    held_conn = Some(conn);
                }
                Err(e) => {
                    warn!("pool acquire failed: {}", e);
                    stats.total_errors.fetch_add(1, Ordering::Relaxed);
                    send_error(client, "08006", &format!("connection pool: {}", e)).await;
                    continue;
                }
            }
        }

        if starts_transaction {
            in_transaction = true;
            stats.total_transactions.fetch_add(1, Ordering::Relaxed);
        }

        stats.total_queries.fetch_add(1, Ordering::Relaxed);

        // Forward the full frame to upstream.
        let upstream = held_conn.as_mut().unwrap().stream();
        let mut frame = Vec::with_capacity(HEADER_SIZE + payload_len);
        frame.extend_from_slice(&hdr);
        frame.extend_from_slice(&payload);

        if let Err(e) = upstream.write_all(&frame).await {
            error!("upstream write error: {}", e);
            stats.total_errors.fetch_add(1, Ordering::Relaxed);
            held_conn.take().unwrap().discard();
            send_error(client, "08006", "upstream connection lost").await;
            in_transaction = false;
            continue;
        }
        if let Err(e) = upstream.flush().await {
            error!("upstream flush error: {}", e);
            stats.total_errors.fetch_add(1, Ordering::Relaxed);
            held_conn.take().unwrap().discard();
            send_error(client, "08006", "upstream connection lost").await;
            in_transaction = false;
            continue;
        }

        // Read the response header from upstream.
        let mut resp_hdr = [0u8; HEADER_SIZE];
        if let Err(e) = upstream.read_exact(&mut resp_hdr).await {
            error!("upstream read error: {}", e);
            stats.total_errors.fetch_add(1, Ordering::Relaxed);
            held_conn.take().unwrap().discard();
            send_error(client, "08006", "upstream connection lost").await;
            in_transaction = false;
            continue;
        }

        let resp_payload_len =
            u32::from_le_bytes([resp_hdr[1], resp_hdr[2], resp_hdr[3], resp_hdr[4]]) as usize;

        let mut resp_payload = vec![0u8; resp_payload_len];
        if resp_payload_len > 0 {
            if let Err(e) = upstream.read_exact(&mut resp_payload).await {
                error!("upstream read payload error: {}", e);
                stats.total_errors.fetch_add(1, Ordering::Relaxed);
                held_conn.take().unwrap().discard();
                send_error(client, "08006", "upstream connection lost").await;
                in_transaction = false;
                continue;
            }
        }

        if resp_hdr[0] == RESP_ERROR {
            stats.total_errors.fetch_add(1, Ordering::Relaxed);
        }

        // Forward the response to the client.
        let mut resp_frame = Vec::with_capacity(HEADER_SIZE + resp_payload_len);
        resp_frame.extend_from_slice(&resp_hdr);
        resp_frame.extend_from_slice(&resp_payload);
        if let Err(_) = client.write_all(&resp_frame).await {
            if let Some(conn) = held_conn.take() {
                conn.discard();
            }
            return;
        }
        let _ = client.flush().await;

        // Decide whether to release the connection back to the pool.
        if ends_transaction {
            in_transaction = false;
        }

        if !in_transaction {
            if let Some(conn) = held_conn.take() {
                conn.release().await;
            }
        }
    }
}

/// Session-level pooling: acquire one connection for the entire session.
async fn handle_session_mode(
    client: &mut TcpStream,
    pool: &Arc<ConnectionPool>,
    stats: &Arc<PoolStats>,
) {
    // First check for SHOW STATS or PING before acquiring upstream.
    // We need to peek at the first message. For session mode, acquire immediately.
    let mut upstream = match pool.acquire().await {
        Ok(conn) => conn,
        Err(e) => {
            warn!("pool acquire failed for session: {}", e);
            stats.total_errors.fetch_add(1, Ordering::Relaxed);
            send_error(client, "08006", &format!("connection pool: {}", e)).await;
            return;
        }
    };

    loop {
        // Read client frame.
        let mut hdr = [0u8; HEADER_SIZE];
        if let Err(_) = client.read_exact(&mut hdr).await {
            upstream.release().await;
            return;
        }

        let msg_type = hdr[0];
        let payload_len = u32::from_le_bytes([hdr[1], hdr[2], hdr[3], hdr[4]]) as usize;

        let mut payload = vec![0u8; payload_len];
        if payload_len > 0 {
            if let Err(_) = client.read_exact(&mut payload).await {
                upstream.discard();
                return;
            }
        }

        if msg_type == MSG_QUIT {
            upstream.release().await;
            return;
        }

        // Handle PING locally.
        if msg_type == MSG_PING {
            let pong: [u8; 5] = [RESP_PONG, 0x00, 0x00, 0x00, 0x00];
            let _ = client.write_all(&pong).await;
            let _ = client.flush().await;
            continue;
        }

        // Handle SHOW STATS.
        if msg_type == MSG_QUERY {
            let sql_text = std::str::from_utf8(&payload).unwrap_or("");
            if sql_text.trim().eq_ignore_ascii_case("SHOW STATS") {
                send_stats_response(client, stats).await;
                continue;
            }
        }

        stats.total_queries.fetch_add(1, Ordering::Relaxed);

        // Forward to upstream.
        let us = upstream.stream();
        let mut frame = Vec::with_capacity(HEADER_SIZE + payload_len);
        frame.extend_from_slice(&hdr);
        frame.extend_from_slice(&payload);

        if let Err(e) = us.write_all(&frame).await {
            error!("upstream write error: {}", e);
            stats.total_errors.fetch_add(1, Ordering::Relaxed);
            upstream.discard();
            send_error(client, "08006", "upstream connection lost").await;
            return;
        }
        let _ = us.flush().await;

        // Read response.
        let mut resp_hdr = [0u8; HEADER_SIZE];
        if let Err(e) = us.read_exact(&mut resp_hdr).await {
            error!("upstream read error: {}", e);
            stats.total_errors.fetch_add(1, Ordering::Relaxed);
            upstream.discard();
            send_error(client, "08006", "upstream connection lost").await;
            return;
        }

        let resp_payload_len =
            u32::from_le_bytes([resp_hdr[1], resp_hdr[2], resp_hdr[3], resp_hdr[4]]) as usize;
        let mut resp_payload = vec![0u8; resp_payload_len];
        if resp_payload_len > 0 {
            if let Err(e) = us.read_exact(&mut resp_payload).await {
                error!("upstream read error: {}", e);
                stats.total_errors.fetch_add(1, Ordering::Relaxed);
                upstream.discard();
                send_error(client, "08006", "upstream connection lost").await;
                return;
            }
        }

        if resp_hdr[0] == RESP_ERROR {
            stats.total_errors.fetch_add(1, Ordering::Relaxed);
        }

        let mut resp_frame = Vec::with_capacity(HEADER_SIZE + resp_payload_len);
        resp_frame.extend_from_slice(&resp_hdr);
        resp_frame.extend_from_slice(&resp_payload);
        if let Err(_) = client.write_all(&resp_frame).await {
            upstream.discard();
            return;
        }
        let _ = client.flush().await;
    }
}

/// Send a PWire ERROR response to the client.
async fn send_error(client: &mut TcpStream, sqlstate: &str, message: &str) {
    let msg_bytes = message.as_bytes();
    let msg_len = msg_bytes.len().min(u16::MAX as usize) as u16;
    let mut payload = Vec::with_capacity(5 + 2 + msg_len as usize);

    // SQLSTATE: exactly 5 bytes, pad with spaces.
    let ss = sqlstate.as_bytes();
    for i in 0..5 {
        payload.push(if i < ss.len() { ss[i] } else { b' ' });
    }
    payload.extend_from_slice(&msg_len.to_le_bytes());
    payload.extend_from_slice(&msg_bytes[..msg_len as usize]);

    let total_len = payload.len() as u32;
    let mut frame = Vec::with_capacity(HEADER_SIZE + payload.len());
    frame.push(RESP_ERROR);
    frame.extend_from_slice(&total_len.to_le_bytes());
    frame.extend_from_slice(&payload);

    let _ = client.write_all(&frame).await;
    let _ = client.flush().await;
}

/// Send pool stats as a JSON-formatted PWire OK response.
async fn send_stats_response(client: &mut TcpStream, stats: &Arc<PoolStats>) {
    let snapshot = stats.snapshot();
    let json = serde_json::to_string_pretty(&snapshot).unwrap_or_else(|_| "{}".into());
    let json_bytes = json.as_bytes();

    // Encode as an OK response with rows_affected=0 and tag=json.
    // Since the tag is limited to 255 bytes in normal OK, we send the JSON as
    // a special text-based response. We use the same OK encoding but with
    // the tag containing the JSON (truncated to 255 for protocol compat).
    // Better approach: send it as a RESULT_SET with one column "stats" and one row.
    //
    // Build a RESULT_SET with a single TEXT column "stats" and one row.
    let col_count: u16 = 1;
    let col_name = b"stats";
    let col_name_len: u8 = col_name.len() as u8;
    let type_text: u8 = 3; // TYPE_TEXT
    let row_count: u32 = 1;

    let json_len = json_bytes.len().min(u16::MAX as usize) as u16;
    let _null_bitmap_len = 1usize; // (1 + 7) / 8

    let mut payload = Vec::new();
    // Column count
    payload.extend_from_slice(&col_count.to_le_bytes());
    // Column def: name_len, name, type_tag
    payload.push(col_name_len);
    payload.extend_from_slice(col_name);
    payload.push(type_text);
    // Row count
    payload.extend_from_slice(&row_count.to_le_bytes());
    // Row: null bitmap (no nulls)
    payload.push(0x00);
    // Value: len u16 LE + bytes
    payload.extend_from_slice(&json_len.to_le_bytes());
    payload.extend_from_slice(&json_bytes[..json_len as usize]);

    let resp_type: u8 = 0x01; // RESP_RESULT_SET
    let total_len = payload.len() as u32;
    let mut frame = Vec::with_capacity(HEADER_SIZE + payload.len());
    frame.push(resp_type);
    frame.extend_from_slice(&total_len.to_le_bytes());
    frame.extend_from_slice(&payload);

    let _ = client.write_all(&frame).await;
    let _ = client.flush().await;
}
