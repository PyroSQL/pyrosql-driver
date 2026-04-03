//! End-to-end integration test: TCP server ↔ client with real LiveSync flow.
//!
//! 1. Starts a mock RMP server on a random port
//! 2. Client connects via TCP
//! 3. Client subscribes → receives snapshot → mirror populated
//! 4. Client sends MUTATE (insert) → server processes → pushes Delta → mirror updated
//! 5. Client sends MUTATE (update) → mirror reflects change
//! 6. Client sends MUTATE (delete) → mirror removes row
//! 7. Client unsubscribes → clean shutdown

use bytes::{BufMut, BytesMut};
use dashmap::DashMap;
use pyrosql_rmp::protocol::*;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

// ── Mock RMP Server ─────────────────────────────────────────────────────────

struct MockDataSource {
    tables: DashMap<String, DashMap<Vec<u8>, Vec<u8>>>,
    versions: DashMap<String, AtomicU64>,
}

impl MockDataSource {
    fn new() -> Self {
        Self {
            tables: DashMap::new(),
            versions: DashMap::new(),
        }
    }

    fn create_table(&self, name: &str, rows: Vec<(Vec<u8>, Vec<u8>)>) {
        let map = DashMap::new();
        for (pk, row) in rows {
            map.insert(pk, row);
        }
        self.tables.insert(name.to_string(), map);
        self.versions
            .insert(name.to_string(), AtomicU64::new(1));
    }

    fn get_snapshot(&self, table: &str) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.tables
            .get(table)
            .map(|t| t.iter().map(|e| (e.key().clone(), e.value().clone())).collect())
            .unwrap_or_default()
    }

    fn get_version(&self, table: &str) -> u64 {
        self.versions
            .get(table)
            .map(|v| v.load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    fn apply_mutation(&self, table: &str, op: DeltaOp, pk: &[u8], row: Option<&[u8]>) -> u64 {
        let tbl = self.tables.entry(table.to_string()).or_insert_with(DashMap::new);
        match op {
            DeltaOp::Insert | DeltaOp::Update => {
                if let Some(data) = row {
                    tbl.insert(pk.to_vec(), data.to_vec());
                }
            }
            DeltaOp::Delete => {
                tbl.remove(pk);
            }
        }
        let ver = self
            .versions
            .entry(table.to_string())
            .or_insert_with(|| AtomicU64::new(1));
        ver.fetch_add(1, Ordering::Relaxed) + 1
    }
}

/// Subscription tracked by the server per connection.
struct ServerSub {
    sub_id: u64,
    table: String,
}

/// Read exactly one TLV frame from the stream.
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

/// Run the mock server: accepts one connection and processes messages.
async fn run_mock_server(
    listener: TcpListener,
    data_source: Arc<MockDataSource>,
    ready_tx: mpsc::Sender<()>,
) {
    ready_tx.send(()).await.unwrap();

    let (mut stream, _addr) = listener.accept().await.unwrap();
    let mut subs: Vec<ServerSub> = Vec::new();

    loop {
        let frame = match read_frame(&mut stream).await {
            Some(f) => f,
            None => break, // client disconnected
        };

        let msg = match decode_message(&frame) {
            Ok(m) => m,
            Err(_) => break,
        };

        match msg {
            Message::Subscribe(sub) => {
                let rows = data_source.get_snapshot(&sub.table);
                let version = data_source.get_version(&sub.table);

                let snapshot = Snapshot {
                    sub_id: sub.sub_id,
                    version,
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
                };

                let encoded = encode_message(&Message::Snapshot(snapshot));
                stream.write_all(&encoded).await.unwrap();

                subs.push(ServerSub {
                    sub_id: sub.sub_id,
                    table: sub.table,
                });
            }
            Message::Mutate(mutate) => {
                let new_version =
                    data_source.apply_mutation(&mutate.table, mutate.op, &mutate.pk, mutate.row.as_deref());

                // Push deltas to all subscriptions on this table
                for sub in &subs {
                    if sub.table == mutate.table {
                        let delta = Delta {
                            sub_id: sub.sub_id,
                            version: new_version,
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
            }
            Message::Unsubscribe(unsub) => {
                subs.retain(|s| s.sub_id != unsub.sub_id);
                // If no more subscriptions, we could close but let's keep listening
            }
            _ => {}
        }
    }
}

// ── Client helpers ──────────────────────────────────────────────────────────

/// A simple TCP-based RMP client for testing.
struct TestClient {
    stream: TcpStream,
    mirrors: DashMap<u64, Arc<pyrosql_rmp::mirror::TableMirror>>,
    next_sub_id: AtomicU64,
}

impl TestClient {
    async fn connect(addr: &str) -> Self {
        let stream = TcpStream::connect(addr).await.unwrap();
        Self {
            stream,
            mirrors: DashMap::new(),
            next_sub_id: AtomicU64::new(1),
        }
    }

    async fn subscribe(&mut self, table: &str) -> Arc<pyrosql_rmp::mirror::TableMirror> {
        let sub_id = self.next_sub_id.fetch_add(1, Ordering::Relaxed);

        // Send Subscribe frame
        let msg = Message::Subscribe(Subscribe {
            sub_id,
            table: table.to_string(),
            predicate: Predicate::All,
        });
        let encoded = encode_message(&msg);
        self.stream.write_all(&encoded).await.unwrap();

        // Read Snapshot response
        let frame = read_frame(&mut self.stream).await.expect("snapshot frame");
        let resp = decode_message(&frame).expect("decode snapshot");
        let snapshot = match resp {
            Message::Snapshot(s) => s,
            other => panic!("expected Snapshot, got {:?}", other),
        };

        assert_eq!(snapshot.sub_id, sub_id);

        let mirror = Arc::new(pyrosql_rmp::mirror::TableMirror::new(sub_id));
        mirror.load_snapshot(snapshot);

        self.mirrors.insert(sub_id, Arc::clone(&mirror));
        mirror
    }

    async fn mutate(&mut self, table: &str, op: DeltaOp, pk: &[u8], row: Option<&[u8]>) {
        let msg = Message::Mutate(Mutate {
            table: table.to_string(),
            op,
            pk: pk.to_vec(),
            row: row.map(|r| r.to_vec()),
        });
        let encoded = encode_message(&msg);
        self.stream.write_all(&encoded).await.unwrap();

        // Read the Delta response pushed by the server
        let frame = read_frame(&mut self.stream).await.expect("delta frame");
        let resp = decode_message(&frame).expect("decode delta");
        let delta = match resp {
            Message::Delta(d) => d,
            other => panic!("expected Delta, got {:?}", other),
        };

        // Apply delta to the mirror
        if let Some(mirror) = self.mirrors.get(&delta.sub_id) {
            mirror.apply_delta(&delta);
        }
    }

    async fn unsubscribe(&mut self, sub_id: u64) {
        let msg = Message::Unsubscribe(Unsubscribe { sub_id });
        let encoded = encode_message(&msg);
        self.stream.write_all(&encoded).await.unwrap();
        self.mirrors.remove(&sub_id);
    }
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[tokio::test]
async fn e2e_subscribe_snapshot_mutate_delta() {
    // 1. Setup: create mock data source with 100 rows
    let data_source = Arc::new(MockDataSource::new());
    let rows: Vec<(Vec<u8>, Vec<u8>)> = (0u64..100)
        .map(|i| (i.to_le_bytes().to_vec(), format!("user_{i}").into_bytes()))
        .collect();
    data_source.create_table("users", rows);

    // 2. Start server on random port
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let (ready_tx, mut ready_rx) = mpsc::channel(1);

    let ds = Arc::clone(&data_source);
    tokio::spawn(async move {
        run_mock_server(listener, ds, ready_tx).await;
    });
    ready_rx.recv().await.unwrap();

    // 3. Client connects
    let mut client = TestClient::connect(&addr).await;

    // 4. Subscribe → verify snapshot has 100 rows
    let mirror = client.subscribe("users").await;
    assert_eq!(mirror.len(), 100);

    // Verify specific rows
    let pk0 = 0u64.to_le_bytes().to_vec();
    assert_eq!(mirror.get(&pk0).unwrap().as_slice(), b"user_0");
    let pk99 = 99u64.to_le_bytes().to_vec();
    assert_eq!(mirror.get(&pk99).unwrap().as_slice(), b"user_99");

    // 5. MUTATE insert → mirror has 101 rows
    let pk100 = 100u64.to_le_bytes().to_vec();
    client
        .mutate("users", DeltaOp::Insert, &pk100, Some(b"user_100"))
        .await;
    assert_eq!(mirror.len(), 101);
    assert_eq!(mirror.get(&pk100).unwrap().as_slice(), b"user_100");

    // 6. MUTATE update → mirror reflects change
    let pk50 = 50u64.to_le_bytes().to_vec();
    client
        .mutate("users", DeltaOp::Update, &pk50, Some(b"user_50_UPDATED"))
        .await;
    assert_eq!(mirror.len(), 101);
    assert_eq!(mirror.get(&pk50).unwrap().as_slice(), b"user_50_UPDATED");

    // 7. MUTATE delete → mirror has 100 rows
    let pk25 = 25u64.to_le_bytes().to_vec();
    client.mutate("users", DeltaOp::Delete, &pk25, None).await;
    assert_eq!(mirror.len(), 100);
    assert!(mirror.get(&pk25).is_none());

    // 8. Unsubscribe
    let sub_id = mirror.sub_id();
    client.unsubscribe(sub_id).await;
    assert!(client.mirrors.get(&sub_id).is_none());

    // 9. Verify server data source is consistent
    let server_rows = data_source.get_snapshot("users");
    assert_eq!(server_rows.len(), 100); // 100 original + 1 insert - 1 delete = 100
}

#[tokio::test]
async fn e2e_mirror_read_throughput() {
    // Setup server with 1000 rows
    let data_source = Arc::new(MockDataSource::new());
    let rows: Vec<(Vec<u8>, Vec<u8>)> = (0u64..1000)
        .map(|i| (i.to_le_bytes().to_vec(), format!("row_{i:04}").into_bytes()))
        .collect();
    data_source.create_table("bench", rows);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let (ready_tx, mut ready_rx) = mpsc::channel(1);

    let ds = Arc::clone(&data_source);
    tokio::spawn(async move {
        run_mock_server(listener, ds, ready_tx).await;
    });
    ready_rx.recv().await.unwrap();

    let mut client = TestClient::connect(&addr).await;
    let mirror = client.subscribe("bench").await;
    assert_eq!(mirror.len(), 1000);

    // Benchmark: 4 threads reading from mirror for 2 seconds
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let total = Arc::new(AtomicU64::new(0));

    let mut handles = Vec::new();
    for _ in 0..4 {
        let mirror = Arc::clone(&mirror);
        let stop = Arc::clone(&stop);
        let total = Arc::clone(&total);
        handles.push(std::thread::spawn(move || {
            let mut rng_state: u64 = 42;
            let mut count = 0u64;
            while !stop.load(Ordering::Relaxed) {
                // Simple xorshift for speed (avoid rand dependency in test)
                rng_state ^= rng_state << 13;
                rng_state ^= rng_state >> 7;
                rng_state ^= rng_state << 17;
                let id = (rng_state % 1000).to_le_bytes().to_vec();
                let _ = mirror.get(&id);
                count += 1;
            }
            total.fetch_add(count, Ordering::Relaxed);
        }));
    }

    std::thread::sleep(std::time::Duration::from_secs(2));
    stop.store(true, Ordering::Relaxed);
    for h in handles {
        h.join().unwrap();
    }

    let reads = total.load(Ordering::Relaxed);
    let qps = reads / 2;
    let ns_per_read = if qps > 0 { 1_000_000_000 / qps } else { 0 };

    eprintln!("LiveSync mirror read throughput: {} reads/sec ({} ns/read)", qps, ns_per_read);

    // Should be at least 1M reads/sec (usually 10M+)
    assert!(qps > 1_000_000, "expected >1M reads/sec, got {}", qps);
}
