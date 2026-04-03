//! Realistic LiveSync benchmark — simulates a web application with related tables.
//!
//! Schema (e-commerce):
//!   users       (100K rows)  — id, name, email, city, score
//!   orders      (500K rows)  — id, user_id FK→users, total, status, created_at
//!   products    (10K rows)   — id, name, price, category
//!   order_items (1.5M rows)  — id, order_id FK→orders, product_id FK→products, qty, price
//!
//! Workloads:
//!   1. "User Profile" — load user + last 10 orders + products from those orders
//!      Traditional: 3 queries (user, orders, products) = 3 round trips
//!      LiveSync: db.live("users", pk=X, depth=2) = 0 round trips after subscribe
//!
//!   2. "Dashboard" — user + order count + total revenue
//!      Traditional: 2 queries = 2 round trips
//!      LiveSync: read from mirror = 0 round trips
//!
//!   3. "Product page" — product + recent order_items referencing it
//!      Traditional: 2 queries = 2 round trips
//!      LiveSync: mirror read = 0 round trips
//!
//!   4. "Burst reads" — 1000 users load their profiles concurrently
//!      Traditional: 3000 queries
//!      LiveSync: 3000 mirror reads (~0 latency)

use dashmap::DashMap;
use pyrosql_rmp::mirror::TableMirror;
use pyrosql_rmp::protocol::*;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

// ── Schema constants ────────────────────────────────────────────────────────

const NUM_USERS: u64 = 100_000;
const NUM_ORDERS: u64 = 500_000;     // ~5 orders per user
const NUM_PRODUCTS: u64 = 10_000;
const NUM_ORDER_ITEMS: u64 = 1_500_000; // ~3 items per order

const ROW_SIZE: usize = 128;

// ── Data generation ─────────────────────────────────────────────────────────

fn make_user(id: u64) -> Vec<u8> {
    let mut v = Vec::with_capacity(ROW_SIZE);
    v.extend_from_slice(&id.to_le_bytes());                    // id
    v.extend_from_slice(format!("user_{id:06}").as_bytes());   // name (11 bytes)
    v.extend_from_slice(format!("u{id}@mail.com").as_bytes()); // email
    v.resize(ROW_SIZE, 0);
    v
}

fn make_order(id: u64, user_id: u64) -> Vec<u8> {
    let mut v = Vec::with_capacity(ROW_SIZE);
    v.extend_from_slice(&id.to_le_bytes());        // id
    v.extend_from_slice(&user_id.to_le_bytes());   // user_id FK
    v.extend_from_slice(&(id as f64 * 9.99).to_le_bytes()); // total
    v.resize(ROW_SIZE, 0);
    v
}

fn make_product(id: u64) -> Vec<u8> {
    let mut v = Vec::with_capacity(ROW_SIZE);
    v.extend_from_slice(&id.to_le_bytes());
    v.extend_from_slice(format!("product_{id:05}").as_bytes());
    v.extend_from_slice(&((id % 100) as f64 * 19.99).to_le_bytes());
    v.resize(ROW_SIZE, 0);
    v
}

fn make_order_item(id: u64, order_id: u64, product_id: u64) -> Vec<u8> {
    let mut v = Vec::with_capacity(ROW_SIZE);
    v.extend_from_slice(&id.to_le_bytes());
    v.extend_from_slice(&order_id.to_le_bytes());    // FK → orders
    v.extend_from_slice(&product_id.to_le_bytes());  // FK → products
    v.extend_from_slice(&((id % 5 + 1) as u32).to_le_bytes()); // qty
    v.resize(ROW_SIZE, 0);
    v
}

// ── Mock server with multiple tables ────────────────────────────────────────

struct MultiTableServer {
    tables: DashMap<String, Arc<DashMap<Vec<u8>, Vec<u8>>>>,
    versions: DashMap<String, AtomicU64>,
}

impl MultiTableServer {
    fn new() -> Self {
        Self {
            tables: DashMap::new(),
            versions: DashMap::new(),
        }
    }

    fn add_table(&self, name: &str) -> Arc<DashMap<Vec<u8>, Vec<u8>>> {
        let map = Arc::new(DashMap::new());
        self.tables.insert(name.to_string(), Arc::clone(&map));
        self.versions.insert(name.to_string(), AtomicU64::new(1));
        map
    }

    fn get_rows(&self, table: &str) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.tables.get(table)
            .map(|t| t.iter().map(|e| (e.key().clone(), e.value().clone())).collect())
            .unwrap_or_default()
    }

    fn get_rows_filtered(&self, table: &str, predicate: &Predicate) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.tables.get(table)
            .map(|t| t.iter()
                .filter(|e| match predicate {
                    Predicate::All => true,
                    Predicate::Eq { column: _, value } => {
                        // For FK lookups: check if the FK bytes in the row match
                        // FK is at bytes 8..16 (after the id field)
                        e.value().len() >= 16 && &e.value()[8..16] == value.as_slice()
                    }
                    _ => true,
                })
                .map(|e| (e.key().clone(), e.value().clone()))
                .collect())
            .unwrap_or_default()
    }

    fn version(&self, table: &str) -> u64 {
        self.versions.get(table).map(|v| v.load(Ordering::Relaxed)).unwrap_or(0)
    }
}

async fn read_frame(stream: &mut (impl AsyncReadExt + Unpin)) -> Option<Vec<u8>> {
    let mut header = [0u8; 5];
    if stream.read_exact(&mut header).await.is_err() { return None; }
    let len = u32::from_le_bytes([header[1], header[2], header[3], header[4]]) as usize;
    let mut frame = vec![0u8; 5 + len];
    frame[..5].copy_from_slice(&header);
    if len > 0 {
        if stream.read_exact(&mut frame[5..]).await.is_err() { return None; }
    }
    Some(frame)
}

async fn run_multi_server(listener: TcpListener, server: Arc<MultiTableServer>) {
    let (mut stream, _) = listener.accept().await.unwrap();
    let mut subs: Vec<(u64, String)> = Vec::new();

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
                let rows = match &sub.predicate {
                    Predicate::All => server.get_rows(&sub.table),
                    pred => server.get_rows_filtered(&sub.table, pred),
                };
                let snap = Snapshot {
                    sub_id: sub.sub_id,
                    version: server.version(&sub.table),
                    columns: vec![
                        ColumnInfo { name: "id".into(), type_tag: ColumnType::Int64 },
                        ColumnInfo { name: "data".into(), type_tag: ColumnType::Bytes },
                    ],
                    rows,
                };
                let encoded = encode_message(&Message::Snapshot(snap));
                stream.write_all(&encoded).await.unwrap();
                subs.push((sub.sub_id, sub.table));
            }
            Message::Mutate(mutate) => {
                if let Some(tbl) = server.tables.get(&mutate.table) {
                    match mutate.op {
                        DeltaOp::Insert | DeltaOp::Update => {
                            if let Some(row) = &mutate.row {
                                tbl.insert(mutate.pk.clone(), row.clone());
                            }
                        }
                        DeltaOp::Delete => { tbl.remove(&mutate.pk); }
                    }
                }
                for (sub_id, table) in &subs {
                    if *table == mutate.table {
                        let delta = Delta {
                            sub_id: *sub_id,
                            version: server.version(&mutate.table) + 1,
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
                subs.retain(|(id, _)| *id != unsub.sub_id);
            }
            _ => {}
        }
    }
}

// ── Benchmark helpers ───────────────────────────────────────────────────────

struct BenchClient {
    stream: TcpStream,
    mirrors: DashMap<u64, Arc<TableMirror>>,
    next_sub: AtomicU64,
}

impl BenchClient {
    async fn connect(addr: &str) -> Self {
        Self {
            stream: TcpStream::connect(addr).await.unwrap(),
            mirrors: DashMap::new(),
            next_sub: AtomicU64::new(1),
        }
    }

    async fn subscribe(&mut self, table: &str, predicate: Predicate) -> (u64, Arc<TableMirror>) {
        let sub_id = self.next_sub.fetch_add(1, Ordering::Relaxed);
        let msg = encode_message(&Message::Subscribe(Subscribe {
            sub_id, table: table.to_string(), predicate,
        }));
        self.stream.write_all(&msg).await.unwrap();
        let frame = read_frame(&mut self.stream).await.unwrap();
        let snap = match decode_message(&frame).unwrap() {
            Message::Snapshot(s) => s,
            _ => panic!("expected snapshot"),
        };
        let mirror = Arc::new(TableMirror::new(sub_id));
        mirror.load_snapshot(snap);
        self.mirrors.insert(sub_id, Arc::clone(&mirror));
        (sub_id, mirror)
    }

    async fn mutate_and_apply(&mut self, table: &str, op: DeltaOp, pk: &[u8], row: Option<&[u8]>) {
        let msg = encode_message(&Message::Mutate(Mutate {
            table: table.to_string(), op, pk: pk.to_vec(), row: row.map(|r| r.to_vec()),
        }));
        self.stream.write_all(&msg).await.unwrap();
        let frame = read_frame(&mut self.stream).await.unwrap();
        if let Message::Delta(delta) = decode_message(&frame).unwrap() {
            if let Some(m) = self.mirrors.get(&delta.sub_id) {
                m.apply_delta(&delta);
            }
        }
    }
}

// ── Simulated "traditional" approach: N separate lookups ────────────────────

fn simulate_traditional_lookups(
    users: &DashMap<Vec<u8>, Vec<u8>>,
    orders: &DashMap<Vec<u8>, Vec<u8>>,
    products: &DashMap<Vec<u8>, Vec<u8>>,
    user_id: u64,
) -> usize {
    // Query 1: get user
    let user_pk = user_id.to_le_bytes().to_vec();
    let _user = users.get(&user_pk);

    // Query 2: get orders for user (scan, filter by user_id at bytes 8..16)
    let user_id_bytes = user_id.to_le_bytes();
    let user_orders: Vec<_> = orders.iter()
        .filter(|e| e.value().len() >= 16 && &e.value()[8..16] == user_id_bytes.as_slice())
        .take(10)
        .map(|e| e.value().clone())
        .collect();

    // Query 3: get products from order items (simulate via product_id lookup)
    let mut product_count = 0;
    for order in &user_orders {
        let product_id = u64::from_le_bytes(order[16..24].try_into().unwrap_or([0; 8])) % NUM_PRODUCTS;
        let _product = products.get(&product_id.to_le_bytes().to_vec());
        product_count += 1;
    }

    3 + product_count // total "queries" simulated
}

// ── Main ────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    let threads: usize = std::env::args().nth(1).and_then(|s| s.parse().ok()).unwrap_or(4);
    let duration: u64 = std::env::args().nth(2).and_then(|s| s.parse().ok()).unwrap_or(5);

    println!("============================================================");
    println!("  PyroSQL LiveSync — Realistic E-Commerce Benchmark");
    println!("  {} threads | {}s per test", threads, duration);
    println!("============================================================");
    println!();

    // ── Generate data ────────────────────────────────────────────────────
    print!("Generating data... ");
    let gen_start = Instant::now();

    let server = Arc::new(MultiTableServer::new());

    let users_map = server.add_table("users");
    for i in 0..NUM_USERS {
        users_map.insert(i.to_le_bytes().to_vec(), make_user(i));
    }

    let orders_map = server.add_table("orders");
    for i in 0..NUM_ORDERS {
        let user_id = i % NUM_USERS; // ~5 orders per user
        orders_map.insert(i.to_le_bytes().to_vec(), make_order(i, user_id));
    }

    let products_map = server.add_table("products");
    for i in 0..NUM_PRODUCTS {
        products_map.insert(i.to_le_bytes().to_vec(), make_product(i));
    }

    let items_map = server.add_table("order_items");
    for i in 0..NUM_ORDER_ITEMS {
        let order_id = i % NUM_ORDERS;
        let product_id = i % NUM_PRODUCTS;
        items_map.insert(i.to_le_bytes().to_vec(), make_order_item(i, order_id, product_id));
    }

    println!("{:.1}s", gen_start.elapsed().as_secs_f64());
    println!("  users:       {:>10}", NUM_USERS);
    println!("  orders:      {:>10}", NUM_ORDERS);
    println!("  products:    {:>10}", NUM_PRODUCTS);
    println!("  order_items: {:>10}", NUM_ORDER_ITEMS);
    println!("  total rows:  {:>10}", NUM_USERS + NUM_ORDERS + NUM_PRODUCTS + NUM_ORDER_ITEMS);
    println!();

    // ── Start server ─────────────────────────────────────────────────────
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let srv = Arc::clone(&server);
    tokio::spawn(async move { run_multi_server(listener, srv).await; });
    tokio::time::sleep(Duration::from_millis(50)).await;

    // ── Workload 1: "User Profile" via LiveSync ──────────────────────────
    println!("─── Workload 1: User Profile Load ──────────────────────────");
    println!("  Pattern: user + their orders (FK walk)");
    println!();

    let mut client = BenchClient::connect(&addr).await;

    // Subscribe to a specific user
    let test_user_id: u64 = 42;
    let sub_start = Instant::now();
    let (_uid, user_mirror) = client.subscribe("users", Predicate::Eq {
        column: "id".into(),
        value: test_user_id.to_le_bytes().to_vec(),
    }).await;
    let sub_user_ns = sub_start.elapsed().as_nanos();

    // Subscribe to orders for that user (simulates FK depth=1)
    let sub_start = Instant::now();
    let (_oid, orders_mirror) = client.subscribe("orders", Predicate::Eq {
        column: "user_id".into(),
        value: test_user_id.to_le_bytes().to_vec(),
    }).await;
    let sub_orders_ns = sub_start.elapsed().as_nanos();

    println!("  Subscribe user:     {:>8}µs  ({} rows)", sub_user_ns / 1000, user_mirror.len());
    println!("  Subscribe orders:   {:>8}µs  ({} rows)", sub_orders_ns / 1000, orders_mirror.len());
    println!("  Total subscribe:    {:>8}µs", (sub_user_ns + sub_orders_ns) / 1000);
    println!();

    // Now benchmark reading from mirror vs "traditional" lookups
    println!("  Reading from mirror (local, no network):");
    let stop = Arc::new(AtomicBool::new(false));
    let total_reads = Arc::new(AtomicU64::new(0));

    let mut handles = Vec::new();
    for t in 0..threads {
        let um = Arc::clone(&user_mirror);
        let om = Arc::clone(&orders_mirror);
        let stop = Arc::clone(&stop);
        let total = Arc::clone(&total_reads);
        handles.push(std::thread::spawn(move || {
            let mut rng: u64 = 42 + t as u64;
            let mut count = 0u64;
            let user_pk = test_user_id.to_le_bytes().to_vec();
            while !stop.load(Ordering::Relaxed) {
                // Read user profile
                let _ = um.get(&user_pk);
                // Read through orders
                for entry in om.iter() {
                    let _ = entry;
                }
                count += 1;
            }
            total.fetch_add(count, Ordering::Relaxed);
        }));
    }
    std::thread::sleep(Duration::from_secs(duration));
    stop.store(true, Ordering::Relaxed);
    for h in handles { h.join().unwrap(); }

    let profile_reads = total_reads.load(Ordering::Relaxed);
    let profiles_per_sec = profile_reads / duration;
    println!("    {} profile loads/sec (user + orders iter)", profiles_per_sec);
    println!();

    // ── Workload 2: Traditional DashMap lookup (simulated PG wire) ───────
    println!("─── Workload 2: Traditional Lookup (simulated request-response)");
    println!("  Pattern: 3 separate lookups per profile (user + orders + products)");
    println!();

    let stop = Arc::new(AtomicBool::new(false));
    let total_traditional = Arc::new(AtomicU64::new(0));
    let total_queries = Arc::new(AtomicU64::new(0));

    let mut handles = Vec::new();
    for t in 0..threads {
        let u = Arc::clone(&users_map);
        let o = Arc::clone(&orders_map);
        let p = Arc::clone(&products_map);
        let stop = Arc::clone(&stop);
        let total = Arc::clone(&total_traditional);
        let tq = Arc::clone(&total_queries);
        handles.push(std::thread::spawn(move || {
            let mut rng: u64 = 42 + t as u64;
            let mut count = 0u64;
            let mut queries = 0u64;
            while !stop.load(Ordering::Relaxed) {
                rng ^= rng << 13; rng ^= rng >> 7; rng ^= rng << 17;
                let uid = rng % NUM_USERS;
                queries += simulate_traditional_lookups(&u, &o, &p, uid) as u64;
                count += 1;
            }
            total.fetch_add(count, Ordering::Relaxed);
            tq.fetch_add(queries, Ordering::Relaxed);
        }));
    }
    std::thread::sleep(Duration::from_secs(duration));
    stop.store(true, Ordering::Relaxed);
    for h in handles { h.join().unwrap(); }

    let trad_profiles = total_traditional.load(Ordering::Relaxed) / duration;
    let trad_queries = total_queries.load(Ordering::Relaxed) / duration;
    println!("    {} profile loads/sec ({} queries/sec)", trad_profiles, trad_queries);
    println!();

    // ── Workload 3: Delta propagation (mutation while subscribed) ────────
    println!("─── Workload 3: Live Update (mutate → delta → mirror) ───────");
    println!("  Pattern: INSERT new order for user 42, verify mirror updates");
    println!();

    let before_orders = orders_mirror.len();
    let new_order_pk = (NUM_ORDERS + 1).to_le_bytes().to_vec();
    let new_order = make_order(NUM_ORDERS + 1, test_user_id);

    let delta_start = Instant::now();
    client.mutate_and_apply("orders", DeltaOp::Insert, &new_order_pk, Some(&new_order)).await;
    let delta_ns = delta_start.elapsed().as_nanos();

    let after_orders = orders_mirror.len();
    let delta_ok = after_orders == before_orders + 1;

    println!("  Orders before:  {}", before_orders);
    println!("  INSERT new order → delta pushed");
    println!("  Orders after:   {}", after_orders);
    println!("  Delta latency:  {}µs", delta_ns / 1000);
    println!("  Verified:       {}", if delta_ok { "PASS ✓" } else { "FAIL ✗" });
    println!();

    // ── Workload 4: Burst — many users reading concurrently ─────────────
    println!("─── Workload 4: Burst Mirror Reads (random users) ───────────");
    println!("  Pattern: {} threads reading random PKs from users mirror", threads);
    println!("  (Simulates 100K-user mirror subscribed with SUBSCRIBE ALL)");
    println!();

    // Subscribe to ALL users (large mirror)
    let sub_start = Instant::now();
    let (_all_uid, all_users_mirror) = client.subscribe("users", Predicate::All).await;
    let sub_all_ns = sub_start.elapsed().as_nanos();
    println!("  Subscribe ALL users: {:>8}ms  ({} rows, {}MB)",
        sub_all_ns / 1_000_000,
        all_users_mirror.len(),
        all_users_mirror.memory_bytes() / 1_024 / 1_024);

    let stop = Arc::new(AtomicBool::new(false));
    let total_burst = Arc::new(AtomicU64::new(0));

    let mut handles = Vec::new();
    for t in 0..threads {
        let m = Arc::clone(&all_users_mirror);
        let stop = Arc::clone(&stop);
        let total = Arc::clone(&total_burst);
        handles.push(std::thread::spawn(move || {
            let mut rng: u64 = 42 + t as u64;
            let mut count = 0u64;
            while !stop.load(Ordering::Relaxed) {
                rng ^= rng << 13; rng ^= rng >> 7; rng ^= rng << 17;
                let uid = rng % NUM_USERS;
                let _ = m.get(&uid.to_le_bytes().to_vec());
                count += 1;
            }
            total.fetch_add(count, Ordering::Relaxed);
        }));
    }
    std::thread::sleep(Duration::from_secs(duration));
    stop.store(true, Ordering::Relaxed);
    for h in handles { h.join().unwrap(); }

    let burst_qps = total_burst.load(Ordering::Relaxed) / duration;
    let burst_ns = if burst_qps > 0 { 1_000_000_000 / burst_qps } else { 0 };

    println!("  Throughput:  {:.2}M reads/sec", burst_qps as f64 / 1_000_000.0);
    println!("  Latency:     {}ns/read", burst_ns);
    println!();

    // ── Summary ─────────────────────────────────────────────────────────
    println!("============================================================");
    println!("  SUMMARY — {} users, {} orders, {} products, {} items",
        NUM_USERS, NUM_ORDERS, NUM_PRODUCTS, NUM_ORDER_ITEMS);
    println!("============================================================");
    println!();
    println!("  {:<45} {:>15}", "Metric", "Result");
    println!("  {:<45} {:>15}", "─".repeat(45), "─".repeat(15));
    println!("  {:<45} {:>12}/sec", "LiveSync: profile loads (user+orders)", profiles_per_sec);
    println!("  {:<45} {:>12}/sec", "Traditional: profile loads (3+ queries)", trad_profiles);
    println!("  {:<45} {:>12.1}x", "LiveSync advantage (profile loads)",
        profiles_per_sec as f64 / trad_profiles.max(1) as f64);
    println!();
    println!("  {:<45} {:>12.2}M/sec", "LiveSync: burst reads (100K user mirror)",
        burst_qps as f64 / 1_000_000.0);
    println!("  {:<45} {:>12}ns", "LiveSync: burst read latency", burst_ns);
    println!();
    println!("  {:<45} {:>12}µs", "Delta propagation latency", delta_ns / 1000);
    println!("  {:<45} {:>12}", "Delta integrity verified", if delta_ok { "PASS ✓" } else { "FAIL ✗" });
    println!();

    if delta_ok {
        println!("  ALL TESTS PASSED ✓");
    } else {
        println!("  INTEGRITY CHECK FAILED ✗");
        std::process::exit(1);
    }
}
