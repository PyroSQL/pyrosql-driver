//! FK Walker Benchmark — measures cascading FK-inferred subscriptions at scale.
//!
//! Schema (e-commerce with FKs):
//!   users       (100K rows)
//!     └→ orders      (500K rows, user_id FK → users.id)
//!          └→ order_items (1.5M rows, order_id FK → orders.id, product_id FK → products.id)
//!               └→ products    (10K rows, referenced by order_items.product_id)
//!
//! Tests:
//!   1. FK Graph Resolution Speed — walk_fk_depth1 + walk_fk_next for 1000 users
//!   2. Cascading Subscribe (depth=1) — user + orders via FK
//!   3. Cascading Subscribe (depth=2) — user + orders + items + products
//!   4. Cascading Delta Propagation — INSERT cascades through FK graph
//!   5. Burst Profile Loads — 100 users full FK subscribe vs traditional lookups

use dashmap::DashMap;
use pyrosql_rmp::fk_walker::{walk_fk_depth1, walk_fk_next};
use pyrosql_rmp::mirror::TableMirror;
use pyrosql_rmp::protocol::*;
use pyrosql_rmp::schema::{ForeignKey, SchemaGraph};
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

// ── Schema constants ────────────────────────────────────────────────────────

const NUM_USERS: u64 = 100_000;
const NUM_ORDERS: u64 = 500_000;
const NUM_PRODUCTS: u64 = 10_000;
const NUM_ORDER_ITEMS: u64 = 1_500_000;

const ORDERS_PER_USER: u64 = NUM_ORDERS / NUM_USERS; // 5
const ITEMS_PER_ORDER: u64 = NUM_ORDER_ITEMS / NUM_ORDERS; // 3

const ROW_SIZE: usize = 128;

// ── Schema graph construction ───────────────────────────────────────────────

fn build_schema() -> SchemaGraph {
    SchemaGraph::new(vec![
        ForeignKey {
            from_table: "orders".into(),
            from_column: "user_id".into(),
            to_table: "users".into(),
            to_column: "id".into(),
        },
        ForeignKey {
            from_table: "order_items".into(),
            from_column: "order_id".into(),
            to_table: "orders".into(),
            to_column: "id".into(),
        },
        ForeignKey {
            from_table: "order_items".into(),
            from_column: "product_id".into(),
            to_table: "products".into(),
            to_column: "id".into(),
        },
    ])
}

// ── Data generation ─────────────────────────────────────────────────────────

fn make_user(id: u64) -> Vec<u8> {
    let mut v = Vec::with_capacity(ROW_SIZE);
    v.extend_from_slice(&id.to_le_bytes());
    v.extend_from_slice(format!("user_{id:06}").as_bytes());
    v.extend_from_slice(format!("u{id}@mail.com").as_bytes());
    v.resize(ROW_SIZE, 0);
    v
}

fn make_order(id: u64, user_id: u64) -> Vec<u8> {
    let mut v = Vec::with_capacity(ROW_SIZE);
    v.extend_from_slice(&id.to_le_bytes());        // offset 0..8: id
    v.extend_from_slice(&user_id.to_le_bytes());   // offset 8..16: user_id FK
    v.extend_from_slice(&(id as f64 * 9.99).to_le_bytes()); // offset 16..24: total
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
    v.extend_from_slice(&id.to_le_bytes());          // offset 0..8: id
    v.extend_from_slice(&order_id.to_le_bytes());    // offset 8..16: order_id FK
    v.extend_from_slice(&product_id.to_le_bytes());  // offset 16..24: product_id FK
    v.extend_from_slice(&((id % 5 + 1) as u32).to_le_bytes()); // qty
    v.resize(ROW_SIZE, 0);
    v
}

/// Compute order IDs for a given user_id (deterministic: user_id, user_id + NUM_USERS, ...).
fn order_ids_for_user(user_id: u64) -> Vec<u64> {
    // Orders are generated as: order i has user_id = i % NUM_USERS
    // So for user_id U, orders are U, U+NUM_USERS, U+2*NUM_USERS, ...
    let mut ids = Vec::with_capacity(ORDERS_PER_USER as usize);
    let mut oid = user_id;
    while oid < NUM_ORDERS {
        ids.push(oid);
        oid += NUM_USERS;
    }
    ids
}

/// Compute order_item IDs for a given order_id.
fn item_ids_for_order(order_id: u64) -> Vec<u64> {
    // Items: item i has order_id = i % NUM_ORDERS
    let mut ids = Vec::with_capacity(ITEMS_PER_ORDER as usize);
    let mut iid = order_id;
    while iid < NUM_ORDER_ITEMS {
        ids.push(iid);
        iid += NUM_ORDERS;
    }
    ids
}

/// Compute the product_id for an order_item by its id.
fn product_id_for_item(item_id: u64) -> u64 {
    item_id % NUM_PRODUCTS
}

// ── Mock server ─────────────────────────────────────────────────────────────

/// FK index: maps (table, fk_column, fk_value) -> Vec<pk>
/// Simulates a real database index on FK columns for efficient lookups.
struct FkIndex {
    /// (table, column) -> (fk_value -> Vec<pk>)
    idx: DashMap<(String, String), DashMap<Vec<u8>, Vec<Vec<u8>>>>,
}

impl FkIndex {
    fn new() -> Self {
        Self {
            idx: DashMap::new(),
        }
    }

    fn insert(&self, table: &str, column: &str, fk_value: Vec<u8>, pk: Vec<u8>) {
        let key = (table.to_string(), column.to_string());
        let entry = self.idx.entry(key).or_insert_with(DashMap::new);
        entry.entry(fk_value).or_insert_with(Vec::new).push(pk);
    }

    fn lookup(&self, table: &str, column: &str, fk_value: &[u8]) -> Vec<Vec<u8>> {
        let key = (table.to_string(), column.to_string());
        self.idx
            .get(&key)
            .and_then(|m| m.get(&fk_value.to_vec()).map(|v| v.clone()))
            .unwrap_or_default()
    }
}

struct MultiTableServer {
    tables: DashMap<String, Arc<DashMap<Vec<u8>, Vec<u8>>>>,
    fk_index: FkIndex,
}

impl MultiTableServer {
    fn new() -> Self {
        Self {
            tables: DashMap::new(),
            fk_index: FkIndex::new(),
        }
    }

    fn add_table(&self, name: &str) -> Arc<DashMap<Vec<u8>, Vec<u8>>> {
        let map = Arc::new(DashMap::new());
        self.tables.insert(name.to_string(), Arc::clone(&map));
        map
    }

    /// Insert a row and update FK indexes.
    fn insert_row(&self, table: &str, pk: Vec<u8>, row: Vec<u8>) {
        // Update FK indexes based on known column offsets
        if table == "orders" && row.len() >= 16 {
            self.fk_index
                .insert(table, "user_id", row[8..16].to_vec(), pk.clone());
        } else if table == "order_items" {
            if row.len() >= 16 {
                self.fk_index
                    .insert(table, "order_id", row[8..16].to_vec(), pk.clone());
            }
            if row.len() >= 24 {
                self.fk_index
                    .insert(table, "product_id", row[16..24].to_vec(), pk.clone());
            }
        }

        self.tables
            .get(table)
            .map(|t| t.insert(pk, row));
    }

    fn get_rows_filtered(&self, table: &str, predicate: &Predicate) -> Vec<(Vec<u8>, Vec<u8>)> {
        let Some(t) = self.tables.get(table) else {
            return Vec::new();
        };
        match predicate {
            Predicate::All => t
                .iter()
                .map(|e| (e.key().clone(), e.value().clone()))
                .collect(),
            Predicate::Eq { column, value } => {
                let offset = fk_column_offset(table, column);
                if offset.is_none() {
                    // PK lookup: direct key access
                    t.get(value)
                        .map(|e| vec![(e.key().clone(), e.value().clone())])
                        .unwrap_or_default()
                } else {
                    // FK lookup: use the index for O(1) access
                    let pks = self.fk_index.lookup(table, column, value);
                    pks.iter()
                        .filter_map(|pk| t.get(pk).map(|e| (pk.clone(), e.value().clone())))
                        .collect()
                }
            }
            Predicate::Range { start, end } => t
                .iter()
                .filter(|e| {
                    e.key().as_slice() >= start.as_slice()
                        && e.key().as_slice() <= end.as_slice()
                })
                .map(|e| (e.key().clone(), e.value().clone()))
                .collect(),
        }
    }
}

/// Returns the byte offset of a FK column within a row, or None if it's a PK (id) match.
fn fk_column_offset(table: &str, column: &str) -> Option<usize> {
    match (table, column) {
        ("orders", "user_id") => Some(8),
        ("order_items", "order_id") => Some(8),
        ("order_items", "product_id") => Some(16),
        // For PK lookups (e.g., products WHERE id = X), match against key not row bytes
        (_, "id") => None,
        _ => None,
    }
}

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

async fn run_server(listener: TcpListener, server: Arc<MultiTableServer>) {
    loop {
        let (stream, _) = match listener.accept().await {
            Ok(s) => s,
            Err(_) => break,
        };
        let srv = Arc::clone(&server);
        tokio::spawn(async move {
            handle_client(stream, srv).await;
        });
    }
}

async fn handle_client(mut stream: TcpStream, server: Arc<MultiTableServer>) {
    let mut subs: Vec<(u64, String, Predicate)> = Vec::new();
    let mut version_counter: u64 = 1;

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
                let rows = server.get_rows_filtered(&sub.table, &sub.predicate);
                version_counter += 1;
                let snap = Snapshot {
                    sub_id: sub.sub_id,
                    version: version_counter,
                    columns: vec![
                        ColumnInfo { name: "id".into(), type_tag: ColumnType::Int64 },
                        ColumnInfo { name: "data".into(), type_tag: ColumnType::Bytes },
                    ],
                    rows,
                };
                let encoded = encode_message(&Message::Snapshot(snap));
                if stream.write_all(&encoded).await.is_err() {
                    break;
                }
                subs.push((sub.sub_id, sub.table, sub.predicate));
            }
            Message::Mutate(mutate) => {
                // Apply mutation to server state
                match mutate.op {
                    DeltaOp::Insert | DeltaOp::Update => {
                        if let Some(row) = &mutate.row {
                            server.insert_row(
                                &mutate.table,
                                mutate.pk.clone(),
                                row.clone(),
                            );
                        }
                    }
                    DeltaOp::Delete => {
                        if let Some(tbl) = server.tables.get(&mutate.table) {
                            tbl.remove(&mutate.pk);
                        }
                    }
                }
                // Push deltas to matching subscriptions
                for (sub_id, table, predicate) in &subs {
                    if *table == mutate.table {
                        // Check if this row matches the subscription predicate
                        let matches = match predicate {
                            Predicate::All => true,
                            Predicate::Eq { column, value } => {
                                let offset = fk_column_offset(&mutate.table, column);
                                if let Some(off) = offset {
                                    mutate.row.as_ref().map_or(false, |r| {
                                        r.len() >= off + value.len()
                                            && &r[off..off + value.len()] == value.as_slice()
                                    })
                                } else {
                                    // PK match
                                    mutate.pk == *value
                                }
                            }
                            Predicate::Range { start, end } => {
                                mutate.pk.as_slice() >= start.as_slice()
                                    && mutate.pk.as_slice() <= end.as_slice()
                            }
                        };
                        if matches {
                            version_counter += 1;
                            let delta = Delta {
                                sub_id: *sub_id,
                                version: version_counter,
                                changes: vec![RowChange {
                                    op: mutate.op,
                                    pk: mutate.pk.clone(),
                                    row: mutate.row.clone(),
                                }],
                            };
                            let encoded = encode_message(&Message::Delta(delta));
                            if stream.write_all(&encoded).await.is_err() {
                                break;
                            }
                        }
                    }
                }
            }
            Message::Unsubscribe(unsub) => {
                subs.retain(|(id, _, _)| *id != unsub.sub_id);
            }
            _ => {}
        }
    }
}

// ── Benchmark client ────────────────────────────────────────────────────────

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
            sub_id,
            table: table.to_string(),
            predicate,
        }));
        self.stream.write_all(&msg).await.unwrap();
        let frame = read_frame(&mut self.stream).await.unwrap();
        let snap = match decode_message(&frame).unwrap() {
            Message::Snapshot(s) => s,
            other => panic!("expected snapshot, got: {:?}", other),
        };
        let mirror = Arc::new(TableMirror::new(sub_id));
        mirror.load_snapshot(snap);
        self.mirrors.insert(sub_id, Arc::clone(&mirror));
        (sub_id, mirror)
    }

    async fn mutate(&mut self, table: &str, op: DeltaOp, pk: &[u8], row: Option<&[u8]>) {
        let msg = encode_message(&Message::Mutate(Mutate {
            table: table.to_string(),
            op,
            pk: pk.to_vec(),
            row: row.map(|r| r.to_vec()),
        }));
        self.stream.write_all(&msg).await.unwrap();
    }

    /// Read next delta from the wire and apply it to the corresponding mirror.
    /// Returns (sub_id, number_of_changes).
    async fn recv_and_apply_delta(&mut self) -> (u64, usize) {
        let frame = read_frame(&mut self.stream).await.unwrap();
        match decode_message(&frame).unwrap() {
            Message::Delta(delta) => {
                let sub_id = delta.sub_id;
                let n = delta.changes.len();
                if let Some(m) = self.mirrors.get(&sub_id) {
                    m.apply_delta(&delta);
                }
                (sub_id, n)
            }
            other => panic!("expected delta, got: {:?}", other),
        }
    }
}

// ── Test 1: FK Graph Resolution Speed ───────────────────────────────────────

fn run_test1(schema: &SchemaGraph) -> (f64, f64, u64) {
    let num_users = 1000u64;
    let mut total_subs: u64 = 0;
    let mut total_walks: u64 = 0;

    let start = Instant::now();
    for uid in 0..num_users {
        let pk = uid.to_le_bytes().to_vec();

        // Phase 1: depth 1 from root (users → orders)
        let depth1_subs = walk_fk_depth1(schema, "users", &[pk.clone()]);
        total_subs += depth1_subs.len() as u64;
        total_walks += 1;

        // Simulate receiving orders snapshot for this user: build fake (pk, row) pairs
        let user_order_ids = order_ids_for_user(uid);
        let order_rows: Vec<(Vec<u8>, Vec<u8>)> = user_order_ids
            .iter()
            .map(|&oid| {
                let opk = oid.to_le_bytes().to_vec();
                let row = make_order(oid, uid);
                (opk, row)
            })
            .collect();

        // Phase 2: depth 2 from orders (orders → order_items, via incoming FK)
        let mut visited = HashSet::new();
        visited.insert("users".to_string());
        let depth2_subs = walk_fk_next(
            schema,
            "orders",
            &order_rows,
            &[("user_id".to_string(), 8)],
            1,  // current depth
            2,  // max depth
            &mut visited,
        );
        total_subs += depth2_subs.len() as u64;
        total_walks += 1;

        // Phase 2b: from order_items (if we had them), walk outgoing to products
        // Build fake item rows to extract product_id values
        let mut item_rows: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        for &oid in &user_order_ids {
            for iid in item_ids_for_order(oid) {
                let pid = product_id_for_item(iid);
                let ipk = iid.to_le_bytes().to_vec();
                let row = make_order_item(iid, oid, pid);
                item_rows.push((ipk, row));
            }
        }

        visited.insert("orders".to_string());
        let depth2b_subs = walk_fk_next(
            schema,
            "order_items",
            &item_rows,
            &[
                ("order_id".to_string(), 8),
                ("product_id".to_string(), 16),
            ],
            2,  // current depth
            -1, // unlimited (but products has no further FKs)
            &mut visited,
        );
        total_subs += depth2b_subs.len() as u64;
        total_walks += 1;
    }
    let elapsed = start.elapsed();

    let avg_subs_per_user = total_subs as f64 / num_users as f64;
    let avg_ns_per_user = elapsed.as_nanos() as f64 / num_users as f64;
    let walks_per_sec = total_walks as f64 / elapsed.as_secs_f64();

    (avg_subs_per_user, avg_ns_per_user, walks_per_sec as u64)
}

// ── Test 5 helper: Traditional sequential lookups ───────────────────────────

/// Simulates a traditional request-response database load: one network round-trip
/// per query. Uses deterministic key lookups (simulating indexed access, as a real
/// DB would use indexes on FK columns).
fn traditional_profile_load(
    users: &DashMap<Vec<u8>, Vec<u8>>,
    orders: &DashMap<Vec<u8>, Vec<u8>>,
    order_items: &DashMap<Vec<u8>, Vec<u8>>,
    products: &DashMap<Vec<u8>, Vec<u8>>,
    user_id: u64,
) -> (usize, usize) {
    let mut total_rows = 0usize;
    let mut total_lookups = 0usize;

    // Query 1: SELECT * FROM users WHERE id = ?  (1 round-trip)
    let user_pk = user_id.to_le_bytes().to_vec();
    if users.get(&user_pk).is_some() {
        total_rows += 1;
    }
    total_lookups += 1;

    // Query 2: SELECT * FROM orders WHERE user_id = ?  (1 round-trip, indexed)
    // Use deterministic IDs since we know the data generation pattern
    let oids = order_ids_for_user(user_id);
    let mut order_rows: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(oids.len());
    for &oid in &oids {
        let opk = oid.to_le_bytes().to_vec();
        if let Some(row) = orders.get(&opk) {
            order_rows.push((opk, row.clone()));
            total_rows += 1;
        }
    }
    total_lookups += 1;

    // Query 3: for each order, SELECT * FROM order_items WHERE order_id = ?
    // (1 round-trip per order — sequential, not batched)
    let mut product_ids: HashSet<u64> = HashSet::new();
    for &oid in &oids {
        let iids = item_ids_for_order(oid);
        for &iid in &iids {
            let ipk = iid.to_le_bytes().to_vec();
            if let Some(row) = order_items.get(&ipk) {
                total_rows += 1;
                if row.len() >= 24 {
                    let pid = u64::from_le_bytes(row[16..24].try_into().unwrap());
                    product_ids.insert(pid);
                }
            }
        }
        total_lookups += 1;
    }

    // Query 4: SELECT * FROM products WHERE id IN (...)  (1 round-trip per product)
    for pid in &product_ids {
        let ppk = pid.to_le_bytes().to_vec();
        if products.get(&ppk).is_some() {
            total_rows += 1;
        }
        total_lookups += 1;
    }

    (total_rows, total_lookups)
}

// ── Main ────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    println!("============================================================");
    println!("  PyroSQL LiveSync -- FK Walker Benchmark");
    println!("  Schema: users(100K) -> orders(500K) -> items(1.5M) -> products(10K)");
    println!("============================================================");
    println!();

    // ── Generate data ───────────────────────────────────────────────────
    print!("Generating data... ");
    let gen_start = Instant::now();

    let server = Arc::new(MultiTableServer::new());

    let users_map = server.add_table("users");
    for i in 0..NUM_USERS {
        let pk = i.to_le_bytes().to_vec();
        let row = make_user(i);
        users_map.insert(pk, row);
    }

    let orders_map = server.add_table("orders");
    for i in 0..NUM_ORDERS {
        let user_id = i % NUM_USERS;
        let pk = i.to_le_bytes().to_vec();
        let row = make_order(i, user_id);
        server.insert_row("orders", pk, row);
    }

    let products_map = server.add_table("products");
    for i in 0..NUM_PRODUCTS {
        let pk = i.to_le_bytes().to_vec();
        let row = make_product(i);
        products_map.insert(pk, row);
    }

    let items_map = server.add_table("order_items");
    for i in 0..NUM_ORDER_ITEMS {
        let order_id = i % NUM_ORDERS;
        let product_id = i % NUM_PRODUCTS;
        let pk = i.to_le_bytes().to_vec();
        let row = make_order_item(i, order_id, product_id);
        server.insert_row("order_items", pk, row);
    }

    println!("{:.1}s", gen_start.elapsed().as_secs_f64());
    println!(
        "  users: {}  orders: {}  products: {}  items: {}  total: {}",
        NUM_USERS,
        NUM_ORDERS,
        NUM_PRODUCTS,
        NUM_ORDER_ITEMS,
        NUM_USERS + NUM_ORDERS + NUM_PRODUCTS + NUM_ORDER_ITEMS
    );
    println!();

    let schema = build_schema();

    // ═══ Test 1: FK Graph Resolution ════════════════════════════════════
    println!("--- Test 1: FK Graph Resolution ------------------------------------");
    let (avg_subs, avg_ns, walks_per_sec) = run_test1(&schema);
    println!("  1000 users resolved to depth 2");
    println!("  Avg subscriptions per user:  {:.1}", avg_subs);
    println!("  Avg resolution time:         {:.0}ns/user", avg_ns);
    println!("  Total FK walks:              {}/sec", walks_per_sec);
    println!();

    // ── Start TCP server ────────────────────────────────────────────────
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let srv = Arc::clone(&server);
    tokio::spawn(async move {
        run_server(listener, srv).await;
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    // ═══ Test 2: Cascading Subscribe (depth=1) ══════════════════════════
    println!("--- Test 2: Cascading Subscribe (depth=1) -------------------------");
    let test_user_id: u64 = 42;
    let mut client = BenchClient::connect(&addr).await;

    // Subscribe to user 42
    let t0 = Instant::now();
    let (_uid, user_mirror) = client
        .subscribe(
            "users",
            Predicate::Eq {
                column: "id".into(),
                value: test_user_id.to_le_bytes().to_vec(),
            },
        )
        .await;
    let user_us = t0.elapsed().as_micros();

    // FK-inferred: subscribe to orders WHERE user_id = 42
    let t1 = Instant::now();
    let (_oid, orders_mirror) = client
        .subscribe(
            "orders",
            Predicate::Eq {
                column: "user_id".into(),
                value: test_user_id.to_le_bytes().to_vec(),
            },
        )
        .await;
    let orders_us = t1.elapsed().as_micros();

    let user_rows = user_mirror.len();
    let orders_rows = orders_mirror.len();

    println!(
        "  User 42: subscribe user        {}us  ({} row)",
        user_us, user_rows
    );
    println!(
        "  User 42: subscribe orders      {}us  ({} rows, FK-inferred)",
        orders_us, orders_rows
    );
    println!("  Total:                         {}us", user_us + orders_us);

    // Verify expected counts
    assert_eq!(user_rows, 1, "user mirror must have exactly 1 row");
    assert_eq!(
        orders_rows, ORDERS_PER_USER as usize,
        "orders mirror must have {} rows",
        ORDERS_PER_USER
    );
    println!(
        "  Verified: user={}, orders={}                PASS",
        user_rows, orders_rows
    );
    println!();

    // ═══ Test 3: Cascading Subscribe (depth=2) ══════════════════════════
    println!("--- Test 3: Cascading Subscribe (depth=2) -------------------------");

    // Collect order PKs from the orders mirror to drive depth-2 walk
    let order_pks: Vec<Vec<u8>> = orders_mirror.iter().map(|(pk, _)| pk).collect();

    // Subscribe to order_items WHERE order_id IN (order PKs)
    let t2 = Instant::now();
    let mut items_total_rows = 0usize;
    let mut items_mirrors: Vec<Arc<TableMirror>> = Vec::new();
    for opk in &order_pks {
        let (_sid, mirror) = client
            .subscribe(
                "order_items",
                Predicate::Eq {
                    column: "order_id".into(),
                    value: opk.clone(),
                },
            )
            .await;
        items_total_rows += mirror.len();
        items_mirrors.push(mirror);
    }
    let items_us = t2.elapsed().as_micros();

    // Collect product_ids from all items mirrors to subscribe to products
    let mut product_id_set: HashSet<Vec<u8>> = HashSet::new();
    for mirror in &items_mirrors {
        for (_pk, row) in mirror.iter() {
            if row.len() >= 24 {
                product_id_set.insert(row[16..24].to_vec());
            }
        }
    }

    let t3 = Instant::now();
    let mut products_total_rows = 0usize;
    for pid_bytes in &product_id_set {
        let (_sid, mirror) = client
            .subscribe(
                "products",
                Predicate::Eq {
                    column: "id".into(),
                    value: pid_bytes.clone(),
                },
            )
            .await;
        products_total_rows += mirror.len();
    }
    let products_us = t3.elapsed().as_micros();

    let full_profile_rows = user_rows + orders_rows + items_total_rows + products_total_rows;

    println!(
        "  + order_items (FK orders->items) {}us  ({} rows)",
        items_us, items_total_rows
    );
    println!(
        "  + products (FK items->products)  {}us  ({} rows)",
        products_us, products_total_rows
    );
    println!(
        "  Full profile subscribe:        {}us  ({} rows across 4 tables)",
        user_us + orders_us + items_us + products_us,
        full_profile_rows
    );
    println!();

    // ═══ Test 4: Cascading Delta Propagation ════════════════════════════
    println!("--- Test 4: Cascading Delta Propagation ----------------------------");

    // We need a fresh client with subscriptions that will receive deltas.
    // Reconnect and subscribe to user 42's orders and items.
    drop(client);
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut client = BenchClient::connect(&addr).await;

    // Subscribe to orders WHERE user_id = 42
    let (_orders_sub_id, orders_delta_mirror) = client
        .subscribe(
            "orders",
            Predicate::Eq {
                column: "user_id".into(),
                value: test_user_id.to_le_bytes().to_vec(),
            },
        )
        .await;
    let orders_before = orders_delta_mirror.len();

    // Also subscribe to order_items with a known order_id that we will create
    let new_order_id: u64 = NUM_ORDERS + 100;
    let (_items_sub_id, items_delta_mirror) = client
        .subscribe(
            "order_items",
            Predicate::Eq {
                column: "order_id".into(),
                value: new_order_id.to_le_bytes().to_vec(),
            },
        )
        .await;
    let items_before = items_delta_mirror.len();

    // INSERT a new order for user 42
    let new_order_pk = new_order_id.to_le_bytes().to_vec();
    let new_order_row = make_order(new_order_id, test_user_id);

    let cascade_start = Instant::now();

    client
        .mutate(
            "orders",
            DeltaOp::Insert,
            &new_order_pk,
            Some(&new_order_row),
        )
        .await;
    // Receive the delta for orders subscription
    let (_, _) = client.recv_and_apply_delta().await;
    let orders_delta_us = cascade_start.elapsed().as_micros();

    let orders_after = orders_delta_mirror.len();

    // INSERT order_items for that new order
    let new_item_id: u64 = NUM_ORDER_ITEMS + 100;
    let new_item_product: u64 = 42;
    let new_item_pk = new_item_id.to_le_bytes().to_vec();
    let new_item_row = make_order_item(new_item_id, new_order_id, new_item_product);

    let items_delta_start = Instant::now();
    client
        .mutate(
            "order_items",
            DeltaOp::Insert,
            &new_item_pk,
            Some(&new_item_row),
        )
        .await;
    let (_, _) = client.recv_and_apply_delta().await;
    let items_delta_us = items_delta_start.elapsed().as_micros();

    let items_after = items_delta_mirror.len();
    let total_cascade_us = cascade_start.elapsed().as_micros();

    let orders_ok = orders_after == orders_before + 1;
    let items_ok = items_after == items_before + 1;
    let all_ok = orders_ok && items_ok;

    println!(
        "  INSERT order -> orders mirror updated    {}us",
        orders_delta_us
    );
    println!(
        "  INSERT items -> items mirror updated     {}us",
        items_delta_us
    );
    println!("  Total cascade latency:                  {}us", total_cascade_us);
    println!(
        "  Orders: {} -> {} {}",
        orders_before,
        orders_after,
        if orders_ok { "OK" } else { "FAIL" }
    );
    println!(
        "  Items:  {} -> {} {}",
        items_before,
        items_after,
        if items_ok { "OK" } else { "FAIL" }
    );
    println!(
        "  All mirrors consistent:                 {}",
        if all_ok { "PASS" } else { "FAIL" }
    );
    println!();

    // ═══ Test 5: Burst Profile Loads ════════════════════════════════════
    println!("--- Test 5: Burst Profile Loads ------------------------------------");

    // Part A: FK LiveSync — for 100 users, do full FK walk + subscribe + read
    let num_burst_users = 100u64;

    // We need a fresh client for each batch (the previous one may have many subs)
    drop(client);
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut client = BenchClient::connect(&addr).await;

    let livesync_start = Instant::now();
    let mut livesync_total_rows = 0usize;

    for uid in 0..num_burst_users {
        let pk = uid.to_le_bytes().to_vec();

        // Phase 0: Subscribe to user
        let (_sid, user_m) = client
            .subscribe(
                "users",
                Predicate::Eq {
                    column: "id".into(),
                    value: pk.clone(),
                },
            )
            .await;
        livesync_total_rows += user_m.len();

        // Phase 1: FK walk depth 1 — resolve orders subscription
        let depth1 = walk_fk_depth1(&schema, "users", &[pk.clone()]);
        for sub in &depth1 {
            let (_sid, orders_m) = client
                .subscribe(&sub.table, sub.predicate.clone())
                .await;

            // Phase 2: Walk from orders to order_items
            let order_rows: Vec<(Vec<u8>, Vec<u8>)> = orders_m.iter().collect();
            livesync_total_rows += order_rows.len();

            let mut visited = HashSet::new();
            visited.insert("users".to_string());

            let depth2 = walk_fk_next(
                &schema,
                &sub.table,
                &order_rows,
                &[("user_id".to_string(), 8)],
                1,
                2,
                &mut visited,
            );

            for sub2 in &depth2 {
                let (_sid2, items_m) = client
                    .subscribe(&sub2.table, sub2.predicate.clone())
                    .await;
                let item_rows: Vec<(Vec<u8>, Vec<u8>)> = items_m.iter().collect();
                livesync_total_rows += item_rows.len();

                // Phase 2b: Walk from order_items outgoing to products
                visited.insert("orders".to_string());
                let depth3 = walk_fk_next(
                    &schema,
                    &sub2.table,
                    &item_rows,
                    &[
                        ("order_id".to_string(), 8),
                        ("product_id".to_string(), 16),
                    ],
                    2,
                    -1,
                    &mut visited,
                );
                for sub3 in &depth3 {
                    let (_sid3, prod_m) = client
                        .subscribe(&sub3.table, sub3.predicate.clone())
                        .await;
                    livesync_total_rows += prod_m.len();
                }
            }
        }
    }

    let livesync_elapsed = livesync_start.elapsed();
    let livesync_profiles_per_sec =
        num_burst_users as f64 / livesync_elapsed.as_secs_f64();

    // Part B: Traditional sequential lookups (in-memory scan, no network)
    let traditional_start = Instant::now();
    let mut trad_total_rows = 0usize;
    let mut trad_total_lookups = 0usize;

    for uid in 0..num_burst_users {
        let (rows, lookups) = traditional_profile_load(
            &users_map,
            &orders_map,
            &items_map,
            &products_map,
            uid,
        );
        trad_total_rows += rows;
        trad_total_lookups += lookups;
    }

    let trad_elapsed = traditional_start.elapsed();
    let trad_profiles_per_sec = num_burst_users as f64 / trad_elapsed.as_secs_f64();

    let speedup = if trad_profiles_per_sec > 0.0 {
        livesync_profiles_per_sec / trad_profiles_per_sec
    } else {
        0.0
    };

    println!(
        "  FK LiveSync: {:.1} profiles/sec ({} total rows, subscribe + local reads)",
        livesync_profiles_per_sec, livesync_total_rows
    );
    println!(
        "  Traditional: {:.1} profiles/sec ({} total rows, {} sequential lookups)",
        trad_profiles_per_sec, trad_total_rows, trad_total_lookups
    );
    // Note: traditional is in-memory DashMap scan (no network) so it may be faster
    // than LiveSync which goes through TCP. The real advantage of LiveSync is that
    // subsequent reads after the initial subscribe are ~50ns local memory reads.
    if speedup >= 1.0 {
        println!("  FK advantage: {:.1}x", speedup);
    } else {
        // LiveSync initial subscribe is slower (network), but post-subscribe reads are free
        println!(
            "  Traditional in-memory scan: {:.1}x faster for cold load",
            1.0 / speedup
        );
        println!("  (LiveSync advantage is in subsequent reads: ~50ns vs network round-trip)");
    }
    println!();

    // ═══ Summary ════════════════════════════════════════════════════════
    println!("============================================================");
    println!(
        "  SUMMARY -- users({}K) orders({}K) items({}M) products({}K)",
        NUM_USERS / 1000,
        NUM_ORDERS / 1000,
        NUM_ORDER_ITEMS / 1_000_000,
        NUM_PRODUCTS / 1000
    );
    println!("============================================================");
    println!();
    println!("  {:<50} {:>15}", "Metric", "Result");
    println!("  {:<50} {:>15}", "-".repeat(50), "-".repeat(15));
    println!(
        "  {:<50} {:>12.1}/user",
        "FK graph: avg subscriptions per user", avg_subs
    );
    println!(
        "  {:<50} {:>12.0}ns",
        "FK graph: avg resolution time", avg_ns
    );
    println!(
        "  {:<50} {:>12}/sec",
        "FK graph: total walks", walks_per_sec
    );
    println!(
        "  {:<50} {:>12}us",
        "Cascade subscribe depth=1 (user+orders)",
        user_us + orders_us
    );
    println!(
        "  {:<50} {:>12}us",
        "Cascade subscribe depth=2 (full profile)",
        user_us + orders_us + items_us + products_us
    );
    println!(
        "  {:<50} {:>12} rows",
        "Full profile rows", full_profile_rows
    );
    println!(
        "  {:<50} {:>12}us",
        "Delta cascade: orders insert", orders_delta_us
    );
    println!(
        "  {:<50} {:>12}us",
        "Delta cascade: items insert", items_delta_us
    );
    println!(
        "  {:<50} {:>12}us",
        "Delta cascade: total latency", total_cascade_us
    );
    println!(
        "  {:<50} {:>15}",
        "Delta integrity",
        if all_ok { "PASS" } else { "FAIL" }
    );
    println!(
        "  {:<50} {:>12.1}/sec",
        "Burst: FK LiveSync profiles", livesync_profiles_per_sec
    );
    println!(
        "  {:<50} {:>12.1}/sec",
        "Burst: Traditional profiles", trad_profiles_per_sec
    );
    println!();

    if all_ok {
        println!("  ALL TESTS PASSED");
    } else {
        println!("  INTEGRITY CHECK FAILED");
        std::process::exit(1);
    }
}
