//! Benchmark: local query engine vs server PWire for the same queries.
//!
//! Usage: bench_local_query [server_addr] [duration_secs]

use pyrosql_rmp::local_query::{try_execute_local, LocalResult};
use pyrosql_rmp::mirror::TableMirror;
use pyrosql_rmp::protocol::*;
use pyrosql_rmp::row::{encode_row, Value};
use dashmap::DashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

const NUM_PRODUCTS: usize = 5_000;
const NUM_REVIEWS: usize = 25_000;

fn setup_mirrors() -> (DashMap<String, Arc<TableMirror>>, DashMap<String, Vec<ColumnInfo>>) {
    let mirrors: DashMap<String, Arc<TableMirror>> = DashMap::new();
    let schemas: DashMap<String, Vec<ColumnInfo>> = DashMap::new();

    // Products mirror
    let product_cols = vec![
        ColumnInfo { name: "id".into(), type_tag: ColumnType::Int64 },
        ColumnInfo { name: "name".into(), type_tag: ColumnType::Text },
        ColumnInfo { name: "price".into(), type_tag: ColumnType::Float64 },
        ColumnInfo { name: "category".into(), type_tag: ColumnType::Text },
    ];
    let product_mirror = Arc::new(TableMirror::new(1));
    let cats = ["electronics", "clothing", "sports", "books", "home"];
    let rows: Vec<(Vec<u8>, Vec<u8>)> = (1..=NUM_PRODUCTS).map(|i| {
        let pk = (i as u64).to_le_bytes().to_vec();
        let row = encode_row(&[
            Value::Int64(i as i64),
            Value::Text(format!("product_{}", i)),
            Value::Float64(10.0 + (i as f64 * 7.3) % 500.0),
            Value::Text(cats[i % cats.len()].to_string()),
        ]);
        (pk, row)
    }).collect();
    product_mirror.load_snapshot(Snapshot {
        sub_id: 1, version: 1, columns: product_cols.clone(), rows,
    });
    mirrors.insert("products".to_string(), product_mirror);
    schemas.insert("products".to_string(), product_cols);

    // Reviews mirror
    let review_cols = vec![
        ColumnInfo { name: "id".into(), type_tag: ColumnType::Int64 },
        ColumnInfo { name: "product_id".into(), type_tag: ColumnType::Int64 },
        ColumnInfo { name: "rating".into(), type_tag: ColumnType::Int64 },
        ColumnInfo { name: "body".into(), type_tag: ColumnType::Text },
    ];
    let review_mirror = Arc::new(TableMirror::new(2));
    let rows: Vec<(Vec<u8>, Vec<u8>)> = (1..=NUM_REVIEWS).map(|i| {
        let pk = (i as u64).to_le_bytes().to_vec();
        let row = encode_row(&[
            Value::Int64(i as i64),
            Value::Int64((i % NUM_PRODUCTS + 1) as i64),
            Value::Int64((i % 5 + 1) as i64),
            Value::Text(format!("Review text {}", i)),
        ]);
        (pk, row)
    }).collect();
    review_mirror.load_snapshot(Snapshot {
        sub_id: 2, version: 1, columns: review_cols.clone(), rows,
    });
    mirrors.insert("reviews".to_string(), review_mirror);
    schemas.insert("reviews".to_string(), review_cols);

    (mirrors, schemas)
}

fn bench_query(name: &str, sql: &str, mirrors: &DashMap<String, Arc<TableMirror>>,
               schemas: &DashMap<String, Vec<ColumnInfo>>, duration_secs: u64) -> f64 {
    // Warmup
    for _ in 0..50 {
        let _ = try_execute_local(sql, mirrors, schemas);
    }
    let start = Instant::now();
    let mut count = 0u64;
    let deadline = start + Duration::from_secs(duration_secs);
    while Instant::now() < deadline {
        let _ = try_execute_local(sql, mirrors, schemas);
        count += 1;
    }
    let elapsed = start.elapsed().as_secs_f64();
    let qps = count as f64 / elapsed;
    println!("  {:<55} {:>10.0} QPS", name, qps);
    qps
}

fn main() {
    let duration: u64 = std::env::args().nth(1).and_then(|s| s.parse().ok()).unwrap_or(3);

    println!("============================================================");
    println!("  LiveSync Local Query Engine Benchmark");
    println!("  {} products | {} reviews | {}s per query", NUM_PRODUCTS, NUM_REVIEWS, duration);
    println!("============================================================");
    println!();

    let (mirrors, schemas) = setup_mirrors();
    println!("Mirrors loaded: products={}, reviews={}", NUM_PRODUCTS, NUM_REVIEWS);
    println!();

    println!("─── SQL Standard Syntax ────────────────────────────────────");
    bench_query("SELECT * FROM products WHERE id = 42",
        "SELECT * FROM products WHERE id = 42", &mirrors, &schemas, duration);
    bench_query("SELECT * FROM products WHERE category = 'electronics'",
        "SELECT * FROM products WHERE category = 'electronics'", &mirrors, &schemas, duration);
    bench_query("SELECT * FROM products WHERE price > 200 LIMIT 10",
        "SELECT * FROM products WHERE price > 200 LIMIT 10", &mirrors, &schemas, duration);
    bench_query("SELECT * FROM products ORDER BY price DESC LIMIT 20",
        "SELECT * FROM products ORDER BY price DESC LIMIT 20", &mirrors, &schemas, duration);
    bench_query("SELECT name, price FROM products WHERE id = 42",
        "SELECT name, price FROM products WHERE id = 42", &mirrors, &schemas, duration);
    bench_query("SELECT * FROM reviews WHERE product_id = 42",
        "SELECT * FROM reviews WHERE product_id = 42", &mirrors, &schemas, duration);
    bench_query("JOIN products + reviews WHERE product_id",
        "SELECT p.name, r.rating FROM products p JOIN reviews r ON r.product_id = p.id WHERE p.id = 42",
        &mirrors, &schemas, duration);

    println!();
    println!("─── PyroSQL Native Syntax ──────────────────────────────────");
    bench_query("FIND products WHERE id = 42",
        "FIND products WHERE id = 42", &mirrors, &schemas, duration);
    bench_query("FIND products WHERE category = 'electronics'",
        "FIND products WHERE category = 'electronics'", &mirrors, &schemas, duration);
    bench_query("FIND TOP 20 products SORT BY price",
        "FIND TOP 20 products SORT BY price", &mirrors, &schemas, duration);
    bench_query("FIND products.name, products.price WHERE id = 42",
        "FIND products.name, products.price WHERE id = 42", &mirrors, &schemas, duration);
    bench_query("FIND products WITH reviews ON product_id = id WHERE id = 42",
        "FIND products WITH reviews ON reviews.product_id = products.id WHERE products.id = 42",
        &mirrors, &schemas, duration);

    println!();
    println!("─── Comparison Reference ──────────────────────────────────");
    println!("  PyroSQL server (PWire):                           ~18,000 QPS");
    println!("  PostgreSQL 18.3 (PG wire):                        ~14,000 QPS");
    println!("  LiveSync mirror read (DashMap.get, no SQL):   35,000,000 QPS");
    println!();
}
