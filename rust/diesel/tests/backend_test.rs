//! Integration-style tests for the PyroSQL Diesel backend.
//! These tests validate protocol encoding/decoding and query building
//! without requiring a live database connection.

use diesel_pyrosql::pwire;
use diesel_pyrosql::query_builder::PyroSqlQueryBuilder;
use diesel_pyrosql::{PyroSqlBackend, PyroSqlTypeMetadata, PyroSqlValue};
use diesel::deserialize::FromSql;
use diesel::query_builder::QueryBuilder;
use diesel::sql_types::*;

// ── PWire codec tests ──────────────────────────────────────────────────────

#[test]
fn test_frame_structure() {
    let frame = pwire::frame(0x01, b"SELECT 1");
    assert_eq!(frame[0], 0x01);
    let len = u32::from_le_bytes([frame[1], frame[2], frame[3], frame[4]]);
    assert_eq!(len, 8);
    assert_eq!(&frame[5..], b"SELECT 1");
}

#[test]
fn test_frame_empty_payload() {
    let frame = pwire::frame(0xFF, &[]);
    assert_eq!(frame.len(), 5);
    assert_eq!(frame[0], 0xFF);
    let len = u32::from_le_bytes([frame[1], frame[2], frame[3], frame[4]]);
    assert_eq!(len, 0);
}

#[test]
fn test_encode_auth() {
    let frame = pwire::encode_auth("admin", "secret");
    assert_eq!(frame[0], pwire::MSG_AUTH);
    let payload_len = u32::from_le_bytes([frame[1], frame[2], frame[3], frame[4]]) as usize;
    let payload = &frame[5..5 + payload_len];
    assert_eq!(payload[0], 5); // "admin" length
    assert_eq!(&payload[1..6], b"admin");
    assert_eq!(payload[6], 6); // "secret" length
    assert_eq!(&payload[7..13], b"secret");
}

#[test]
fn test_encode_query() {
    let frame = pwire::encode_query("SELECT 42");
    assert_eq!(frame[0], pwire::MSG_QUERY);
    let payload_len = u32::from_le_bytes([frame[1], frame[2], frame[3], frame[4]]) as usize;
    assert_eq!(&frame[5..5 + payload_len], b"SELECT 42");
}

#[test]
fn test_encode_prepare() {
    let frame = pwire::encode_prepare("SELECT $1");
    assert_eq!(frame[0], pwire::MSG_PREPARE);
}

#[test]
fn test_encode_execute() {
    let frame = pwire::encode_execute(42, &["hello".to_string(), "world".to_string()]);
    assert_eq!(frame[0], pwire::MSG_EXECUTE);
    let payload_len = u32::from_le_bytes([frame[1], frame[2], frame[3], frame[4]]) as usize;
    let payload = &frame[5..5 + payload_len];
    let handle = u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]);
    assert_eq!(handle, 42);
    let param_count = u16::from_le_bytes([payload[4], payload[5]]);
    assert_eq!(param_count, 2);
}

#[test]
fn test_encode_close() {
    let frame = pwire::encode_close(99);
    assert_eq!(frame[0], pwire::MSG_CLOSE);
    let payload = &frame[5..9];
    let handle = u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]);
    assert_eq!(handle, 99);
}

#[test]
fn test_encode_ping() {
    let frame = pwire::encode_ping();
    assert_eq!(frame[0], pwire::MSG_PING);
    assert_eq!(frame.len(), 5);
}

#[test]
fn test_encode_quit() {
    let frame = pwire::encode_quit();
    assert_eq!(frame[0], pwire::MSG_QUIT);
    assert_eq!(frame.len(), 5);
}

// ── Decode tests ───────────────────────────────────────────────────────────

#[test]
fn test_decode_result_set_text() {
    let mut payload = Vec::new();
    payload.extend_from_slice(&1u16.to_le_bytes()); // 1 column
    payload.push(4); // name length
    payload.extend_from_slice(b"name");
    payload.push(pwire::TYPE_TEXT);
    payload.extend_from_slice(&1u32.to_le_bytes()); // 1 row
    payload.push(0x00); // null bitmap (no nulls)
    payload.extend_from_slice(&5u16.to_le_bytes()); // text length
    payload.extend_from_slice(b"alice");

    let rs = pwire::decode_result_set(&payload).unwrap();
    assert_eq!(rs.columns.len(), 1);
    assert_eq!(rs.columns[0].name, "name");
    assert_eq!(rs.columns[0].type_tag, pwire::TYPE_TEXT);
    assert_eq!(rs.rows.len(), 1);
    match &rs.rows[0][0] {
        Some(pwire::Value::Text(s)) => assert_eq!(s, "alice"),
        other => panic!("Expected Text, got {:?}", other),
    }
}

#[test]
fn test_decode_result_set_i64_with_null() {
    let mut payload = Vec::new();
    payload.extend_from_slice(&2u16.to_le_bytes()); // 2 columns
    payload.push(2); payload.extend_from_slice(b"id"); payload.push(pwire::TYPE_I64);
    payload.push(4); payload.extend_from_slice(b"name"); payload.push(pwire::TYPE_TEXT);
    payload.extend_from_slice(&1u32.to_le_bytes()); // 1 row
    payload.push(0b00000010); // null bitmap: second column null
    payload.extend_from_slice(&42i64.to_le_bytes()); // id = 42

    let rs = pwire::decode_result_set(&payload).unwrap();
    assert_eq!(rs.rows.len(), 1);
    match &rs.rows[0][0] {
        Some(pwire::Value::I64(v)) => assert_eq!(*v, 42),
        other => panic!("Expected I64, got {:?}", other),
    }
    assert!(rs.rows[0][1].is_none());
}

#[test]
fn test_decode_result_set_f64_bool() {
    let mut payload = Vec::new();
    payload.extend_from_slice(&2u16.to_le_bytes());
    payload.push(5); payload.extend_from_slice(b"score"); payload.push(pwire::TYPE_F64);
    payload.push(6); payload.extend_from_slice(b"active"); payload.push(pwire::TYPE_BOOL);
    payload.extend_from_slice(&1u32.to_le_bytes());
    payload.push(0x00); // no nulls
    payload.extend_from_slice(&3.14f64.to_le_bytes());
    payload.push(1); // true

    let rs = pwire::decode_result_set(&payload).unwrap();
    match &rs.rows[0][0] {
        Some(pwire::Value::F64(v)) => assert!((*v - 3.14).abs() < 0.001),
        other => panic!("Expected F64, got {:?}", other),
    }
    match &rs.rows[0][1] {
        Some(pwire::Value::Bool(v)) => assert!(*v),
        other => panic!("Expected Bool, got {:?}", other),
    }
}

#[test]
fn test_decode_result_set_bytes() {
    let mut payload = Vec::new();
    payload.extend_from_slice(&1u16.to_le_bytes());
    payload.push(4); payload.extend_from_slice(b"data"); payload.push(pwire::TYPE_BYTES);
    payload.extend_from_slice(&1u32.to_le_bytes());
    payload.push(0x00);
    payload.extend_from_slice(&3u16.to_le_bytes());
    payload.extend_from_slice(&[0x01, 0x02, 0x03]);

    let rs = pwire::decode_result_set(&payload).unwrap();
    match &rs.rows[0][0] {
        Some(pwire::Value::Bytes(v)) => assert_eq!(v, &[1, 2, 3]),
        other => panic!("Expected Bytes, got {:?}", other),
    }
}

#[test]
fn test_decode_ok() {
    let mut payload = Vec::new();
    payload.extend_from_slice(&42i64.to_le_bytes());
    payload.push(5); // tag length
    payload.extend_from_slice(b"DONE!");

    let ok = pwire::decode_ok(&payload).unwrap();
    assert_eq!(ok.rows_affected, 42);
    assert_eq!(ok.tag, "DONE!");
}

#[test]
fn test_decode_error() {
    let mut payload = Vec::new();
    payload.extend_from_slice(b"42P01");
    payload.extend_from_slice(&13u16.to_le_bytes());
    payload.extend_from_slice(b"table missing");

    let err = pwire::decode_error(&payload).unwrap();
    assert_eq!(err.sql_state, "42P01");
    assert_eq!(err.message, "table missing");
}

#[test]
fn test_read_frame() {
    let frame_data = pwire::frame(0x05, b"test");
    let mut cursor = std::io::Cursor::new(frame_data);
    let (msg_type, payload) = pwire::read_frame(&mut cursor).unwrap();
    assert_eq!(msg_type, 0x05);
    assert_eq!(payload, b"test");
}

#[test]
fn test_read_frame_empty() {
    let frame_data = pwire::frame(0xFF, &[]);
    let mut cursor = std::io::Cursor::new(frame_data);
    let (msg_type, payload) = pwire::read_frame(&mut cursor).unwrap();
    assert_eq!(msg_type, 0xFF);
    assert!(payload.is_empty());
}

// ── Query builder tests ────────────────────────────────────────────────────

#[test]
fn test_query_builder_select() {
    let mut qb = PyroSqlQueryBuilder::new();
    qb.push_sql("SELECT * FROM ");
    qb.push_identifier("users").unwrap();
    qb.push_sql(" WHERE ");
    qb.push_identifier("id").unwrap();
    qb.push_sql(" = ");
    qb.push_bind_param();
    assert_eq!(qb.finish(), "SELECT * FROM \"users\" WHERE \"id\" = $1");
}

#[test]
fn test_query_builder_insert() {
    let mut qb = PyroSqlQueryBuilder::new();
    qb.push_sql("INSERT INTO ");
    qb.push_identifier("orders").unwrap();
    qb.push_sql(" (");
    qb.push_identifier("product_id").unwrap();
    qb.push_sql(", ");
    qb.push_identifier("qty").unwrap();
    qb.push_sql(") VALUES (");
    qb.push_bind_param();
    qb.push_sql(", ");
    qb.push_bind_param();
    qb.push_sql(")");
    assert_eq!(
        qb.finish(),
        "INSERT INTO \"orders\" (\"product_id\", \"qty\") VALUES ($1, $2)"
    );
}

// ── Type system tests ──────────────────────────────────────────────────────

#[test]
fn test_from_sql_i64_from_text() {
    let val = pwire::Value::Text("123".to_string());
    let pv = PyroSqlValue {
        inner: Some(&val),
        type_metadata: PyroSqlTypeMetadata { type_tag: pwire::TYPE_TEXT },
    };
    let result: i64 = FromSql::<BigInt, PyroSqlBackend>::from_sql(pv).unwrap();
    assert_eq!(result, 123);
}

#[test]
fn test_from_sql_bool_from_text() {
    let val = pwire::Value::Text("true".to_string());
    let pv = PyroSqlValue {
        inner: Some(&val),
        type_metadata: PyroSqlTypeMetadata { type_tag: pwire::TYPE_TEXT },
    };
    let result: bool = FromSql::<Bool, PyroSqlBackend>::from_sql(pv).unwrap();
    assert!(result);
}

#[test]
fn test_from_sql_f64_from_i64() {
    let val = pwire::Value::I64(10);
    let pv = PyroSqlValue {
        inner: Some(&val),
        type_metadata: PyroSqlTypeMetadata { type_tag: pwire::TYPE_I64 },
    };
    let result: f64 = FromSql::<Double, PyroSqlBackend>::from_sql(pv).unwrap();
    assert_eq!(result, 10.0);
}
