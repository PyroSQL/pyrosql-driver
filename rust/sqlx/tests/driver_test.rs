//! Tests for the PyroSQL sqlx driver.
//! These validate protocol encoding/decoding, type info, and row operations
//! without requiring a live database connection.

use sqlx_pyrosql::pwire;
use sqlx_pyrosql::type_info::PyroSqlTypeInfo;
use sqlx_pyrosql::row::{PyroSqlColumn, PyroSqlRow, PyroSqlValueOwned, PyroSqlValueRef};
use sqlx_pyrosql::{PyroSql, PyroSqlQueryResult, PyroSqlArguments};
use sqlx_pyrosql::connection::PyroSqlConnectOptions;
use std::sync::Arc;

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
fn test_frame_empty() {
    let frame = pwire::frame(0xFF, &[]);
    assert_eq!(frame.len(), 5);
    assert_eq!(frame[0], 0xFF);
}

#[test]
fn test_encode_auth() {
    let frame = pwire::encode_auth("admin", "secret");
    assert_eq!(frame[0], pwire::MSG_AUTH);
    let payload_len = u32::from_le_bytes([frame[1], frame[2], frame[3], frame[4]]) as usize;
    let payload = &frame[5..5 + payload_len];
    assert_eq!(payload[0], 5); // "admin" len
    assert_eq!(&payload[1..6], b"admin");
    assert_eq!(payload[6], 6); // "secret" len
    assert_eq!(&payload[7..13], b"secret");
}

#[test]
fn test_encode_query() {
    let frame = pwire::encode_query("SELECT 42");
    assert_eq!(frame[0], pwire::MSG_QUERY);
}

#[test]
fn test_encode_prepare() {
    let frame = pwire::encode_prepare("SELECT $1");
    assert_eq!(frame[0], pwire::MSG_PREPARE);
}

#[test]
fn test_encode_execute() {
    let frame = pwire::encode_execute(7, &["abc".into(), "def".into()]);
    assert_eq!(frame[0], pwire::MSG_EXECUTE);
    let payload_len = u32::from_le_bytes([frame[1], frame[2], frame[3], frame[4]]) as usize;
    let payload = &frame[5..5 + payload_len];
    let handle = u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]);
    assert_eq!(handle, 7);
    let param_count = u16::from_le_bytes([payload[4], payload[5]]);
    assert_eq!(param_count, 2);
}

#[test]
fn test_encode_close() {
    let frame = pwire::encode_close(42);
    assert_eq!(frame[0], pwire::MSG_CLOSE);
    let payload = &frame[5..9];
    assert_eq!(u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]), 42);
}

#[test]
fn test_encode_ping_quit() {
    let ping = pwire::encode_ping();
    assert_eq!(ping[0], pwire::MSG_PING);
    assert_eq!(ping.len(), 5);

    let quit = pwire::encode_quit();
    assert_eq!(quit[0], pwire::MSG_QUIT);
    assert_eq!(quit.len(), 5);
}

// ── Decode tests ───────────────────────────────────────────────────────────

#[test]
fn test_decode_result_set_text() {
    let mut payload = Vec::new();
    payload.extend_from_slice(&1u16.to_le_bytes());
    payload.push(4);
    payload.extend_from_slice(b"name");
    payload.push(pwire::TYPE_TEXT);
    payload.extend_from_slice(&1u32.to_le_bytes());
    payload.push(0x00);
    payload.extend_from_slice(&5u16.to_le_bytes());
    payload.extend_from_slice(b"alice");

    let rs = pwire::decode_result_set(&payload).unwrap();
    assert_eq!(rs.columns.len(), 1);
    assert_eq!(rs.columns[0].name, "name");
    assert_eq!(rs.rows.len(), 1);
    match &rs.rows[0][0] {
        pwire::Value::Text(s) => assert_eq!(s, "alice"),
        other => panic!("Expected Text, got {:?}", other),
    }
}

#[test]
fn test_decode_result_set_i64_with_null() {
    let mut payload = Vec::new();
    payload.extend_from_slice(&2u16.to_le_bytes());
    payload.push(2); payload.extend_from_slice(b"id"); payload.push(pwire::TYPE_I64);
    payload.push(4); payload.extend_from_slice(b"name"); payload.push(pwire::TYPE_TEXT);
    payload.extend_from_slice(&1u32.to_le_bytes());
    payload.push(0b00000010);
    payload.extend_from_slice(&42i64.to_le_bytes());

    let rs = pwire::decode_result_set(&payload).unwrap();
    match &rs.rows[0][0] {
        pwire::Value::I64(v) => assert_eq!(*v, 42),
        other => panic!("Expected I64, got {:?}", other),
    }
    assert!(matches!(rs.rows[0][1], pwire::Value::Null));
}

#[test]
fn test_decode_result_set_f64_and_bool() {
    let mut payload = Vec::new();
    payload.extend_from_slice(&2u16.to_le_bytes());
    payload.push(1); payload.extend_from_slice(b"x"); payload.push(pwire::TYPE_F64);
    payload.push(1); payload.extend_from_slice(b"y"); payload.push(pwire::TYPE_BOOL);
    payload.extend_from_slice(&1u32.to_le_bytes());
    payload.push(0x00);
    payload.extend_from_slice(&2.718f64.to_le_bytes());
    payload.push(1);

    let rs = pwire::decode_result_set(&payload).unwrap();
    match &rs.rows[0][0] {
        pwire::Value::F64(v) => assert!((*v - 2.718).abs() < 0.001),
        other => panic!("Expected F64, got {:?}", other),
    }
    match &rs.rows[0][1] {
        pwire::Value::Bool(v) => assert!(*v),
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
    payload.extend_from_slice(&[0xDE, 0xAD, 0xFF]);

    let rs = pwire::decode_result_set(&payload).unwrap();
    match &rs.rows[0][0] {
        pwire::Value::Bytes(v) => assert_eq!(v, &[0xDE, 0xAD, 0xFF]),
        other => panic!("Expected Bytes, got {:?}", other),
    }
}

#[test]
fn test_decode_ok() {
    let mut payload = Vec::new();
    payload.extend_from_slice(&10i64.to_le_bytes());
    payload.push(6);
    payload.extend_from_slice(b"INSERT");

    let ok = pwire::decode_ok(&payload).unwrap();
    assert_eq!(ok.rows_affected, 10);
    assert_eq!(ok.tag, "INSERT");
}

#[test]
fn test_decode_error() {
    let mut payload = Vec::new();
    payload.extend_from_slice(b"42P01");
    payload.extend_from_slice(&11u16.to_le_bytes());
    payload.extend_from_slice(b"not found!");

    // Note: the message is 10 bytes but we said 11, let's fix:
    let mut payload = Vec::new();
    payload.extend_from_slice(b"42P01");
    payload.extend_from_slice(&10u16.to_le_bytes());
    payload.extend_from_slice(b"not found!");

    let err = pwire::decode_error(&payload).unwrap();
    assert_eq!(err.sql_state, "42P01");
    assert_eq!(err.message, "not found!");
}

// ── Type info tests ────────────────────────────────────────────────────────

#[test]
fn test_type_info_from_tag() {
    assert_eq!(PyroSqlTypeInfo::from_tag(pwire::TYPE_I64), PyroSqlTypeInfo::BIGINT);
    assert_eq!(PyroSqlTypeInfo::from_tag(pwire::TYPE_F64), PyroSqlTypeInfo::DOUBLE);
    assert_eq!(PyroSqlTypeInfo::from_tag(pwire::TYPE_TEXT), PyroSqlTypeInfo::TEXT);
    assert_eq!(PyroSqlTypeInfo::from_tag(pwire::TYPE_BOOL), PyroSqlTypeInfo::BOOLEAN);
    assert_eq!(PyroSqlTypeInfo::from_tag(pwire::TYPE_BYTES), PyroSqlTypeInfo::BLOB);
    assert_eq!(PyroSqlTypeInfo::from_tag(pwire::TYPE_NULL), PyroSqlTypeInfo::NULL);
}

#[test]
fn test_type_info_properties() {
    assert!(PyroSqlTypeInfo::BIGINT.is_numeric());
    assert!(PyroSqlTypeInfo::DOUBLE.is_numeric());
    assert!(!PyroSqlTypeInfo::TEXT.is_numeric());
    assert!(PyroSqlTypeInfo::TEXT.is_text());
    assert!(PyroSqlTypeInfo::BLOB.is_binary());
    assert!(PyroSqlTypeInfo::NULL.is_null());
}

// ── Row tests ──────────────────────────────────────────────────────────────

fn make_test_row() -> PyroSqlRow {
    let columns = Arc::new(vec![
        PyroSqlColumn { ordinal: 0, name: "id".into(), type_info: PyroSqlTypeInfo::BIGINT },
        PyroSqlColumn { ordinal: 1, name: "name".into(), type_info: PyroSqlTypeInfo::TEXT },
        PyroSqlColumn { ordinal: 2, name: "score".into(), type_info: PyroSqlTypeInfo::DOUBLE },
        PyroSqlColumn { ordinal: 3, name: "active".into(), type_info: PyroSqlTypeInfo::BOOLEAN },
        PyroSqlColumn { ordinal: 4, name: "data".into(), type_info: PyroSqlTypeInfo::BLOB },
        PyroSqlColumn { ordinal: 5, name: "nullable".into(), type_info: PyroSqlTypeInfo::TEXT },
    ]);
    let values = vec![
        PyroSqlValueOwned { inner: pwire::Value::I64(42) },
        PyroSqlValueOwned { inner: pwire::Value::Text("alice".into()) },
        PyroSqlValueOwned { inner: pwire::Value::F64(9.5) },
        PyroSqlValueOwned { inner: pwire::Value::Bool(true) },
        PyroSqlValueOwned { inner: pwire::Value::Bytes(vec![1, 2, 3]) },
        PyroSqlValueOwned { inner: pwire::Value::Null },
    ];
    PyroSqlRow { columns, values }
}

#[test]
fn test_row_get_i64() {
    let row = make_test_row();
    assert_eq!(row.get_i64(0), Some(42));
    assert_eq!(row.get_i64(1), None); // text column
}

#[test]
fn test_row_get_string() {
    let row = make_test_row();
    assert_eq!(row.get_string(1), Some("alice".into()));
    assert_eq!(row.get_string(0), Some("42".into())); // i64 -> string
}

#[test]
fn test_row_get_f64() {
    let row = make_test_row();
    assert_eq!(row.get_f64(2), Some(9.5));
    assert_eq!(row.get_f64(0), Some(42.0)); // i64 -> f64
}

#[test]
fn test_row_get_bool() {
    let row = make_test_row();
    assert_eq!(row.get_bool(3), Some(true));
}

#[test]
fn test_row_get_bytes() {
    let row = make_test_row();
    assert_eq!(row.get_bytes(4), Some([1u8, 2, 3].as_slice()));
}

#[test]
fn test_row_is_null() {
    let row = make_test_row();
    assert!(!row.is_null(0));
    assert!(row.is_null(5));
}

#[test]
fn test_row_get_by_name() {
    let row = make_test_row();
    assert_eq!(row.get_by_name("name"), Some("alice".into()));
    assert_eq!(row.get_by_name("id"), Some("42".into()));
    assert_eq!(row.get_by_name("nonexistent"), None);
}

#[test]
fn test_row_out_of_bounds() {
    let row = make_test_row();
    assert!(row.is_null(99)); // out of bounds returns true for null
    assert_eq!(row.get_i64(99), None);
}

// ── Query result tests ─────────────────────────────────────────────────────

#[test]
fn test_query_result() {
    let r = PyroSqlQueryResult::new(5);
    assert_eq!(r.rows_affected, 5);
}

#[test]
fn test_query_result_extend() {
    let mut r = PyroSqlQueryResult::new(3);
    r.extend(vec![PyroSqlQueryResult::new(2), PyroSqlQueryResult::new(4)]);
    assert_eq!(r.rows_affected, 9);
}

// ── Arguments tests ────────────────────────────────────────────────────────

#[test]
fn test_arguments() {
    let mut args = PyroSqlArguments::new();
    args.add(42);
    args.add("hello");
    args.add(3.14);
    assert_eq!(args.values.len(), 3);
    assert_eq!(args.values[0], "42");
    assert_eq!(args.values[1], "hello");
    assert_eq!(args.values[2], "3.14");
}

// ── Connect options tests ──────────────────────────────────────────────────

#[test]
fn test_connect_options_default() {
    let opts = PyroSqlConnectOptions::default();
    assert_eq!(opts.host, "localhost");
    assert_eq!(opts.port, 12520);
}

#[test]
fn test_connect_options_builder() {
    let opts = PyroSqlConnectOptions::new()
        .host("db.example.com")
        .port(9999)
        .username("admin")
        .password("secret")
        .database("mydb");
    assert_eq!(opts.host, "db.example.com");
    assert_eq!(opts.port, 9999);
    assert_eq!(opts.username, "admin");
    assert_eq!(opts.password, "secret");
    assert_eq!(opts.database, "mydb");
}

#[test]
fn test_connect_options_to_url() {
    let opts = PyroSqlConnectOptions::new()
        .host("localhost")
        .port(12520)
        .username("admin")
        .password("secret")
        .database("test");
    assert_eq!(opts.to_url(), "pyrosql://admin:secret@localhost:12520/test");
}

#[test]
fn test_connect_options_from_str() {
    let opts: PyroSqlConnectOptions = "pyrosql://user:pass@myhost:5555/db1".parse().unwrap();
    assert_eq!(opts.host, "myhost");
    assert_eq!(opts.port, 5555);
    assert_eq!(opts.username, "user");
    assert_eq!(opts.password, "pass");
    assert_eq!(opts.database, "db1");
}

#[test]
fn test_connect_options_from_str_invalid() {
    let result: Result<PyroSqlConnectOptions, _> = "postgres://user:pass@host/db".parse();
    assert!(result.is_err());
}
