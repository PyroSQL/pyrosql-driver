use pyrosql_dump::pwire::*;

#[test]
fn test_encode_query_frame_structure() {
    let frame = encode_query_frame("SELECT 1");
    // Type byte
    assert_eq!(frame[0], 0x01); // MSG_QUERY
    // Length (LE u32) = 8 bytes for "SELECT 1"
    assert_eq!(frame[1], 8);
    assert_eq!(frame[2], 0);
    assert_eq!(frame[3], 0);
    assert_eq!(frame[4], 0);
    // Payload
    assert_eq!(&frame[5..], b"SELECT 1");
}

#[test]
fn test_encode_query_frame_empty() {
    let frame = encode_query_frame("");
    assert_eq!(frame[0], 0x01);
    assert_eq!(frame[1], 0); // zero length
    assert_eq!(frame.len(), 5); // header only
}

#[test]
fn test_encode_auth_frame_structure() {
    let frame = encode_auth_frame("root", "secret");
    assert_eq!(frame[0], 0x06); // MSG_AUTH
    // Payload: 1 byte user_len + "root" + 1 byte pass_len + "secret" = 1+4+1+6 = 12
    let payload_len = u32::from_le_bytes([frame[1], frame[2], frame[3], frame[4]]);
    assert_eq!(payload_len, 12);
    // user_len
    assert_eq!(frame[5], 4);
    // user
    assert_eq!(&frame[6..10], b"root");
    // pass_len
    assert_eq!(frame[10], 6);
    // password
    assert_eq!(&frame[11..17], b"secret");
}

#[test]
fn test_encode_auth_frame_empty_credentials() {
    let frame = encode_auth_frame("", "");
    assert_eq!(frame[0], 0x06);
    let payload_len = u32::from_le_bytes([frame[1], frame[2], frame[3], frame[4]]);
    assert_eq!(payload_len, 2); // just two length bytes (both 0)
    assert_eq!(frame[5], 0);
    assert_eq!(frame[6], 0);
}

#[test]
fn test_decode_result_set_single_i64_column() {
    let columns = vec![Column {
        name: "id".to_string(),
        type_tag: TYPE_I64,
    }];
    let rows = vec![
        vec![Value::I64(42)],
        vec![Value::I64(-1)],
        vec![Value::I64(0)],
    ];

    let payload = encode_result_set_payload(&columns, &rows);
    let decoded = decode_result_set(&payload).unwrap();

    assert_eq!(decoded.columns.len(), 1);
    assert_eq!(decoded.columns[0].name, "id");
    assert_eq!(decoded.columns[0].type_tag, TYPE_I64);
    assert_eq!(decoded.rows.len(), 3);
    assert_eq!(decoded.rows[0][0], Value::I64(42));
    assert_eq!(decoded.rows[1][0], Value::I64(-1));
    assert_eq!(decoded.rows[2][0], Value::I64(0));
}

#[test]
fn test_decode_result_set_all_types() {
    let columns = vec![
        Column { name: "a".to_string(), type_tag: TYPE_I64 },
        Column { name: "b".to_string(), type_tag: TYPE_F64 },
        Column { name: "c".to_string(), type_tag: TYPE_TEXT },
        Column { name: "d".to_string(), type_tag: TYPE_BOOL },
        Column { name: "e".to_string(), type_tag: TYPE_BYTES },
    ];
    let rows = vec![vec![
        Value::I64(100),
        Value::F64(2.718),
        Value::Text("hello".to_string()),
        Value::Bool(true),
        Value::Bytes(vec![0xDE, 0xAD]),
    ]];

    let payload = encode_result_set_payload(&columns, &rows);
    let decoded = decode_result_set(&payload).unwrap();

    assert_eq!(decoded.columns.len(), 5);
    assert_eq!(decoded.rows.len(), 1);
    let row = &decoded.rows[0];
    assert_eq!(row[0], Value::I64(100));
    assert_eq!(row[1], Value::F64(2.718));
    assert_eq!(row[2], Value::Text("hello".to_string()));
    assert_eq!(row[3], Value::Bool(true));
    assert_eq!(row[4], Value::Bytes(vec![0xDE, 0xAD]));
}

#[test]
fn test_decode_result_set_with_nulls() {
    let columns = vec![
        Column { name: "x".to_string(), type_tag: TYPE_I64 },
        Column { name: "y".to_string(), type_tag: TYPE_TEXT },
        Column { name: "z".to_string(), type_tag: TYPE_BOOL },
    ];
    let rows = vec![
        vec![Value::Null, Value::Text("visible".to_string()), Value::Null],
        vec![Value::I64(5), Value::Null, Value::Bool(false)],
    ];

    let payload = encode_result_set_payload(&columns, &rows);
    let decoded = decode_result_set(&payload).unwrap();

    assert_eq!(decoded.rows.len(), 2);
    assert_eq!(decoded.rows[0][0], Value::Null);
    assert_eq!(decoded.rows[0][1], Value::Text("visible".to_string()));
    assert_eq!(decoded.rows[0][2], Value::Null);
    assert_eq!(decoded.rows[1][0], Value::I64(5));
    assert_eq!(decoded.rows[1][1], Value::Null);
    assert_eq!(decoded.rows[1][2], Value::Bool(false));
}

#[test]
fn test_decode_result_set_empty_rows() {
    let columns = vec![Column {
        name: "id".to_string(),
        type_tag: TYPE_I64,
    }];
    let rows: Vec<Vec<Value>> = vec![];

    let payload = encode_result_set_payload(&columns, &rows);
    let decoded = decode_result_set(&payload).unwrap();

    assert_eq!(decoded.columns.len(), 1);
    assert_eq!(decoded.rows.len(), 0);
}

#[test]
fn test_decode_result_set_empty_string_value() {
    let columns = vec![Column {
        name: "val".to_string(),
        type_tag: TYPE_TEXT,
    }];
    let rows = vec![vec![Value::Text("".to_string())]];

    let payload = encode_result_set_payload(&columns, &rows);
    let decoded = decode_result_set(&payload).unwrap();

    assert_eq!(decoded.rows[0][0], Value::Text("".to_string()));
}

#[test]
fn test_decode_result_set_many_columns_null_bitmap() {
    // 10 columns to test multi-byte null bitmap
    let columns: Vec<Column> = (0..10)
        .map(|i| Column {
            name: format!("c{}", i),
            type_tag: TYPE_I64,
        })
        .collect();

    // Alternate null and non-null
    let row: Vec<Value> = (0..10)
        .map(|i| {
            if i % 2 == 0 {
                Value::Null
            } else {
                Value::I64(i as i64)
            }
        })
        .collect();

    let payload = encode_result_set_payload(&columns, &vec![row.clone()]);
    let decoded = decode_result_set(&payload).unwrap();

    assert_eq!(decoded.rows.len(), 1);
    for i in 0..10 {
        if i % 2 == 0 {
            assert_eq!(decoded.rows[0][i], Value::Null, "column {} should be null", i);
        } else {
            assert_eq!(
                decoded.rows[0][i],
                Value::I64(i as i64),
                "column {} should be {}",
                i,
                i
            );
        }
    }
}

#[test]
fn test_decode_result_set_malformed_too_short() {
    let result = decode_result_set(&[]);
    assert!(result.is_err());

    let result = decode_result_set(&[0]);
    assert!(result.is_err());
}

#[test]
fn test_decode_result_set_malformed_missing_row_count() {
    // 1 column, but no row count after column def
    let columns = vec![Column {
        name: "a".to_string(),
        type_tag: TYPE_I64,
    }];
    let payload = encode_result_set_payload(&columns, &[]);
    // Truncate before row count
    let truncated = &payload[..payload.len() - 4];
    let result = decode_result_set(truncated);
    assert!(result.is_err());
}

#[test]
fn test_encode_ok_frame_structure() {
    let frame = encode_ok_frame(42, "INSERT");
    assert_eq!(frame[0], 0x02); // RESP_OK
    let payload_len = u32::from_le_bytes([frame[1], frame[2], frame[3], frame[4]]);
    // 8 (rows_affected) + 1 (tag_len) + 6 (INSERT) = 15
    assert_eq!(payload_len, 15);
    // rows_affected
    let ra = u64::from_le_bytes([
        frame[5], frame[6], frame[7], frame[8], frame[9], frame[10], frame[11], frame[12],
    ]);
    assert_eq!(ra, 42);
    // tag_len
    assert_eq!(frame[13], 6);
    // tag
    assert_eq!(&frame[14..20], b"INSERT");
}

#[test]
fn test_encode_error_frame_structure() {
    let frame = encode_error_frame("42000", "syntax error");
    assert_eq!(frame[0], 0x03); // RESP_ERROR
    // sqlstate
    assert_eq!(&frame[5..10], b"42000");
    // message length
    let msg_len = u16::from_le_bytes([frame[10], frame[11]]);
    assert_eq!(msg_len, 12); // "syntax error"
    // message
    assert_eq!(&frame[12..24], b"syntax error");
}

#[test]
fn test_roundtrip_result_set_f64_precision() {
    let columns = vec![Column {
        name: "val".to_string(),
        type_tag: TYPE_F64,
    }];
    let original = std::f64::consts::PI;
    let rows = vec![vec![Value::F64(original)]];

    let payload = encode_result_set_payload(&columns, &rows);
    let decoded = decode_result_set(&payload).unwrap();

    match &decoded.rows[0][0] {
        Value::F64(v) => assert_eq!(*v, original),
        other => panic!("Expected F64, got {:?}", other),
    }
}

#[test]
fn test_roundtrip_result_set_large_text() {
    let columns = vec![Column {
        name: "data".to_string(),
        type_tag: TYPE_TEXT,
    }];
    let big_string = "x".repeat(60000);
    let rows = vec![vec![Value::Text(big_string.clone())]];

    let payload = encode_result_set_payload(&columns, &rows);
    let decoded = decode_result_set(&payload).unwrap();

    assert_eq!(decoded.rows[0][0], Value::Text(big_string));
}

#[test]
fn test_roundtrip_bool_values() {
    let columns = vec![Column {
        name: "flag".to_string(),
        type_tag: TYPE_BOOL,
    }];
    let rows = vec![vec![Value::Bool(true)], vec![Value::Bool(false)]];

    let payload = encode_result_set_payload(&columns, &rows);
    let decoded = decode_result_set(&payload).unwrap();

    assert_eq!(decoded.rows[0][0], Value::Bool(true));
    assert_eq!(decoded.rows[1][0], Value::Bool(false));
}

#[test]
fn test_encode_result_set_frame_has_correct_type() {
    let columns = vec![Column {
        name: "id".to_string(),
        type_tag: TYPE_I64,
    }];
    let rows = vec![vec![Value::I64(1)]];
    let frame = encode_result_set_frame(&columns, &rows);
    assert_eq!(frame[0], 0x01); // RESP_RESULT_SET
}

#[test]
fn test_all_nulls_row() {
    let columns = vec![
        Column { name: "a".to_string(), type_tag: TYPE_I64 },
        Column { name: "b".to_string(), type_tag: TYPE_TEXT },
        Column { name: "c".to_string(), type_tag: TYPE_BOOL },
    ];
    let rows = vec![vec![Value::Null, Value::Null, Value::Null]];

    let payload = encode_result_set_payload(&columns, &rows);
    let decoded = decode_result_set(&payload).unwrap();

    assert_eq!(decoded.rows[0][0], Value::Null);
    assert_eq!(decoded.rows[0][1], Value::Null);
    assert_eq!(decoded.rows[0][2], Value::Null);
}
