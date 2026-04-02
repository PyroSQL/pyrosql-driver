use pyrosql_dump::format::*;
use pyrosql_dump::pwire::Value;

#[test]
fn test_escape_string_plain() {
    assert_eq!(escape_string("hello"), "'hello'");
}

#[test]
fn test_escape_string_single_quotes() {
    assert_eq!(escape_string("it's"), "'it''s'");
    assert_eq!(escape_string("a''b"), "'a''''b'");
}

#[test]
fn test_escape_string_backslash() {
    assert_eq!(escape_string("back\\slash"), "'back\\\\slash'");
}

#[test]
fn test_escape_string_null_byte() {
    assert_eq!(escape_string("before\0after"), "'before\\0after'");
}

#[test]
fn test_escape_string_newline() {
    assert_eq!(escape_string("line1\nline2"), "'line1\\nline2'");
}

#[test]
fn test_escape_string_carriage_return() {
    assert_eq!(escape_string("cr\rhere"), "'cr\\rhere'");
}

#[test]
fn test_escape_string_tab() {
    assert_eq!(escape_string("col1\tcol2"), "'col1\\tcol2'");
}

#[test]
fn test_escape_string_backspace() {
    assert_eq!(escape_string("back\x08space"), "'back\\bspace'");
}

#[test]
fn test_escape_string_sub_char() {
    assert_eq!(escape_string("ctrl\x1a"), "'ctrl\\Z'");
}

#[test]
fn test_escape_string_empty() {
    assert_eq!(escape_string(""), "''");
}

#[test]
fn test_escape_string_unicode() {
    // Combining accent is preserved as-is (no normalization)
    assert_eq!(escape_string("cafe\u{0301}"), "'cafe\u{0301}'");
    // Precomposed character
    assert_eq!(escape_string("caf\u{e9}"), "'caf\u{e9}'");
}

#[test]
fn test_escape_string_mixed_special() {
    assert_eq!(
        escape_string("it's a \"test\"\nwith\\stuff"),
        "'it''s a \"test\"\\nwith\\\\stuff'"
    );
}

#[test]
fn test_escape_bytes_empty() {
    assert_eq!(escape_bytes(&[]), "X''");
}

#[test]
fn test_escape_bytes_simple() {
    assert_eq!(escape_bytes(&[0xDE, 0xAD, 0xBE, 0xEF]), "X'deadbeef'");
}

#[test]
fn test_escape_bytes_all_zeros() {
    assert_eq!(escape_bytes(&[0, 0, 0]), "X'000000'");
}

#[test]
fn test_escape_bytes_full_range() {
    assert_eq!(escape_bytes(&[0x00, 0x7F, 0x80, 0xFF]), "X'007f80ff'");
}

#[test]
fn test_quote_identifier_simple() {
    assert_eq!(quote_identifier("users"), "`users`");
}

#[test]
fn test_quote_identifier_with_backtick() {
    assert_eq!(quote_identifier("my`table"), "`my``table`");
}

#[test]
fn test_quote_identifier_with_spaces() {
    assert_eq!(quote_identifier("my table"), "`my table`");
}

#[test]
fn test_quote_identifier_reserved_word() {
    assert_eq!(quote_identifier("select"), "`select`");
}

#[test]
fn test_format_value_null() {
    assert_eq!(format_value(&Value::Null), "NULL");
}

#[test]
fn test_format_value_i64() {
    assert_eq!(format_value(&Value::I64(42)), "42");
    assert_eq!(format_value(&Value::I64(-1)), "-1");
    assert_eq!(format_value(&Value::I64(0)), "0");
    assert_eq!(format_value(&Value::I64(i64::MAX)), "9223372036854775807");
    assert_eq!(format_value(&Value::I64(i64::MIN)), "-9223372036854775808");
}

#[test]
fn test_format_value_f64() {
    let result = format_value(&Value::F64(3.14));
    assert!(result.contains("3.14"));
    // Should be in scientific notation
    assert!(result.contains('e'));
}

#[test]
fn test_format_value_f64_nan() {
    assert_eq!(format_value(&Value::F64(f64::NAN)), "'NaN'");
}

#[test]
fn test_format_value_f64_infinity() {
    assert_eq!(format_value(&Value::F64(f64::INFINITY)), "'Infinity'");
    assert_eq!(format_value(&Value::F64(f64::NEG_INFINITY)), "'-Infinity'");
}

#[test]
fn test_format_value_text() {
    assert_eq!(format_value(&Value::Text("hello".to_string())), "'hello'");
    assert_eq!(
        format_value(&Value::Text("it's".to_string())),
        "'it''s'"
    );
}

#[test]
fn test_format_value_bool() {
    assert_eq!(format_value(&Value::Bool(true)), "TRUE");
    assert_eq!(format_value(&Value::Bool(false)), "FALSE");
}

#[test]
fn test_format_value_bytes() {
    assert_eq!(
        format_value(&Value::Bytes(vec![0xCA, 0xFE])),
        "X'cafe'"
    );
}

#[test]
fn test_format_row() {
    let row = vec![
        Value::I64(1),
        Value::Text("alice".to_string()),
        Value::Null,
        Value::Bool(true),
    ];
    assert_eq!(format_row(&row), "(1, 'alice', NULL, TRUE)");
}

#[test]
fn test_format_row_empty() {
    let row: Vec<Value> = vec![];
    assert_eq!(format_row(&row), "()");
}

#[test]
fn test_format_column_list() {
    let cols = vec!["id".to_string(), "name".to_string(), "email".to_string()];
    assert_eq!(format_column_list(&cols), "(`id`, `name`, `email`)");
}

#[test]
fn test_format_insert_statements_single_batch() {
    let columns = vec!["id".to_string(), "name".to_string()];
    let rows = vec![
        vec![Value::I64(1), Value::Text("alice".to_string())],
        vec![Value::I64(2), Value::Text("bob".to_string())],
    ];
    let stmts = format_insert_statements("users", &columns, &rows, 1000);
    assert_eq!(stmts.len(), 1);
    assert!(stmts[0].starts_with("INSERT INTO `users`"));
    assert!(stmts[0].contains("(1, 'alice')"));
    assert!(stmts[0].contains("(2, 'bob')"));
    assert!(stmts[0].ends_with(';'));
}

#[test]
fn test_format_insert_statements_multiple_batches() {
    let columns = vec!["id".to_string()];
    let rows = vec![
        vec![Value::I64(1)],
        vec![Value::I64(2)],
        vec![Value::I64(3)],
    ];
    let stmts = format_insert_statements("t", &columns, &rows, 2);
    assert_eq!(stmts.len(), 2);
    // First batch: rows 1 and 2
    assert!(stmts[0].contains("(1)"));
    assert!(stmts[0].contains("(2)"));
    // Second batch: row 3
    assert!(stmts[1].contains("(3)"));
    assert!(!stmts[1].contains("(1)"));
}

#[test]
fn test_format_insert_empty_rows() {
    let columns = vec!["id".to_string()];
    let rows: Vec<Vec<Value>> = vec![];
    let stmts = format_insert_statements("t", &columns, &rows, 1000);
    assert!(stmts.is_empty());
}

#[test]
fn test_format_dump_header() {
    let header = format_dump_header("mydb");
    assert!(header.contains("mydb"));
    assert!(header.contains("pyrosql-dump"));
    assert!(header.contains("FOREIGN_KEY_CHECKS = 0"));
}

#[test]
fn test_format_dump_footer() {
    let footer = format_dump_footer();
    assert!(footer.contains("FOREIGN_KEY_CHECKS = 1"));
    assert!(footer.contains("Dump completed"));
}
