//! Output formatters: pretty table (default), expanded (one row per
//! screen, psql's `\x`), line-delimited JSON, and CSV.

use std::io::Write;

use anyhow::Result;
use comfy_table::{presets::UTF8_FULL, ContentArrangement, Table};
use pyrosql::{QueryResult, Value};
use serde_json::{Map, Value as JsonValue};

use crate::args::OutputFormat;

/// Convert a driver [`Value`] to the textual representation we want to
/// show in tables / CSV.  JSON output uses [`value_to_json`] instead.
fn value_to_text(v: &Value) -> String {
    match v {
        Value::Null => String::new(),
        Value::Bool(true) => "t".into(),
        Value::Bool(false) => "f".into(),
        Value::Int(n) => n.to_string(),
        Value::Float(f) => format!("{f}"),
        Value::Text(s) => s.clone(),
    }
}

fn value_to_json(v: &Value) -> JsonValue {
    match v {
        Value::Null => JsonValue::Null,
        Value::Bool(b) => JsonValue::Bool(*b),
        Value::Int(n) => JsonValue::Number((*n).into()),
        Value::Float(f) => serde_json::Number::from_f64(*f)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null),
        Value::Text(s) => JsonValue::String(s.clone()),
    }
}

/// CSV-escape a value per RFC 4180.
fn csv_escape(s: &str) -> String {
    if s.contains(',') || s.contains('"') || s.contains('\n') || s.contains('\r') {
        let escaped = s.replace('"', "\"\"");
        format!("\"{escaped}\"")
    } else {
        s.to_owned()
    }
}

/// Dispatch a [`QueryResult`] to the right formatter.
pub fn print_result<W: Write>(
    qr: &QueryResult,
    format: OutputFormat,
    expanded: bool,
    out: &mut W,
) -> Result<()> {
    // A bare "rows_affected" style response has no columns; print a
    // status line instead of an empty table.
    if qr.columns.is_empty() {
        if qr.rows_affected > 0 {
            writeln!(out, "{} rows affected", qr.rows_affected)?;
        } else {
            writeln!(out, "OK")?;
        }
        return Ok(());
    }

    match format {
        OutputFormat::Table => {
            if expanded {
                print_expanded(qr, out)
            } else {
                print_table(qr, out)
            }
        }
        OutputFormat::Json => print_json(qr, out),
        OutputFormat::Csv => print_csv(qr, out),
    }
}

fn print_table<W: Write>(qr: &QueryResult, out: &mut W) -> Result<()> {
    let mut t = Table::new();
    t.load_preset(UTF8_FULL)
        .set_content_arrangement(ContentArrangement::Dynamic);
    t.set_header(qr.columns.iter().cloned());
    for row in &qr.rows {
        t.add_row(row.values().iter().map(value_to_text));
    }
    writeln!(out, "{t}")?;
    let n = qr.rows.len();
    writeln!(out, "({n} row{})", if n == 1 { "" } else { "s" })?;
    Ok(())
}

fn print_expanded<W: Write>(qr: &QueryResult, out: &mut W) -> Result<()> {
    let name_width = qr
        .columns
        .iter()
        .map(String::len)
        .max()
        .unwrap_or(0);
    for (i, row) in qr.rows.iter().enumerate() {
        writeln!(out, "-[ RECORD {} ]", i + 1)?;
        for (col, val) in qr.columns.iter().zip(row.values().iter()) {
            writeln!(out, "{col:<name_width$} | {}", value_to_text(val))?;
        }
    }
    let n = qr.rows.len();
    writeln!(out, "({n} row{})", if n == 1 { "" } else { "s" })?;
    Ok(())
}

fn print_json<W: Write>(qr: &QueryResult, out: &mut W) -> Result<()> {
    // One JSON object per line — easy to pipe into jq.
    for row in &qr.rows {
        let mut m = Map::with_capacity(qr.columns.len());
        for (col, val) in qr.columns.iter().zip(row.values().iter()) {
            m.insert(col.clone(), value_to_json(val));
        }
        writeln!(out, "{}", JsonValue::Object(m))?;
    }
    Ok(())
}

fn print_csv<W: Write>(qr: &QueryResult, out: &mut W) -> Result<()> {
    let header = qr
        .columns
        .iter()
        .map(|c| csv_escape(c))
        .collect::<Vec<_>>()
        .join(",");
    writeln!(out, "{header}")?;
    for row in &qr.rows {
        let line = row
            .values()
            .iter()
            .map(|v| csv_escape(&value_to_text(v)))
            .collect::<Vec<_>>()
            .join(",");
        writeln!(out, "{line}")?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use pyrosql::{ColumnMeta, Row};
    use std::sync::Arc;

    fn mk_result() -> QueryResult {
        let columns = vec!["id".to_owned(), "name".to_owned()];
        let meta: Arc<ColumnMeta> = ColumnMeta::new(columns.clone());
        let rows = vec![
            Row::new(meta.clone(), vec![Value::Int(1), Value::Text("alice".into())]),
            Row::new(meta, vec![Value::Int(2), Value::Text("bob, the".into())]),
        ];
        QueryResult { columns, rows, rows_affected: 0 }
    }

    #[test]
    fn csv_quotes_commas() {
        let qr = mk_result();
        let mut buf = Vec::new();
        print_csv(&qr, &mut buf).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(s.contains("\"bob, the\""));
        assert!(s.contains("id,name"));
    }

    #[test]
    fn json_one_per_line() {
        let qr = mk_result();
        let mut buf = Vec::new();
        print_json(&qr, &mut buf).unwrap();
        let s = String::from_utf8(buf).unwrap();
        let lines: Vec<_> = s.lines().collect();
        assert_eq!(lines.len(), 2);
        assert!(lines[0].starts_with('{'));
        assert!(lines[1].contains("alice") || lines[1].contains("bob, the"));
    }

    #[test]
    fn expanded_format_labels_records() {
        let qr = mk_result();
        let mut buf = Vec::new();
        print_expanded(&qr, &mut buf).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(s.contains("RECORD 1"));
        assert!(s.contains("RECORD 2"));
        assert!(s.contains("alice"));
    }

    #[test]
    fn table_format_has_borders() {
        let qr = mk_result();
        let mut buf = Vec::new();
        print_table(&qr, &mut buf).unwrap();
        let s = String::from_utf8(buf).unwrap();
        // Dynamic arrangement may wrap output but should show both names.
        assert!(s.contains("alice"));
        assert!(s.contains("(2 rows)"));
    }

    #[test]
    fn empty_columns_prints_ok_or_affected() {
        let qr = QueryResult { columns: vec![], rows: vec![], rows_affected: 3 };
        let mut buf = Vec::new();
        print_result(&qr, OutputFormat::Table, false, &mut buf).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(s.contains("3 rows affected"));
    }
}
