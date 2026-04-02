use anyhow::{bail, Context, Result};
use std::io::Write;

use crate::format;
use crate::pwire::{PwireClient, Value};

/// Configuration for a dump operation.
pub struct DumpConfig {
    pub database: String,
    pub tables: Option<Vec<String>>,
    pub schema_only: bool,
    pub data_only: bool,
    pub batch_size: usize,
}

/// Run a full database dump, writing SQL to the provided writer (typically stdout).
pub async fn dump_database<W: Write>(
    client: &mut PwireClient,
    config: &DumpConfig,
    out: &mut W,
) -> Result<()> {
    // Select the database
    client
        .execute(&format!("USE {}", format::quote_identifier(&config.database)))
        .await
        .with_context(|| format!("Failed to select database '{}'", config.database))?;

    eprintln!("Connected to database '{}'", config.database);

    // Get the list of tables
    let tables = match &config.tables {
        Some(t) => t.clone(),
        None => {
            let table_list = get_table_list(client).await?;
            eprintln!("Found {} table(s)", table_list.len());
            table_list
        }
    };

    if tables.is_empty() {
        eprintln!("No tables to dump.");
        return Ok(());
    }

    // Write header
    write!(out, "{}", format::format_dump_header(&config.database))?;

    for (i, table) in tables.iter().enumerate() {
        eprintln!(
            "[{}/{}] Dumping table '{}'...",
            i + 1,
            tables.len(),
            table
        );

        // Schema
        if !config.data_only {
            let create_sql = get_create_table(client, table).await?;
            writeln!(out, "--")?;
            writeln!(out, "-- Table structure for {}", format::quote_identifier(table))?;
            writeln!(out, "--\n")?;
            writeln!(
                out,
                "DROP TABLE IF EXISTS {};\n",
                format::quote_identifier(table)
            )?;
            writeln!(out, "{};\n", create_sql)?;
        }

        // Data
        if !config.schema_only {
            dump_table_data(client, table, config.batch_size, out).await?;
        }
    }

    // Write footer
    write!(out, "{}", format::format_dump_footer())?;
    out.flush()?;

    eprintln!("Dump completed successfully.");
    Ok(())
}

/// Get the list of tables in the current database.
async fn get_table_list(client: &mut PwireClient) -> Result<Vec<String>> {
    let rs = client
        .query_resultset("SHOW TABLES")
        .await
        .context("Failed to list tables")?;

    let mut tables = Vec::new();
    for row in &rs.rows {
        if let Some(val) = row.first() {
            match val {
                Value::Text(s) => tables.push(s.clone()),
                Value::Bytes(b) => tables.push(String::from_utf8_lossy(b).to_string()),
                _ => tables.push(format!("{:?}", val)),
            }
        }
    }
    Ok(tables)
}

/// Get the CREATE TABLE statement for a table.
async fn get_create_table(client: &mut PwireClient, table: &str) -> Result<String> {
    let sql = format!("SHOW CREATE TABLE {}", format::quote_identifier(table));
    let rs = client
        .query_resultset(&sql)
        .await
        .with_context(|| format!("Failed to get CREATE TABLE for '{}'", table))?;

    // The CREATE TABLE statement is typically in the second column of the first row.
    if rs.rows.is_empty() {
        bail!("No CREATE TABLE result for '{}'", table);
    }

    let row = &rs.rows[0];
    // Try second column first (common pattern: table_name, create_statement)
    let create_col = if row.len() >= 2 { &row[1] } else { &row[0] };

    match create_col {
        Value::Text(s) => Ok(s.clone()),
        Value::Bytes(b) => Ok(String::from_utf8_lossy(b).to_string()),
        _ => bail!(
            "Unexpected value type for CREATE TABLE of '{}': {:?}",
            table,
            create_col
        ),
    }
}

/// Dump table data as INSERT statements.
async fn dump_table_data<W: Write>(
    client: &mut PwireClient,
    table: &str,
    batch_size: usize,
    out: &mut W,
) -> Result<()> {
    let sql = format!("SELECT * FROM {}", format::quote_identifier(table));
    let rs = client
        .query_resultset(&sql)
        .await
        .with_context(|| format!("Failed to select data from '{}'", table))?;

    if rs.rows.is_empty() {
        eprintln!("  -> 0 rows");
        return Ok(());
    }

    let column_names: Vec<String> = rs.columns.iter().map(|c| c.name.clone()).collect();

    eprintln!("  -> {} row(s)", rs.rows.len());

    writeln!(out, "--")?;
    writeln!(out, "-- Data for {}", format::quote_identifier(table))?;
    writeln!(out, "--\n")?;

    let statements =
        format::format_insert_statements(table, &column_names, &rs.rows, batch_size);
    for stmt in &statements {
        writeln!(out, "{}\n", stmt)?;
    }

    Ok(())
}
