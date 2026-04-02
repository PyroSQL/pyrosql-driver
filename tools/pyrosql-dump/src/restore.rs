use anyhow::{Context, Result};
use std::io::Read;

use crate::format;
use crate::pwire::PwireClient;

/// Configuration for a restore operation.
pub struct RestoreConfig {
    pub database: String,
}

/// Restore a database from a SQL dump read from the provided reader (typically stdin).
pub async fn restore_database<R: Read>(
    client: &mut PwireClient,
    config: &RestoreConfig,
    input: &mut R,
) -> Result<()> {
    // Select the database
    client
        .execute(&format!(
            "USE {}",
            format::quote_identifier(&config.database)
        ))
        .await
        .with_context(|| format!("Failed to select database '{}'", config.database))?;

    eprintln!("Connected to database '{}'", config.database);

    // Read entire input
    let mut sql_content = String::new();
    input
        .read_to_string(&mut sql_content)
        .context("Failed to read SQL input")?;

    // Parse into individual statements
    let statements = split_statements(&sql_content);
    let total = statements.len();
    eprintln!("Parsed {} statement(s) from input", total);

    let mut executed = 0u64;
    let mut errors = 0u64;

    for (i, stmt) in statements.iter().enumerate() {
        let trimmed = stmt.trim();
        if trimmed.is_empty() {
            continue;
        }

        // Progress every 100 statements or at milestones
        if (i + 1) % 100 == 0 || i + 1 == total {
            eprintln!("[{}/{}] Executing statements...", i + 1, total);
        }

        match client.query(trimmed).await {
            Ok(_) => {
                // Some statements like SET return result sets; that's fine
                executed += 1;
            }
            Err(e) => {
                eprintln!(
                    "WARNING: Statement {} failed: {}",
                    i + 1,
                    e
                );
                eprintln!("  Statement: {}", truncate_sql(trimmed, 120));
                errors += 1;
            }
        }
    }

    eprintln!(
        "Restore completed: {} executed, {} errors out of {} total statements",
        executed, errors, total
    );

    if errors > 0 {
        eprintln!("WARNING: {} statement(s) failed during restore", errors);
    }

    Ok(())
}

/// Split SQL content into individual statements on semicolons,
/// respecting string literals (single-quoted) so semicolons inside strings
/// are not treated as statement terminators.
fn split_statements(sql: &str) -> Vec<String> {
    let mut statements = Vec::new();
    let mut current = String::new();
    let mut in_single_quote = false;
    let mut in_line_comment = false;
    let mut in_block_comment = false;
    let mut chars = sql.chars().peekable();

    while let Some(ch) = chars.next() {
        // Handle line comments
        if in_line_comment {
            current.push(ch);
            if ch == '\n' {
                in_line_comment = false;
            }
            continue;
        }

        // Handle block comments
        if in_block_comment {
            current.push(ch);
            if ch == '*' {
                if let Some(&'/') = chars.peek() {
                    let slash = chars.next().unwrap();
                    current.push(slash);
                    in_block_comment = false;
                }
            }
            continue;
        }

        // Handle string literals
        if in_single_quote {
            current.push(ch);
            if ch == '\'' {
                // Check for escaped quote ('')
                if let Some(&'\'') = chars.peek() {
                    let next = chars.next().unwrap();
                    current.push(next);
                } else {
                    in_single_quote = false;
                }
            } else if ch == '\\' {
                // Backslash escape inside string
                if chars.peek().is_some() {
                    let next = chars.next().unwrap();
                    current.push(next);
                }
            }
            continue;
        }

        // Start of line comment
        if ch == '-' {
            if let Some(&'-') = chars.peek() {
                let next = chars.next().unwrap();
                current.push(ch);
                current.push(next);
                in_line_comment = true;
                continue;
            }
        }

        // Start of block comment
        if ch == '/' {
            if let Some(&'*') = chars.peek() {
                let next = chars.next().unwrap();
                current.push(ch);
                current.push(next);
                in_block_comment = true;
                continue;
            }
        }

        // Start of string literal
        if ch == '\'' {
            in_single_quote = true;
            current.push(ch);
            continue;
        }

        // Statement terminator
        if ch == ';' {
            let trimmed = current.trim();
            if !trimmed.is_empty() && !is_comment_only(trimmed) {
                statements.push(current.clone());
            }
            current.clear();
            continue;
        }

        current.push(ch);
    }

    // Handle last statement without trailing semicolon
    let trimmed = current.trim();
    if !trimmed.is_empty() && !is_comment_only(trimmed) {
        statements.push(current);
    }

    statements
}

/// Check if a string is only whitespace and/or comments.
fn is_comment_only(s: &str) -> bool {
    let mut remaining = s.trim();
    while !remaining.is_empty() {
        if remaining.starts_with("--") {
            // Skip to end of line
            match remaining.find('\n') {
                Some(pos) => remaining = remaining[pos + 1..].trim(),
                None => return true,
            }
        } else if remaining.starts_with("/*") {
            match remaining.find("*/") {
                Some(pos) => remaining = remaining[pos + 2..].trim(),
                None => return true,
            }
        } else {
            return false;
        }
    }
    true
}

/// Truncate a SQL string for display purposes.
fn truncate_sql(sql: &str, max_len: usize) -> String {
    let one_line = sql.replace('\n', " ");
    if one_line.len() <= max_len {
        one_line
    } else {
        format!("{}...", &one_line[..max_len])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_simple_statements() {
        let sql = "SELECT 1; SELECT 2;";
        let stmts = split_statements(sql);
        assert_eq!(stmts.len(), 2);
        assert_eq!(stmts[0].trim(), "SELECT 1");
        assert_eq!(stmts[1].trim(), "SELECT 2");
    }

    #[test]
    fn test_split_with_string_semicolons() {
        let sql = "INSERT INTO t VALUES ('hello;world'); SELECT 1;";
        let stmts = split_statements(sql);
        assert_eq!(stmts.len(), 2);
        assert!(stmts[0].contains("hello;world"));
    }

    #[test]
    fn test_split_with_comments() {
        let sql = "-- this is a comment\nSELECT 1; /* block ; comment */ SELECT 2;";
        let stmts = split_statements(sql);
        assert_eq!(stmts.len(), 2);
    }

    #[test]
    fn test_split_escaped_quotes() {
        let sql = "INSERT INTO t VALUES ('it''s a test'); SELECT 1;";
        let stmts = split_statements(sql);
        assert_eq!(stmts.len(), 2);
        assert!(stmts[0].contains("it''s a test"));
    }

    #[test]
    fn test_comment_only_detection() {
        assert!(is_comment_only("-- just a comment"));
        assert!(is_comment_only("/* block */"));
        assert!(is_comment_only("-- line\n/* block */"));
        assert!(!is_comment_only("-- comment\nSELECT 1"));
        assert!(!is_comment_only("SELECT 1"));
    }
}
