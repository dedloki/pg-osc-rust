//! Everything that needs the Postgres parser (libpg_query): validating and
//! rewriting the ALTER statement, validating --filter. Pure functions, no DB.

use anyhow::{anyhow, Result};
use pg_query::protobuf::{AlterTableType, SetOperation};
use pg_query::NodeEnum;

fn parse(sql: &str) -> Option<pg_query::ParseResult> {
    pg_query::parse(sql, 0).ok()
}

fn stmts(parsed: &pg_query::ParseResult) -> impl Iterator<Item = Option<&NodeEnum>> {
    parsed
        .protobuf
        .stmts
        .iter()
        .map(|raw| raw.stmt.as_ref().and_then(|n| n.node.as_ref()))
}

/// Relation names of every ALTER TABLE / RENAME statement, in order.
fn relnames(parsed: &pg_query::ParseResult) -> Vec<String> {
    stmts(parsed)
        .filter_map(|node| match node {
            Some(NodeEnum::AlterTableStmt(s)) => s.relation.as_ref().map(|r| r.relname.clone()),
            Some(NodeEnum::RenameStmt(s)) => s.relation.as_ref().map(|r| r.relname.clone()),
            _ => None,
        })
        .collect()
}

/// Every statement is an ALTER TABLE or a RENAME.
pub fn is_alter_statement(query: &str) -> bool {
    let Some(parsed) = parse(query) else {
        return false;
    };
    let mut any = false;
    for node in stmts(&parsed) {
        any = true;
        if !matches!(
            node,
            Some(NodeEnum::AlterTableStmt(_)) | Some(NodeEnum::RenameStmt(_))
        ) {
            return false;
        }
    }
    any
}

/// All statements touch the same table.
pub fn same_table(query: &str) -> bool {
    let Some(parsed) = parse(query) else {
        return false;
    };
    let mut names = relnames(&parsed);
    names.dedup();
    names.sort();
    names.dedup();
    names.len() == 1
}

/// The table the ALTER statement works on (unquoted, as Postgres folded it).
pub fn table(query: &str) -> Result<String> {
    let parsed = parse(query).ok_or_else(|| anyhow!("Not a valid ALTER statement: {query}"))?;
    relnames(&parsed)
        .into_iter()
        .next()
        .ok_or_else(|| anyhow!("Can't find the table in: {query}"))
}

/// The table name to use in SQL: quoted if the user quoted a mixed case name.
pub fn table_name(query: &str, table: &str) -> String {
    let quoted = format!("\"{table}\"");
    if table.chars().any(|c| c.is_ascii_uppercase())
        && query.contains(&quoted)
        && !table.starts_with('"')
    {
        quoted
    } else {
        table.to_string()
    }
}

/// The ALTER statement re-pointed at the shadow table.
pub fn alter_statement_for(query: &str, shadow_table: &str) -> Result<String> {
    let mut parsed = pg_query::parse(query, 0)?;
    for raw in parsed.protobuf.stmts.iter_mut() {
        let Some(node) = raw.stmt.as_mut().and_then(|n| n.node.as_mut()) else {
            continue;
        };
        match node {
            NodeEnum::AlterTableStmt(s) => {
                if let Some(r) = s.relation.as_mut() {
                    r.relname = shadow_table.to_string();
                }
            }
            NodeEnum::RenameStmt(s) => {
                if let Some(r) = s.relation.as_mut() {
                    r.relname = shadow_table.to_string();
                }
            }
            _ => {}
        }
    }
    Ok(parsed.deparse(Default::default())?)
}

pub fn dropped_columns(query: &str) -> Vec<String> {
    let Some(parsed) = parse(query) else {
        return vec![];
    };
    let mut out = vec![];
    for node in stmts(&parsed) {
        if let Some(NodeEnum::AlterTableStmt(s)) = node {
            for cmd in &s.cmds {
                if let Some(NodeEnum::AlterTableCmd(c)) = cmd.node.as_ref() {
                    if c.subtype == AlterTableType::AtDropColumn as i32 {
                        out.push(c.name.clone());
                    }
                }
            }
        }
    }
    out
}

/// (old_name, new_name) for every RENAME COLUMN.
pub fn renamed_columns(query: &str) -> Vec<(String, String)> {
    let Some(parsed) = parse(query) else {
        return vec![];
    };
    stmts(&parsed)
        .filter_map(|node| match node {
            Some(NodeEnum::RenameStmt(s)) => Some((s.subname.clone(), s.newname.clone())),
            _ => None,
        })
        .collect()
}

/// The filter is an SQL boolean expression over the columns of the primary
/// table (original names, before the ALTER), e.g. "email IS NOT NULL".
pub fn valid_filter(filter: &str) -> bool {
    let Some(parsed) = parse(&format!("SELECT 1 FROM t WHERE ({filter})")) else {
        return false;
    };
    let all: Vec<_> = stmts(&parsed).collect();
    if all.len() != 1 {
        return false;
    }
    match all[0] {
        Some(NodeEnum::SelectStmt(s)) => {
            s.op == SetOperation::SetopNone as i32
                && s.limit_count.is_none()
                && s.sort_clause.is_empty()
                && s.where_clause.is_some()
        }
        _ => false,
    }
}

/// Strips whitespace and an optional leading WHERE.
pub fn normalize_filter(filter: Option<&str>) -> Option<String> {
    let f = filter?.trim();
    let f = if f.len() >= 5
        && f[..5].eq_ignore_ascii_case("where")
        && f[5..].starts_with(char::is_whitespace)
    {
        f[5..].trim()
    } else {
        f
    };
    (!f.is_empty()).then(|| f.to_string())
}

/// Same as PG::Connection#quote_ident: always quotes.
pub fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

/// Contents of a '...' literal (standard_conforming_strings = on).
pub fn escape_string(s: &str) -> String {
    s.replace('\'', "''")
}

pub fn quote_literal(s: &str) -> String {
    format!("'{}'", escape_string(s))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alter_statements() {
        assert!(is_alter_statement(
            "ALTER TABLE books ADD COLUMN purchased BOOLEAN DEFAULT FALSE;"
        ));
        assert!(is_alter_statement(
            "ALTER TABLE books RENAME COLUMN email TO new_email;"
        ));
        assert!(!is_alter_statement("CREATE DATABASE foo"));
        assert!(!is_alter_statement(
            "ALTER TABLE books ADD COLUMN x int; DROP TABLE books"
        ));
        assert!(!is_alter_statement("ALTER TABLE"));
        assert!(!is_alter_statement(""));
    }

    #[test]
    fn same_table_check() {
        assert!(same_table(
            "ALTER TABLE books ADD COLUMN a int; ALTER TABLE books RENAME COLUMN b TO c;"
        ));
        assert!(!same_table(
            "ALTER TABLE books ADD COLUMN a int; ALTER TABLE cards ADD COLUMN a int;"
        ));
    }

    #[test]
    fn table_names() {
        assert_eq!(
            table("ALTER TABLE books ADD COLUMN a int").unwrap(),
            "books"
        );
        assert_eq!(
            table("ALTER TABLE books RENAME COLUMN a TO b").unwrap(),
            "books"
        );
        let q = "ALTER TABLE \"Books\" ADD COLUMN a int";
        assert_eq!(table(q).unwrap(), "Books");
        assert_eq!(table_name(q, "Books"), "\"Books\"");
        assert_eq!(table_name("ALTER TABLE books ADD a int", "books"), "books");
    }

    #[test]
    fn rewrite_to_shadow() {
        let q = "ALTER TABLE books ADD COLUMN purchased BOOLEAN DEFAULT FALSE; ALTER TABLE books RENAME COLUMN email TO new_email;";
        assert_eq!(
            alter_statement_for(q, "pgosc_st_books_abc").unwrap(),
            "ALTER TABLE pgosc_st_books_abc ADD COLUMN purchased boolean DEFAULT false; ALTER TABLE pgosc_st_books_abc RENAME COLUMN email TO new_email"
        );
    }

    #[test]
    fn dropped_and_renamed() {
        let q = "ALTER TABLE books DROP COLUMN email; ALTER TABLE books DROP \"Name\"; ALTER TABLE books RENAME COLUMN a TO b;";
        assert_eq!(dropped_columns(q), vec!["email", "Name"]);
        assert_eq!(renamed_columns(q), vec![("a".to_string(), "b".to_string())]);
    }

    #[test]
    fn filters() {
        assert!(valid_filter("email IS NOT NULL"));
        assert!(valid_filter("a > 5 AND (b = 'x' OR c IS NULL)"));
        assert!(!valid_filter("email IS NOT NULL; DROP TABLE books"));
        assert!(!valid_filter("true) UNION (SELECT 1"));
        assert!(!valid_filter("true LIMIT 1"));
        assert!(!valid_filter(""));
        assert_eq!(
            normalize_filter(Some("  WHERE x IS NOT NULL ")).as_deref(),
            Some("x IS NOT NULL")
        );
        assert_eq!(
            normalize_filter(Some("wherever IS NULL")).as_deref(),
            Some("wherever IS NULL")
        );
        assert_eq!(normalize_filter(Some("  ")), None);
        assert_eq!(normalize_filter(None), None);
    }

    #[test]
    fn quoting() {
        assert_eq!(quote_ident("a\"b"), "\"a\"\"b\"");
        assert_eq!(quote_literal("it's"), "'it''s'");
    }
}
