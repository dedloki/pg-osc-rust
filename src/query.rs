//! Catalog queries and SQL builders (the Ruby Query class).

use crate::ctx::Ctx;
use crate::db::{sqlstate, Db, Row};
use crate::sql::{escape_string, quote_ident, quote_literal};
use anyhow::Result;
use postgres::error::SqlState;

pub const LOCK_ATTEMPT: u32 = 4;

#[derive(Clone, Debug)]
pub struct Column {
    /// quoted
    pub column_name: String,
    pub column_name_regular: String,
    pub type_: String,
    pub column_position: i32,
}

#[derive(Clone, Debug)]
pub struct Constraint {
    pub table_on: String,
    pub table_from: Option<String>,
    pub constraint_type: String,
    pub constraint_name: String,
    pub definition: String,
}

fn val(row: &Row, col: &str) -> String {
    row.get(col).unwrap_or_default().to_string()
}

/// audit tables of running (or crashed) pg-osc operations on the table,
/// see Ctx::new for the naming
pub fn audit_tables_for(db: &mut Db, schema: &str, table: &str) -> Result<Vec<String>> {
    let prefix = quote_literal(&format!("pgosc_at_{}_", table.to_lowercase()));
    let query = format!(
        "SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE c.relkind = 'r' AND n.nspname = {schema}
           AND left(c.relname, length({prefix})) = {prefix}
           AND substr(c.relname, length({prefix}) + 1) ~ '^[0-9a-f]{{6}}$'
         ORDER BY 1",
        schema = quote_literal(schema),
    );
    Ok(db
        .run(&query, false)?
        .iter()
        .map(|r| val(r, "relname"))
        .collect())
}

pub fn get_table_size(db: &mut Db, schema: &str, table_name: &str) -> i64 {
    let rel = quote_literal(&format!("{}.{}", quote_ident(schema), table_name));
    match db.first_value(&format!("SELECT pg_table_size({rel});"), false) {
        Ok(v) => v.and_then(|v| v.parse().ok()).unwrap_or(0),
        Err(e) => {
            crate::error!(format!("Error getting table size: {e}"));
            0
        }
    }
}

/// An index of the shadow table, with the constraint it backs (p, u or x).
#[derive(Clone, Debug)]
pub struct ShadowIndex {
    /// as regclass prints it (quoted if needed)
    pub name: String,
    pub indexdef: String,
    pub is_primary: bool,
    pub conname: Option<String>,
    pub contype: Option<String>,
    pub condef: Option<String>,
}

impl Ctx {
    pub fn server_version_num(&mut self) -> Result<i64> {
        Ok(self
            .db
            .first_value("SHOW server_version_num;", false)?
            .and_then(|v| v.parse().ok())
            .unwrap_or(0))
    }

    /// Heap pages of the table right now.
    pub fn relation_pages(&mut self, table: &str) -> Result<u64> {
        let sql = format!(
            "SELECT pg_relation_size({}::regclass) / current_setting('block_size')::bigint;",
            quote_literal(&self.qualified(table))
        );
        Ok(self
            .db
            .first_value(&sql, false)?
            .and_then(|v| v.parse().ok())
            .unwrap_or(0))
    }

    /// Changes on every rewrite of the table (VACUUM FULL, CLUSTER, TRUNCATE,
    /// rewriting ALTER TABLE, pg_repack, pg_squeeze), so ctids may have moved.
    pub fn relation_filenode(&mut self, table: &str) -> Result<String> {
        self.filenode_query(table, false)
    }

    /// Same, inside the open transaction.
    pub fn relation_filenode_in_tx(&mut self, table: &str) -> Result<String> {
        self.filenode_query(table, true)
    }

    fn filenode_query(&mut self, table: &str, reuse: bool) -> Result<String> {
        let sql = format!(
            "SELECT pg_relation_filenode({}::regclass);",
            quote_literal(&self.qualified(table))
        );
        Ok(self.db.first_value(&sql, reuse)?.unwrap_or_default())
    }

    /// Indexes of the shadow table, primary key first.
    pub fn shadow_indexes(&mut self, reuse: bool) -> Result<Vec<ShadowIndex>> {
        let sql = format!(
            "SELECT i.indexrelid::regclass::text AS name, pg_get_indexdef(i.indexrelid) AS indexdef,
                    i.indisprimary AS is_primary, c.conname, c.contype, pg_get_constraintdef(c.oid) AS condef
             FROM pg_index i
             LEFT JOIN pg_constraint c ON c.conindid = i.indexrelid AND c.conrelid = i.indrelid
                                      AND c.contype IN ('p', 'u', 'x')
             WHERE i.indrelid = {}::regclass
             ORDER BY i.indisprimary DESC, i.indexrelid;",
            quote_literal(&self.qualified(&self.shadow_table))
        );
        Ok(self
            .db
            .run(&sql, reuse)?
            .iter()
            .map(|r| ShadowIndex {
                name: val(r, "name"),
                indexdef: val(r, "indexdef"),
                is_primary: r.get("is_primary") == Some("t"),
                conname: r.get("conname").map(str::to_string),
                contype: r.get("contype").map(str::to_string),
                condef: r.get("condef").map(str::to_string),
            })
            .collect())
    }

    /// Primary key column of the shadow table (renamed by the ALTER, if so), quoted.
    pub fn shadow_primary_key(&mut self) -> Result<String> {
        let pk = self
            .primary_key()?
            .ok_or_else(|| anyhow::anyhow!("no primary key"))?;
        Ok(quote_ident(
            self.renamed_columns
                .iter()
                .find(|(old, _)| *old == pk)
                .map(|(_, new)| new.as_str())
                .unwrap_or(&pk),
        ))
    }

    /// schema-qualified, for regclass casts
    pub(crate) fn qualified(&self, table: &str) -> String {
        format!("{}.{}", quote_ident(&self.opts.schema), table)
    }

    pub fn table_columns(&mut self, table: Option<&str>, reuse: bool) -> Result<Vec<Column>> {
        let table = table.unwrap_or(&self.opts.table_name).to_string();
        let sql = format!(
            "SELECT attname as column_name, format_type(atttypid, atttypmod) as type, attnum as column_position FROM pg_attribute
             WHERE attrelid = {}::regclass AND attnum > 0 AND NOT attisdropped
             ORDER BY attnum;",
            quote_literal(&table)
        );
        Ok(self
            .db
            .run(&sql, reuse)?
            .iter()
            .map(|r| Column {
                column_name: quote_ident(&val(r, "column_name")),
                column_name_regular: val(r, "column_name"),
                type_: val(r, "type"),
                column_position: val(r, "column_position").parse().unwrap_or(0),
            })
            .collect())
    }

    pub fn get_indexes_for(&mut self, table: &str) -> Result<Vec<String>> {
        let sql = format!(
            "SELECT indexdef, schemaname FROM pg_indexes WHERE schemaname = {} AND tablename = {}",
            quote_literal(&self.opts.schema),
            quote_literal(table)
        );
        Ok(self
            .db
            .run(&sql, false)?
            .iter()
            .map(|r| val(r, "indexdef"))
            .collect())
    }

    pub fn get_sequence_name(&mut self, table: &str, column: &str) -> Result<Option<String>> {
        let sql = format!(
            "SELECT pg_get_serial_sequence({}, {});",
            quote_literal(table),
            quote_literal(column)
        );
        self.db.first_value(&sql, false)
    }

    pub fn get_triggers_for(&mut self, table: &str) -> Result<String> {
        let sql = format!(
            "SELECT pg_get_triggerdef(oid) as tdef FROM pg_trigger
             WHERE tgrelid = {}::regclass AND tgisinternal = FALSE;",
            quote_literal(&self.qualified(table))
        );
        let triggers: Vec<String> = self
            .db
            .run(&sql, false)?
            .iter()
            .map(|r| format!("{};", val(r, "tdef")))
            .collect();
        Ok(triggers.join(";"))
    }

    pub fn get_all_constraints(&mut self) -> Result<Vec<Constraint>> {
        let sql = "SELECT conrelid::regclass AS table_on,
                          confrelid::regclass AS table_from,
                          contype as constraint_type,
                          conname AS constraint_name,
                          convalidated AS constraint_validated,
                          pg_get_constraintdef(oid) AS definition
                   FROM pg_constraint
                   WHERE contype IN ('f', 'p')";
        Ok(self
            .db
            .run(sql, false)?
            .iter()
            .map(|r| Constraint {
                table_on: val(r, "table_on"),
                // confrelid = 0 for primary keys
                table_from: r
                    .get("table_from")
                    .filter(|v| *v != "-")
                    .map(str::to_string),
                constraint_type: val(r, "constraint_type"),
                constraint_name: val(r, "constraint_name"),
                definition: val(r, "definition"),
            })
            .collect())
    }

    pub fn get_primary_keys_for(&mut self, table: &str) -> Result<Vec<Constraint>> {
        Ok(self
            .get_all_constraints()?
            .into_iter()
            .filter(|c| c.table_on == table && c.constraint_type == "p")
            .collect())
    }

    pub fn get_foreign_keys_for(&mut self, table: &str) -> Result<Vec<Constraint>> {
        Ok(self
            .get_all_constraints()?
            .into_iter()
            .filter(|c| c.table_on == table && c.constraint_type == "f")
            .collect())
    }

    fn add_constraint_statement(c: &Constraint) -> String {
        if c.definition.ends_with("NOT VALID") {
            format!(
                "ALTER TABLE {} ADD CONSTRAINT {} {};",
                c.table_on,
                quote_ident(&c.constraint_name),
                c.definition
            )
        } else {
            format!(
                "ALTER TABLE {} ADD CONSTRAINT {} {} NOT VALID;",
                c.table_on,
                quote_ident(&c.constraint_name),
                c.definition
            )
        }
    }

    pub fn referential_foreign_keys_to_refresh(&mut self, table: &str) -> Result<String> {
        Ok(self
            .get_all_constraints()?
            .iter()
            .filter(|c| c.table_from.as_deref() == Some(table) && c.constraint_type == "f")
            .map(|c| {
                format!(
                    "ALTER TABLE {} DROP CONSTRAINT {}; {}",
                    c.table_on,
                    quote_ident(&c.constraint_name),
                    Self::add_constraint_statement(c)
                )
            })
            .collect())
    }

    /// Run in the swap after the renames: the table's own foreign keys move
    /// to the new table. The old table loses them, otherwise it stays a child
    /// of the referenced tables (every ON DELETE CASCADE would also go
    /// through the frozen old table, and a delete there needs its locks).
    pub fn self_foreign_keys_to_refresh(&mut self, table: &str) -> Result<String> {
        let old = self.qualified(&self.old_primary_table);
        Ok(self
            .get_all_constraints()?
            .iter()
            .filter(|c| c.table_on == table && c.constraint_type == "f")
            .map(|c| {
                format!(
                    "{} ALTER TABLE {old} DROP CONSTRAINT {};",
                    Self::add_constraint_statement(c),
                    quote_ident(&c.constraint_name)
                )
            })
            .collect())
    }

    pub fn get_foreign_keys_to_validate(&mut self, table: &str) -> Result<Vec<String>> {
        let all = self.get_all_constraints()?;
        let referential = all
            .iter()
            .filter(|c| c.table_from.as_deref() == Some(table) && c.constraint_type == "f");
        let own = all
            .iter()
            .filter(|c| c.table_on == table && c.constraint_type == "f");
        Ok(referential
            .chain(own)
            .map(|c| {
                format!(
                    "ALTER TABLE {} VALIDATE CONSTRAINT {};",
                    c.table_on,
                    quote_ident(&c.constraint_name)
                )
            })
            .collect())
    }

    pub fn primary_key_for(&mut self, table: &str) -> Result<Option<String>> {
        let sql = format!(
            "SELECT pg_attribute.attname as column_name
             FROM pg_index, pg_class, pg_attribute, pg_namespace
             WHERE pg_class.oid = {}::regclass AND
                   indrelid = pg_class.oid AND
                   nspname = {} AND
                   pg_class.relnamespace = pg_namespace.oid AND
                   pg_attribute.attrelid = pg_class.oid AND
                   pg_attribute.attnum = any(pg_index.indkey)
               AND indisprimary",
            quote_literal(table),
            quote_literal(&self.opts.schema)
        );
        Ok(self
            .db
            .run(&sql, false)?
            .first()
            .map(|r| val(r, "column_name")))
    }

    pub fn primary_key(&mut self) -> Result<Option<String>> {
        if self.primary_key.is_none() {
            let t = self.opts.table_name.clone();
            self.primary_key = self.primary_key_for(&t)?;
        }
        Ok(self.primary_key.clone())
    }

    pub fn storage_parameters_for(&mut self, table: &str, reuse: bool) -> Result<Option<String>> {
        let sql = format!(
            "SELECT array_to_string(c.reloptions, ',') as params FROM pg_class c
             JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE c.relname = {} AND n.nspname = {};",
            quote_literal(table.trim_matches('"')),
            quote_literal(&self.opts.schema)
        );
        self.db.first_value(&sql, reuse)
    }

    /// [(schema-qualified view name, definition)]
    pub fn view_definitions_for(&mut self, table: &str) -> Result<Vec<(String, String)>> {
        let sql = format!(
            "SELECT DISTINCT
               dependent_view.relname AS view_name,
               pg_get_viewdef(dependent_view.oid) AS view_definition,
               view_ns.nspname AS schema_name
             FROM pg_class AS source_table
             JOIN pg_depend ON pg_depend.refobjid = source_table.oid
             JOIN pg_rewrite ON pg_rewrite.oid = pg_depend.objid
             JOIN pg_class AS dependent_view ON dependent_view.oid = pg_rewrite.ev_class
             JOIN pg_namespace AS view_ns ON dependent_view.relnamespace = view_ns.oid
             AND dependent_view.relkind = 'v'
             AND source_table.oid = {}::regclass;",
            // the Ruby version matched the name in every schema and could
            // re-point views of another schema's table
            quote_literal(&self.qualified(table))
        );
        Ok(self
            .db
            .run(&sql, false)?
            .iter()
            .map(|r| {
                (
                    format!(
                        "{}.{}",
                        quote_ident(&val(r, "schema_name")),
                        quote_ident(&val(r, "view_name"))
                    ),
                    val(r, "view_definition").trim().to_string(),
                )
            })
            .collect())
    }

    /// Acquires the lock and keeps the transaction open: on success the caller
    /// must COMMIT. On failure retries (killing backends if asked) and returns false.
    pub fn open_lock_exclusive(&mut self) -> Result<bool> {
        let mut attempts = 1;
        loop {
            self.check_terminate()?;
            let sql = format!(
                "SET lock_timeout = '{}s';\nLOCK TABLE {} IN ACCESS EXCLUSIVE MODE;",
                self.opts.wait_time_for_lock, self.opts.table_name
            );
            match self.db.run(&sql, true) {
                Ok(_) => return Ok(true),
                Err(e)
                    if matches!(
                        sqlstate(&e),
                        Some(&SqlState::LOCK_NOT_AVAILABLE)
                            | Some(&SqlState::IN_FAILED_SQL_TRANSACTION)
                    ) && self.flags.terminating().is_none() =>
                {
                    attempts += 1;
                    if attempts < LOCK_ATTEMPT {
                        crate::info!(format!("Couldn't acquire lock, attempt: {attempts}"));
                        self.db.run("RESET lock_timeout;", false)?;
                        self.kill_backends()?;
                        continue;
                    }
                    crate::info!("Lock acquire failed");
                    self.db.run("RESET lock_timeout;", false)?;
                    return Ok(false);
                }
                Err(e) => return Err(e),
            }
        }
    }

    pub fn kill_backends(&mut self) -> Result<()> {
        if !self.opts.kill_backends {
            return Ok(());
        }
        crate::info!("Terminating other backends");
        let sql = format!(
            "SELECT pg_terminate_backend(pid) FROM pg_locks WHERE locktype = 'relation' AND relation = {}::regclass::oid AND pid <> pg_backend_pid()",
            quote_literal(&self.opts.table_name)
        );
        self.db.run(&sql, false)?;
        Ok(())
    }

    /// (insert into columns, select columns), both quoted: maps the columns of
    /// the primary table onto the shadow table (dropped columns are skipped,
    /// renamed columns get their new name on the insert side).
    pub fn copy_columns(&mut self, reuse: bool) -> Result<(Vec<String>, Vec<String>)> {
        let mut select: Vec<String> = self
            .table_columns(None, reuse)?
            .into_iter()
            .map(|c| c.column_name_regular)
            .collect();
        select.retain(|c| !self.dropped_columns.contains(c));

        let insert: Vec<String> = select
            .iter()
            .map(|c| {
                let renamed = self.renamed_columns.iter().find(|(old, _)| old == c);
                quote_ident(renamed.map(|(_, new)| new).unwrap_or(c))
            })
            .collect();
        let select = select.iter().map(|c| quote_ident(c)).collect();
        Ok((insert, select))
    }

    pub fn copy_data_statement(&mut self, reuse: bool) -> Result<String> {
        let (insert, select) = self.copy_columns(reuse)?;
        let mut sql = format!(
            "INSERT INTO {}({})\nSELECT {}\nFROM ONLY {}\n",
            self.shadow_table,
            insert.join(", "),
            select.join(", "),
            self.opts.table_name
        );
        if let Some(f) = &self.opts.filter {
            sql.push_str(&format!("WHERE ({f})\n"));
        }
        Ok(sql)
    }

    pub fn query_for_primary_key_refresh(
        &mut self,
        primary_key: &str,
        reuse: bool,
    ) -> Result<String> {
        let sql = format!(
            "SELECT pg_get_serial_sequence({}, {}) as sequence_name",
            quote_literal(&self.shadow_table),
            quote_literal(primary_key)
        );
        if self.db.first_value(&sql, reuse)?.is_none() {
            return Ok(String::new());
        }
        Ok(format!(
            "SELECT setval((select pg_get_serial_sequence('{}', '{}')), (SELECT max({}) FROM {}));",
            escape_string(&self.shadow_table),
            escape_string(primary_key),
            primary_key,
            self.opts.table_name
        ))
    }
}
