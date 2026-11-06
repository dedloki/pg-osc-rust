//! Replays the audit table onto the shadow table.
//!
//! Unlike the Ruby version, values are never sent back as text literals: each
//! audit row is copied with INSERT ... SELECT from the audit table itself
//! (it has the same columns and types as the primary table). That keeps NULLs
//! (the Ruby replay dropped NULL values, so `UPDATE ... SET x = NULL` was lost),
//! and lets Postgres evaluate --filter on the row:
//!   INSERT -> insert if the new row matches the filter
//!   UPDATE -> delete by primary key, insert again if the new row matches
//!   DELETE -> delete by primary key
//! Without --filter the filter is simply TRUE.

use crate::ctx::Ctx;
use crate::db::Row;
use crate::sql::{quote_ident, quote_literal};
use anyhow::{anyhow, Result};

impl Ctx {
    pub fn rows_to_play(&mut self, reuse: bool) -> Result<Vec<Row>> {
        let key = quote_ident(
            &self
                .primary_key()?
                .ok_or_else(|| anyhow!("no primary key"))?,
        );
        let sql = format!(
            "SELECT {pk}, {op}, {key} FROM {audit} ORDER BY {pk} LIMIT {limit};",
            pk = self.audit_table_pk,
            op = self.operation_type_column,
            audit = self.audit_table,
            limit = self.opts.pull_batch_count,
        );
        self.db.run(&sql, reuse)
    }

    fn replay_columns(&mut self, reuse: bool) -> Result<(Vec<String>, Vec<String>)> {
        if self.copy_columns_cache.is_none() {
            self.copy_columns_cache = Some(self.copy_columns(reuse)?);
        }
        Ok(self.copy_columns_cache.clone().unwrap())
    }

    pub fn play(&mut self, rows: &[Row], reuse: bool) -> Result<()> {
        let filtered = if self.opts.filter.is_some() {
            " (filtered)"
        } else {
            ""
        };
        crate::info!(format!("Replaying rows{filtered}, count: {}", rows.len()));
        if rows.is_empty() {
            return Ok(());
        }

        let (insert_cols, select_cols) = self.replay_columns(reuse)?;
        let primary_key = self
            .primary_key()?
            .ok_or_else(|| anyhow!("no primary key"))?;
        let filter = self.opts.filter.clone().unwrap_or_else(|| "TRUE".into());
        // the primary key may be renamed by the ALTER
        let shadow_pk = quote_ident(
            self.renamed_columns
                .iter()
                .find(|(old, _)| *old == primary_key)
                .map(|(_, new)| new.as_str())
                .unwrap_or(&primary_key),
        );

        let mut replayed = String::new();
        let mut audit_ids = Vec::with_capacity(rows.len());

        for row in rows {
            let audit_id = quote_literal(row.get(&self.audit_table_pk).unwrap_or_default());
            let pk_value = row.get(&primary_key).map(quote_literal);

            let insert_sql = format!(
                "INSERT INTO {} ({}) SELECT {} FROM {} WHERE {} = {} AND ({});\n",
                self.shadow_table,
                insert_cols.join(","),
                select_cols.join(","),
                self.audit_table,
                self.audit_table_pk,
                audit_id,
                filter
            );
            let delete_sql = match &pk_value {
                Some(v) => format!(
                    "DELETE FROM {} WHERE {}={};\n",
                    self.shadow_table, shadow_pk, v
                ),
                None => String::new(),
            };

            match row.get(&self.operation_type_column) {
                Some("INSERT") => replayed.push_str(&insert_sql),
                Some("UPDATE") => {
                    replayed.push_str(&delete_sql);
                    replayed.push_str(&insert_sql);
                }
                Some("DELETE") => replayed.push_str(&delete_sql),
                _ => {}
            }
            audit_ids.push(audit_id);
        }

        if !replayed.is_empty() {
            self.db.run(&replayed, reuse)?;
        }

        let delete_query = format!(
            "DELETE FROM {} WHERE {} IN ({})",
            self.audit_table,
            self.audit_table_pk,
            audit_ids.join(",")
        );
        self.db.run(&delete_query, reuse)?;
        Ok(())
    }

    /// Replays batches until fewer than delta_count rows are left.
    pub fn replay_until_below_delta(&mut self) -> Result<()> {
        loop {
            self.check_terminate()?;
            let rows = self.rows_to_play(false)?;
            if rows.len() as i64 <= self.opts.delta_count {
                return Ok(());
            }
            self.play(&rows, false)?;
        }
    }
}
