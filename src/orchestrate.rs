//! The whole operation (the Ruby Orchestrate class).

use crate::ctx::{random_id, AccessExclusiveLockNotAcquired, Ctx, Flags};
use crate::db::{is_pg_error, sqlstate, Db};
use crate::functions::{FUNC_CREATE_TABLE_ALL, FUNC_FIX_SERIAL_SEQUENCE};
use crate::options::{Options, RewriteAction};
use crate::query::get_table_size;
use crate::sql::{
    alter_statement_for, dropped_columns, quote_ident, quote_literal, renamed_columns,
};
use anyhow::{anyhow, bail, Result};
use postgres::error::SqlState;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

pub const SWAP_STATEMENT_TIMEOUT: &str = "5s";
pub const TRACK_PROGRESS_INTERVAL: Duration = Duration::from_secs(60);
pub const SWAP_REQUEST_MARKER: &str = "pgosc:swap-requested";

/// Connects, installs the helper functions and collects everything the
/// operation needs to know about the table.
pub fn setup(opts: Options, flags: Arc<Flags>) -> Result<Ctx> {
    let db = Db::connect(&opts.conn)?;
    let _ = crate::db::CANCEL.set(db.cancel_token());
    let mut ctx = Ctx::new(opts, db, flags, &random_id());

    let sql = format!(
        "SET statement_timeout = 0;\nSET client_min_messages = warning;\nSET search_path TO {};",
        quote_ident(&ctx.opts.schema)
    );
    ctx.db.run(&sql, false)?;
    ctx.db.run(FUNC_FIX_SERIAL_SEQUENCE, false)?;
    ctx.db.run(FUNC_CREATE_TABLE_ALL, false)?;
    ctx.server_version = ctx.server_version_num()?;

    let table_name = ctx.opts.table_name.clone();
    ctx.primary_table_storage_parameters = ctx
        .storage_parameters_for(&table_name, false)?
        .unwrap_or_default();
    ctx.referential_foreign_key_statements =
        ctx.referential_foreign_keys_to_refresh(&table_name)?;
    ctx.self_foreign_key_statements = ctx.self_foreign_keys_to_refresh(&table_name)?;
    ctx.trigger_statements = ctx.get_triggers_for(&table_name)?;
    Ok(ctx)
}

/// `perform`: on failure the trigger, audit and shadow tables are removed,
/// the old table never (with --filter it holds the filtered-out rows).
pub fn run(opts: Options, flags: Arc<Flags>) -> Result<()> {
    let target = serde_json::json!({
        "host": opts.conn.host, "port": opts.conn.port, "dbname": opts.conn.dbname,
        "username": opts.conn.user, "schema": opts.schema, "table": opts.table_name,
    });
    // nothing to clean up yet, but the error must not go unnoticed
    let mut ctx = match setup(opts, flags) {
        Ok(ctx) => ctx,
        Err(e) => {
            crate::fatal!(format!("Setup failed: {e:#}"), { "target": target });
            return Err(e);
        }
    };
    ctx.flags.set_cleanup_sql(Some(ctx.manual_cleanup_sql()));
    match run_steps(&mut ctx) {
        Ok(()) => {
            crate::info!("All tasks successfully completed");
            Ok(())
        }
        Err(e) => {
            if let Some(t) = e.downcast_ref::<crate::ctx::Terminated>() {
                crate::info!(format!("Signal {} received, cleaning up", t.0));
            } else {
                crate::fatal!(format!("Something went wrong: {e:#}"), { "e": format!("{e:?}") });
            }
            if let Err(cleanup) = ctx.drop_and_cleanup(false) {
                crate::error!(format!("Cleanup failed: {cleanup:#}. Clean up by hand (drop the trigger first, it needs the lock; until then writes go to the audit table)"),
                    { "cleanup_sql": ctx.manual_cleanup_sql() });
            }
            Err(e)
        }
    }
}

fn run_steps(ctx: &mut Ctx) -> Result<()> {
    if ctx.primary_key()?.is_none() {
        bail!("Parent table has no primary key, exiting...");
    }
    ctx.preflight()?;
    if ctx.batched() {
        let version = ctx.server_version;
        if version < 140000 {
            bail!("--copy-batch-pages needs PostgreSQL 14+ (TID range scans), the server is {version}");
        }
    }

    crate::info!("Performing some house keeping....");
    ctx.run_analyze()?;
    if ctx.opts.skip_vacuum {
        crate::info!("Skipping VACUUM (--skip-vacuum)");
    } else {
        ctx.run_vacuum()?;
    }

    ctx.setup_audit_table()?;
    ctx.setup_trigger()?;
    ctx.setup_shadow_table()?; // starts the serializable transaction (not with --copy-batch-pages)
    ctx.run_alter_statement()?; // same transaction
    ctx.copy_data()?; // same transaction, commits it
    ctx.finish_copy()?; // dedup after batches, deferred indexes
    ctx.check_carry()?; // the swap will be able to carry grants, owner, ...
    ctx.run_analyze()?;
    ctx.replay_and_swap()?;
    ctx.run_analyze()?;
    ctx.validate_constraints()?;
    ctx.replace_views()?;
    ctx.drop_and_cleanup(ctx.opts.drop)?;
    Ok(())
}

impl Ctx {
    /// --copy-batch-pages: copy in short transactions instead of one serializable one
    pub fn batched(&self) -> bool {
        self.opts.copy_batch_pages.is_some()
    }

    pub fn setup_audit_table(&mut self) -> Result<()> {
        crate::info!("Setting up audit table", { "audit_table": self.audit_table });
        let sql = format!(
            "CREATE TABLE {} ({} SERIAL PRIMARY KEY, {} text, {} timestamp, LIKE {}) WITH (autovacuum_enabled = false);",
            self.audit_table, self.audit_table_pk, self.operation_type_column, self.trigger_time_column, self.opts.table_name
        );
        self.db.run(&sql, false)?;
        let (audit, pk) = (self.audit_table.clone(), self.audit_table_pk.clone());
        self.audit_table_pk_sequence = self.get_sequence_name(&audit, &pk)?;
        Ok(())
    }

    /// Takes an ACCESS EXCLUSIVE lock (killing backends if asked) to add the
    /// trigger that feeds the audit table.
    pub fn setup_trigger(&mut self) -> Result<()> {
        let result = (|| -> Result<()> {
            if !self.open_lock_exclusive()? {
                return Err(AccessExclusiveLockNotAcquired.into());
            }
            crate::info!("Setting up triggers");
            let seq = self
                .audit_table_pk_sequence
                .clone()
                .ok_or_else(|| anyhow!("audit table sequence not found"))?;
            // schema-qualified: the trigger runs with the search_path of whoever writes to the table
            let audit = format!(
                "{}.{}",
                quote_ident(&self.opts.schema),
                quote_ident(&self.audit_table)
            );
            let table = &self.opts.table_name;
            let sql = format!(
                r#"DROP TRIGGER IF EXISTS primary_to_audit_table_trigger ON {table};

CREATE OR REPLACE FUNCTION primary_to_audit_table_trigger()
RETURNS TRIGGER AS
$$
BEGIN
  IF ( TG_OP = 'INSERT') THEN
    INSERT INTO {audit} select nextval('{seq}'), 'INSERT', clock_timestamp(), NEW.* ;
    RETURN NEW;
  ELSIF ( TG_OP = 'UPDATE') THEN
    INSERT INTO {audit} select nextval('{seq}'), 'UPDATE', clock_timestamp(),  NEW.* ;
    RETURN NEW;
  ELSIF ( TG_OP = 'DELETE') THEN
    INSERT INTO {audit} select nextval('{seq}'), 'DELETE', clock_timestamp(), OLD.* ;
    RETURN NEW;
  END IF;
END;
$$ LANGUAGE PLPGSQL SECURITY DEFINER;

CREATE TRIGGER primary_to_audit_table_trigger
AFTER INSERT OR UPDATE OR DELETE ON {table}
FOR EACH ROW EXECUTE PROCEDURE primary_to_audit_table_trigger();
"#
            );
            self.db.run(&sql, true)?;
            self.setup_guards()?;
            Ok(())
        })();
        let commit = self.db.run("COMMIT;", false);
        result.and(commit.map(|_| ()))
    }

    /// From here till the end of copy_data everything runs in one serializable
    /// transaction: the audit table is emptied and the data copied in the same
    /// snapshot, so the replay afterwards has no duplicates.
    pub fn setup_shadow_table(&mut self) -> Result<()> {
        crate::info!("Setting up shadow table", { "shadow_table": self.shadow_table });
        let sql = format!(
            "SELECT create_table_all({}, {});",
            quote_literal(&self.opts.table_name),
            quote_literal(&self.shadow_table)
        );
        self.db.run(&sql, false)?;
        let serial = !self.batched();
        if serial {
            self.db
                .run("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE", true)?;
        }
        let sql = format!(
            "SELECT fix_serial_sequence({}, {});",
            quote_literal(&self.opts.table_name),
            quote_literal(&self.shadow_table)
        );
        self.db.run(&sql, serial)?;
        Ok(())
    }

    pub fn run_alter_statement(&mut self) -> Result<()> {
        let statement = alter_statement_for(&self.opts.alter_statement, &self.shadow_table)?;
        crate::info!("Running alter statement on shadow table",
            { "shadow_table": self.shadow_table, "parent_table": self.opts.table_name });
        let serial = !self.batched();
        self.db.run(&statement, serial)?;
        self.dropped_columns = dropped_columns(&self.opts.alter_statement);
        self.renamed_columns = renamed_columns(&self.opts.alter_statement);
        if self.opts.defer_indexes {
            self.drop_shadow_indexes(serial)?;
        }
        Ok(())
    }

    /// --defer-indexes: remember the shadow table's indexes (as they are after
    /// the ALTER) and drop them, finish_copy builds them again.
    fn drop_shadow_indexes(&mut self, reuse: bool) -> Result<()> {
        let indexes = self.shadow_indexes(reuse)?;
        for idx in &indexes {
            let sql = match &idx.conname {
                Some(con) => format!(
                    "ALTER TABLE {} DROP CONSTRAINT {};",
                    self.shadow_table,
                    quote_ident(con)
                ),
                None => format!("DROP INDEX {};", idx.name),
            };
            self.db.run(&sql, reuse)?;
        }
        crate::info!("Indexes of the shadow table deferred till after the copy",
            { "indexes": indexes.iter().map(|i| i.name.clone()).collect::<Vec<_>>() });
        self.deferred_indexes = indexes;
        Ok(())
    }

    pub fn copy_data(&mut self) -> Result<()> {
        if self.batched() {
            return self.copy_data_batched();
        }
        let finished = Arc::new(AtomicBool::new(false));
        let result = (|| -> Result<()> {
            crate::info!("Clearing contents of audit table before copy..",
                { "shadow_table": self.shadow_table, "parent_table": self.opts.table_name });
            let sql = format!("DELETE FROM {}", self.audit_table);
            self.db.run(&sql, true)?;

            if let Some(statement) = self.opts.copy_statement.clone() {
                let query = statement.replace("%{shadow_table}", &self.shadow_table);
                self.db.run(&query, true)?;
                return Ok(());
            }

            crate::info!("Copying contents..",
                { "shadow_table": self.shadow_table, "parent_table": self.opts.table_name });
            self.log_progress(finished.clone());
            let sql = self.copy_data_statement(true)?;
            self.db.run(&sql, true)?;
            Ok(())
        })();
        finished.store(true, Ordering::SeqCst);
        let commit = self.db.run("COMMIT;", false); // commits the serializable transaction
        result.and(commit.map(|_| ()))
    }

    /// --copy-batch-pages: INSERT ... SELECT by ctid ranges, each batch in its
    /// own transaction. Without one snapshot for the whole copy a row changed
    /// meanwhile can be copied twice (old and new version in different
    /// batches) or missed (new version in an already copied range). Every such
    /// row was changed after the trigger was set up, so its primary key is in
    /// the audit table: finish_copy removes those rows from the shadow table
    /// and the replay of the full audit table brings them back in their final
    /// state. That's why the audit table is not emptied here.
    fn copy_data_batched(&mut self) -> Result<()> {
        let pages_per_batch = self.opts.copy_batch_pages.unwrap_or(1);
        let table = self.opts.table_name.clone();
        // before the page count: a rewrite in between is caught by the first batch
        let mut filenode = self.relation_filenode(&table)?;
        let mut total_pages = self.relation_pages(&table)?;
        let (insert, select) = self.copy_columns(false)?;
        let filter = self
            .opts
            .filter
            .as_ref()
            .map(|f| format!(" AND ({f})"))
            .unwrap_or_default();
        // with the indexes in place a second version of a row would violate
        // the primary key: skip it, finish_copy removes the row anyway
        let on_conflict = if self.opts.defer_indexes {
            ""
        } else {
            " ON CONFLICT DO NOTHING"
        };

        crate::info!("Copying contents in batches..", {
            "shadow_table": self.shadow_table, "parent_table": table,
            "pages": total_pages, "pages_per_batch": pages_per_batch,
        });

        let started = Instant::now();
        let mut last_log = Instant::now();
        let mut copied: u64 = 0;
        let mut start: u64 = 0;
        let mut restarts = 0u32;
        loop {
            self.check_terminate()?;
            let end = start + pages_per_batch;
            // the last batch is open ended: pages added during the copy hold
            // only rows written after the trigger, copying them is harmless
            let last = end >= total_pages;
            let range = if last {
                format!("ctid >= '({start},0)'::tid")
            } else {
                format!("ctid >= '({start},0)'::tid AND ctid < '({end},0)'::tid")
            };
            let sql = format!(
                "INSERT INTO {} ({}) SELECT {} FROM ONLY {} WHERE {}{}{};",
                self.shadow_table,
                insert.join(", "),
                select.join(", "),
                table,
                range,
                filter,
                on_conflict
            );
            // the INSERT locks the table till the end of the batch, a rewrite
            // can't happen after it: check in the same transaction
            let n = self.db.execute(&sql, true)?;
            let now = self.relation_filenode_in_tx(&table)?;
            if now != filenode {
                self.db.run("ROLLBACK;", false)?;
                let msg = format!(
                    "The table was rewritten during the batched copy (VACUUM FULL, CLUSTER, pg_repack, pg_squeeze, TRUNCATE or a rewriting ALTER TABLE): row addresses changed, the pages copied so far don't line up with the rest. Relfilenode {filenode} -> {now}"
                );
                match self.opts.on_table_rewrite {
                    RewriteAction::Fail => bail!("{msg}. Don't run those on the table during the copy, or use --on-table-rewrite restart"),
                    RewriteAction::Restart => {
                        crate::warn!(format!("{msg}. Restarting the copy from the first page (--on-table-rewrite restart)"));
                        // the audit table is kept: it has every change since the trigger
                        self.db.run(&format!("TRUNCATE {};", self.shadow_table), false)?;
                        filenode = self.relation_filenode(&table)?;
                        total_pages = self.relation_pages(&table)?;
                        copied = 0;
                        start = 0;
                        restarts += 1;
                        crate::info!("Copying contents in batches again..", { "pages": total_pages, "restarts": restarts });
                        continue;
                    }
                }
            }
            self.db.run("COMMIT;", false)?;
            copied += n;

            let done = end.min(total_pages);
            if last || last_log.elapsed() >= TRACK_PROGRESS_INTERVAL {
                let pct = if total_pages == 0 {
                    100.0
                } else {
                    done as f64 / total_pages as f64 * 100.0
                };
                let elapsed = started.elapsed().as_secs();
                let eta = if done > 0 && !last {
                    (elapsed as f64 * (total_pages - done) as f64 / done as f64) as u64
                } else {
                    0
                };
                crate::info!(format!("Copy progress: {pct:.2}% of pages"), {
                    "pages_done": done, "pages_total": total_pages, "rows_copied": copied,
                    "elapsed_s": elapsed, "eta_s": eta,
                });
                last_log = Instant::now();
            }
            if last {
                break;
            }
            start = end;
        }
        crate::info!("Batched copy done", { "rows_copied": copied, "elapsed_s": started.elapsed().as_secs() });
        Ok(())
    }

    /// After the copy: drop rows changed during a batched copy (the replay
    /// rebuilds them), build the deferred indexes, ANALYZE the shadow table.
    pub fn finish_copy(&mut self) -> Result<()> {
        if self.batched() {
            self.check_terminate()?;
            let pk = self
                .primary_key()?
                .ok_or_else(|| anyhow!("no primary key"))?;
            let shadow_pk = self.shadow_primary_key()?;
            let started = Instant::now();
            let sql = format!(
                "DELETE FROM {} s USING (SELECT DISTINCT {} AS k FROM {}) a WHERE s.{} = a.k;",
                self.shadow_table,
                quote_ident(&pk),
                self.audit_table,
                shadow_pk
            );
            let removed = self.db.execute(&sql, false)?;
            crate::info!("Removed rows changed during the copy, the replay brings them back",
                { "rows": removed, "elapsed_s": started.elapsed().as_secs() });
        }

        if self.opts.defer_indexes && !self.deferred_indexes.is_empty() {
            self.build_deferred_indexes()?;
        }

        if self.batched() || self.opts.defer_indexes {
            crate::info!("Performing ANALYZE on the shadow table!");
            self.db
                .exec_raw(&format!("ANALYZE {};", self.shadow_table))?;
        }
        Ok(())
    }

    fn build_deferred_indexes(&mut self) -> Result<()> {
        if let Some(mem) = self.opts.maintenance_work_mem.clone() {
            self.db.run(
                &format!("SET maintenance_work_mem = {};", quote_literal(&mem)),
                false,
            )?;
        }
        let indexes = std::mem::take(&mut self.deferred_indexes);
        let total = indexes.len();
        for (n, idx) in indexes.iter().enumerate() {
            self.check_terminate()?;
            crate::info!(format!("Building index {}/{}", n + 1, total),
                { "index": idx.name, "constraint": idx.conname });
            let started = Instant::now();
            match (idx.contype.as_deref(), &idx.conname) {
                (Some(kind @ ("p" | "u")), Some(con)) => {
                    self.db.run(&idx.indexdef, false)?;
                    let condef = idx.condef.clone().unwrap_or_default();
                    let mut deferrable = String::new();
                    if condef.contains("INITIALLY DEFERRED") {
                        deferrable = " DEFERRABLE INITIALLY DEFERRED".into();
                    } else if condef.contains("DEFERRABLE") && !condef.contains("NOT DEFERRABLE") {
                        deferrable = " DEFERRABLE".into();
                    }
                    let sql = format!(
                        "ALTER TABLE {} ADD CONSTRAINT {} {} USING INDEX {}{};",
                        self.shadow_table,
                        quote_ident(con),
                        if kind == "p" { "PRIMARY KEY" } else { "UNIQUE" },
                        idx.name,
                        deferrable
                    );
                    self.db.run(&sql, false)?;
                }
                (Some("x"), Some(con)) => {
                    let sql = format!(
                        "ALTER TABLE {} ADD CONSTRAINT {} {};",
                        self.shadow_table,
                        quote_ident(con),
                        idx.condef.clone().unwrap_or_default()
                    );
                    self.db.run(&sql, false)?;
                }
                _ => {
                    self.db.run(&idx.indexdef, false)?;
                }
            }
            crate::info!(format!("Index {}/{} built", n + 1, total),
                { "index": idx.name, "elapsed_s": started.elapsed().as_secs() });
        }
        if self.opts.maintenance_work_mem.is_some() {
            self.db.run("RESET maintenance_work_mem;", false)?;
        }
        Ok(())
    }

    /// Estimated copy progress every minute from a second connection.
    fn log_progress(&self, finished: Arc<AtomicBool>) {
        let conn = self.opts.conn.clone();
        let (schema, table_name, shadow) = (
            self.opts.schema.clone(),
            self.opts.table_name.clone(),
            self.shadow_table.clone(),
        );
        std::thread::spawn(move || {
            let mut db = match Db::connect(&conn) {
                Ok(db) => db,
                Err(e) => {
                    crate::info!(format!("Reporting progress failed: {e}"));
                    return;
                }
            };
            let source = get_table_size(&mut db, &schema, &table_name).max(1);
            while !finished.load(Ordering::SeqCst) {
                let size = get_table_size(&mut db, &schema, &shadow);
                let progress = size as f64 / source as f64 * 100.0;
                crate::info!(format!(
                    "Estimated copy progress: {:.2}% complete",
                    progress
                ));
                if progress >= 100.0 {
                    break;
                }
                let start = Instant::now();
                while start.elapsed() < TRACK_PROGRESS_INTERVAL && !finished.load(Ordering::SeqCst)
                {
                    std::thread::sleep(Duration::from_millis(200));
                }
            }
        });
    }

    pub fn replay_and_swap(&mut self) -> Result<()> {
        if self.opts.wait_for_swap {
            return self.sync_until_swap_requested();
        }
        self.replay_until_below_delta()?;
        crate::info!("Remaining rows below delta count, proceeding towards swap");
        self.swap()
    }

    /// --wait-for-swap: keep replaying the audit table into the shadow table
    /// until a swap is requested, either by `pg-online-schema-change swap`
    /// (marks the audit table with a comment) or by SIGUSR1 to this process.
    /// If the swap fails (e.g. can't get its lock) keep syncing and wait for
    /// a new request.
    pub fn sync_until_swap_requested(&mut self) -> Result<()> {
        self.enable_autovacuum_for_sync()?;
        crate::info!("Initial copy done, keeping shadow table in sync until a swap is requested", {
            "shadow_table": self.shadow_table,
            "audit_table": self.audit_table,
            "how_to_swap": format!(
                "pg-online-schema-change swap -t {} -s {} ... or kill -USR1 {}",
                self.opts.table, self.opts.schema, std::process::id()
            ),
        });

        let mut last_status: Option<Instant> = None;
        loop {
            self.check_terminate()?;
            let rows = self.rows_to_play(false)?;

            if rows.len() as i64 > self.opts.delta_count {
                self.play(&rows, false)?;
                continue;
            }

            if self.swap_requested()? {
                crate::info!(
                    "Swap requested, remaining rows below delta count, proceeding towards swap"
                );
                match self.swap() {
                    Ok(()) => return Ok(()),
                    Err(e)
                        if e.is::<AccessExclusiveLockNotAcquired>()
                            || (is_pg_error(&e) && self.flags.terminating().is_none()) =>
                    {
                        // the swap runs in one transaction, on failure it is rolled
                        // back and the trigger keeps feeding the audit table
                        crate::error!("Swap failed, keeping in sync. Request the swap again.", { "error": format!("{e:#}") });
                        self.clear_swap_request()?;
                        continue;
                    }
                    Err(e) => return Err(e),
                }
            }

            if !rows.is_empty() {
                self.play(&rows, false)?;
            }

            if last_status.is_none_or(|t| t.elapsed() >= TRACK_PROGRESS_INTERVAL) {
                crate::info!("In sync, waiting for swap request", { "pending_rows": rows.len(), "pid": std::process::id() });
                last_status = Some(Instant::now());
            }

            self.sleep_interruptible(Duration::from_secs_f64(self.opts.sync_interval));
        }
    }

    fn sleep_interruptible(&self, total: Duration) {
        let start = Instant::now();
        while start.elapsed() < total
            && self.flags.terminating().is_none()
            && !self.flags.swap_requested.load(Ordering::SeqCst)
        {
            std::thread::sleep(
                Duration::from_millis(50).min(total.saturating_sub(start.elapsed())),
            );
        }
    }

    pub fn swap_requested(&mut self) -> Result<bool> {
        if self.flags.swap_requested.load(Ordering::SeqCst) {
            return Ok(true);
        }
        let sql = format!(
            "SELECT obj_description({}::regclass, 'pg_class') AS comment;",
            quote_literal(&self.audit_table)
        );
        Ok(self.db.first_value(&sql, false)?.as_deref() == Some(SWAP_REQUEST_MARKER))
    }

    pub fn clear_swap_request(&mut self) -> Result<()> {
        self.flags.swap_requested.store(false, Ordering::SeqCst);
        let sql = format!("COMMENT ON TABLE {} IS NULL;", self.audit_table);
        self.db.run(&sql, false)?;
        Ok(())
    }

    /// The sync may run for days with constant deletes/updates on the audit
    /// and shadow tables, let autovacuum take care of them meanwhile.
    /// storage_params_reset restores the original parameters on swap.
    fn enable_autovacuum_for_sync(&mut self) -> Result<()> {
        let sql = format!(
            "ALTER TABLE {} SET (autovacuum_enabled = true); ALTER TABLE {} SET (autovacuum_enabled = true);",
            self.audit_table, self.shadow_table
        );
        self.db.run(&sql, false)?;
        Ok(())
    }

    /// Everything in one transaction under an ACCESS EXCLUSIVE lock: replay
    /// the rest, rename the tables, refresh FKs, triggers and storage params.
    pub fn swap(&mut self) -> Result<()> {
        crate::info!("Performing swap!");
        let result = (|| -> Result<()> {
            if !self.open_lock_exclusive()? {
                return Err(AccessExclusiveLockNotAcquired.into());
            }
            self.db.run(
                &format!("SET statement_timeout to '{SWAP_STATEMENT_TIMEOUT}';"),
                true,
            )?;

            // ALTER TABLE on the table is ours from here on (rolled back with the swap)
            let drop_guard = self.drop_ddl_guard_sql(true)?;
            self.db.run(&drop_guard, true)?;
            let rows = self.rows_to_play(true)?;
            self.play(&rows, true)?;
            // before the renames, while the source is still `table_name`
            let carry = self.capture_carry()?;
            for w in &carry.warnings {
                crate::warn!(w.clone());
            }

            let primary_key = self
                .primary_key()?
                .ok_or_else(|| anyhow!("no primary key"))?;
            let pk_refresh = self.query_for_primary_key_refresh(&primary_key, true)?;
            let t = &self.opts.table_name;
            let sql = format!(
                "{pk_refresh};
ALTER TABLE {t} RENAME to {old};
ALTER TABLE {shadow} RENAME to {t};
{ref_fk}
{self_fk}
{triggers}
{storage}
DROP TRIGGER IF EXISTS primary_to_audit_table_trigger ON {t};
DROP TRIGGER IF EXISTS primary_to_audit_table_trigger ON {old};
DROP TRIGGER IF EXISTS {truncate_guard} ON {old};
{old_table}
{names}
{properties}
",
                truncate_guard = self.truncate_guard_name(),
                old_table = carry.old_table_statements.join("\n"),
                names = carry.name_statements.join("\n"),
                properties = carry
                    .statements
                    .iter()
                    .map(|s| format!("{s};"))
                    .collect::<Vec<_>>()
                    .join("\n"),
                old = self.old_primary_table,
                shadow = self.shadow_table,
                ref_fk = self.referential_foreign_key_statements,
                self_fk = self.self_foreign_key_statements,
                triggers = self.trigger_statements,
                storage = self.storage_params_reset(),
            );
            self.db.run(&sql, true)?;
            Ok(())
        })();
        let commit = self.db.run("COMMIT;", false);
        let reset = self.db.run("SET statement_timeout = 0;", false);
        result.and(commit.map(|_| ())).and(reset.map(|_| ()))
    }

    pub fn run_analyze(&mut self) -> Result<()> {
        crate::info!("Performing ANALYZE!");
        let sql = format!(
            "ANALYZE VERBOSE {}.{};",
            quote_ident(&self.opts.schema),
            self.opts.table_name
        );
        self.db.exec_raw(&sql)
    }

    pub fn run_vacuum(&mut self) -> Result<()> {
        crate::info!("Performing VACUUM!");
        let sql = format!(
            "VACUUM VERBOSE {}.{};",
            quote_ident(&self.opts.schema),
            self.opts.table_name
        );
        self.db.exec_raw(&sql)
    }

    pub fn validate_constraints(&mut self) -> Result<()> {
        if self.opts.skip_foreign_key_validation {
            return Ok(());
        }
        let table_name = self.opts.table_name.clone();
        for statement in self.get_foreign_keys_to_validate(&table_name)? {
            crate::info!("Validating constraints!", { "statement": statement });
            self.db.run(&statement, false)?;
        }
        Ok(())
    }

    pub fn replace_views(&mut self) -> Result<()> {
        let old = self.old_primary_table.clone();
        for (view_name, definition) in self.view_definitions_for(&old)? {
            let definition = definition.replace(&old, &self.opts.table);
            crate::info!(format!("Replacing view {view_name}"));
            self.db.run(
                &format!("CREATE OR REPLACE VIEW {view_name} AS {definition}"),
                false,
            )?;
        }
        Ok(())
    }

    /// Trigger first (only when it is there, under lock_timeout, retried),
    /// then the audit and shadow tables: dropping the audit table while the
    /// trigger still writes into it would fail every write to the table.
    pub fn drop_and_cleanup(&mut self, drop_primary: bool) -> Result<()> {
        // the cancel request sent on a stop signal is asynchronous: it can land
        // on one of the cleanup statements instead of the query it was meant
        // for. Every step is idempotent, so just go again.
        let mut attempt = 1;
        loop {
            match self.drop_and_cleanup_once(drop_primary) {
                Err(e) if sqlstate(&e) == Some(&SqlState::QUERY_CANCELED) && attempt < 3 => {
                    crate::warn!("Cleanup statement canceled (late cancel request of the stop signal), retrying");
                    attempt += 1;
                }
                r => return r,
            }
        }
    }

    fn drop_and_cleanup_once(&mut self, drop_primary: bool) -> Result<()> {
        crate::info!("Cleaning up", { "audit_table": self.audit_table, "shadow_table": self.shadow_table });
        self.drop_audit_trigger()?;
        let primary_drop = if drop_primary {
            format!("DROP TABLE IF EXISTS {};", self.old_primary_table)
        } else {
            String::new()
        };
        let drop_guard = self.drop_ddl_guard_sql(false)?;
        let sql = format!(
            "{drop_guard}
DROP FUNCTION IF EXISTS {ddl_guard}();
DROP FUNCTION IF EXISTS {truncate_guard}();
DROP TABLE IF EXISTS {};
DROP TABLE IF EXISTS {};
{}
RESET statement_timeout;
RESET client_min_messages;
RESET lock_timeout;",
            self.audit_table,
            self.shadow_table,
            primary_drop,
            ddl_guard = self.ddl_guard_name(),
            truncate_guard = self.truncate_guard_name(),
        );
        self.db.run(&sql, false)?;
        self.flags.set_cleanup_sql(None);
        Ok(())
    }

    fn audit_trigger_exists(&mut self) -> Result<bool> {
        let sql = format!(
            "SELECT 1 FROM pg_trigger WHERE tgrelid = to_regclass({}) AND tgname IN ('primary_to_audit_table_trigger', {});",
            quote_literal(&format!("{}.{}", quote_ident(&self.opts.schema), self.opts.table_name)),
            quote_literal(&self.truncate_guard_name())
        );
        Ok(!self.db.run(&sql, false)?.is_empty())
    }

    /// DROP TRIGGER takes ACCESS EXCLUSIVE on the table, even with IF EXISTS
    /// and no trigger: without a lock_timeout it would wait behind any long
    /// query and stall every query on the table queued behind it. So: only
    /// when the trigger exists, with lock_timeout, until it succeeds (a second
    /// signal exits and logs the SQL to finish by hand).
    fn drop_audit_trigger(&mut self) -> Result<()> {
        let mut attempt = 0;
        while self.audit_trigger_exists()? {
            attempt += 1;
            let sql = format!(
                "SET LOCAL lock_timeout = '{}s';\nDROP TRIGGER IF EXISTS primary_to_audit_table_trigger ON {t};\nDROP TRIGGER IF EXISTS {} ON {t};",
                self.opts.wait_time_for_lock.max(1),
                self.truncate_guard_name(),
                t = self.opts.table_name
            );
            match self.db.run(&sql, false) {
                Ok(_) => {
                    crate::info!("Audit trigger dropped");
                    return Ok(());
                }
                Err(e) if sqlstate(&e) == Some(&SqlState::LOCK_NOT_AVAILABLE) => {
                    crate::warn!(format!(
                        "Couldn't get the lock to drop the audit trigger, attempt {attempt}, retrying (long transactions on the table block it)"
                    ));
                }
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    /// What drop_and_cleanup does, to run by hand.
    pub fn manual_cleanup_sql(&self) -> String {
        let s = quote_ident(&self.opts.schema);
        format!(
            "SET lock_timeout = '5s'; DROP TRIGGER IF EXISTS primary_to_audit_table_trigger ON {s}.{t}; DROP TRIGGER IF EXISTS {tg} ON {s}.{t}; RESET lock_timeout; DROP EVENT TRIGGER IF EXISTS {eg}; DROP FUNCTION IF EXISTS {s}.{eg}(); DROP FUNCTION IF EXISTS {s}.{tg}(); DROP TABLE IF EXISTS {s}.{a}; DROP TABLE IF EXISTS {s}.{sh};",
            t = self.opts.table_name,
            tg = self.truncate_guard_name(),
            eg = self.ddl_guard_name(),
            a = self.audit_table,
            sh = self.shadow_table
        )
    }

    pub fn ddl_guard_name(&self) -> String {
        format!("pgosc_ddl_guard_{}", self.id)
    }

    /// DROP EVENT TRIGGER needs a superuser even with IF EXISTS, so only
    /// when it is there.
    fn drop_ddl_guard_sql(&mut self, reuse: bool) -> Result<String> {
        let name = self.ddl_guard_name();
        let exists = self.db.first_value(
            &format!(
                "SELECT 1 FROM pg_event_trigger WHERE evtname = {};",
                quote_literal(&name)
            ),
            reuse,
        )?;
        Ok(if exists.is_some() {
            format!("DROP EVENT TRIGGER {name};")
        } else {
            String::new()
        })
    }

    pub fn truncate_guard_name(&self) -> String {
        format!("pgosc_truncate_guard_{}", self.id)
    }

    /// While the trigger feeds the audit table, a change of the table's
    /// columns would make every write to it fail (the audit table has the old
    /// columns) and TRUNCATE isn't captured at all. TRUNCATE is refused by a
    /// statement trigger; ALTER TABLE by an event trigger, which only a
    /// superuser can create (otherwise a warning). Both go away with the swap
    /// or the cleanup. Runs in the transaction that adds the audit trigger.
    fn setup_guards(&mut self) -> Result<()> {
        let schema = quote_ident(&self.opts.schema);
        let table = self.opts.table_name.clone();
        let tg = self.truncate_guard_name();
        let sql = format!(
            r#"CREATE OR REPLACE FUNCTION {schema}.{tg}() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'pg-osc is rebuilding table %.%: TRUNCATE is refused until it finishes (the change would not reach the new table)', TG_TABLE_SCHEMA, TG_TABLE_NAME;
END $$;
CREATE TRIGGER {tg} BEFORE TRUNCATE ON {table} FOR EACH STATEMENT EXECUTE PROCEDURE {schema}.{tg}();"#
        );
        self.db.run(&sql, true)?;

        let superuser = self.db.first_value(
            "SELECT rolsuper FROM pg_roles WHERE rolname = current_user;",
            true,
        )?;
        if superuser.as_deref() != Some("t") {
            crate::warn!(format!(
                "Not a superuser: can't install the event trigger that refuses ALTER TABLE on {table} during the run. Don't change the table's columns until the swap: every write to it would fail"
            ));
            return Ok(());
        }
        let eg = self.ddl_guard_name();
        let oid = self
            .db
            .first_value(
                &format!(
                    "SELECT {}::regclass::oid;",
                    quote_literal(&self.qualified(&table))
                ),
                true,
            )?
            .unwrap_or_default();
        let sql = format!(
            r#"CREATE OR REPLACE FUNCTION {schema}.{eg}() RETURNS event_trigger LANGUAGE plpgsql AS $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_event_trigger_ddl_commands() WHERE classid = 'pg_class'::regclass AND objid = {oid}) THEN
    RAISE EXCEPTION 'pg-osc is rebuilding table %: ALTER TABLE on it is refused until it finishes (every write would fail). To force it: DROP EVENT TRIGGER {eg}', {table_lit};
  END IF;
END $$;
CREATE EVENT TRIGGER {eg} ON ddl_command_end WHEN TAG IN ('ALTER TABLE') EXECUTE PROCEDURE {schema}.{eg}();"#,
            table_lit = quote_literal(&self.qualified(&table)),
        );
        self.db.run(&sql, true)?;
        Ok(())
    }

    fn storage_params_reset(&self) -> String {
        let t = &self.opts.table_name;
        let mut s =
            format!("ALTER TABLE {t} RESET (autovacuum_enabled, toast.autovacuum_enabled);");
        if !self.primary_table_storage_parameters.is_empty() {
            s.push_str(&format!(
                "ALTER TABLE {t} SET ({});",
                self.primary_table_storage_parameters
            ));
        }
        s
    }
}

/// `swap` command: marks the audit table of the running sync.
pub fn request_swap(conn: &crate::db::ConnOpts, schema: &str, table: &str) -> Result<()> {
    let mut db = Db::connect(conn)?;
    let tables = crate::query::audit_tables_for(&mut db, schema, table)?;
    match tables.as_slice() {
        [] => bail!("No running pg-osc sync found for {schema}.{table}"),
        [audit] => {
            let sql = format!(
                "COMMENT ON TABLE {}.{} IS {}",
                quote_ident(schema),
                quote_ident(audit),
                quote_literal(SWAP_REQUEST_MARKER)
            );
            db.run(&sql, false)?;
            crate::info!("Swap requested", { "audit_table": audit });
            Ok(())
        }
        many => bail!(
            "Several pg-osc audit tables found for {schema}.{table}: {}. Drop the stale ones or send SIGUSR1 to the right process.",
            many.join(", ")
        ),
    }
}
