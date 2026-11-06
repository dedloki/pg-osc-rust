//! The whole operation (the Ruby Orchestrate class).

use crate::ctx::{random_id, AccessExclusiveLockNotAcquired, Ctx, Flags};
use crate::db::{is_pg_error, Db};
use crate::functions::{FUNC_CREATE_TABLE_ALL, FUNC_FIX_SERIAL_SEQUENCE};
use crate::options::Options;
use crate::query::get_table_size;
use crate::sql::{
    alter_statement_for, dropped_columns, quote_ident, quote_literal, renamed_columns,
};
use anyhow::{anyhow, bail, Result};
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
    let mut ctx = setup(opts, flags)?;
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
                crate::error!(format!("Cleanup failed: {cleanup:#}"));
            }
            Err(e)
        }
    }
}

fn run_steps(ctx: &mut Ctx) -> Result<()> {
    if ctx.primary_key()?.is_none() {
        bail!("Parent table has no primary key, exiting...");
    }

    crate::info!("Performing some house keeping....");
    ctx.run_analyze()?;
    ctx.run_vacuum()?;

    ctx.setup_audit_table()?;
    ctx.setup_trigger()?;
    ctx.setup_shadow_table()?; // starts the serializable transaction
    ctx.run_alter_statement()?; // same transaction
    ctx.copy_data()?; // same transaction, commits it
    ctx.run_analyze()?;
    ctx.replay_and_swap()?;
    ctx.run_analyze()?;
    ctx.validate_constraints()?;
    ctx.replace_views()?;
    ctx.drop_and_cleanup(ctx.opts.drop)?;
    Ok(())
}

impl Ctx {
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
            let (audit, table) = (&self.audit_table, &self.opts.table_name);
            let sql = format!(
                r#"DROP TRIGGER IF EXISTS primary_to_audit_table_trigger ON {table};

CREATE OR REPLACE FUNCTION primary_to_audit_table_trigger()
RETURNS TRIGGER AS
$$
BEGIN
  IF ( TG_OP = 'INSERT') THEN
    INSERT INTO "{audit}" select nextval('{seq}'), 'INSERT', clock_timestamp(), NEW.* ;
    RETURN NEW;
  ELSIF ( TG_OP = 'UPDATE') THEN
    INSERT INTO "{audit}" select nextval('{seq}'), 'UPDATE', clock_timestamp(),  NEW.* ;
    RETURN NEW;
  ELSIF ( TG_OP = 'DELETE') THEN
    INSERT INTO "{audit}" select nextval('{seq}'), 'DELETE', clock_timestamp(), OLD.* ;
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
        self.db
            .run("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE", true)?;
        let sql = format!(
            "SELECT fix_serial_sequence({}, {});",
            quote_literal(&self.opts.table_name),
            quote_literal(&self.shadow_table)
        );
        self.db.run(&sql, true)?;
        Ok(())
    }

    pub fn run_alter_statement(&mut self) -> Result<()> {
        let statement = alter_statement_for(&self.opts.alter_statement, &self.shadow_table)?;
        crate::info!("Running alter statement on shadow table",
            { "shadow_table": self.shadow_table, "parent_table": self.opts.table_name });
        self.db.run(&statement, true)?;
        self.dropped_columns = dropped_columns(&self.opts.alter_statement);
        self.renamed_columns = renamed_columns(&self.opts.alter_statement);
        Ok(())
    }

    pub fn copy_data(&mut self) -> Result<()> {
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

            let rows = self.rows_to_play(true)?;
            self.play(&rows, true)?;

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
",
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

    pub fn drop_and_cleanup(&mut self, drop_primary: bool) -> Result<()> {
        let primary_drop = if drop_primary {
            format!("DROP TABLE IF EXISTS {};", self.old_primary_table)
        } else {
            String::new()
        };
        let sql = format!(
            "DROP TRIGGER IF EXISTS primary_to_audit_table_trigger ON {};
DROP TABLE IF EXISTS {};
DROP TABLE IF EXISTS {};
{}
RESET statement_timeout;
RESET client_min_messages;
RESET lock_timeout;",
            self.opts.table_name, self.audit_table, self.shadow_table, primary_drop
        );
        self.db.run(&sql, false)?;
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
