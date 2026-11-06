//! Command line and validated options (the Ruby CLI + Client).

use crate::db::ConnOpts;
use crate::sql;
use anyhow::{bail, Result};
use clap::{ArgAction, Args, Parser, Subcommand};

pub const PULL_BATCH_COUNT: i64 = 1000;
pub const DELTA_COUNT: i64 = 20;
pub const SYNC_INTERVAL: f64 = 1.0;

#[derive(Parser, Debug)]
#[command(
    name = "pg-online-schema-change",
    version,
    about = "Safely apply schema changes with minimal locks"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Safely apply schema changes with minimal locks
    #[command(disable_help_flag = true)]
    Perform(PerformArgs),
    /// Request the swap for a running `perform --wait-for-swap` on the given table
    #[command(disable_help_flag = true)]
    Swap(SwapArgs),
    /// Print the version
    Version,
}

#[derive(Args, Debug, Clone)]
pub struct ConnArgs {
    /// The schema in which the table is
    #[arg(short = 's', long, default_value = "public")]
    pub schema: String,
    /// Name of the database
    #[arg(short = 'd', long)]
    pub dbname: String,
    /// Server host where the Database is located
    #[arg(short = 'h', long)]
    pub host: String,
    /// Username for the Database
    #[arg(short = 'u', long)]
    pub username: String,
    /// Port for the Database
    #[arg(short = 'p', long, default_value_t = 5432)]
    pub port: u16,
    /// Password for the Database, prefer the PGPASSWORD environment variable
    #[arg(long, env = "PGPASSWORD", hide_env_values = true)]
    pub password: Option<String>,
    /// disable, prefer or require (the certificate is not verified, as in libpq)
    #[arg(long, env = "PGSSLMODE", default_value = "prefer")]
    pub sslmode: String,
    /// Print help
    #[arg(long, action = ArgAction::Help)]
    pub help: Option<bool>,
}

impl ConnArgs {
    pub fn conn_opts(&self) -> ConnOpts {
        ConnOpts {
            host: self.host.clone(),
            port: self.port,
            dbname: self.dbname.clone(),
            user: self.username.clone(),
            password: self.password.clone(),
            sslmode: self.sslmode.clone(),
        }
    }
}

#[derive(Args, Debug, Clone)]
pub struct PerformArgs {
    /// The ALTER statement to perform the schema change
    #[arg(short = 'a', long)]
    pub alter_statement: String,
    #[command(flatten)]
    pub conn: ConnArgs,
    /// Emit logs in debug mode
    #[arg(short = 'v', long)]
    pub verbose: bool,
    /// Drop the original table in the end after the swap
    #[arg(short = 'f', long)]
    pub drop: bool,
    /// Kill other competing queries/backends when trying to acquire lock for the shadow table creation and swap. It will wait for --wait-time-for-lock duration before killing backends and try upto 3 times.
    #[arg(short = 'k', long)]
    pub kill_backends: bool,
    /// Time to wait before killing backends to acquire lock and/or retrying upto 3 times. It will kill backends if --kill-backends is true, otherwise try upto 3 times and exit if it cannot acquire a lock.
    #[arg(short = 'w', long, default_value_t = 10)]
    pub wait_time_for_lock: u64,
    /// Takes a .sql file location where you can provide a custom query to be played (ex: backfills) when pgosc copies data from the primary to the shadow table. %{shadow_table} is replaced with the shadow table name.
    #[arg(short = 'c', long)]
    pub copy_statement: Option<String>,
    /// Number of rows to be replayed on each iteration after copy. This can be tuned for faster catch up and swap. Best used with delta-count.
    #[arg(short = 'b', long, default_value_t = PULL_BATCH_COUNT)]
    pub pull_batch_count: i64,
    /// Indicates how many rows should be remaining before a swap should be performed. This can be tuned for faster catch up and swap, especially on highly volume tables. Best used with pull-batch-count.
    #[arg(short = 'e', long, default_value_t = DELTA_COUNT)]
    pub delta_count: i64,
    /// Skip foreign key validation after swap. You shouldn't need this unless you have a very specific use case, like manually validating foreign key constraints after swap.
    #[arg(short = 'o', long)]
    pub skip_foreign_key_validation: bool,
    /// Copy only rows matching this SQL condition, e.g. "email IS NOT NULL" (a leading WHERE is optional). Uses the original column names. Applied to the initial copy and to the replay: rows that stop matching after an UPDATE are removed from the shadow table, rows that start matching are added. Rows not matching stay only in the old table after the swap. Can't be combined with --copy-statement.
    #[arg(short = 'r', long)]
    pub filter: Option<String>,
    /// Don't swap automatically. After the copy, keep the shadow table in sync until a swap is requested with `pg-online-schema-change swap` (or SIGUSR1 to this process).
    #[arg(short = 'W', long)]
    pub wait_for_swap: bool,
    /// With --wait-for-swap: seconds to sleep between replay iterations once caught up.
    #[arg(long, default_value_t = SYNC_INTERVAL)]
    pub sync_interval: f64,
}

#[derive(Args, Debug, Clone)]
pub struct SwapArgs {
    /// Table the running perform works on
    #[arg(short = 't', long)]
    pub table: String,
    #[command(flatten)]
    pub conn: ConnArgs,
}

/// Validated options for `perform`.
#[derive(Clone, Debug)]
pub struct Options {
    pub alter_statement: String,
    pub schema: String,
    pub conn: ConnOpts,
    pub drop: bool,
    pub kill_backends: bool,
    pub wait_time_for_lock: u64,
    pub copy_statement: Option<String>,
    pub pull_batch_count: i64,
    pub delta_count: i64,
    pub skip_foreign_key_validation: bool,
    pub filter: Option<String>,
    pub wait_for_swap: bool,
    pub sync_interval: f64,
    /// table as Postgres sees it (unquoted)
    pub table: String,
    /// table as it goes into SQL (quoted if needed)
    pub table_name: String,
}

impl Options {
    pub fn from_args(a: &PerformArgs) -> Result<Self> {
        let copy_statement = match a.copy_statement.as_deref().filter(|s| !s.is_empty()) {
            None => None,
            Some(path) => {
                let p = std::path::absolute(path)?;
                if !p.is_file() {
                    bail!("File not found: {}", p.display());
                }
                Some(std::fs::read_to_string(&p)?)
            }
        };

        let alter = a.alter_statement.clone();
        if !sql::is_alter_statement(&alter) {
            bail!("Not a valid ALTER statement: {alter}");
        }
        if a.delta_count > a.pull_batch_count {
            bail!("Value for delta_count should be smaller than the value for pull_batch_count");
        }
        if !sql::same_table(&alter) {
            bail!("All statements should belong to the same table: {alter}");
        }

        let filter = sql::normalize_filter(a.filter.as_deref());
        if let Some(f) = &filter {
            if copy_statement.is_some() {
                bail!("--filter and --copy-statement can't be used together, put the WHERE clause into the copy statement or drop it");
            }
            if !sql::valid_filter(f) {
                bail!("Not a valid filter expression: {f}");
            }
        }
        if a.sync_interval.is_nan() || a.sync_interval < 0.0 {
            bail!("--sync-interval should be >= 0");
        }

        let table = sql::table(&alter)?;
        let table_name = sql::table_name(&alter, &table);

        Ok(Self {
            alter_statement: alter,
            schema: a.conn.schema.clone(),
            conn: a.conn.conn_opts(),
            drop: a.drop,
            kill_backends: a.kill_backends,
            wait_time_for_lock: a.wait_time_for_lock,
            copy_statement,
            pull_batch_count: a.pull_batch_count,
            delta_count: a.delta_count,
            skip_foreign_key_validation: a.skip_foreign_key_validation,
            filter,
            wait_for_swap: a.wait_for_swap,
            sync_interval: a.sync_interval,
            table,
            table_name,
        })
    }
}
