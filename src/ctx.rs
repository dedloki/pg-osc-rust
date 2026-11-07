//! State of one `perform` run: options, the connection and every name the
//! Ruby version kept in its global Store.

use crate::db::Db;
use crate::options::Options;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::sync::Arc;

/// Flags set from the signal thread.
#[derive(Default, Debug)]
pub struct Flags {
    pub swap_requested: AtomicBool,
    /// signal number that asked us to stop, 0 = none
    pub terminate: AtomicI32,
}

impl Flags {
    pub fn terminating(&self) -> Option<i32> {
        match self.terminate.load(Ordering::SeqCst) {
            0 => None,
            s => Some(s),
        }
    }
}

/// Returned when a TERM/INT/QUIT arrived; the caller cleans up and exits.
#[derive(Debug)]
pub struct Terminated(pub i32);

impl std::fmt::Display for Terminated {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "terminated by signal {}", self.0)
    }
}
impl std::error::Error for Terminated {}

#[derive(Debug)]
pub struct AccessExclusiveLockNotAcquired;

impl std::fmt::Display for AccessExclusiveLockNotAcquired {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Couldn't acquire ACCESS EXCLUSIVE lock")
    }
}
impl std::error::Error for AccessExclusiveLockNotAcquired {}

pub struct Ctx {
    pub opts: Options,
    pub db: Db,
    pub flags: Arc<Flags>,

    pub old_primary_table: String,
    pub audit_table: String,
    pub operation_type_column: String,
    pub trigger_time_column: String,
    pub audit_table_pk: String,
    pub shadow_table: String,
    pub audit_table_pk_sequence: Option<String>,

    pub primary_table_storage_parameters: String,
    pub referential_foreign_key_statements: String,
    pub self_foreign_key_statements: String,
    pub trigger_statements: String,
    pub primary_key: Option<String>,

    pub dropped_columns: Vec<String>,
    pub renamed_columns: Vec<(String, String)>,
    /// (insert into columns, select columns) for the replay, cached
    pub copy_columns_cache: Option<(Vec<String>, Vec<String>)>,
    /// indexes and constraints removed from the shadow table for --defer-indexes
    pub deferred_indexes: Vec<crate::query::ShadowIndex>,
}

impl Ctx {
    pub fn new(opts: Options, db: Db, flags: Arc<Flags>, id: &str) -> Self {
        let t = opts.table.to_lowercase();
        Self {
            old_primary_table: format!("pgosc_op_table_{t}"),
            audit_table: format!("pgosc_at_{t}_{id}"),
            operation_type_column: format!("operation_type_{id}"),
            trigger_time_column: format!("trigger_time_{id}"),
            audit_table_pk: format!("at_{id}_id"),
            shadow_table: format!("pgosc_st_{t}_{id}"),
            audit_table_pk_sequence: None,
            primary_table_storage_parameters: String::new(),
            referential_foreign_key_statements: String::new(),
            self_foreign_key_statements: String::new(),
            trigger_statements: String::new(),
            primary_key: None,
            dropped_columns: vec![],
            renamed_columns: vec![],
            copy_columns_cache: None,
            deferred_indexes: vec![],
            opts,
            db,
            flags,
        }
    }

    pub fn check_terminate(&self) -> anyhow::Result<()> {
        match self.flags.terminating() {
            Some(sig) => Err(Terminated(sig).into()),
            None => Ok(()),
        }
    }
}

pub fn random_id() -> String {
    let mut buf = [0u8; 3];
    getrandom::fill(&mut buf).expect("getrandom");
    buf.iter().map(|b| format!("{b:02x}")).collect()
}
