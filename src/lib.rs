//! Rust port of pg-osc (pg-online-schema-change): online schema changes for
//! PostgreSQL through a shadow table, an audit table fed by a trigger and a
//! short ACCESS EXCLUSIVE lock for the swap.

#[macro_use]
pub mod log;
pub mod ctx;
pub mod db;
pub mod functions;
pub mod options;
pub mod orchestrate;
pub mod query;
pub mod replay;
pub mod sql;
