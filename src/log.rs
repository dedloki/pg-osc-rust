//! JSON lines logger, same shape as the Ruby version (ougai / bunyan):
//! {"name","hostname","pid","level","time","v","msg",...fields,"version"}

use serde_json::{Map, Value};
use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::OnceLock;

static VERBOSE: AtomicBool = AtomicBool::new(false);
static HOSTNAME: OnceLock<String> = OnceLock::new();

pub const TRACE: u8 = 10;
pub const INFO: u8 = 30;
pub const WARN: u8 = 40;
pub const ERROR: u8 = 50;
pub const FATAL: u8 = 60;

pub fn set_verbose(verbose: bool) {
    VERBOSE.store(verbose, Ordering::Relaxed);
}

pub fn log(level: u8, msg: &str, fields: Value) {
    if level < INFO && !VERBOSE.load(Ordering::Relaxed) {
        return;
    }
    let hostname =
        HOSTNAME.get_or_init(|| gethostname::gethostname().to_string_lossy().into_owned());

    let mut obj = Map::new();
    obj.insert("name".into(), "pg-online-schema-change".into());
    obj.insert("hostname".into(), hostname.as_str().into());
    obj.insert("pid".into(), std::process::id().into());
    obj.insert("level".into(), level.into());
    obj.insert(
        "time".into(),
        chrono::Local::now()
            .format("%Y-%m-%dT%H:%M:%S%.3f%:z")
            .to_string()
            .into(),
    );
    obj.insert("v".into(), 0.into());
    obj.insert("msg".into(), msg.into());
    if let Value::Object(extra) = fields {
        obj.extend(extra);
    }
    obj.insert("version".into(), env!("CARGO_PKG_VERSION").into());

    // one write per line, stdout is not buffered by us, so the lines show up
    // immediately in files and pod logs
    let line = format!("{}\n", Value::Object(obj));
    let _ = std::io::stdout().lock().write_all(line.as_bytes());
}

#[macro_export]
macro_rules! info {
    ($msg:expr) => { $crate::log::log($crate::log::INFO, &$msg, serde_json::Value::Null) };
    ($msg:expr, $($fields:tt)+) => { $crate::log::log($crate::log::INFO, &$msg, serde_json::json!($($fields)+)) };
}

#[macro_export]
macro_rules! debug {
    ($msg:expr) => { $crate::log::log($crate::log::TRACE, &$msg, serde_json::Value::Null) };
    ($msg:expr, $($fields:tt)+) => { $crate::log::log($crate::log::TRACE, &$msg, serde_json::json!($($fields)+)) };
}

#[macro_export]
macro_rules! warn {
    ($msg:expr) => { $crate::log::log($crate::log::WARN, &$msg, serde_json::Value::Null) };
    ($msg:expr, $($fields:tt)+) => { $crate::log::log($crate::log::WARN, &$msg, serde_json::json!($($fields)+)) };
}

#[macro_export]
macro_rules! error {
    ($msg:expr) => { $crate::log::log($crate::log::ERROR, &$msg, serde_json::Value::Null) };
    ($msg:expr, $($fields:tt)+) => { $crate::log::log($crate::log::ERROR, &$msg, serde_json::json!($($fields)+)) };
}

#[macro_export]
macro_rules! fatal {
    ($msg:expr) => { $crate::log::log($crate::log::FATAL, &$msg, serde_json::Value::Null) };
    ($msg:expr, $($fields:tt)+) => { $crate::log::log($crate::log::FATAL, &$msg, serde_json::json!($($fields)+)) };
}
