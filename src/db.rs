//! Connection wrapper with the transaction semantics of the Ruby Query.run:
//! every call opens a transaction, `reuse = true` leaves it open for the
//! next call, an error rolls it back. All values come back as text.

use anyhow::{bail, Result};
use postgres::config::SslMode;
use postgres::error::SqlState;
use postgres::{CancelToken, Client, Config, SimpleQueryMessage};
use postgres_native_tls::MakeTlsConnector;

#[derive(Clone, Debug)]
pub struct ConnOpts {
    pub host: String,
    pub port: u16,
    pub dbname: String,
    pub user: String,
    pub password: Option<String>,
    pub sslmode: String,
}

impl ConnOpts {
    fn config(&self) -> Result<Config> {
        let mut c = Config::new();
        c.host(&self.host)
            .port(self.port)
            .dbname(&self.dbname)
            .user(&self.user)
            .application_name("pg-osc");
        if let Some(p) = self.password.as_deref().filter(|p| !p.is_empty()) {
            c.password(p);
        }
        c.ssl_mode(match self.sslmode.as_str() {
            "disable" => SslMode::Disable,
            "prefer" => SslMode::Prefer,
            "require" => SslMode::Require,
            other => bail!("Unsupported sslmode {other}, use disable, prefer or require"),
        });
        Ok(c)
    }
}

/// libpq semantics for prefer/require: encrypt, don't verify the certificate.
pub fn tls() -> Result<MakeTlsConnector> {
    let connector = native_tls::TlsConnector::builder()
        .danger_accept_invalid_certs(true)
        .danger_accept_invalid_hostnames(true)
        .build()?;
    Ok(MakeTlsConnector::new(connector))
}

/// One row, values as text (NULL is None), columns in query order.
#[derive(Clone, Debug, Default)]
pub struct Row {
    pub columns: Vec<String>,
    pub values: Vec<Option<String>>,
}

impl Row {
    pub fn get(&self, column: &str) -> Option<&str> {
        let i = self.columns.iter().position(|c| c == column)?;
        self.values[i].as_deref()
    }
}

/// Cancel token of the main connection, used by the signal thread.
pub static CANCEL: std::sync::OnceLock<CancelToken> = std::sync::OnceLock::new();

pub struct Db {
    client: Client,
}

impl Db {
    pub fn connect(opts: &ConnOpts) -> Result<Self> {
        let client = opts.config()?.connect(tls()?)?;
        crate::debug!("Connection established");
        Ok(Self { client })
    }

    pub fn cancel_token(&self) -> CancelToken {
        self.client.cancel_token()
    }

    pub fn run(&mut self, sql: &str, reuse_transaction: bool) -> Result<Vec<Row>> {
        crate::debug!("Running query", { "query": sql });

        // BEGIN inside an open transaction is only a warning, same as in the Ruby version
        let result = self
            .client
            .batch_execute("BEGIN;")
            .and_then(|_| self.client.simple_query(sql));

        match result {
            Ok(messages) => {
                if !reuse_transaction {
                    self.client.batch_execute("COMMIT;")?;
                }
                Ok(to_rows(messages))
            }
            Err(e) => {
                crate::info!("Exception raised, rolling back query", { "rollback": true, "query": sql });
                let _ = self.client.batch_execute("ROLLBACK;");
                Err(e.into())
            }
        }
    }

    /// Outside of any transaction (VACUUM can't run in one).
    pub fn exec_raw(&mut self, sql: &str) -> Result<()> {
        crate::debug!("Running query", { "query": sql });
        Ok(self.client.batch_execute(sql)?)
    }

    pub fn first_value(&mut self, sql: &str, reuse_transaction: bool) -> Result<Option<String>> {
        Ok(self
            .run(sql, reuse_transaction)?
            .into_iter()
            .next()
            .and_then(|r| r.values.into_iter().next().flatten()))
    }
}

fn to_rows(messages: Vec<SimpleQueryMessage>) -> Vec<Row> {
    messages
        .into_iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(r) => Some(Row {
                columns: r.columns().iter().map(|c| c.name().to_string()).collect(),
                values: (0..r.len()).map(|i| r.get(i).map(str::to_string)).collect(),
            }),
            _ => None,
        })
        .collect()
}

pub fn sqlstate(e: &anyhow::Error) -> Option<&SqlState> {
    e.downcast_ref::<postgres::Error>().and_then(|e| e.code())
}

pub fn is_pg_error(e: &anyhow::Error) -> bool {
    e.downcast_ref::<postgres::Error>().is_some()
}
