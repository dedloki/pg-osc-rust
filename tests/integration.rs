//! End-to-end tests against a real Postgres (see README: `docker compose up`).
//! Connection: PGHOST (127.0.0.1), PGPORT (5432), PGUSER (jamesbond),
//! PGDATABASE (postgres), PGPASSWORD. Every test works in its own schema,
//! so they run in parallel.

use pg_osc::ctx::{Flags, Terminated};
use pg_osc::db::{ConnOpts, Db};
use pg_osc::options::{ConnArgs, Options, PerformArgs};
use pg_osc::orchestrate::{self, request_swap};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

fn conn_args(schema: &str) -> ConnArgs {
    ConnArgs {
        schema: schema.into(),
        dbname: std::env::var("PGDATABASE").unwrap_or("postgres".into()),
        host: std::env::var("PGHOST").unwrap_or("127.0.0.1".into()),
        username: std::env::var("PGUSER").unwrap_or("jamesbond".into()),
        port: std::env::var("PGPORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(5432),
        password: std::env::var("PGPASSWORD").ok(),
        sslmode: "disable".into(),
        help: None,
    }
}

fn args(schema: &str, alter: &str) -> PerformArgs {
    PerformArgs {
        alter_statement: alter.into(),
        conn: conn_args(schema),
        verbose: false,
        drop: false,
        kill_backends: false,
        wait_time_for_lock: 5,
        copy_statement: None,
        pull_batch_count: 1000,
        delta_count: 20,
        skip_foreign_key_validation: false,
        filter: None,
        wait_for_swap: false,
        sync_interval: 0.05,
        copy_batch_pages: None,
        defer_indexes: false,
        maintenance_work_mem: None,
        skip_vacuum: false,
    }
}

struct TestDb {
    schema: String,
    db: Db,
}

impl TestDb {
    /// Same tables as the Ruby specs (spec/support/database_helpers.rb).
    fn new(name: &str) -> Self {
        let schema = format!("pgosc_test_{name}");
        let mut db = Db::connect(&conn(&schema)).expect("Postgres for tests, see README");
        let sql = format!(
            r#"
            DROP SCHEMA IF EXISTS "{s}" CASCADE;
            CREATE SCHEMA "{s}";
            SET search_path TO "{s}";
            CREATE TABLE sellers (id serial PRIMARY KEY, name VARCHAR(50) UNIQUE NOT NULL, "createdOn" TIMESTAMP NOT NULL, last_login TIMESTAMP);
            CREATE TABLE books (
              user_id serial PRIMARY KEY,
              username VARCHAR(50) UNIQUE NOT NULL,
              seller_id SERIAL REFERENCES sellers NOT NULL,
              password VARCHAR(50) NOT NULL,
              email VARCHAR(255) UNIQUE NOT NULL,
              "createdOn" TIMESTAMP NOT NULL,
              last_login TIMESTAMP
            ) WITH (autovacuum_enabled=true,autovacuum_vacuum_scale_factor=0,autovacuum_vacuum_threshold=20000);
            CREATE TABLE book_audits (id serial PRIMARY KEY, book_id SERIAL REFERENCES books NOT NULL, changed_on TIMESTAMP(6) NOT NULL);
            CREATE OR REPLACE FUNCTION email_changes() RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
            BEGIN
              IF NEW.email <> OLD.email THEN
                INSERT INTO book_audits(book_id, changed_on) VALUES (OLD.user_id, now());
              END IF;
              RETURN NEW;
            END; $$;
            CREATE TRIGGER email_changes AFTER UPDATE ON books FOR EACH ROW EXECUTE PROCEDURE email_changes();
            CREATE TABLE chapters (id serial PRIMARY KEY, name VARCHAR(50) UNIQUE NOT NULL, book_id SERIAL REFERENCES books NOT NULL);
            INSERT INTO sellers(name, "createdOn", last_login) VALUES ('local shop', clock_timestamp(), clock_timestamp());
            INSERT INTO books(seller_id, username, password, email, "createdOn", last_login) VALUES
              (1, 'jamesbond2', '007', 'james1@bond.com', clock_timestamp(), clock_timestamp()),
              (1, 'jamesbond3', '008', 'james2@bond.com', clock_timestamp(), clock_timestamp()),
              (1, 'jamesbond4', '009', 'james3@bond.com', clock_timestamp(), clock_timestamp());
            INSERT INTO chapters(name, book_id) VALUES ('ch1', 1);
            CREATE VIEW books_view AS SELECT * FROM books WHERE seller_id = 1;
            "#,
            s = schema
        );
        db.run(&sql, false).unwrap();
        Self { schema, db }
    }

    fn exec(&mut self, sql: &str) {
        self.db.run(sql, false).unwrap();
    }

    fn values(&mut self, sql: &str) -> Vec<String> {
        self.db
            .run(sql, false)
            .unwrap()
            .into_iter()
            .map(|r| {
                r.values
                    .into_iter()
                    .map(|v| v.unwrap_or("NULL".into()))
                    .collect::<Vec<_>>()
                    .join("|")
            })
            .collect()
    }

    fn pgosc_tables(&mut self) -> Vec<String> {
        let s = self.schema.clone();
        self.values(&format!(
            "SELECT relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname = '{s}' AND relkind = 'r' AND relname LIKE 'pgosc%' ORDER BY 1"
        ))
    }
}

impl Drop for TestDb {
    fn drop(&mut self) {
        let _ = self.db.run(
            &format!("DROP SCHEMA IF EXISTS \"{}\" CASCADE", self.schema),
            false,
        );
    }
}

fn conn(schema: &str) -> ConnOpts {
    conn_args(schema).conn_opts()
}

fn wait_until(what: &str, mut cond: impl FnMut() -> bool) {
    let start = Instant::now();
    while !cond() {
        assert!(
            start.elapsed() < Duration::from_secs(30),
            "timeout waiting for {what}"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn spawn_perform(a: PerformArgs) -> (Arc<Flags>, std::thread::JoinHandle<anyhow::Result<()>>) {
    let flags = Arc::new(Flags::default());
    let f = flags.clone();
    let opts = Options::from_args(&a).unwrap();
    (flags, std::thread::spawn(move || orchestrate::run(opts, f)))
}

/// Waits until the background perform is in its sync loop (audit table has a
/// comment-able state and the copy is done: shadow table has the rows).
fn wait_for_sync(t: &mut TestDb, min_rows: usize) {
    wait_until("initial copy", || {
        let tables = t.pgosc_tables();
        let Some(shadow) = tables.iter().find(|n| n.starts_with("pgosc_st_")) else {
            return false;
        };
        let Some(audit) = tables.iter().find(|n| n.starts_with("pgosc_at_")) else {
            return false;
        };
        // autovacuum is switched on right before the sync loop starts
        let ready = t.values(&format!(
            "SELECT 1 FROM pg_class WHERE relname = '{audit}' AND reloptions @> ARRAY['autovacuum_enabled=true']"
        ));
        !ready.is_empty()
            && t.values(&format!("SELECT count(*) FROM {shadow}"))[0]
                .parse::<usize>()
                .unwrap()
                >= min_rows
    });
}

#[test]
fn options_validation() {
    let ok = |a: &PerformArgs| Options::from_args(a).map(|_| ()).map_err(|e| e.to_string());

    assert!(ok(&args("s", "CREATE DATABASE foo"))
        .unwrap_err()
        .contains("Not a valid ALTER statement"));

    let mut a = args("s", "ALTER TABLE books ADD COLUMN a int");
    a.delta_count = 100;
    a.pull_batch_count = 50;
    assert!(ok(&a).unwrap_err().contains("delta_count"));

    let a = args(
        "s",
        "ALTER TABLE books ADD COLUMN a int; ALTER TABLE cards ADD COLUMN a int",
    );
    assert!(ok(&a).unwrap_err().contains("same table"));

    let mut a = args("s", "ALTER TABLE books ADD COLUMN a int");
    a.filter = Some("x IS NOT NULL; DROP TABLE books".into());
    assert!(ok(&a).unwrap_err().contains("Not a valid filter"));

    let mut a = args("s", "ALTER TABLE books ADD COLUMN a int");
    a.filter = Some("WHERE x IS NOT NULL".into());
    a.copy_statement = Some("Cargo.toml".into());
    assert!(ok(&a).unwrap_err().contains("can't be used together"));

    let mut a = args("s", "ALTER TABLE books ADD COLUMN a int");
    a.copy_statement = Some("no/such/file.sql".into());
    assert!(ok(&a).unwrap_err().contains("File not found"));

    let mut a = args("s", "ALTER TABLE \"Books\" ADD COLUMN a int");
    a.filter = Some("  where x IS NOT NULL ".into());
    let o = Options::from_args(&a).unwrap();
    assert_eq!(o.filter.as_deref(), Some("x IS NOT NULL"));
    assert_eq!(
        (o.table.as_str(), o.table_name.as_str()),
        ("Books", "\"Books\"")
    );
}

#[test]
fn perform_adds_column_and_swaps() {
    let mut t = TestDb::new("add_column");
    let s = t.schema.clone();

    let a = args(
        &s,
        "ALTER TABLE books ADD COLUMN \"purchased\" BOOLEAN DEFAULT FALSE;",
    );
    orchestrate::run(Options::from_args(&a).unwrap(), Arc::new(Flags::default())).unwrap();

    t.exec(&format!("SET search_path TO \"{s}\""));
    assert_eq!(
        t.values("SELECT user_id, username, purchased FROM books ORDER BY 1"),
        vec!["1|jamesbond2|f", "2|jamesbond3|f", "3|jamesbond4|f"]
    );
    // old table kept (no --drop), helper tables gone
    assert_eq!(t.pgosc_tables(), vec!["pgosc_op_table_books"]);
    // FKs from other tables point to the new table and are validated
    assert_eq!(
        t.values("SELECT conrelid::regclass, confrelid::regclass, convalidated FROM pg_constraint WHERE contype = 'f' AND confrelid = 'books'::regclass ORDER BY 1"),
        vec!["book_audits|books|t", "chapters|books|t"]
    );
    // own FK, trigger, view and storage params moved over
    assert_eq!(t.values("SELECT confrelid::regclass FROM pg_constraint WHERE contype = 'f' AND conrelid = 'books'::regclass"), vec!["sellers"]);
    assert_eq!(
        t.values(
            "SELECT tgname FROM pg_trigger WHERE tgrelid = 'books'::regclass AND NOT tgisinternal"
        ),
        vec!["email_changes"]
    );
    assert_eq!(t.values("SELECT count(*) FROM books_view"), vec!["3"]);
    assert_eq!(
        t.values("SELECT array_to_string(reloptions, ',') FROM pg_class WHERE oid = 'books'::regclass"),
        vec!["autovacuum_enabled=true,autovacuum_vacuum_scale_factor=0,autovacuum_vacuum_threshold=20000"]
    );
    // the sequence continues after the copied ids
    t.exec("INSERT INTO books(seller_id, username, password, email, \"createdOn\") VALUES (1, 'new', 'x', 'new@b.c', now())");
    assert_eq!(t.values("SELECT max(user_id) FROM books"), vec!["4"]);
}

#[test]
fn perform_with_drop_removes_old_table() {
    let mut t = TestDb::new("drop_old");
    let s = t.schema.clone();
    let mut a = args(
        &s,
        "ALTER TABLE books ADD COLUMN purchased BOOLEAN DEFAULT FALSE;",
    );
    a.drop = true;
    orchestrate::run(Options::from_args(&a).unwrap(), Arc::new(Flags::default())).unwrap();
    assert!(t.pgosc_tables().is_empty());
}

#[test]
fn replay_with_rename_and_drop_keeps_nulls() {
    let mut t = TestDb::new("rename_drop");
    let s = t.schema.clone();
    // books_view is SELECT *, CREATE OR REPLACE VIEW can't drop its password column
    t.exec(&format!("DROP VIEW \"{s}\".books_view"));
    let mut a = args(&s, "ALTER TABLE books RENAME COLUMN email TO new_email; ALTER TABLE books DROP COLUMN password;");
    a.wait_for_swap = true;
    let (_flags, handle) = spawn_perform(a);
    wait_for_sync(&mut t, 3);

    t.exec(&format!(
        r#"SET search_path TO "{s}";
        INSERT INTO books(seller_id, username, password, email, "createdOn", last_login) VALUES (1, 'it''s me', 'p', 'q''uote@b.c', now(), NULL);
        UPDATE books SET last_login = NULL, email = 'changed@b.c' WHERE user_id = 1;
        DELETE FROM chapters; DELETE FROM book_audits; DELETE FROM books WHERE user_id = 2;"#
    ));
    request_swap(&conn(&s), &s, "books").unwrap();
    handle.join().unwrap().unwrap();

    t.exec(&format!("SET search_path TO \"{s}\""));
    assert_eq!(
        t.values("SELECT user_id, username, new_email, last_login FROM books ORDER BY 1"),
        vec![
            "1|jamesbond2|changed@b.c|NULL".to_string(),
            format!(
                "3|jamesbond4|james3@bond.com|{}",
                t.values("SELECT last_login FROM pgosc_op_table_books WHERE user_id = 3")[0]
            ),
            "4|it's me|q'uote@b.c|NULL".to_string(),
        ]
    );
    assert!(t.values("SELECT 1 FROM information_schema.columns WHERE table_schema = current_schema AND table_name = 'books' AND column_name = 'password'").is_empty());
}

#[test]
fn filter_copies_and_replays_matching_rows_only() {
    let mut t = TestDb::new("filter");
    let s = t.schema.clone();
    t.exec(&format!(
        "SET search_path TO \"{s}\"; UPDATE books SET last_login = NULL WHERE user_id = 2"
    ));

    let mut a = args(
        &s,
        "ALTER TABLE books ALTER COLUMN last_login SET NOT NULL;",
    );
    a.filter = Some("WHERE last_login IS NOT NULL".into());
    a.wait_for_swap = true;
    // chapters/book_audits reference rows that are filtered out
    a.skip_foreign_key_validation = true;
    let (_flags, handle) = spawn_perform(a);
    wait_for_sync(&mut t, 2);

    let shadow = t
        .pgosc_tables()
        .into_iter()
        .find(|n| n.starts_with("pgosc_st_"))
        .unwrap();
    assert_eq!(
        t.values(&format!("SELECT user_id FROM \"{s}\".{shadow} ORDER BY 1")),
        vec!["1", "3"]
    );

    t.exec(&format!(
        r#"SET search_path TO "{s}";
        INSERT INTO books (user_id, seller_id, username, password, email, "createdOn", last_login)
          VALUES (10, 1, 'with_login', 'x', 'a@b.c', now(), now()),
                 (11, 1, 'without_login', 'x', 'b@b.c', now(), NULL);
        UPDATE books SET last_login = now() WHERE user_id = 2;    -- moves in
        DELETE FROM chapters; UPDATE books SET last_login = NULL WHERE user_id = 1; -- moves out
        UPDATE books SET password = 'changed' WHERE user_id = 3;  -- stays, changed
        UPDATE books SET password = 'changed' WHERE user_id = 11; -- stays out
        DELETE FROM books WHERE user_id = 10;"#
    ));
    request_swap(&conn(&s), &s, "books").unwrap();
    handle.join().unwrap().unwrap();

    t.exec(&format!("SET search_path TO \"{s}\""));
    assert_eq!(
        t.values("SELECT user_id, password FROM books ORDER BY 1"),
        vec!["2|008", "3|changed"]
    );
    assert_eq!(t.values("SELECT attnotnull FROM pg_attribute WHERE attrelid = 'books'::regclass AND attname = 'last_login'"), vec!["t"]);
    assert_eq!(
        t.values("SELECT user_id FROM pgosc_op_table_books ORDER BY 1"),
        vec!["1", "2", "3", "11"]
    );
    // the old table stays writable: no trigger pointing to the dropped audit table
    t.exec("UPDATE pgosc_op_table_books SET password = 'later' WHERE user_id = 11");
}

#[test]
fn swap_waits_for_request_and_survives_lock_failure() {
    let mut t = TestDb::new("lock_failure");
    let s = t.schema.clone();
    let mut a = args(
        &s,
        "ALTER TABLE books ADD COLUMN purchased BOOLEAN DEFAULT FALSE;",
    );
    a.wait_for_swap = true;
    a.wait_time_for_lock = 1;
    let (flags, handle) = spawn_perform(a);
    wait_for_sync(&mut t, 3);

    // no request -> no swap
    std::thread::sleep(Duration::from_millis(500));
    assert!(!handle.is_finished());

    // hold a lock so the swap can't get ACCESS EXCLUSIVE (3 attempts x 1s)
    let mut blocker = Db::connect(&conn(&s)).unwrap();
    blocker
        .run(
            &format!("LOCK TABLE \"{s}\".books IN ACCESS SHARE MODE"),
            true,
        )
        .unwrap();
    flags.swap_requested.store(true, Ordering::SeqCst); // what SIGUSR1 does

    wait_until("failed swap clears the request", || {
        !flags.swap_requested.load(Ordering::SeqCst)
    });
    blocker.run("COMMIT", false).unwrap();
    assert!(!handle.is_finished());
    assert!(
        !t.pgosc_tables()
            .contains(&"pgosc_op_table_books".to_string()),
        "not swapped yet"
    );

    t.exec(&format!("INSERT INTO \"{s}\".books(seller_id, username, password, email, \"createdOn\") VALUES (1, 'late', 'x', 'late@b.c', now())"));
    request_swap(&conn(&s), &s, "books").unwrap();
    handle.join().unwrap().unwrap();
    assert_eq!(
        t.values(&format!(
            "SELECT count(*), count(purchased) FROM \"{s}\".books"
        )),
        vec!["4|4"]
    );
}

#[test]
fn terminate_cleans_up_and_keeps_the_table() {
    let mut t = TestDb::new("terminate");
    let s = t.schema.clone();
    let mut a = args(
        &s,
        "ALTER TABLE books ADD COLUMN purchased BOOLEAN DEFAULT FALSE;",
    );
    a.wait_for_swap = true;
    let (flags, handle) = spawn_perform(a);
    wait_for_sync(&mut t, 3);

    flags.terminate.store(15, Ordering::SeqCst);
    let err = handle.join().unwrap().unwrap_err();
    assert_eq!(err.downcast_ref::<Terminated>().map(|t| t.0), Some(15));

    assert!(t.pgosc_tables().is_empty());
    t.exec(&format!("SET search_path TO \"{s}\""));
    assert_eq!(t.values("SELECT count(*) FROM books"), vec!["3"]);
    assert!(t.values("SELECT 1 FROM pg_trigger WHERE tgrelid = 'books'::regclass AND tgname = 'primary_to_audit_table_trigger'").is_empty());
}

#[test]
fn failure_after_swap_never_drops_the_old_table() {
    let mut t = TestDb::new("keep_old");
    let s = t.schema.clone();
    t.exec(&format!(
        "SET search_path TO \"{s}\"; UPDATE books SET last_login = NULL WHERE user_id = 1"
    ));

    // chapters references book 1, which is filtered out -> VALIDATE CONSTRAINT fails after the swap
    let mut a = args(
        &s,
        "ALTER TABLE books ALTER COLUMN last_login SET NOT NULL;",
    );
    a.filter = Some("last_login IS NOT NULL".into());
    a.drop = true;
    let err =
        orchestrate::run(Options::from_args(&a).unwrap(), Arc::new(Flags::default())).unwrap_err();
    assert!(
        format!("{err:#}").contains("violates foreign key"),
        "{err:#}"
    );

    assert_eq!(t.pgosc_tables(), vec!["pgosc_op_table_books"]);
    assert_eq!(
        t.values("SELECT count(*) FROM pgosc_op_table_books"),
        vec!["3"]
    );
}

#[test]
fn copy_statement_is_used() {
    let mut t = TestDb::new("copy_statement");
    let s = t.schema.clone();
    let dir = std::env::temp_dir().join(format!("pgosc_copy_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let file = dir.join("copy.sql");
    std::fs::write(
        &file,
        "INSERT INTO %{shadow_table}(user_id, seller_id, username, password, email, \"createdOn\", last_login, purchased)\n\
         SELECT user_id, seller_id, username, password, email, \"createdOn\", last_login, true FROM ONLY books WHERE username LIKE 'james%'",
    )
    .unwrap();
    let mut a = args(
        &s,
        "ALTER TABLE books ADD COLUMN purchased BOOLEAN DEFAULT FALSE;",
    );
    a.copy_statement = Some(file.to_string_lossy().into());
    orchestrate::run(Options::from_args(&a).unwrap(), Arc::new(Flags::default())).unwrap();
    assert_eq!(
        t.values(&format!(
            "SELECT count(*) FROM \"{s}\".books WHERE purchased"
        )),
        vec!["3"]
    );
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn swap_request_errors() {
    let t = TestDb::new("swap_errors");
    let err = request_swap(&conn(&t.schema), &t.schema, "books").unwrap_err();
    assert!(err.to_string().contains("No running pg-osc sync"));
}

#[test]
fn binary_reports_setup_errors() {
    // used to exit with 1 without a single log line
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_pg-online-schema-change"))
        .args([
            "perform",
            "-a",
            "ALTER TABLE b ALTER COLUMN stock_id SET NOT NULL",
            "-d",
            "no_such_db_pgosc",
            "-h",
            "127.0.0.1",
            "-u",
            "postgres",
            "-p",
            "1",
            "--filter",
            "WHERE stock_id IS NOT NULL",
            "--wait-for-swap",
        ])
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert_eq!(out.status.code(), Some(1));
    assert!(stdout.contains("\"msg\":\"Setup failed: "), "{stdout}");
    assert!(
        stdout.contains("\"dbname\":\"no_such_db_pgosc\""),
        "{stdout}"
    );
}

/// Table like the production `barcode`: uuid primary key, unique column,
/// nullable stock_id, plain index. Writes keep going during the copy, the
/// sync and up to the swap; afterwards the new table must equal the old one
/// (filtered).
fn copy_under_load(name: &str, batch_pages: Option<u64>, defer: bool, filter: bool) {
    let mut t = TestDb::new(name);
    let s = t.schema.clone();
    t.exec(&format!(
        r#"SET search_path TO "{s}";
        CREATE TABLE barcode (
          id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
          stock_id int,
          barcode text NOT NULL UNIQUE,
          quantity int,
          marked boolean,
          is_child boolean
        );
        CREATE INDEX barcode_stock_id_idx ON barcode (stock_id);
        INSERT INTO barcode (stock_id, barcode, quantity, marked, is_child)
          SELECT CASE WHEN g % 4 = 0 THEN NULL ELSE g % 1000 END, 'bc' || g, g % 7, g % 2 = 0, false
          FROM generate_series(1, 40000) g;"#
    ));

    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let writes = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let writer = {
        let (stop, writes, s) = (stop.clone(), writes.clone(), s.clone());
        std::thread::spawn(move || {
            let mut db = Db::connect(&conn(&s)).unwrap();
            db.run(&format!("SET search_path TO \"{s}\""), false)
                .unwrap();
            let mut n: u64 = 0;
            while !stop.load(Ordering::SeqCst) {
                n += 1;
                let pick = "SELECT id FROM barcode TABLESAMPLE SYSTEM (2) LIMIT 5";
                let sql = match n % 6 {
                    // non-HOT: unique column changes, row moves to another page
                    0 => format!("UPDATE barcode SET barcode = barcode || '-{n}', quantity = quantity + 1 WHERE id IN ({pick})"),
                    // moves rows in and out of the filter
                    1 => format!("UPDATE barcode SET stock_id = NULL WHERE id IN ({pick})"),
                    2 => format!("UPDATE barcode SET stock_id = {} WHERE id IN ({pick})", n % 1000),
                    3 => format!("INSERT INTO barcode (stock_id, barcode, quantity) VALUES ({}, 'new{n}', 1), (NULL, 'newnull{n}', 1)", n % 1000),
                    4 => format!("DELETE FROM barcode WHERE id IN ({pick})"),
                    _ => format!("UPDATE barcode SET marked = NOT coalesce(marked, false) WHERE id IN ({pick})"),
                };
                if db.run(&sql, false).is_ok() {
                    writes.fetch_add(1, Ordering::SeqCst);
                }
            }
        })
    };

    let mut a = args(
        &s,
        "ALTER TABLE barcode ALTER COLUMN stock_id SET NOT NULL;",
    );
    a.copy_batch_pages = batch_pages;
    a.defer_indexes = defer;
    a.maintenance_work_mem = defer.then(|| "64MB".to_string());
    a.wait_for_swap = true;
    if filter {
        a.filter = Some("stock_id IS NOT NULL".into());
    } else {
        a.alter_statement = "ALTER TABLE barcode ADD COLUMN note text;".into();
    }
    let (_flags, handle) = spawn_perform(a);

    // keep writing during the sync for a while, then swap under load
    wait_until("sync loop", || {
        t.pgosc_tables().iter().any(|n| n.starts_with("pgosc_at_"))
            && !t
                .values(&format!(
                    "SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
                     WHERE n.nspname = '{s}' AND relname LIKE 'pgosc_at_%' AND reloptions @> ARRAY['autovacuum_enabled=true']"
                ))
                .is_empty()
    });
    std::thread::sleep(Duration::from_millis(500));
    // after the swap `barcode` is the new table and further writes would go
    // there only, so stop writing first (whatever is left in the audit table
    // is replayed by the swap itself)
    stop.store(true, Ordering::SeqCst);
    writer.join().unwrap();
    request_swap(&conn(&s), &s, "barcode").unwrap();
    handle.join().unwrap().unwrap();
    assert!(
        writes.load(Ordering::SeqCst) > 50,
        "the writer did too little"
    );

    t.exec(&format!("SET search_path TO \"{s}\""));
    let cols = "id, stock_id, barcode, quantity, marked, is_child";
    let old = if filter {
        "pgosc_op_table_barcode WHERE stock_id IS NOT NULL"
    } else {
        "pgosc_op_table_barcode"
    };
    // no writes since before the swap: both tables must be equal
    let diff = t.values(&format!(
        "SELECT (SELECT count(*) FROM (SELECT {cols} FROM barcode EXCEPT SELECT {cols} FROM {old}) d),
                (SELECT count(*) FROM (SELECT {cols} FROM {old} EXCEPT SELECT {cols} FROM barcode) d),
                (SELECT count(*) FROM barcode), (SELECT count(*) FROM {old})"
    ));
    let parts: Vec<&str> = diff[0].split('|').collect();
    assert_eq!((parts[0], parts[1]), ("0", "0"), "new vs old: {diff:?}");
    assert_eq!(parts[2], parts[3]);
    assert!(parts[2].parse::<u64>().unwrap() > 20000);

    // indexes and constraints are back, primary key included
    assert_eq!(
        t.values(
            "SELECT string_agg(contype::text, ',' ORDER BY contype) FROM pg_constraint WHERE conrelid = 'barcode'::regclass AND contype IN ('p','u')"
        ),
        vec!["p,u"]
    );
    assert_eq!(
        t.values("SELECT count(*) FROM pg_indexes WHERE schemaname = current_schema AND tablename = 'barcode'"),
        vec!["3"]
    );
    if filter {
        assert_eq!(
            t.values("SELECT attnotnull FROM pg_attribute WHERE attrelid = 'barcode'::regclass AND attname = 'stock_id'"),
            vec!["t"]
        );
    }
}

#[test]
fn batched_copy_deferred_indexes_filter_under_load() {
    copy_under_load("batched_defer_filter", Some(1), true, true);
}

#[test]
fn batched_copy_with_indexes_under_load() {
    copy_under_load("batched_indexes", Some(3), false, false);
}

#[test]
fn deferred_indexes_single_copy_under_load() {
    copy_under_load("defer_single", None, true, true);
}

#[test]
fn batched_copy_option_conflicts() {
    let mut a = args("s", "ALTER TABLE books ADD COLUMN a int");
    a.copy_batch_pages = Some(0);
    assert!(Options::from_args(&a)
        .unwrap_err()
        .to_string()
        .contains("> 0"));
    a.copy_batch_pages = Some(100);
    a.copy_statement = Some("Cargo.toml".into());
    assert!(Options::from_args(&a)
        .unwrap_err()
        .to_string()
        .contains("can't be used together"));
}

fn vacuum_count_of_old_table(t: &mut TestDb) -> String {
    let s = t.schema.clone();
    t.values(&format!(
        "SELECT vacuum_count FROM pg_stat_user_tables WHERE schemaname = '{s}' AND relname = 'pgosc_op_table_books'"
    ))[0]
        .clone()
}

#[test]
fn vacuum_runs_by_default_and_can_be_skipped() {
    for (name, skip, expected) in [
        ("vacuum_default", false, "1"),
        ("vacuum_skipped", true, "0"),
    ] {
        let mut t = TestDb::new(name);
        let s = t.schema.clone();
        let mut a = args(
            &s,
            "ALTER TABLE books ADD COLUMN purchased BOOLEAN DEFAULT FALSE;",
        );
        a.skip_vacuum = skip;
        orchestrate::run(Options::from_args(&a).unwrap(), Arc::new(Flags::default())).unwrap();
        // the stats belong to the relation, which is the old table after the swap
        assert_eq!(vacuum_count_of_old_table(&mut t), expected, "{name}");
    }
}

#[test]
fn cleanup_waits_for_the_trigger_lock_without_blocking_writes() {
    let mut t = TestDb::new("cleanup_lock");
    let s = t.schema.clone();
    let mut a = args(
        &s,
        "ALTER TABLE books ADD COLUMN purchased BOOLEAN DEFAULT FALSE;",
    );
    a.wait_for_swap = true;
    a.wait_time_for_lock = 1;
    let (flags, handle) = spawn_perform(a);
    wait_for_sync(&mut t, 3);
    assert!(flags
        .cleanup_sql()
        .unwrap()
        .contains("DROP TRIGGER IF EXISTS"));

    // a long transaction on the table: DROP TRIGGER can't get its lock
    let mut blocker = Db::connect(&conn(&s)).unwrap();
    blocker
        .run(&format!("SELECT count(*) FROM \"{s}\".books"), true)
        .unwrap();
    flags.terminate.store(15, Ordering::SeqCst);
    std::thread::sleep(Duration::from_millis(2500));
    assert!(!handle.is_finished(), "cleanup must wait for the lock");
    // the trigger and its audit table are still there, writes keep working
    // (each wait for the lock is limited to lock_timeout)
    assert_eq!(t.pgosc_tables().len(), 2);
    let mut writer = Db::connect(&conn(&s)).unwrap();
    writer.run("SET lock_timeout = '3s'", false).unwrap();
    writer.run(&format!("INSERT INTO \"{s}\".books(seller_id, username, password, email, \"createdOn\") VALUES (1, 'during', 'x', 'during@b.c', now())"), false).unwrap();

    blocker.run("COMMIT", false).unwrap();
    let err = handle.join().unwrap().unwrap_err();
    assert_eq!(err.downcast_ref::<Terminated>().map(|t| t.0), Some(15));
    assert!(t.pgosc_tables().is_empty());
    assert!(flags.cleanup_sql().is_none());
    t.exec(&format!("SET search_path TO \"{s}\""));
    assert!(t.values("SELECT 1 FROM pg_trigger WHERE tgrelid = 'books'::regclass AND tgname = 'primary_to_audit_table_trigger'").is_empty());
    assert_eq!(t.values("SELECT count(*) FROM books"), vec!["4"]);
}

#[test]
fn binary_cleans_up_on_sighup() {
    // a closed ssh session / terminal sends HUP: used to kill the process
    // without cleanup, leaving the trigger on the table
    let mut t = TestDb::new("sighup");
    let s = t.schema.clone();
    let c = conn_args(&s);
    let child = std::process::Command::new(env!("CARGO_BIN_EXE_pg-online-schema-change"))
        .args([
            "perform",
            "-a",
            "ALTER TABLE books ADD COLUMN purchased BOOLEAN DEFAULT FALSE",
            "-d",
            &c.dbname,
            "-h",
            &c.host,
            "-u",
            &c.username,
            "-p",
            &c.port.to_string(),
            "-s",
            &s,
            "--sslmode",
            "disable",
            "--wait-for-swap",
            "--sync-interval",
            "0.05",
        ])
        .stdout(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    wait_for_sync(&mut t, 3);
    std::process::Command::new("kill")
        .args(["-HUP", &child.id().to_string()])
        .status()
        .unwrap();
    let out = child.wait_with_output().unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert_eq!(out.status.code(), Some(129), "{stdout}");
    assert!(stdout.contains("Audit trigger dropped"), "{stdout}");
    assert!(t.pgosc_tables().is_empty());
    t.exec(&format!("SET search_path TO \"{s}\""));
    assert!(t.values("SELECT 1 FROM pg_trigger WHERE tgrelid = 'books'::regclass AND tgname = 'primary_to_audit_table_trigger'").is_empty());
}
