# pg-osc (Rust)

Rust port of [pg-osc](https://github.com/shayonj/pg-osc) (`pg-online-schema-change`) with the `--filter` / `--wait-for-swap` additions from [dedloki/pg-osc](https://github.com/dedloki/pg-osc). One static-ish binary (needs only glibc ≥ 2.34, OpenSSL is built in), no Ruby.

It applies `ALTER TABLE` to a busy table through a shadow table: a trigger records changes into an audit table, the data is copied into the altered shadow table, the audit table is replayed, and the tables are swapped under a short `ACCESS EXCLUSIVE` lock. See the upstream README for the details and caveats of the approach.

## Install on Debian 12

```
sudo dpkg -i pg-osc_<version>-1_amd64.deb      # or arm64
pg-online-schema-change --version
```

or just copy the `pg-online-schema-change` binary to the host.

## Usage

```
export PGPASSWORD=...
pg-online-schema-change perform \
  --alter-statement 'ALTER TABLE books ADD COLUMN purchased BOOLEAN DEFAULT FALSE' \
  --dbname postgres --host localhost --username jamesbond
```

`pg-online-schema-change perform --help` lists every option. They are the same as in the Ruby version, except:

- `-w` is `--wait-time-for-lock` (the Ruby CLI had it twice); the password goes into `PGPASSWORD` (or `--password`).
- `--sslmode disable|prefer|require` (or `PGSSLMODE`), default `prefer`. As in libpq the certificate is not verified.
- `--help` has no short form, `-h` is the host.
- exit code after `SIGTERM`/`SIGINT`/`SIGQUIT` is 128 + signal.

### Copy a subset of rows and swap on demand

```
pg-online-schema-change perform \
  --alter-statement 'ALTER TABLE books ALTER COLUMN email SET NOT NULL' \
  --filter 'email IS NOT NULL' \
  --wait-for-swap \
  --dbname postgres --host localhost --username jamesbond
```

- `--filter` copies only the rows matching the condition and applies it during the replay too: an `UPDATE` that makes a row match adds it to the shadow table, one that makes it stop matching removes it. Use the original column names. Rows that don't match stay only in the old table `pgosc_op_table_<table>`.
- `--wait-for-swap` keeps the shadow table in sync after the copy (autovacuum is enabled on the audit and shadow tables meanwhile) until a swap is requested:

  ```
  pg-online-schema-change swap --table books --schema public --dbname postgres --host localhost --username jamesbond
  ```

  or `kill -USR1 <pid>` (the pid is in the logs). If the swap can't get its lock, the sync goes on and waits for the next request. `Ctrl-C`/`SIGTERM` stops it and removes the trigger, audit and shadow tables; a second signal exits immediately without cleanup.
- On failure the old table is never dropped, even with `--drop`.
- Foreign keys from other tables pointing to filtered-out rows fail `VALIDATE CONSTRAINT` after the swap (the swap is done by then, the constraints stay `NOT VALID`). Fix those rows first or use `--skip-foreign-key-validation`.

### Big tables: batched copy and deferred indexes

By default the copy is one `INSERT ... SELECT` in one SERIALIZABLE transaction, into a shadow table that already has all indexes. On a table with hundreds of millions of rows that means many hours of an open snapshot (vacuum can't remove dead rows anywhere in the database meanwhile, the xid horizon doesn't move) and an index insert per row per index.

```
pg-online-schema-change perform \
  --alter-statement 'ALTER TABLE barcode ALTER COLUMN stock_id SET NOT NULL' \
  --filter 'stock_id IS NOT NULL' --wait-for-swap \
  --copy-batch-pages 10000 --defer-indexes --maintenance-work-mem 4GB \
  --dbname mydb --host db.host --username myuser
```

- `--copy-batch-pages N` (PostgreSQL 14+) copies by ctid ranges of N heap pages (8kB each), every batch in its own short transaction, so the primary key type doesn't matter (uuid is fine). Without one snapshot a row changed during the copy can be copied twice or missed; every such row was changed after the trigger was set up, so it is in the audit table. After the copy the shadow rows whose primary key is in the audit table are deleted and the replay of the whole audit table brings them back in their final state. Progress is logged every minute with pages done and an ETA.
- `--defer-indexes` drops the shadow table's indexes, primary key and unique/exclusion constraints after the `ALTER` and builds them after the copy (primary key first), one sorted build per index instead of a random insert per row. Watch it in `pg_stat_progress_create_index`. `--maintenance-work-mem` is set for those builds; parallel workers follow `max_parallel_maintenance_workers`.
- Both work with `--filter`, together or apart. `--copy-batch-pages` can't be combined with `--copy-statement`.
- `--skip-vacuum` skips the `VACUUM` of the table that pg-osc runs before starting (kept from upstream as "house keeping"). The copy doesn't depend on it; it mainly sets hint bits and the visibility map, which matters only if the table has a lot of recently changed rows that autovacuum hasn't processed yet.
- `--verbose` logs only the first 2000 characters of each query (replay batches are thousands of statements).

## Differences from the Ruby version

- The replay copies rows with `INSERT ... SELECT` from the audit table instead of re-sending values as text literals. `UPDATE ... SET col = NULL` is replayed (the Ruby version dropped NULL values from replayed rows, so such updates were lost), and the types never round-trip through text.
- An `UPDATE` is replayed as delete + insert by primary key.
- Views are looked up by the old table's oid, not by name in every schema (the Ruby version could re-point views of a same-named table in another schema).
- After the swap the audit trigger is dropped from the old table too (the Ruby version left it there, pointing to the dropped audit table, so any write to the old table failed).
- `--copy-statement`: only `%{shadow_table}` is substituted, other `%` signs are left alone.
- Logs are the same JSON lines (bunyan format), unbuffered.

Known limitation kept from upstream: a view defined as `SELECT *` over a dropped column can't be recreated after the swap (`CREATE OR REPLACE VIEW` can't drop a column), the run then ends with an error after the swap. Recreate such views yourself.

## Build

Debian 12 binaries and `.deb` packages from macOS or Linux, cross-compiled with zig against glibc 2.34:

```
scripts/build-debian12.sh            # amd64 + arm64 into dist/
```

Needs `rustup`, `zig` and `cargo install cargo-zigbuild cargo-deb`.

Or natively inside Debian 12 with Docker:

```
docker build --platform linux/amd64 -f Dockerfile.debian12 --output dist .
```

## Tests

```
docker compose up -d        # Postgres on localhost:5432, user jamesbond
cargo test
```

Unit tests cover the SQL parsing, integration tests (`tests/integration.rs`) run whole operations against Postgres, each in its own schema: add column with FKs, triggers, views and storage parameters; rename + drop column with NULL updates; `--filter` with rows moving in and out; `--wait-for-swap` with a failed lock attempt; termination cleanup; the old table kept on failure; `--copy-statement`.
