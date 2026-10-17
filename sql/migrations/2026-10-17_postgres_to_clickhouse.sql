-- One-off migration (17.10.2026): ClickHouse became the main storage, PostgreSQL dwh
-- was archived. Copies ODS snapshots and technical metadata from PostgreSQL with the same
-- load_id / loaded_at, so the history since 16.10 is preserved.
-- Run inside the clickhouse container while dwh-postgres is up:
--   clickhouse-client -u dwh --password dwh --multiquery < 2026-10-17_postgres_to_clickhouse.sql

-- ODS: the tables held the storage_benchmark copy of the same data; replace it.
TRUNCATE TABLE ods.satcat;
TRUNCATE TABLE ods.gp;
TRUNCATE TABLE ods.supgp;
TRUNCATE TABLE ods.socrates;
-- ods.satcat is NOT copied here: postgresql() reads PostgreSQL `date` as ClickHouse `Date`
-- (range starts in 1970), so launches before 1970 wrapped around (1957 -> 2137). It was
-- copied through CSV instead, which keeps Date32 intact:
--   docker compose exec -T dwh-postgres psql -U dwh -d dwh \
--     -c "COPY ods.satcat TO STDOUT WITH (FORMAT csv, HEADER true, NULL '\N')" \
--   | docker compose exec -T clickhouse clickhouse-client -u dwh --password dwh \
--     --date_time_input_format best_effort -q "INSERT INTO ods.satcat FORMAT CSVWithNames"
INSERT INTO ods.gp      SELECT * FROM postgresql('dwh-postgres:5432', 'dwh', 'gp',       'dwh', 'dwh', 'ods');
INSERT INTO ods.supgp    SELECT * FROM postgresql('dwh-postgres:5432', 'dwh', 'supgp',    'dwh', 'dwh', 'ods');
INSERT INTO ods.socrates SELECT * FROM postgresql('dwh-postgres:5432', 'dwh', 'socrates', 'dwh', 'dwh', 'ods');

-- meta
TRUNCATE TABLE meta.raw_files;
TRUNCATE TABLE meta.load_log;
TRUNCATE TABLE meta.benchmark_results;
INSERT INTO meta.raw_files
SELECT load_id, source, s3_key, url, bytes, sha256, fetched_at, dag_run_id
FROM postgresql('dwh-postgres:5432', 'dwh', 'raw_files', 'dwh', 'dwh', 'meta');

-- The snapshots now live in ClickHouse ODS, so their load log entries point there.
INSERT INTO meta.load_log
SELECT load_id, source, 'clickhouse', rows_loaded, duration_sec, status, error, finished_at
FROM postgresql('dwh-postgres:5432', 'dwh', 'load_log', 'dwh', 'dwh', 'meta');

INSERT INTO meta.benchmark_results
SELECT run_id, run_at, dbms, test_group, test_name, metric, value, rows
FROM postgresql('dwh-postgres:5432', 'dwh', 'benchmark_results', 'dwh', 'dwh', 'meta');
