-- Technical metadata of the platform: raw file registry, load log, benchmark results.
CREATE SCHEMA IF NOT EXISTS meta;

-- One row per file fetched from CelesTrak and stored in S3 (SeaweedFS).
CREATE TABLE IF NOT EXISTS meta.raw_files (
    load_id      TEXT PRIMARY KEY,
    source       TEXT        NOT NULL,
    s3_key       TEXT        NOT NULL,
    url          TEXT        NOT NULL,
    bytes        BIGINT      NOT NULL,
    sha256       TEXT        NOT NULL,
    fetched_at   TIMESTAMP   NOT NULL,
    dag_run_id   TEXT
);
CREATE INDEX IF NOT EXISTS ix_raw_files_source ON meta.raw_files (source, fetched_at DESC);

-- One row per (raw file, target DBMS) load attempt.
CREATE TABLE IF NOT EXISTS meta.load_log (
    id            BIGSERIAL PRIMARY KEY,
    load_id       TEXT        NOT NULL REFERENCES meta.raw_files (load_id),
    source        TEXT        NOT NULL,
    target_db     TEXT        NOT NULL,
    rows_loaded   BIGINT,
    duration_sec  DOUBLE PRECISION,
    status        TEXT        NOT NULL,
    error         TEXT,
    finished_at   TIMESTAMP   NOT NULL DEFAULT (now() AT TIME ZONE 'utc'),
    UNIQUE (load_id, target_db)
);

-- Results of the storage_benchmark DAG.
CREATE TABLE IF NOT EXISTS meta.benchmark_results (
    id          BIGSERIAL PRIMARY KEY,
    run_id      TEXT        NOT NULL,
    run_at      TIMESTAMP   NOT NULL,
    dbms        TEXT        NOT NULL,
    test_group  TEXT        NOT NULL,   -- load | query | storage
    test_name   TEXT        NOT NULL,
    metric      TEXT        NOT NULL,   -- seconds | rows_per_sec | bytes
    value       DOUBLE PRECISION NOT NULL,
    rows        BIGINT
);
CREATE INDEX IF NOT EXISTS ix_benchmark_run ON meta.benchmark_results (run_id);
