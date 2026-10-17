-- Technical metadata of the platform: raw file registry, load log, benchmark results,
-- vault load log and data quality results. ClickHouse has no UPDATE/UPSERT, so tables
-- that get rewritten use ReplacingMergeTree and are read with FINAL.
CREATE DATABASE IF NOT EXISTS meta;

-- One row per file fetched from CelesTrak and stored in S3 (SeaweedFS).
CREATE TABLE IF NOT EXISTS meta.raw_files
(
    load_id     String,
    source      LowCardinality(String),
    s3_key      String,
    url         String,
    bytes       UInt64,
    sha256      String,
    fetched_at  DateTime64(6),
    dag_run_id  Nullable(String)
)
ENGINE = ReplacingMergeTree
ORDER BY load_id;

-- One row per (raw file, target DB) load; a retry replaces the row (newest finished_at wins).
CREATE TABLE IF NOT EXISTS meta.load_log
(
    load_id       String,
    source        LowCardinality(String),
    target_db     LowCardinality(String),
    rows_loaded   Nullable(UInt64),
    duration_sec  Float64,
    status        LowCardinality(String),
    error         Nullable(String),
    finished_at   DateTime64(6) DEFAULT now64(6)
)
ENGINE = ReplacingMergeTree(finished_at)
ORDER BY (load_id, target_db);

-- Results of the storage_benchmark DAG.
CREATE TABLE IF NOT EXISTS meta.benchmark_results
(
    run_id      String,
    run_at      DateTime64(6),
    dbms        LowCardinality(String),
    test_group  LowCardinality(String),   -- load | query | storage
    test_name   LowCardinality(String),
    metric      LowCardinality(String),   -- seconds | rows_per_sec | bytes
    value       Float64,
    rows        Nullable(UInt64)
)
ENGINE = MergeTree
ORDER BY (run_id, dbms, test_group, test_name, metric);

-- One row per ODS snapshot loaded into the raw vault; also the incremental watermark.
CREATE TABLE IF NOT EXISTS meta.vault_log
(
    load_id       String,
    source        LowCardinality(String),
    ods_rows      UInt64,
    stage_rows    UInt64,
    steps         String,                 -- JSON: rows written by each SQL step
    duration_sec  Float64,
    finished_at   DateTime64(6) DEFAULT now64(6)
)
ENGINE = ReplacingMergeTree(finished_at)
ORDER BY load_id;

-- One row per check per data_quality run.
CREATE TABLE IF NOT EXISTS meta.dq_results
(
    run_id       String,
    checked_at   DateTime64(6) DEFAULT now64(6),
    check_name   LowCardinality(String),
    severity     LowCardinality(String),  -- critical | warning
    description  String,
    violations   UInt64,
    sample       String,
    passed       Bool
)
ENGINE = MergeTree
ORDER BY (run_id, check_name);
