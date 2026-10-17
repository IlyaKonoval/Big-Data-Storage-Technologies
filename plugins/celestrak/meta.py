"""Technical metadata in ClickHouse (database meta): raw file registry and load log.

ClickHouse has no UPSERT: rewritten rows (load_log, vault_log) live in ReplacingMergeTree
tables and are read with FINAL, which keeps only the newest version of each key.
"""
from contextlib import contextmanager

import clickhouse_connect

from celestrak import config


@contextmanager
def client():
    s = config.clickhouse()
    ch = clickhouse_connect.get_client(
        host=s.host, port=s.port, username=s.user, password=s.password, database="meta",
        send_receive_timeout=900,
    )
    try:
        yield ch
    finally:
        ch.close()


def last_sha256(source: str) -> str | None:
    """Hash of the latest raw file of the source that was loaded everywhere successfully."""
    with client() as ch:
        rows = ch.query(
            """
            SELECT f.sha256
            FROM meta.raw_files AS f FINAL
            WHERE f.source = {source:String}
              AND f.load_id IN (SELECT load_id FROM meta.load_log FINAL
                                GROUP BY load_id HAVING countIf(status != 'success') = 0)
            ORDER BY f.fetched_at DESC
            LIMIT 1
            """,
            parameters={"source": source},
        ).result_rows
    return rows[0][0] if rows else None


def register_raw_file(load_id, source, s3_key, url, size, sha256, fetched_at, dag_run_id):
    with client() as ch:
        ch.insert(
            "meta.raw_files",
            [(load_id, source, s3_key, url, size, sha256, fetched_at, dag_run_id)],
            column_names=["load_id", "source", "s3_key", "url", "bytes", "sha256",
                          "fetched_at", "dag_run_id"],
        )


def log_load(load_id, source, target_db, rows, duration_sec, status, error=None):
    with client() as ch:
        ch.insert(
            "meta.load_log",
            [(load_id, source, target_db, rows, duration_sec, status, error)],
            column_names=["load_id", "source", "target_db", "rows_loaded", "duration_sec",
                          "status", "error"],
        )


BENCHMARK_COLUMNS = ["run_id", "run_at", "dbms", "test_group", "test_name", "metric",
                     "value", "rows"]


def benchmark_rows(rows: list[tuple]) -> None:
    """rows: (run_id, run_at, dbms, test_group, test_name, metric, value, rows)."""
    with client() as ch:
        ch.insert("meta.benchmark_results", rows, column_names=BENCHMARK_COLUMNS)


def benchmark_results(run_id: str) -> list[tuple]:
    with client() as ch:
        return ch.query(
            """
            SELECT dbms, test_group, test_name, metric, value, rows
            FROM meta.benchmark_results WHERE run_id = {run_id:String}
            """,
            parameters={"run_id": run_id},
        ).result_rows


def raw_bytes_by_source() -> dict[str, int]:
    with client() as ch:
        rows = ch.query("SELECT source, sum(bytes) FROM meta.raw_files FINAL GROUP BY source")
    return {source: int(total) for source, total in rows.result_rows}
