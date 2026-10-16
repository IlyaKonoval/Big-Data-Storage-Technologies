"""Technical metadata in PostgreSQL (schema meta): raw file registry and load log."""
from contextlib import contextmanager

import psycopg2

from celestrak import config


@contextmanager
def _cursor():
    s = config.postgres()
    conn = psycopg2.connect(
        host=s.host, port=s.port, dbname=s.database, user=s.user, password=s.password
    )
    try:
        with conn, conn.cursor() as cur:
            yield cur
    finally:
        conn.close()


def last_sha256(source: str) -> str | None:
    """Hash of the latest raw file of the source that was loaded everywhere successfully."""
    with _cursor() as cur:
        cur.execute(
            """
            SELECT f.sha256
            FROM meta.raw_files f
            WHERE f.source = %s
              AND NOT EXISTS (SELECT 1 FROM meta.load_log l
                              WHERE l.load_id = f.load_id AND l.status <> 'success')
              AND EXISTS (SELECT 1 FROM meta.load_log l WHERE l.load_id = f.load_id)
            ORDER BY f.fetched_at DESC
            LIMIT 1
            """,
            (source,),
        )
        row = cur.fetchone()
    return row[0] if row else None


def register_raw_file(load_id, source, s3_key, url, size, sha256, fetched_at, dag_run_id):
    with _cursor() as cur:
        cur.execute(
            """
            INSERT INTO meta.raw_files
                (load_id, source, s3_key, url, bytes, sha256, fetched_at, dag_run_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (load_id) DO NOTHING
            """,
            (load_id, source, s3_key, url, size, sha256, fetched_at, dag_run_id),
        )


def log_load(load_id, source, target_db, rows, duration_sec, status, error=None):
    with _cursor() as cur:
        cur.execute(
            """
            INSERT INTO meta.load_log
                (load_id, source, target_db, rows_loaded, duration_sec, status, error)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (load_id, target_db) DO UPDATE
               SET rows_loaded = EXCLUDED.rows_loaded,
                   duration_sec = EXCLUDED.duration_sec,
                   status = EXCLUDED.status,
                   error = EXCLUDED.error,
                   finished_at = now() AT TIME ZONE 'utc'
            """,
            (load_id, source, target_db, rows, duration_sec, status, error),
        )


def benchmark_rows(rows: list[tuple]) -> None:
    """rows: (run_id, run_at, dbms, test_group, test_name, metric, value, rows)."""
    with _cursor() as cur:
        cur.executemany(
            """
            INSERT INTO meta.benchmark_results
                (run_id, run_at, dbms, test_group, test_name, metric, value, rows)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            rows,
        )


def benchmark_results(run_id: str) -> list[tuple]:
    with _cursor() as cur:
        cur.execute(
            """
            SELECT dbms, test_group, test_name, metric, value, rows
            FROM meta.benchmark_results WHERE run_id = %s ORDER BY id
            """,
            (run_id,),
        )
        return cur.fetchall()
