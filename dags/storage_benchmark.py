# airflow dag: storage_benchmark — compares PostgreSQL, MySQL and ClickHouse on CelesTrak ODS data
import logging
import os
import tempfile
import time
from datetime import datetime, timezone

from airflow.exceptions import AirflowFailException
from airflow.sdk import dag, task

from celestrak import benchmark, config, meta, report, s3, targets
from celestrak.sources import SOURCES

log = logging.getLogger(__name__)

EXPORT_BUCKET = "exports"
TABLES = list(SOURCES)

DOC = """
### Сравнение хранилищ (ЛР1, п.5)
Требует профиль `compare`: `docker compose --profile compare up -d`.

1. **export_ods** — выгружает все таблицы ODS из PostgreSQL в CSV (`s3://exports/<run>/`).
2. **bench_postgres → bench_mysql → bench_clickhouse** — по очереди (чтобы СУБД не
   конкурировали за CPU/диск): загрузка одного и того же CSV родным bulk-методом,
   проверка числа строк, 6 аналитических запросов (медиана из 5), размер на диске.
   После этого шага в MySQL и ClickHouse лежит та же копия ODS, что и в PostgreSQL.
3. **render_report** — графики и таблица в `reports/lr1/`, сырые цифры в `meta.benchmark_results`.
"""


@dag(
    dag_id="storage_benchmark",
    description="ODS export -> load + query + size benchmark in PostgreSQL / MySQL / ClickHouse",
    doc_md=DOC,
    schedule=None,
    start_date=datetime(2026, 10, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["lr1", "benchmark"],
)
def storage_benchmark():
    @task
    def export_ods(dag_run=None) -> dict:
        prefix = dag_run.run_id.replace(":", "-").replace("+", "")
        exported = {}
        with targets.connect("postgres") as pg, tempfile.TemporaryDirectory() as tmp:
            for name in TABLES:
                path = os.path.join(tmp, f"{name}.csv")
                pg.export_csv(name, SOURCES[name].ods_columns, path)
                key = f"{prefix}/{name}.csv"
                with open(path, "rb") as f:
                    s3.put(key, f.read(), "text/csv", bucket=EXPORT_BUCKET)
                exported[name] = {"key": key, "rows": pg.count(name),
                                  "bytes": os.path.getsize(path)}
                log.info("Exported ods.%s: %s", name, exported[name])
        if not any(e["rows"] for e in exported.values()):
            raise AirflowFailException("ODS is empty: run the ingest_* DAGs first")
        return exported

    @task
    def bench(dbms: str, exported: dict, dag_run=None) -> None:
        run_at = datetime.now(timezone.utc).replace(tzinfo=None)
        out = []

        def record(group, test, metric, value, rows=None):
            out.append((dag_run.run_id, run_at, dbms, group, test, metric, value, rows))

        # PostgreSQL loads into the bench schema so its ODS stays intact; the others
        # hold nothing but this copy, so they load straight into their ods tables.
        load_schema = "bench" if dbms == "postgres" else "ods"
        with targets.connect(dbms) as db, tempfile.TemporaryDirectory() as tmp:
            for name, info in exported.items():
                path = os.path.join(tmp, f"{name}.csv")
                with open(path, "wb") as f:
                    f.write(s3.get(info["key"], bucket=EXPORT_BUCKET))
                started = time.perf_counter()
                db.load_csv(load_schema, name, SOURCES[name].ods_columns, path)
                seconds = time.perf_counter() - started
                loaded = db.count(name, schema=load_schema)
                if loaded != info["rows"]:
                    raise AirflowFailException(
                        f"{dbms}.{load_schema}.{name}: {loaded} rows, expected {info['rows']}")
                record("load", name, "seconds", seconds, loaded)
                record("load", name, "rows_per_sec", loaded / seconds if seconds else 0, loaded)
                log.info("%s: loaded %s (%d rows) in %.2fs", dbms, name, loaded, seconds)

            for name, seconds, rows in benchmark.run_queries(db):
                record("query", name, "seconds", seconds, rows)
                log.info("%s: %s median %.1f ms (%d rows)", dbms, name, seconds * 1000, rows)

            for name in exported:
                record("storage", name, "bytes", db.table_bytes("ods", name), exported[name]["rows"])
        meta.benchmark_rows(out)

    @task
    def render_report(exported: dict, dag_run=None) -> list[str]:
        results = meta.benchmark_results(dag_run.run_id)
        with targets.connect("postgres") as pg:
            raw = dict(pg.query("SELECT source, sum(bytes) FROM meta.raw_files GROUP BY source"))
        extra = {"csv_bytes": {k: v["bytes"] for k, v in exported.items()},
                 "raw_bytes": {k: int(v) for k, v in raw.items()}}
        files = report.render(dag_run.run_id, results, extra,
                              os.path.join(config.REPORTS_DIR, "lr1"))
        log.info("Report files: %s", files)
        return files

    exported = export_ods()
    pg = bench.override(task_id="bench_postgres")("postgres", exported)
    my = bench.override(task_id="bench_mysql")("mysql", exported)
    ch = bench.override(task_id="bench_clickhouse")("clickhouse", exported)
    exported >> pg >> my >> ch >> render_report(exported)


storage_benchmark()
