"""Factory for the ingestion DAGs: CelesTrak -> S3 raw file -> ODS in PostgreSQL.

    extract ── load_ods ── validate  (outlet: Asset ods_<source>)

extract downloads the dataset once, skips the run when CelesTrak answers 403 or the
file is byte-identical to the last loaded one, and stores the file in SeaweedFS.
load_ods parses the file from S3 into typed rows and appends a snapshot to ods.<source>.
MySQL and ClickHouse are filled from ODS exports by the storage_benchmark DAG, so the
regular ingestion only needs PostgreSQL running.
"""
import hashlib
import logging
import time
from datetime import datetime, timedelta, timezone

from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.sdk import Asset, dag, task

from celestrak import client, meta, parsing, s3, targets
from celestrak.sources import Source

log = logging.getLogger(__name__)

CONTENT_TYPES = {"csv": "text/csv", "json": "application/json"}


def ods_asset(source_name: str) -> Asset:
    """Asset updated when a fresh snapshot of the source is in ODS (used by DDS DAGs)."""
    return Asset(name=f"ods_{source_name}", uri=f"celestrak://ods/{source_name}")


def build_ingest_dag(source: Source, schedule: str, doc_md: str):
    @dag(
        dag_id=f"ingest_{source.name}",
        description=source.description,
        doc_md=doc_md,
        schedule=schedule,
        start_date=datetime(2026, 10, 1, tzinfo=timezone.utc),
        catchup=False,
        max_active_runs=1,
        default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
        tags=["lr1", "ods", "celestrak", source.name],
    )
    def ingest():
        @task
        def extract(dag_run=None) -> dict:
            try:
                content = client.fetch(source.url, source.fmt)
            except client.SourceNotUpdated as exc:
                raise AirflowSkipException(str(exc))

            sha = hashlib.sha256(content).hexdigest()
            if meta.last_sha256(source.name) == sha:
                raise AirflowSkipException("File identical to the last loaded snapshot")

            fetched_at = datetime.now(timezone.utc).replace(tzinfo=None)
            load_id = f"{source.name}_{fetched_at:%Y%m%dT%H%M%S}"
            key = f"{source.name}/dt={fetched_at:%Y-%m-%d}/{load_id}.{source.fmt}"
            s3.put(key, content, CONTENT_TYPES[source.fmt])
            meta.register_raw_file(
                load_id, source.name, key, source.url, len(content), sha, fetched_at,
                dag_run.run_id if dag_run else None,
            )
            log.info("Stored s3://raw/%s (%d bytes)", key, len(content))
            return {"load_id": load_id, "s3_key": key, "fetched_at": fetched_at.isoformat()}

        @task
        def load_ods(raw: dict) -> dict:
            df = parsing.parse(source, s3.get(raw["s3_key"]))
            rows = parsing.to_rows(df, source, {
                "load_id": raw["load_id"],
                "loaded_at": datetime.fromisoformat(raw["fetched_at"]),
                "source_file": f"s3://raw/{raw['s3_key']}",
            })
            started = time.perf_counter()
            try:
                with targets.connect("postgres") as target:
                    n = target.load(source.table, source.ods_columns, rows, raw["load_id"])
            except Exception as exc:
                meta.log_load(raw["load_id"], source.name, "postgres", None,
                              time.perf_counter() - started, "failed", str(exc)[:1000])
                raise
            duration = time.perf_counter() - started
            meta.log_load(raw["load_id"], source.name, "postgres", n, duration, "success")
            log.info("%d rows into ods.%s in %.2fs", n, source.table, duration)
            return {"rows": n, "seconds": round(duration, 3)}

        @task(outlets=[ods_asset(source.name)])
        def validate(raw: dict, result: dict) -> int:
            """The snapshot in ODS must be complete: row count equals rows parsed from the file."""
            with targets.connect("postgres") as target:
                in_db = target.count(source.table, raw["load_id"])
            if in_db == 0 or in_db != result["rows"]:
                raise AirflowFailException(
                    f"ods.{source.table}: {in_db} rows for {raw['load_id']}, expected {result['rows']}"
                )
            log.info("ods.%s: snapshot %s complete, %d rows", source.table, raw["load_id"], in_db)
            return in_db

        raw = extract()
        validate(raw, load_ods(raw))

    return ingest()
