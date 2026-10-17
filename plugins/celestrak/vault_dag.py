"""Factory for the raw vault DAGs: new ODS snapshots -> stage -> hubs, links, satellites.

    ensure_schema ── load_<source> ... ── publish (outlet: Asset vault_<name>)

The DAG is scheduled by the ODS assets of its sources (data-aware scheduling: it runs
right after an ingest DAG published a fresh snapshot). load_<source> processes every
snapshot not yet in meta.vault_log, oldest first, and is skipped when there is none.
Load tasks of all vault DAGs share the one-slot pool `vault`: they write to the same hubs,
and ClickHouse has no unique constraints to arbitrate parallel inserts.
"""
import functools
import operator
from datetime import datetime, timedelta, timezone

from airflow.exceptions import AirflowSkipException
from airflow.sdk import dag, task

from celestrak import vault
from celestrak.assets import asset

VAULT_POOL = "vault"


def build_vault_dag(name: str, sources: list[str], doc_md: str):
    # Any of the sources having a fresh snapshot triggers the DAG (AssetAny).
    schedule = functools.reduce(operator.or_, [asset("ods", s) for s in sources])

    @dag(
        dag_id=f"vault_{name}",
        description=f"Raw vault load from ODS: {', '.join(sources)}",
        doc_md=doc_md,
        schedule=schedule,
        start_date=datetime(2026, 10, 1, tzinfo=timezone.utc),
        catchup=False,
        max_active_runs=1,
        default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
        tags=["lr2", "vault", "dds", *sources],
    )
    def build():
        @task
        def ensure_schema() -> None:
            vault.ensure_schema()

        @task(pool=VAULT_POOL)
        def load(source: str) -> list[dict]:
            pending = vault.pending_loads(source)
            if not pending:
                raise AirflowSkipException(f"No new ODS snapshots of {source}")
            return [vault.load_snapshot(source, load_id) for load_id in pending]

        @task(outlets=[asset("vault", name)], trigger_rule="none_failed_min_one_success")
        def publish(**context) -> None:
            """Announce fresh vault data (only reached when at least one source loaded)."""

        loads = [load.override(task_id=f"load_{s}")(s) for s in sources]
        ensure_schema() >> loads >> publish()

    return build()
