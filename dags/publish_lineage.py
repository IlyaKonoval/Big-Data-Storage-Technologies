# airflow dag: publish_lineage (pipeline lineage S3 -> ods -> vault -> mart into OpenMetadata)
from datetime import datetime, timedelta, timezone

import requests
from airflow.exceptions import AirflowSkipException
from airflow.sdk import dag, task

from celestrak import config, lineage
from celestrak.assets import asset

DOC = """
### Lineage в OpenMetadata (ЛР3)
Запускается по Asset `mart_all` (после `build_marts`) и вручную.

Рёбра выводятся из нашего кода (`celestrak.lineage`): у каждого SQL-шага есть цель
(`INSERT INTO` / `CREATE TABLE` / `CREATE VIEW`) и источники (`FROM` / `JOIN`), Python-шаги
перечислены явно. Каждое ребро привязано к DAG-у (пайплайн в OpenMetadata) и несёт SQL.

Путь: `S3 raw` → `ods.*` → `stage.*` → `rv.*` → `bv.*` → `mart.*`; дальше
`mart.*` → датасеты → дашборды Superset добавляет ingestion-воркфлоу `om-ingest`.
Таблицы в каталог тоже заносит `om-ingest`; ребро на ещё не известную таблицу пропускается.

Адрес и учётные данные — Airflow Connection `openmetadata`. Если OpenMetadata не запущена
(`--profile meta`), задача пропускается.
"""


@dag(
    dag_id="publish_lineage",
    description="Pipeline lineage into OpenMetadata",
    doc_md=DOC,
    schedule=[asset("mart", "all")],
    start_date=datetime(2026, 10, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["lr3", "lineage", "openmetadata"],
)
def publish_lineage():
    @task
    def publish() -> dict:
        url, email, password = config.openmetadata()
        try:
            om = lineage.Client(url, email, password)
        except requests.ConnectionError as e:
            raise AirflowSkipException(f"OpenMetadata is not running: {e}") from e
        return lineage.publish(om)

    publish()


publish_lineage()
