# airflow dag: build_marts (business vault -> data marts in ClickHouse database mart)
from datetime import datetime, timedelta, timezone

from airflow.sdk import dag, task

from celestrak import marts, orbits
from celestrak.assets import asset

DOC = """
### Витрины данных (ЛР3)
Запускается по Asset `dq_passed`, который публикует `data_quality` только при успехе
всех critical-проверок: витрины строятся из проверенного детального слоя.

* SQL-витрины: по файлу на витрину в `sql/mart/` (шапка `mart / insight / order_by`), читают
  только `bv.*` / `rv.*` / `meta.*`; каждая пересобирается в `mart.<name>__new` и атомарно
  подменяет живую таблицу (`EXCHANGE TABLES`).
* Орбитальные витрины (`build_orbits`): SGP4 по последним элементам GP/SupGP — точки сближений,
  дуги «космодром → спутник», GP vs SupGP, трассы избранных объектов, TLE для 3D-глобуса.
  Нужен `conjunction_risk`, поэтому идёт после него.

Журнал сборок: `meta.mart_log`. В конце публикуется Asset `mart_all`.
"""

MARTS = marts.load_marts()


@dag(
    dag_id="build_marts",
    description="Data marts from the business vault",
    doc_md=DOC,
    schedule=[asset("dq", "passed")],
    start_date=datetime(2026, 10, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["lr3", "mart"],
)
def build_marts():
    @task
    def ensure_schema() -> None:
        marts.ensure_schema()

    @task
    def build(name: str, dag_run=None) -> dict:
        mart = next(m for m in MARTS if m.name == name)
        return marts.build(mart, dag_run.run_id)

    @task
    def build_orbits(dag_run=None) -> list[dict]:
        return orbits.build_orbit_marts(dag_run.run_id)

    @task(outlets=[asset("mart", "all")])
    def publish(results: list[dict], orbit_results: list[dict]) -> None:
        for r in [*results, *orbit_results]:
            print(f"mart.{r['mart']}: {r['rows']} rows ({r['seconds']} s)")

    schema = ensure_schema()
    builds = {m.name: build.override(task_id=f"build_{m.name}")(m.name) for m in MARTS}
    schema >> list(builds.values())
    orbit_results = build_orbits()
    builds["conjunction_risk"] >> orbit_results
    publish(list(builds.values()), orbit_results)


build_marts()
