# airflow dag: data_quality (checks of the Data Vault, results in meta.dq_results)
from datetime import datetime, timezone

from airflow.exceptions import AirflowFailException
from airflow.sdk import dag, task

from celestrak import dq
from celestrak.assets import asset

DOC = """
### Data Quality (ЛР2, п.5)
Запускается после `vault_business`. Каждая проверка — SQL-файл в `sql/dq/`, запрос
возвращает нарушения; результат и до 5 примеров пишутся в `meta.dq_results`.

* целостность vault: ключи хабов, «сироты» среди линков и сателлитов (в ClickHouse нет FK),
  повторные версии сателлитов, полнота business vault и PIT;
* сверка объёмов ODS → vault и совпадение последней версии со снимком;
* аномальные значения: элементы орбит и параметры сближений;
* предупреждения: расхождения в данных запуска, объекты вне SATCAT, эпохи в будущем, свежесть GP.

Провал хотя бы одной `critical`-проверки роняет DAG; `warning` только фиксируется.
"""


@dag(
    dag_id="data_quality",
    description="Data quality checks of the Data Vault",
    doc_md=DOC,
    schedule=[asset("vault", "business")],
    start_date=datetime(2026, 10, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["lr2", "dq"],
)
def data_quality():
    @task
    def run_checks(dag_run=None) -> list[dict]:
        return dq.run_checks(dag_run.run_id)

    @task
    def gate(results: list[dict]) -> None:
        failed = [r for r in results if r["severity"] == "critical" and not r["passed"]]
        warnings = [r["check"] for r in results if r["severity"] == "warning" and not r["passed"]]
        if warnings:
            print(f"Warnings: {', '.join(warnings)}")
        if failed:
            raise AirflowFailException(
                "Critical checks failed: "
                + "; ".join(f"{r['check']} ({r['violations']})" for r in failed))

    gate(run_checks())


data_quality()
