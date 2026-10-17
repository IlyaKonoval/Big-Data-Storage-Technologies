# airflow dag: vault_business (raw vault -> business vault: derived orbit geometry, PIT)
from datetime import datetime, timedelta, timezone

from airflow.sdk import dag, task

from celestrak import vault
from celestrak.assets import asset
from celestrak.vault_dag import VAULT_POOL

DOC = """
### Business vault (ЛР2)
Запускается после любого DAG raw vault (`vault_satcat | vault_orbit | vault_conjunction`).

* `bv.sat_object_orbit_derived` — вычисляемый сателлит: период, большая полуось, высоты
  перигея/апогея, режим орбиты LEO/MEO/GEO/HEO (обогащение);
* `bv.pit_object` — point-in-time по дням: какая версия каждого сателлита действовала на
  конец дня (агрегация, опора витрин ЛР3).
"""


@dag(
    dag_id="vault_business",
    description="Business vault: computed satellite and point-in-time table",
    doc_md=DOC,
    schedule=(asset("vault", "satcat") | asset("vault", "orbit") | asset("vault", "conjunction")),
    start_date=datetime(2026, 10, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["lr2", "vault", "dds", "business"],
)
def vault_business():
    @task
    def ensure_schema() -> None:
        vault.ensure_schema()

    @task(pool=VAULT_POOL, outlets=[asset("vault", "business")])
    def build() -> dict:
        return vault.build_business_vault()

    ensure_schema() >> build()


vault_business()
