# airflow dag: ingest_satcat (CelesTrak -> S3 raw -> ODS in PostgreSQL)
from celestrak.ingest import build_ingest_dag
from celestrak.sources import SATCAT

build_ingest_dag(
    SATCAT,
    schedule="0 3 * * *",  # catalog changes slowly: once a day
    doc_md="""
### SATCAT -> ODS
Полный каталог объектов на орбите и сошедших с неё (~70k строк, CSV).
Источник: `https://celestrak.org/pub/satcat.csv`. Используется как измерение «объект»
(владелец, тип, дата запуска/схода, площадка) в DDS.
""",
)
