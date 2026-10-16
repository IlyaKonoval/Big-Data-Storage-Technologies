# airflow dag: ingest_socrates (CelesTrak -> S3 raw -> ODS in PostgreSQL)
from celestrak.ingest import build_ingest_dag
from celestrak.sources import SOCRATES

build_ingest_dag(
    SOCRATES,
    schedule="45 */12 * * *",
    doc_md="""
### SOCRATES -> ODS
Прогноз сближений пар объектов на 7 дней (~200k строк, CSV): время максимального
сближения (TCA), минимальная дистанция, относительная скорость, максимальная вероятность
столкновения. Неизменившиеся выгрузки отбрасываются по sha256.
""",
)
