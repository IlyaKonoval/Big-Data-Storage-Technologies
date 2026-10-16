# airflow dag: ingest_supgp (CelesTrak -> S3 raw -> ODS in PostgreSQL)
from celestrak.ingest import build_ingest_dag
from celestrak.sources import SUPGP

build_ingest_dag(
    SUPGP,
    schedule="30 */8 * * *",
    doc_md="""
### SupGP Starlink -> ODS
Орбитальные элементы Starlink, рассчитанные по эфемеридам оператора (SpaceX),
с оценкой точности `rms` и источником `data_source`. Позволяет сравнить их с
«официальными» GP-элементами тех же объектов.
""",
)
