# airflow dag: ingest_gp (CelesTrak -> S3 raw -> ODS in PostgreSQL)
from celestrak.ingest import build_ingest_dag
from celestrak.sources import GP

build_ingest_dag(
    GP,
    schedule="15 */4 * * *",  # GP updates ~every 2h; every 4h keeps us well inside the policy
    doc_md="""
### GP (active) -> ODS
Орбитальные элементы (OMM, JSON) всех активных спутников, группа `active`.
Каждая загрузка — снимок; история снимков даёт временные ряды орбит.
CelesTrak отвечает 403 на повторный запрос до обновления данных — такой запуск пропускается (skipped).
""",
)
