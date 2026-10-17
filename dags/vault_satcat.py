# airflow dag: vault_satcat (ODS satcat -> raw vault: objects, launches, catalog satellites)
from celestrak.vault_dag import build_vault_dag

DOC = """
### Raw vault: каталог объектов (ЛР2)
Запускается по Asset `ods_satcat`, то есть сразу после новой загрузки SATCAT.

`stage.satcat` (дедуп, ключ запуска `1998-067`, хэши) →
`hub_object`, `hub_launch` → `link_object_launch` →
`sat_object_satcat_desc` (редко меняется), `sat_object_satcat_status` (часто), `sat_launch`.
Сателлиты получают новую версию только у изменившихся объектов (сравнение hashdiff) —
это и есть SCD2: история в `bv.object_desc_history` / `bv.object_status_history`.
"""

build_vault_dag("satcat", ["satcat"], DOC)
