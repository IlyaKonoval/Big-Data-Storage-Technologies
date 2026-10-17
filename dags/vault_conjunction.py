# airflow dag: vault_conjunction (ODS socrates -> raw vault: conjunction link and satellites)
from celestrak.vault_dag import build_vault_dag

DOC = """
### Raw vault: сближения (ЛР2)
Запускается по Asset `ods_socrates`.

Очистка: пара объектов упорядочивается (меньший NORAD первым, `dse_1/dse_2` переставляются),
пары объекта с самим собой и строки без TCA отбрасываются, дубли (пара, TCA) удаляются.
`hub_object` (оба объекта, в том числе отсутствующие в SATCAT) → `link_conjunction` →
`sat_conjunction` (multi-active: все TCA пары версионируются вместе) и
`sts_conjunction` (статус пары: появилась `I` / выпала из прогноза `D`).
"""

build_vault_dag("conjunction", ["socrates"], DOC)
