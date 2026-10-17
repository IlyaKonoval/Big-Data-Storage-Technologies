# airflow dag: vault_orbit (ODS gp + supgp -> raw vault: orbital element satellites)
from celestrak.vault_dag import build_vault_dag

DOC = """
### Raw vault: элементы орбит (ЛР2)
Запускается по `ods_gp | ods_supgp` — после новой загрузки любого из двух источников.

Каждый источник — свой сателлит на общем `hub_object` (правило Data Vault: источники не
перезаписывают друг друга): `sat_object_orbit_gp` (радар 18 SDS) и `sat_object_orbit_supgp`
(эфемериды оператора). Новая версия пишется только при изменении набора элементов.
"""

build_vault_dag("orbit", ["gp", "supgp"], DOC)
