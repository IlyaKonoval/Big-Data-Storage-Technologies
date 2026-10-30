"""openmetadata/catalog.yaml covers the warehouse, the marts and the dashboards."""
import fnmatch
import re
from pathlib import Path

import yaml

from celestrak import dq, lineage, marts

ROOT = Path(__file__).resolve().parents[1]
SPEC = yaml.safe_load((ROOT / "openmetadata" / "catalog.yaml").read_text(encoding="utf-8"))
TABLES = SPEC["tables"]


def test_every_lineage_table_is_described():
    names = {n for edge in lineage.edges(lineage.all_steps()) for n in edge if "." in n}
    assert names <= set(TABLES), sorted(names - set(TABLES))


def test_every_mart_is_described():
    built = {m.name for m in marts.load_marts()} | set(lineage.ORBIT_MARTS)
    assert {f"mart.{m}" for m in built} <= set(TABLES)


def test_schemas_have_layer_tags_and_tiers_cover_tables():
    assert set(SPEC["schemas"]) == set(SPEC["layers"]["tags"])
    for table in TABLES:
        assert table.split(".")[0] in SPEC["schemas"]
        assert any(fnmatch.fnmatch(table, p) for ps in SPEC["tiers"].values() for p in ps), table


def test_glossary_columns_have_descriptions():
    for term in SPEC["glossary"]["terms"].values():
        for column in term.get("columns", []):
            assert column in SPEC["columns"], column


def test_every_chart_and_dashboard_is_described():
    code = (ROOT / "superset" / "bootstrap.py").read_text(encoding="utf-8")
    charts = re.findall(r'^    \("(?:Обзор|История|Риски)", "([^"]+)"', code, re.M)
    dashboards = re.findall(r'^    \("[a-z-]+", "([^"]+)", \[', code, re.M)
    assert charts and set(charts) <= set(SPEC["charts"])
    assert dashboards and set(dashboards) <= set(SPEC["dashboards"])


def test_every_dq_check_lands_on_a_catalogued_table():
    """openmetadata/quality.py attaches each sql/dq check to the first table it reads."""
    for check in dq.load_checks():
        _, sources, _ = lineage.parse("\n".join(lineage.statements(check.sql)))
        assert sources and sources[0] in TABLES, check.name


def test_kpis_use_known_charts_and_dates():
    kpis = SPEC["kpis"]
    assert kpis["start"] < kpis["end"]
    for kpi in kpis["items"].values():
        assert kpi["chart"] in {"percentage_of_data_asset_with_description_kpi",
                                "percentage_of_data_asset_with_owner_kpi"}
        assert 0 < kpi["target"] <= 100
