"""Lineage edges derived from the SQL files and the DAG / mart lists (no OpenMetadata needed)."""
import re
from pathlib import Path

import pytest

from celestrak import lineage, marts

ROOT = Path(__file__).resolve().parents[1]
STEPS = lineage.all_steps()
EDGES = lineage.edges(STEPS)


def upstream(table: str) -> set[str]:
    seen, todo = set(), [table]
    while todo:
        target = todo.pop()
        for source, t in EDGES:
            if t == target and source not in seen:
                seen.add(source)
                todo.append(source)
    return seen


def test_parse_insert_select():
    target, sources, view = lineage.parse(
        "INSERT INTO rv.hub_object SELECT * FROM stage.gp WHERE x NOT IN "
        "(SELECT x FROM rv.hub_object) AND y IN (SELECT y FROM stage.gp)")
    assert (target, sources, view) == ("rv.hub_object", ["stage.gp"], False)


def test_parse_view():
    target, sources, view = lineage.parse(
        "CREATE OR REPLACE VIEW bv.v AS SELECT 1 FROM rv.a AS a FINAL JOIN rv.b ON 1")
    assert (target, sources, view) == ("bv.v", ["rv.a", "rv.b"], True)


def test_statements_drop_comments():
    assert lineage.statements("SELECT 1 FROM rv.a -- FROM rv.x; ok\n;\n") == ["SELECT 1 FROM rv.a"]


@pytest.mark.parametrize("mart", [m.name for m in marts.load_marts()] + list(lineage.ORBIT_MARTS))
def test_every_mart_traces_back_to_s3(mart):
    up = upstream(f"mart.{mart}")
    if mart in ("pipeline_health", "dq_checks"):   # journals of the pipeline itself
        assert up & {"meta.dq_results", "meta.load_log"}
        return
    assert "s3:raw" in up
    assert any(t.startswith("rv.") for t in up) and any(t.startswith("stage.") for t in up)


def test_no_temporary_or_unknown_tables():
    names = {n for edge in EDGES for n in edge}
    assert not [n for n in names if n.endswith("__new")]
    assert {n.split(".")[0] for n in names if not n.startswith("s3:")} <= {
        "ods", "stage", "rv", "bv", "mart", "meta"}


def test_pipelines_are_known_dags():
    dags = {p.stem for p in (ROOT / "dags").glob("*.py")}   # one DAG per file, named alike
    used = {s.pipeline for s in STEPS if s.pipeline}
    assert used <= set(lineage.PIPELINES) <= dags


def test_vault_dags_match_dag_files():
    for path in (ROOT / "dags").glob("vault_*.py"):
        m = re.search(r'build_vault_dag\("([a-z_]+)", (\[[^\]]*\])', path.read_text(encoding="utf-8"))
        if m:
            for source in eval(m.group(2)):
                assert lineage.VAULT_DAGS[source] == f"vault_{m.group(1)}"


def test_orbit_marts_match_orbits_module():
    code = (ROOT / "plugins" / "celestrak" / "orbits.py").read_text(encoding="utf-8")
    built = set(re.findall(r'replace_table\(\s*"([a-z_]+)"', code))
    assert built == set(lineage.ORBIT_MARTS)
