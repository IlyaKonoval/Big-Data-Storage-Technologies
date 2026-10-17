"""Structure of the Data Vault and DQ SQL files (no database needed)."""
import re

import pytest

from celestrak import dq, vault


def test_split_statements_skips_comments_and_empty_parts():
    sql = "-- header; with semicolon\nCREATE DATABASE a;\n\n-- note\nSELECT 1;\nSELECT 2"
    assert vault.split_statements(sql) == ["CREATE DATABASE a", "SELECT 1", "SELECT 2"]


def test_ddl_files_split_into_statements():
    assert vault.DDL_FILES, "no DDL files found"
    for path in vault.DDL_FILES:
        statements = vault.split_statements(path.read_text(encoding="utf-8"))
        assert statements, path
        assert all(not s.endswith(";") for s in statements)


@pytest.mark.parametrize("source", sorted(vault.STAGE_KEYS))
def test_load_steps_are_single_statements_starting_with_stage(source):
    steps = vault.steps(source)
    assert steps[0].stem == "00_stage"
    stage_sql = steps[0].read_text(encoding="utf-8")
    assert f"CREATE TABLE stage.{source}" in stage_sql
    assert "{load_id:String}" in stage_sql
    for path in steps:
        assert len(vault.split_statements(path.read_text(encoding="utf-8"))) == 1, path
    # hubs (1x) before links (2x) before satellites (3x, 4x)
    prefixes = [int(p.stem[:2]) for p in steps]
    assert prefixes == sorted(prefixes)


def test_satellite_steps_are_delta_loads():
    """Every satellite insert compares with the latest version known at this snapshot."""
    for source in vault.STAGE_KEYS:
        for path in vault.steps(source):
            if re.match(r"3\d_sat_", path.name):
                sql = path.read_text(encoding="utf-8")
                assert "argMax(" in sql and "load_dts <=" in sql, path


def test_dq_checks_have_valid_headers_and_unique_names():
    checks = dq.load_checks()
    assert len(checks) >= 10
    names = [c.name for c in checks]
    assert len(names) == len(set(names))
    assert {c.severity for c in checks} <= set(dq.SEVERITIES)
    assert any(c.severity == "critical" for c in checks)
    for c in checks:
        assert c.sql.upper().lstrip().startswith(("SELECT", "WITH")), c.name
        assert not c.sql.endswith(";")


def test_parse_check_rejects_bad_header():
    with pytest.raises(ValueError):
        dq.parse_check("-- check: x\n-- severity: critical\nSELECT 1")
    with pytest.raises(ValueError):
        dq.parse_check("-- check: x\n-- severity: fatal\n-- description: d\nSELECT 1")
