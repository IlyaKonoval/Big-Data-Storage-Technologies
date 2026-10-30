"""Data quality tests of every catalogued table, run by OpenMetadata (ingest.py, after catalog).

Each table gets a basic test suite of native OpenMetadata tests, executed against ClickHouse
by the TestSuite workflow; results show up under Data Observability:

    every table      rows > 0; column count as now (schema drift)
    key columns      hash keys and NORAD ID not null; unique where they are the key
    ranges           eccentricity 0..1, inclination 0..180, probabilities 0..1
    sql/dq/*.sql     our 15 checks as tableCustomSQLQuery on their main table: the query
                     returns violations, so it passes on zero rows (the data_quality DAG gate
                     runs the same SQL)
"""
import logging
import os
from pathlib import Path

from metadata.workflow.data_quality import TestSuiteWorkflow

from celestrak import dq, lineage

log = logging.getLogger("om-quality")

# Columns that identify a row of the table: not null and unique.
KEYS = {
    "rv.hub_object": ["hk_object", "norad_cat_id"],
    "rv.hub_launch": ["hk_launch", "launch_designator"],
    "rv.link_object_launch": ["hk_object_launch"],
    "rv.link_conjunction": ["hk_conjunction"],
    "rv.ref_launch_site": ["code"],
    "rv.ref_object_type": ["code"],
    "rv.ref_ops_status": ["code"],
    "stage.gp": ["hk_object"],
    "stage.supgp": ["hk_object"],
    "stage.satcat": ["hk_object"],
    "bv.object_current": ["norad_cat_id"],
    "mart.orbit_elements": ["norad_cat_id"],
    "mart.chronicle": ["norad_cat_id"],
    "mart.gp_vs_supgp": ["norad_cat_id"],
    "mart.launch_sites": ["code"],
}
NOT_NULL = {"hk_object", "hk_launch", "hk_conjunction", "hk_object_launch", "hk_object_1",
            "hk_object_2", "norad_cat_id", "norad_cat_id_1", "norad_cat_id_2", "load_dts"}
RANGES = {"eccentricity": (0, 1), "inclination": (0, 180), "inclination_deg": (0, 180),
          "max_prob": (0, 1)}


def param(name: str, value) -> dict:
    return {"name": name, "value": str(value)}


def dq_tests() -> dict[str, list[dict]]:
    """sql/dq checks as custom SQL tests, keyed by the first table each check reads."""
    out: dict[str, list[dict]] = {}
    for check in dq.load_checks():
        sql = "\n".join(lineage.statements(check.sql))
        _, sources, _ = lineage.parse(sql)
        out.setdefault(sources[0], []).append({
            "name": f"dq_{check.name}",
            "displayName": f"DQ: {check.name} ({check.severity})",
            "description": check.description,
            "testDefinitionName": "tableCustomSQLQuery",
            "parameterValues": [param("sqlExpression", sql), param("strategy", "ROWS"),
                                param("operator", "<="), param("threshold", 0)]})
    return out


def table_tests(table: str, columns: list[str]) -> list[dict]:
    short = table.replace(".", "_")
    tests = [
        {"name": f"{short}_has_rows", "displayName": "Таблица не пуста",
         "testDefinitionName": "tableRowCountToBeBetween", "parameterValues": [param("minValue", 1)]},
        {"name": f"{short}_column_count", "displayName": "Схема не изменилась (число колонок)",
         "testDefinitionName": "tableColumnCountToEqual",
         "parameterValues": [param("columnCount", len(columns))]},
    ]
    keys = KEYS.get(table, [])
    for col in columns:
        if col in NOT_NULL or col in keys:
            tests.append({"name": f"{short}_{col}_not_null", "displayName": f"{col} не пуст",
                          "testDefinitionName": "columnValuesToBeNotNull", "columnName": col})
        if col in keys:
            tests.append({"name": f"{short}_{col}_unique", "displayName": f"{col} уникален",
                          "testDefinitionName": "columnValuesToBeUnique", "columnName": col})
        if col in RANGES:
            lo, hi = RANGES[col]
            tests.append({"name": f"{short}_{col}_range", "displayName": f"{col} в [{lo}, {hi}]",
                          "testDefinitionName": "columnValuesToBeBetween", "columnName": col,
                          "parameterValues": [param("minValue", lo), param("maxValue", hi)]})
    return tests


def workflow_config(fqn: str, tests: list[dict]) -> dict:
    return {
        "source": {"type": "TestSuite", "serviceName": lineage.CLICKHOUSE_SERVICE,
                   "sourceConfig": {"config": {"type": "TestSuite",
                                               "entityFullyQualifiedName": fqn}}},
        "processor": {"type": "orm-test-runner",
                      "config": {"testCases": tests, "forceUpdate": True}},
        "sink": {"type": "metadata-rest", "config": {}},
        "workflowConfig": {"loggerLevel": "WARN", "openMetadataServerConfig": {
            "hostPort": os.environ["OM_URL"], "authProvider": "openmetadata",
            "securityConfig": {"jwtToken": os.environ["OM_JWT"]}}},
    }


def run_suite(om: lineage.Client, table: str, tests: list[dict]) -> bool:
    fqn = lineage.table_fqn(table)
    # The first run of a table only creates its test suite; run again to execute the tests.
    for _ in range(1 if om.entity("dataQuality/testSuites", f"{fqn}.testSuite") else 2):
        workflow = TestSuiteWorkflow.create(workflow_config(fqn, tests))
        try:
            workflow.execute()
        except Exception:
            log.exception("Test suite of %s failed to run", table)
            return False
        finally:
            workflow.stop()
    return True


def run(om: lineage.Client, tables: list[str]) -> dict:
    """Create/update and execute the tests of the tables; returns counts and failures."""
    custom = dq_tests()
    total, errors = 0, []
    for table in tables:
        e = om.entity("tables", lineage.table_fqn(table))
        if not e:
            continue
        e = om.request("GET", f"/v1/tables/{e['id']}", params={"fields": "columns"})
        tests = table_tests(table, [c["name"] for c in e["columns"]]) + custom.get(table, [])
        total += len(tests)
        if not run_suite(om, table, tests):
            errors.append(table)
    failed = failing_tests(om)
    log.info("Quality: %d tests on %d tables, failing: %s", total, len(tables), failed or "none")
    return {"tests": total, "tables": len(tables), "suite_errors": errors, "failing": failed}


def failing_tests(om: lineage.Client) -> list[str]:
    failed, after = [], None
    while True:
        page = om.request("GET", "/v1/dataQuality/testCases", params={
            "limit": 200, "fields": "testCaseResult", **({"after": after} if after else {})})
        failed += [t["name"] for t in page["data"]
                   if (t.get("testCaseResult") or {}).get("testCaseStatus") not in ("Success", None)]
        after = page.get("paging", {}).get("after")
        if not after:
            return sorted(failed)
