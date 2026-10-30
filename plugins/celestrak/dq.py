"""Data quality checks: one SQL file per check in sql/dq/, the query returns violations.

Each file starts with a header:

    -- check: <name>
    -- severity: critical | warning
    -- description: <what must hold>

A check passes when its query returns no rows. Results go to meta.dq_results; the
data_quality DAG fails when a critical check fails.
"""
import logging
import re
from dataclasses import dataclass
from pathlib import Path

from celestrak import config

log = logging.getLogger(__name__)

DQ_DIR = Path(config.SQL_DIR) / "dq"
SEVERITIES = ("critical", "warning")
SAMPLE_ROWS = 5


@dataclass(frozen=True)
class Check:
    name: str
    severity: str
    description: str
    sql: str


def parse_check(text: str) -> Check:
    header = dict(re.findall(r"^--\s*(check|severity|description):\s*(.+?)\s*$", text, re.M))
    missing = {"check", "severity", "description"} - header.keys()
    if missing:
        raise ValueError(f"DQ check header lacks {sorted(missing)}")
    if header["severity"] not in SEVERITIES:
        raise ValueError(f"Unknown severity {header['severity']!r}")
    sql = "\n".join(line for line in text.splitlines() if not line.lstrip().startswith("--"))
    return Check(header["check"], header["severity"], header["description"],
                 sql.strip().rstrip(";"))


def load_checks() -> list[Check]:
    return [parse_check(p.read_text(encoding="utf-8")) for p in sorted(DQ_DIR.glob("*.sql"))]


def run_checks(run_id: str) -> list[dict]:
    # Imported here: parsing the checks must not need the ClickHouse driver (OpenMetadata
    # runs them as tests from its ingestion image, see openmetadata/quality.py).
    from celestrak import meta

    results = []
    with meta.client() as ch:
        for check in load_checks():
            rows = ch.query(f"SELECT count() AS n, groupArray({SAMPLE_ROWS})(toString(tuple(*))) "
                            f"FROM ({check.sql}) AS v").result_rows
            violations, sample = int(rows[0][0]), rows[0][1]
            results.append({"check": check.name, "severity": check.severity,
                            "description": check.description, "violations": violations,
                            "sample": "\n".join(sample), "passed": violations == 0})
            log.info("%-30s %-8s %s (%d)", check.name, check.severity,
                     "ok" if violations == 0 else "FAILED", violations)
        ch.insert("meta.dq_results",
                  [(run_id, r["check"], r["severity"], r["description"], r["violations"],
                    r["sample"], r["passed"]) for r in results],
                  column_names=["run_id", "check_name", "severity", "description", "violations",
                                "sample", "passed"])
    return results
