"""Data marts (ЛР3) in ClickHouse database `mart`, built from the business vault.

Every SQL mart is one file sql/mart/NN_<name>.sql: a header and a single SELECT.

    -- mart: orbit_population            table name in database mart
    -- insight: ...                      the question the mart answers (shown in docs)
    -- order_by: (orbit_regime, ...)     MergeTree sorting key

A mart is rebuilt in full on every run into mart.<name>__new and then swapped with the live
table (EXCHANGE TABLES is atomic), so dashboards never see a half-built mart. The orbit
marts computed in Python (orbits.py) go through the same swap via `replace_table`.
"""
import logging
import re
import time
from dataclasses import dataclass
from pathlib import Path

from celestrak import config, meta
from celestrak.vault import split_statements

log = logging.getLogger(__name__)

MART_DIR = Path(config.SQL_DIR) / "mart"
DDL = "CREATE DATABASE IF NOT EXISTS mart"

HEADER = re.compile(r"^--\s*(mart|insight|order_by):\s*(.*)$")


@dataclass(frozen=True)
class Mart:
    name: str
    insight: str
    order_by: str
    sql: str
    path: Path


def parse_mart(path: Path) -> Mart:
    fields: dict[str, list[str]] = {}
    key = None
    for line in path.read_text(encoding="utf-8").splitlines():
        m = HEADER.match(line)
        if m:
            key = m.group(1)
            fields[key] = [m.group(2).strip()]
        elif key and line.startswith("--"):
            fields[key].append(line.lstrip("- ").strip())   # continuation line
        else:
            key = None
    missing = {"mart", "order_by"} - fields.keys()
    if missing:
        raise ValueError(f"{path.name}: header misses {sorted(missing)}")
    statements = split_statements(path.read_text(encoding="utf-8"))
    if len(statements) != 1 or not re.match(r"(?is)^(WITH|SELECT)\b", statements[0]):
        raise ValueError(f"{path.name}: must contain exactly one SELECT")
    return Mart(name=fields["mart"][0], insight=" ".join(fields.get("insight", [])),
                order_by=fields["order_by"][0], sql=statements[0], path=path)


def load_marts() -> list[Mart]:
    return [parse_mart(p) for p in sorted(MART_DIR.glob("*.sql"))]


def ensure_schema() -> None:
    with meta.client() as ch:
        ch.command(DDL)


def _swap(ch, name: str) -> int:
    """Make mart.<name>__new the live mart.<name>; returns its row count."""
    if int(ch.command(f"EXISTS TABLE mart.{name}")):
        ch.command(f"EXCHANGE TABLES mart.{name}__new AND mart.{name}")
        ch.command(f"DROP TABLE mart.{name}__new")
    else:
        ch.command(f"RENAME TABLE mart.{name}__new TO mart.{name}")
    return int(ch.command(f"SELECT count() FROM mart.{name}"))


def _log(ch, name: str, rows: int, seconds: float, run_id: str | None) -> None:
    ch.insert("meta.mart_log", [(name, run_id or "", rows, seconds)],
              column_names=["mart", "run_id", "rows", "duration_sec"])


def build(mart: Mart, run_id: str | None = None) -> dict:
    started = time.perf_counter()
    with meta.client() as ch:
        ch.command(f"DROP TABLE IF EXISTS mart.{mart.name}__new")
        ch.command(f"CREATE TABLE mart.{mart.name}__new ENGINE = MergeTree "
                   f"ORDER BY {mart.order_by} SETTINGS allow_nullable_key = 1 AS {mart.sql}")
        rows = _swap(ch, mart.name)
        seconds = time.perf_counter() - started
        _log(ch, mart.name, rows, seconds, run_id)
    log.info("mart.%s: %d rows in %.1f s", mart.name, rows, seconds)
    return {"mart": mart.name, "rows": rows, "seconds": round(seconds, 2)}


def replace_table(name: str, columns: list[tuple[str, str]], order_by: str, rows,
                  run_id: str | None = None) -> dict:
    """Replace mart.<name> with rows (tuples) computed in Python (same atomic swap as SQL
    marts)."""
    started = time.perf_counter()
    names = [c for c, _ in columns]
    with meta.client() as ch:
        ch.command(f"DROP TABLE IF EXISTS mart.{name}__new")
        ch.command(f"CREATE TABLE mart.{name}__new ("
                   + ", ".join(f"{c} {t}" for c, t in columns)
                   + f") ENGINE = MergeTree ORDER BY {order_by}")
        if rows:
            ch.insert(f"mart.{name}__new", rows, column_names=names)
        count = _swap(ch, name)
        seconds = time.perf_counter() - started
        _log(ch, name, count, seconds, run_id)
    log.info("mart.%s: %d rows in %.1f s", name, count, seconds)
    return {"mart": name, "rows": count, "seconds": round(seconds, 2)}
