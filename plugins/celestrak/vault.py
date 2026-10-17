"""Data Vault 2.0 loading in ClickHouse: ODS snapshot -> stage.<source> -> raw vault (rv),
then raw vault -> business vault (bv).

All transformations are SQL files executed in name order:

    sql/vault/ddl/*.sql            databases, tables, views (idempotent)
    sql/vault/load/<source>/*.sql  00_stage builds stage.<source> from one ODS snapshot,
                                   then hubs (1x), links (2x), satellites (3x, 4x)
    sql/vault/bv/*.sql             computed satellite and point-in-time table

Snapshots are processed one by one in download order; every step is an insert of what is
missing, so a failed run can simply be repeated. meta.vault_log records each processed
snapshot and is the incremental watermark.
"""
import json
import logging
import re
import time
from pathlib import Path

from celestrak import config, meta

log = logging.getLogger(__name__)

VAULT_DIR = Path(config.SQL_DIR) / "vault"
# ODS + meta DDL first: initdb scripts run only on a fresh ClickHouse volume.
DDL_FILES = sorted((Path(config.SQL_DIR) / "ods" / "clickhouse").glob("*.sql")) + sorted(
    (VAULT_DIR / "ddl").glob("*.sql"))

# Hash key columns of stage.<source> that must all be present in the vault after a load.
STAGE_KEYS = {
    "satcat": [("hk_object", "rv.hub_object", "hk_object")],
    "gp": [("hk_object", "rv.hub_object", "hk_object")],
    "supgp": [("hk_object", "rv.hub_object", "hk_object")],
    "socrates": [("hk_object_1", "rv.hub_object", "hk_object"),
                 ("hk_object_2", "rv.hub_object", "hk_object"),
                 ("hk_conjunction", "rv.link_conjunction", "hk_conjunction")],
}


def split_statements(sql: str) -> list[str]:
    """Split a script into statements on ';' at line end (no ';' inside our literals)."""
    body = "\n".join(line for line in sql.splitlines() if not line.lstrip().startswith("--"))
    return [s.strip() for s in re.split(r";\s*(?:\n|$)", body) if s.strip()]


def steps(source: str) -> list[Path]:
    return sorted((VAULT_DIR / "load" / source).glob("*.sql"))


def _written(summary) -> int:
    return int(getattr(summary, "written_rows", 0) or 0)


def ensure_schema() -> None:
    with meta.client() as ch:
        for path in DDL_FILES:
            for statement in split_statements(path.read_text(encoding="utf-8")):
                ch.command(statement)
        # Empty stage tables (the stage query with no snapshot) so that DQ checks reading
        # stage.* work before the first load of a source.
        for source in STAGE_KEYS:
            if not int(ch.command(f"EXISTS TABLE stage.{source}")):
                ch.command(_stage_sql(source), parameters={"load_id": ""})
    log.info("Schema ensured from %d DDL files", len(DDL_FILES))


def _stage_sql(source: str) -> str:
    return steps(source)[0].read_text(encoding="utf-8").strip().rstrip(";")


def pending_loads(source: str) -> list[str]:
    """Successful ODS snapshots of the source that are not in the vault yet, oldest first."""
    with meta.client() as ch:
        rows = ch.query(
            """
            SELECT l.load_id
            FROM meta.load_log AS l FINAL
            INNER JOIN meta.raw_files AS f FINAL ON f.load_id = l.load_id
            WHERE l.source = {source:String}
              AND l.target_db = 'clickhouse'
              AND l.status = 'success'
              AND l.load_id NOT IN (SELECT load_id FROM meta.vault_log FINAL)
            ORDER BY f.fetched_at
            """,
            parameters={"source": source},
        ).result_rows
    return [r[0] for r in rows]


def load_snapshot(source: str, load_id: str) -> dict:
    """Stage one ODS snapshot and load it into hubs, links and satellites."""
    started = time.perf_counter()
    written = {}
    with meta.client() as ch:
        ch.command(f"DROP TABLE IF EXISTS stage.{source}")
        for path in steps(source):
            summary = ch.command(path.read_text(encoding="utf-8").strip().rstrip(";"),
                                 parameters={"load_id": load_id})
            written[path.stem] = _written(summary)
        ods_rows = ch.command(f"SELECT count() FROM ods.{source} WHERE load_id = {{id:String}}",
                              parameters={"id": load_id})
        stage_rows = ch.command(f"SELECT count() FROM stage.{source}")
        missing = reconcile(ch, source)
        if missing:
            raise RuntimeError(f"stage.{source} keys missing in the vault: {missing}")
        duration = time.perf_counter() - started
        ch.insert("meta.vault_log",
                  [(load_id, source, int(ods_rows), int(stage_rows), json.dumps(written), duration)],
                  column_names=["load_id", "source", "ods_rows", "stage_rows", "steps",
                                "duration_sec"])
    result = {"load_id": load_id, "ods_rows": int(ods_rows), "stage_rows": int(stage_rows),
              "written": written, "seconds": round(duration, 2)}
    log.info("vault %s: %s", source, result)
    return result


def reconcile(ch, source: str) -> dict:
    """Keys of the staged snapshot that did not reach their hub/link (should be none)."""
    missing = {}
    for column, table, key in STAGE_KEYS[source]:
        n = ch.command(f"SELECT count() FROM stage.{source} "
                       f"WHERE {column} NOT IN (SELECT {key} FROM {table})")
        if int(n):
            missing[f"{column}->{table}"] = int(n)
    return missing


def build_business_vault() -> dict:
    written = {}
    with meta.client() as ch:
        for path in sorted((VAULT_DIR / "bv").glob("*.sql")):
            summary = ch.command(path.read_text(encoding="utf-8").strip().rstrip(";"))
            written[path.stem] = _written(summary)
    log.info("business vault: %s", written)
    return written
