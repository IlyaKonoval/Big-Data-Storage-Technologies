"""Lineage of the platform in OpenMetadata: S3 raw -> ods -> stage -> rv -> bv -> mart.

The edges are derived from our own code, not from ClickHouse's query log: every SQL step
names its target (INSERT INTO / CREATE TABLE / CREATE VIEW) and its sources (FROM / JOIN),
and the few Python steps are listed here. A mart is built as mart.<name>__new and swapped
in, so a query log would only show the temporary table.

Each edge carries the Airflow DAG that runs the step (an OpenMetadata pipeline) and the SQL.
Entities come from the ingestion workflows (openmetadata/workflows); an edge whose table is
not catalogued yet is skipped and reported. Only the standard library and requests are used,
so the module runs both in Airflow and in the OpenMetadata ingestion container.
"""
import base64
import logging
import os
import re
from dataclasses import dataclass
from pathlib import Path

import requests

log = logging.getLogger(__name__)

SQL_DIR = Path(os.environ.get("SQL_DIR", Path(__file__).resolve().parents[2] / "sql"))

# Names of the services as created by the ingestion workflows.
CLICKHOUSE_SERVICE = "clickhouse"
CLICKHOUSE_DATABASE = "default"   # ClickHouse has no catalog level; databases are schemas
STORAGE_SERVICE = "seaweedfs"
RAW_BUCKET = "raw"
PIPELINE_SERVICE = "airflow"
AIRFLOW_UI = os.environ.get("AIRFLOW_UI_URL", "http://localhost:8080")

SOURCES = ["gp", "supgp", "satcat", "socrates"]
# Raw vault DAG per source (dags/vault_*.py).
VAULT_DAGS = {"gp": "vault_orbit", "supgp": "vault_orbit", "satcat": "vault_satcat",
              "socrates": "vault_conjunction"}

# Python marts of build_marts (plugins/celestrak/orbits.py): SGP4 over the latest elements.
ORBIT_OBJECTS = ["bv.object_current", "rv.sat_object_orbit_gp"]
ORBIT_MARTS = {
    "orbit_elements": ORBIT_OBJECTS,
    "ground_tracks": ORBIT_OBJECTS,
    "launch_arcs": [*ORBIT_OBJECTS, "rv.ref_launch_site"],
    "conjunction_geo": [*ORBIT_OBJECTS, "mart.conjunction_risk"],
    "gp_vs_supgp": [*ORBIT_OBJECTS, "rv.sat_object_orbit_supgp"],
}

# DAG id -> (display name, description) of its pipeline in OpenMetadata.
PIPELINES = {
    "ingest_gp": ("Загрузка GP",
        "Скачивает GP (группа `active`) с CelesTrak каждые 4 часа → `raw/gp/` в S3 → "
        "`ods.gp`. Публикует Asset `ods_gp`."),
    "ingest_supgp": ("Загрузка SupGP",
        "Скачивает SupGP операторов каждые 8 часов → `raw/supgp/` → `ods.supgp`. Asset "
        "`ods_supgp`."),
    "ingest_satcat": ("Загрузка SATCAT",
        "Скачивает каталог SATCAT (CSV) раз в сутки → `raw/satcat/` → `ods.satcat`. Asset "
        "`ods_satcat`."),
    "ingest_socrates": ("Загрузка SOCRATES",
        "Скачивает прогноз сближений SOCRATES каждые 12 часов → `raw/socrates/` → "
        "`ods.socrates`. Asset `ods_socrates`."),
    "vault_orbit": ("Raw Vault: орбиты",
        "Новые снимки GP и SupGP → `stage.*` → `rv.hub_object`, "
        "`rv.sat_object_orbit_gp/supgp`."),
    "vault_satcat": ("Raw Vault: каталог",
        "Новые снимки SATCAT → хабы объектов и запусков, линк, сателлиты описания, статуса "
        "и запуска."),
    "vault_conjunction": ("Raw Vault: сближения",
        "Новые снимки SOCRATES → линк и сателлит сближения, статусы «появилось/исчезло»."),
    "vault_business": ("Business Vault",
        "После любого vault-DAG-а: производные орбиты (`bv.sat_object_orbit_derived`) и "
        "PIT-таблица."),
    "data_quality": ("Проверка качества данных",
        "15 SQL-проверок (`sql/dq/`) целостности vault и согласованности с ODS. Только при "
        "успехе critical-проверок публикует Asset `dq_passed`."),
    "build_marts": ("Сборка витрин",
        "По `dq_passed`: SQL-витрины из `sql/mart/` и орбитальные витрины на SGP4; "
        "публикует Asset `mart_all`."),
}

_COMMENT = re.compile(r"--[^\n]*")
_TARGET = re.compile(
    r"\b(?:INSERT\s+INTO|CREATE\s+TABLE(?:\s+IF\s+NOT\s+EXISTS)?|CREATE\s+(?:OR\s+REPLACE\s+)?VIEW)"
    r"\s+([a-z_]+\.[a-z_]+)", re.I)
_SOURCE = re.compile(r"\b(?:FROM|JOIN)\s+([a-z_]+\.[a-z_]+)", re.I)


@dataclass(frozen=True)
class Step:
    """One step of the pipeline: target table filled from source tables by a DAG."""
    target: str              # "<schema>.<table>", or "s3:<bucket>" for the raw bucket
    sources: tuple[str, ...]
    pipeline: str | None     # DAG id; None for a view
    sql: str = ""
    view: bool = False


def statements(sql: str) -> list[str]:
    """SQL text without comments, split into statements."""
    sql = _COMMENT.sub("", sql)
    return [s.strip() for s in sql.split(";") if s.strip()]


def parse(sql: str) -> tuple[str | None, list[str], bool]:
    """(target, sources, is_view) of one statement; sources exclude the target itself."""
    m = _TARGET.search(sql)
    target = m.group(1).lower() if m else None
    sources = list(dict.fromkeys(t.lower() for t in _SOURCE.findall(sql)))
    return target, [t for t in sources if t != target], bool(m and "VIEW" in m.group(0).upper())


def sql_steps(path: Path, pipeline: str | None, target: str | None = None) -> list[Step]:
    """Steps of a SQL file; `target` names the table of a bare SELECT (a mart file)."""
    steps = []
    for stmt in statements(path.read_text(encoding="utf-8")):
        tgt, sources, view = parse(stmt)
        tgt = tgt or target
        if tgt and sources:
            steps.append(Step(tgt, tuple(sources), None if view else pipeline, stmt, view))
    return steps


def mart_name(path: Path) -> str:
    m = re.search(r"^--\s*mart:\s*([a-z_]+)", path.read_text(encoding="utf-8"), re.M)
    if not m:
        raise ValueError(f"{path}: no '-- mart:' header")
    return m.group(1)


def all_steps(sql_dir: Path = SQL_DIR) -> list[Step]:
    steps = [Step(f"ods.{s}", (f"s3:{RAW_BUCKET}",), f"ingest_{s}") for s in SOURCES]
    for source in SOURCES:
        for path in sorted((sql_dir / "vault" / "load" / source).glob("*.sql")):
            steps += sql_steps(path, VAULT_DAGS[source])
    for path in sorted((sql_dir / "vault" / "ddl").glob("*.sql")):
        steps += [s for s in sql_steps(path, None) if s.view]
    for path in sorted((sql_dir / "vault" / "bv").glob("*.sql")):
        steps += sql_steps(path, "vault_business")
    for path in sorted((sql_dir / "dq").glob("*.sql")):
        steps += [Step("meta.dq_results", s.sources, "data_quality", s.sql)
                  for s in sql_steps(path, "data_quality", target="meta.dq_results")]
    for path in sorted((sql_dir / "mart").glob("*.sql")):
        steps += sql_steps(path, "build_marts", target=f"mart.{mart_name(path)}")
    steps += [Step(f"mart.{name}", tuple(src), "build_marts")
              for name, src in ORBIT_MARTS.items()]
    return steps


def edges(steps: list[Step]) -> dict[tuple[str, str], Step]:
    """(source, target) -> the step that makes the edge; repeated edges keep the first SQL."""
    out: dict[tuple[str, str], Step] = {}
    for step in steps:
        for source in step.sources:
            out.setdefault((source, step.target), step)
    return out


# --- OpenMetadata REST API ----------------------------------------------------------------

class Client:
    def __init__(self, url: str | None = None, email: str | None = None,
                 password: str | None = None, timeout: float = 30):
        self.url = (url or os.environ.get("OM_URL", "http://openmetadata-server:8585/api"))
        self.timeout = timeout
        email = email or os.environ.get("OM_ADMIN_EMAIL", "admin@open-metadata.org")
        password = password or os.environ.get("OM_ADMIN_PASSWORD", "admin")
        # Basic auth: the password is sent base64-encoded, the answer is a JWT.
        r = requests.post(f"{self.url}/v1/users/login", timeout=timeout,
                          json={"email": email,
                                "password": base64.b64encode(password.encode()).decode()})
        r.raise_for_status()
        self.session = requests.Session()
        self.session.headers["Authorization"] = f"Bearer {r.json()['accessToken']}"

    def request(self, method: str, path: str, **kwargs) -> dict | None:
        r = self.session.request(method, f"{self.url}{path}", timeout=self.timeout, **kwargs)
        if method == "GET" and r.status_code == 404:
            return None
        if not r.ok:
            raise RuntimeError(f"{method} {path}: {r.status_code} {r.text[:500]}")
        return r.json() if r.content else {}

    def entity(self, kind: str, fqn: str) -> dict | None:
        return self.request("GET", f"/v1/{kind}/name/{requests.utils.quote(fqn, safe='')}")

    def bot_token(self, bot: str = "ingestion-bot") -> str:
        """JWT of a bot user, for the ingestion workflows."""
        user = self.request("GET", f"/v1/bots/name/{bot}")["botUser"]
        return self.request("GET", f"/v1/users/auth-mechanism/{user['id']}")["config"]["JWTToken"]


def table_fqn(name: str) -> str:
    return f"{CLICKHOUSE_SERVICE}.{CLICKHOUSE_DATABASE}.{name}"


def ensure_pipelines(om: Client) -> dict[str, str]:
    """Pipeline service `airflow` and one pipeline per DAG; returns DAG id -> entity id."""
    om.request("PUT", "/v1/services/pipelineServices", json={
        "name": PIPELINE_SERVICE, "serviceType": "Airflow",
        "description": "Airflow 3 of the CelesTrak platform (DAGs in dags/)",
        "connection": {"config": {"type": "Airflow", "hostPort": AIRFLOW_UI,
                                  "connection": {"type": "Backend"}}}})
    ids = {}
    for dag_id, (display_name, description) in PIPELINES.items():
        p = om.request("PUT", "/v1/pipelines", json={
            "name": dag_id, "displayName": display_name, "service": PIPELINE_SERVICE,
            "description": description,
            "sourceUrl": f"{AIRFLOW_UI}/dags/{dag_id}"})
        ids[dag_id] = p["id"]
    return ids


def publish(om: Client | None = None, sql_dir: Path = SQL_DIR) -> dict:
    """Create the pipelines and put every lineage edge; idempotent."""
    om = om or Client()
    pipelines = ensure_pipelines(om)
    cache: dict[str, dict | None] = {}

    def ref(name: str) -> dict | None:
        if name not in cache:
            if name.startswith("s3:"):
                e = om.entity("containers", f"{STORAGE_SERVICE}.{name[3:]}")
                cache[name] = e and {"id": e["id"], "type": "container"}
            else:
                e = om.entity("tables", table_fqn(name))
                cache[name] = e and {"id": e["id"], "type": "table"}
        return cache[name]

    done, missing = 0, set()
    for (source, target), step in edges(all_steps(sql_dir)).items():
        a, b = ref(source), ref(target)
        if not (a and b):
            missing.update(n for n, e in ((source, a), (target, b)) if not e)
            continue
        details = {"source": "ViewLineage" if step.view else "PipelineLineage"}
        if step.sql:
            details["sqlQuery"] = step.sql
        if step.pipeline:
            details["pipeline"] = {"id": pipelines[step.pipeline], "type": "pipeline"}
        om.request("PUT", "/v1/lineage",
                   json={"edge": {"fromEntity": a, "toEntity": b, "lineageDetails": details}})
        done += 1
    if missing:
        log.warning("Not in OpenMetadata yet (edges skipped): %s", ", ".join(sorted(missing)))
    log.info("Lineage edges published: %d, pipelines: %d", done, len(pipelines))
    return {"edges": done, "pipelines": len(pipelines), "missing": sorted(missing)}
