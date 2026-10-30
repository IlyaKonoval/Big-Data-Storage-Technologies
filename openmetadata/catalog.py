"""Curated metadata from catalog.yaml into OpenMetadata (run by ingest.py after ingestion).

    descriptions   services, database schemas, tables, columns, bucket, dashboards, charts
    tags           classification "Layer" (one tag per schema), Tier1-3, glossary terms on columns
    ownership      team data-platform owns every asset
    domain         NearEarthSpace holds every asset; data product near-earth-analytics holds the
                   dashboard marts and the dashboards
    KPIs           description and ownership coverage of the domain (Data Insights)

Idempotent: entities are compared with the file and only the differences are patched.
Entities not ingested yet are skipped, the next round picks them up.
"""
import fnmatch
import html
import json
import logging
from datetime import datetime, time, timezone
from pathlib import Path

import yaml

from celestrak import lineage

log = logging.getLogger("om-catalog")

CATALOG = Path(__file__).with_name("catalog.yaml")


def label(fqn: str, source: str = "Classification") -> dict:
    return {"tagFQN": fqn, "source": source, "labelType": "Manual", "state": "Confirmed"}


class Catalog:
    def __init__(self, om: lineage.Client, spec: dict):
        self.om, self.spec = om, spec
        self.patched = 0

    # --- helpers ---------------------------------------------------------------------

    def put(self, kind: str, body: dict) -> dict:
        return self.om.request("PUT", f"/v1/{kind}", json=body)

    def patch(self, kind: str, entity: dict, ops: list[dict]) -> None:
        if ops:
            self.om.request("PATCH", f"/v1/{kind}/{entity['id']}", data=json.dumps(ops),
                            headers={"Content-Type": "application/json-patch+json"})
            self.patched += 1

    def get(self, kind: str, fqn: str, fields: str = "") -> dict | None:
        e = self.om.entity(kind, fqn)
        if e and fields:
            e = self.om.request("GET", f"/v1/{kind}/{e['id']}", params={"fields": fields})
        return e

    def listing(self, kind: str, **params) -> list[dict]:
        out, after = [], None
        while True:
            page = self.om.request("GET", f"/v1/{kind}",
                                   params={"limit": 100, **params, **({"after": after} if after else {})})
            out += page["data"]
            after = page.get("paging", {}).get("after")
            if not after:
                return out

    def entity_ops(self, entity: dict, description: str | None = None,
                   display_name: str | None = None, tags: list[dict] = (),
                   owner: dict | None = None) -> list[dict]:
        ops = []
        # The server stores descriptions HTML-escaped (` -> &#96;): compare unescaped.
        if description and html.unescape(entity.get("description") or "") != description.strip():
            ops.append({"op": "add", "path": "/description", "value": description.strip()})
        if display_name and entity.get("displayName") != display_name:
            ops.append({"op": "add", "path": "/displayName", "value": display_name})
        have = {t["tagFQN"] for t in entity.get("tags") or []}
        ops += [{"op": "add", "path": "/tags/-", "value": t}
                for t in tags if t["tagFQN"] not in have]
        if owner and not entity.get("owners"):
            ops.append({"op": "add", "path": "/owners", "value": [owner]})
        return ops

    # --- vocabularies, team, domain -------------------------------------------------------

    def ensure_vocabularies(self, owner: dict) -> None:
        layers = self.spec["layers"]
        self.put("classifications", {**layers["classification"], "mutuallyExclusive": True})
        for tag in layers["tags"].values():
            self.put("tags", {**tag, "classification": layers["classification"]["name"]})
        g = self.spec["glossary"]
        self.put("glossaries", {**{k: g[k] for k in ("name", "displayName", "description")},
                                "owners": [owner]})
        for name, term in g["terms"].items():
            self.put("glossaryTerms", {"glossary": g["name"], "name": name,
                                       "displayName": term["displayName"],
                                       "description": term["description"].strip(),
                                       "synonyms": term.get("synonyms", [])})

    def ensure_team(self) -> dict:
        admin = self.om.request("GET", "/v1/users/name/admin")
        team = self.put("teams", {**self.spec["team"], "teamType": "Group",
                                  "users": [admin["id"]]})
        return {"id": team["id"], "type": "team"}

    def ensure_domain(self, owner: dict) -> None:
        d, p = self.spec["domain"], self.spec["data_product"]
        self.put("domains", {**{k: d[k] for k in ("name", "displayName", "domainType")},
                             "description": d["description"].strip(), "owners": [owner]})
        self.put("dataProducts", {**p, "description": p["description"].strip(),
                                  "domains": [d["name"]], "owners": [owner]})

    def ensure_kpis(self, owner: dict) -> None:
        k = self.spec["kpis"]
        ms = lambda d: int(datetime.combine(d, time(), timezone.utc).timestamp() * 1000)  # noqa: E731
        for name, kpi in k["items"].items():
            self.put("kpi", {"name": name, "displayName": kpi["displayName"],
                             "description": kpi["description"].strip(),
                             "dataInsightChart": kpi["chart"], "metricType": "PERCENTAGE",
                             "targetValue": kpi["target"], "startDate": ms(k["start"]),
                             "endDate": ms(k["end"]), "owners": [owner],
                             "domains": [self.spec["domain"]["name"]]})

    def add_assets(self, kind: str, name: str, assets: list[dict]) -> None:
        for i in range(0, len(assets), 100):
            self.om.request("PUT", f"/v1/{kind}/{name}/assets/add",
                            json={"assets": assets[i:i + 100]})

    # --- entities --------------------------------------------------------------------

    def tier_of(self, table: str) -> str | None:
        for tier, patterns in self.spec["tiers"].items():
            if any(fnmatch.fnmatch(table, p) for p in patterns):
                return f"Tier.{tier}"
        return None

    def column_terms(self) -> dict[str, list[dict]]:
        g = self.spec["glossary"]
        out: dict[str, list[dict]] = {}
        for name, term in g["terms"].items():
            for col in term.get("columns", []):
                out.setdefault(col, []).append(label(f"{g['name']}.{name}", "Glossary"))
        return out

    def apply_tables(self, owner: dict) -> dict[str, dict]:
        """Schemas and tables; returns table -> entity reference of the catalogued ones."""
        layer = self.spec["layers"]
        shared, terms = self.spec["columns"], self.column_terms()
        assets = {}
        for schema, description in self.spec["schemas"].items():
            e = self.get("databaseSchemas", lineage.table_fqn(schema), "owners,tags")
            if e:
                tag = layer["tags"][schema]["name"]
                self.patch("databaseSchemas", e, self.entity_ops(
                    e, description, tags=[label(f"{layer['classification']['name']}.{tag}")],
                    owner=owner))
        for table, spec in self.spec["tables"].items():
            e = self.get("tables", lineage.table_fqn(table), "columns,tags,owners")
            if not e:
                log.warning("Table %s is not catalogued yet", table)
                continue
            assets[table] = {"id": e["id"], "type": "table"}
            schema = table.split(".")[0]
            tags = [label(f"{layer['classification']['name']}.{layer['tags'][schema]['name']}")]
            tier = self.tier_of(table)
            if tier and not any(t["tagFQN"].startswith("Tier.") for t in e.get("tags") or []):
                tags.append(label(tier))
            ops = self.entity_ops(e, spec["description"], tags=tags, owner=owner)
            own = spec.get("columns", {})
            for i, col in enumerate(e["columns"]):
                text = own.get(col["name"]) or shared.get(col["name"])
                if text and html.unescape(col.get("description") or "") != text:
                    ops.append({"op": "add", "path": f"/columns/{i}/description", "value": text})
                have = {t["tagFQN"] for t in col.get("tags") or []}
                for t in terms.get(col["name"], []):
                    if t["tagFQN"] not in have:
                        if not col.get("tags"):
                            ops.append({"op": "add", "path": f"/columns/{i}/tags", "value": []})
                            col["tags"] = []
                        ops.append({"op": "add", "path": f"/columns/{i}/tags/-", "value": t})
            self.patch("tables", e, ops)
        return assets

    def apply_services(self, owner: dict) -> None:
        for path, spec in self.spec["services"].items():
            kind, name = path.split("/")
            e = self.get(f"services/{kind}", name, "owners")
            if e:
                self.patch(f"services/{kind}", e, self.entity_ops(
                    e, spec["description"], spec["displayName"], owner=owner))
        db = self.get("databases", f"{lineage.CLICKHOUSE_SERVICE}.{lineage.CLICKHOUSE_DATABASE}",
                      "owners")
        if db:
            self.patch("databases", db, self.entity_ops(
                db, "ClickHouse не имеет уровня каталога: базы ClickHouse — это схемы ниже.",
                owner=owner))

    def apply_bi_and_storage(self, owner: dict) -> tuple[list[dict], list[dict]]:
        """Bucket, dashboards, charts, data models, pipelines; returns (all, dashboards)."""
        assets, dashboards = [], []
        for fqn, spec in self.spec["containers"].items():
            e = self.get("containers", fqn, "owners,tags")
            if e:
                assets.append({"id": e["id"], "type": "container"})
                self.patch("containers", e, self.entity_ops(
                    e, spec["description"], spec["displayName"], owner=owner))
        for kind, texts, ref in (("dashboards", self.spec["dashboards"], "dashboard"),
                                 ("charts", self.spec["charts"], "chart")):
            for e in self.listing(kind, service="superset", fields="owners,tags"):
                assets.append({"id": e["id"], "type": ref})
                tags = []
                if kind == "dashboards":
                    dashboards.append({"id": e["id"], "type": ref})
                    if not any(t["tagFQN"].startswith("Tier.") for t in e.get("tags") or []):
                        tags = [label("Tier.Tier1")]
                self.patch(kind, e, self.entity_ops(
                    e, texts.get(e.get("displayName")), tags=tags, owner=owner))
        for e in self.listing("dashboard/datamodels", service="superset", fields="owners"):
            assets.append({"id": e["id"], "type": "dashboardDataModel"})
            self.patch("dashboard/datamodels", e, self.entity_ops(
                e, f"Набор данных Superset над витриной `mart.{e.get('displayName')}`.",
                owner=owner))
        for e in self.listing("pipelines", service=lineage.PIPELINE_SERVICE, fields="owners"):
            assets.append({"id": e["id"], "type": "pipeline"})
            self.patch("pipelines", e, self.entity_ops(e, owner=owner))
        return assets, dashboards

    def apply(self) -> dict:
        owner = self.ensure_team()
        self.ensure_vocabularies(owner)
        self.ensure_domain(owner)
        self.ensure_kpis(owner)
        self.apply_services(owner)
        tables = self.apply_tables(owner)
        other, dashboards = self.apply_bi_and_storage(owner)
        self.add_assets("domains", self.spec["domain"]["name"], [*tables.values(), *other])
        marts = [tables[t] for t in self.spec["tiers"]["Tier1"] if t in tables]
        self.add_assets("dataProducts", self.spec["data_product"]["name"], marts + dashboards)
        return {"patched": self.patched, "assets": len(tables) + len(other)}


def tables(path: Path = CATALOG) -> list[str]:
    return list(yaml.safe_load(path.read_text(encoding="utf-8"))["tables"])


def apply(om: lineage.Client | None = None, path: Path = CATALOG) -> dict:
    spec = yaml.safe_load(path.read_text(encoding="utf-8"))
    result = Catalog(om or lineage.Client(), spec).apply()
    log.info("Catalog: %s", result)
    return result
