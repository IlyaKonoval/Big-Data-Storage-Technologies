"""Dashboard as code: creates or updates, via the Superset REST API, the ClickHouse
connection, a dataset per mart, the charts and two dashboards: the analytics
"Околоземное пространство" and the 3D pages "Космос в 3D".

Idempotent: objects are looked up by name and updated in place, so the script runs on
every `docker compose --profile bi up` (service superset-bootstrap) and after edits here.
"""
import json
import os
import sys
import time
import uuid

import requests

URL = os.environ.get("SUPERSET_URL", "http://localhost:8088")
USER = os.environ.get("SUPERSET_ADMIN_USER", "admin")
PASSWORD = os.environ.get("SUPERSET_ADMIN_PASSWORD", "admin")
CH_URI = os.environ.get("CH_SUPERSET_URI", "clickhousedb://superset:superset@clickhouse:8123/mart")
GLOBE_URL = os.environ.get("GLOBE_URL", "http://localhost:8090")

DATABASE = "ClickHouse marts"

# (slug, title, tabs, native filters). A dashboard with a single tab is laid out without
# the tab strip. The 3D pages are not charts: their tabs hold an iframe (PAGES).
DASHBOARDS = [
    ("near-earth-space", "Околоземное пространство", ["Обзор", "История", "Риски"], True),
    ("orbit-globe", "Космос в 3D", ["Сейчас", "Хроника 1957–2026"], False),
]

# Datasets of the database that are not listed here are deleted, with their charts.
DATASETS = ["orbit_population", "catalog_growth", "launch_activity", "altitude_shells",
            "conjunction_risk", "gp_vs_supgp"]


# --- REST client --------------------------------------------------------------------

class Superset:
    def __init__(self):
        self.s = requests.Session()
        for _ in range(60):
            try:
                if self.s.get(f"{URL}/health", timeout=5).ok:
                    break
            except requests.ConnectionError:
                pass
            time.sleep(5)
        token = self.s.post(f"{URL}/api/v1/security/login", json={
            "username": USER, "password": PASSWORD, "provider": "db", "refresh": True},
            timeout=30).json()["access_token"]
        self.s.headers["Authorization"] = f"Bearer {token}"
        csrf = self.s.get(f"{URL}/api/v1/security/csrf_token/", timeout=30).json()["result"]
        self.s.headers.update({"X-CSRFToken": csrf, "Referer": URL})

    def call(self, method: str, path: str, **kw):
        r = self.s.request(method, f"{URL}/api/v1/{path}", timeout=120, **kw)
        if not r.ok:
            raise RuntimeError(f"{method} {path}: {r.status_code} {r.text[:500]}")
        return r.json() if r.text else {}

    def find(self, resource: str, key: str, value: str) -> dict | None:
        q = f"(filters:!((col:{key},opr:eq,value:'{value}')),page_size:100)"
        found = self.call("GET", f"{resource}/", params={"q": q})["result"]
        return found[0] if found else None

    def upsert(self, resource: str, key: str, value: str, body: dict) -> int:
        existing = self.find(resource, key, value)
        if existing:
            self.call("PUT", f"{resource}/{existing['id']}", json=body)
            return existing["id"]
        return self.call("POST", f"{resource}/", json=body)["id"]


# --- chart definitions --------------------------------------------------------------

def metric(sql: str, label: str) -> dict:
    return {"expressionType": "SQL", "sqlExpression": sql, "label": label}


def column(sql: str, label: str) -> dict:
    return {"expressionType": "SQL", "sqlExpression": sql, "label": label}


def where(sql: str) -> list[dict]:
    return [{"expressionType": "SQL", "clause": "WHERE", "sqlExpression": sql}]


# (tab, name, dataset, viz_type, params, width, height). Width in a 12-column grid.
CHARTS = [
    # Overview -----------------------------------------------------------------------
    ("Обзор", "Объектов на орбите", "orbit_population", "big_number_total",
     {"metric": metric("SUM(objects)", "Объекты"), "subheader": "в каталоге SATCAT сейчас",
      "y_axis_format": ",d"}, 3, 30),
    ("Обзор", "Активные спутники", "orbit_population", "big_number_total",
     {"metric": metric("SUM(active_payloads)", "Активные"),
      "subheader": "работающие полезные нагрузки", "y_axis_format": ",d"}, 3, 30),
    ("Обзор", "Космический мусор", "orbit_population", "big_number_total",
     {"metric": metric("SUM(debris)", "Обломки"), "subheader": "каталогизированных обломков",
      "y_axis_format": ",d"}, 3, 30),
    ("Обзор", "Starlink на орбите", "orbit_population", "big_number_total",
     {"metric": metric("SUM(starlink)", "Starlink"), "subheader": "спутников одной группировки",
      "y_axis_format": ",d"}, 3, 30),
    ("Обзор", "Состав по орбитам и типам", "orbit_population", "echarts_timeseries_bar",
     {"x_axis": "orbit_regime", "metrics": [metric("SUM(objects)", "Объекты")],
      "groupby": ["object_type_name"], "stack": "Stack", "orientation": "vertical",
      "x_axis_sort_series": "sum", "x_axis_sort_series_ascending": False,
      "show_legend": True, "legendOrientation": "top", "row_limit": 1000,
      "y_axis_format": "SMART_NUMBER", "color_scheme": "supersetColors",
      "rich_tooltip": True}, 6, 60),
    ("Обзор", "Кому принадлежит орбита", "orbit_population", "sunburst_v2",
     {"columns": ["orbit_regime", "object_type_name", "owner"],
      "metric": metric("SUM(objects)", "Объекты"), "row_limit": 1000,
      "color_scheme": "supersetColors", "show_labels": True, "label_type": "key"}, 6, 60),
    # History ------------------------------------------------------------------------
    ("История", "Рост населённости орбиты 1957–2026", "catalog_growth", "echarts_area",
     {"x_axis": "year_start", "time_grain_sqla": None,
      "metrics": [metric("SUM(in_orbit)", "На орбите")], "groupby": ["object_type"],
      "stack": "Stack", "show_legend": True, "legendOrientation": "top",
      "row_limit": 10000, "y_axis_format": "SMART_NUMBER", "x_axis_time_format": "%Y",
      "color_scheme": "supersetColors", "rich_tooltip": True, "opacity": 0.7}, 12, 70),
    ("История", "Запуски по годам и доля Starlink", "launch_activity", "mixed_timeseries",
     {"x_axis": "year_start", "time_grain_sqla": None,
      "metrics": [metric("SUM(launches)", "Запуски")], "groupby": [],
      "metrics_b": [metric("100.0 * SUM(starlink_launches) / SUM(launches)",
                           "Доля Starlink, %")], "groupby_b": [],
      "adhoc_filters": where("year >= 1990"), "adhoc_filters_b": where("year >= 1990"),
      "seriesType": "bar", "seriesTypeB": "line", "yAxisIndex": 0, "yAxisIndexB": 1,
      "markerEnabledB": True, "show_legend": True, "x_axis_time_format": "%Y",
      "y_axis_format": "SMART_NUMBER", "y_axis_format_secondary": ",.0f",
      "row_limit": 10000, "row_limit_b": 10000, "rich_tooltip": True}, 12, 60),
    ("История", "Где теснее всего: высота (км) × наклонение (°)", "altitude_shells", "heatmap_v2",
     # The mart's 25 km x 5 deg cells are regrouped into 50 km x 10 deg ones, labelled
     # with the count: the Starlink shells (~450-550 km at 43/53 deg) and the sun-
     # synchronous band (~97 deg) stand out without a log scale.
     {"x_axis": column("intDiv(altitude_km, 50) * 50", "Высота от, км"),
      "groupby": column("intDiv(inclination_deg, 10) * 10", "Наклонение от, °"),
      "metric": metric("SUM(objects)", "Объектов"), "linear_color_scheme": "oranges",
      "normalize_across": "heatmap", "sort_x_axis": "alpha_asc", "sort_y_axis": "alpha_asc",
      "show_values": True, "show_legend": False, "xscale_interval": 1, "yscale_interval": 1,
      "y_axis_format": ",d", "row_limit": 10000,
      "adhoc_filters": where("altitude_km BETWEEN 300 AND 1299")}, 12, 80),
    # Risks --------------------------------------------------------------------------
    ("Риски", "Самые опасные сближения (прогноз SOCRATES)", "conjunction_risk", "table",
     {"query_mode": "raw",
      "all_columns": ["tca", "object_name_1", "object_type_1", "object_name_2",
                      "object_type_2", "tca_range_km", "relative_speed_km_s", "max_prob"],
      "order_by_cols": [json.dumps(["max_prob", False])], "row_limit": 50,
      "server_pagination": False, "page_length": 15, "include_search": True,
      "conditional_formatting": [
          {"column": "max_prob", "operator": ">=", "targetValue": 0.001,
           "colorScheme": "#E04355"},
          {"column": "tca_range_km", "operator": "<", "targetValue": 0.1,
           "colorScheme": "#FCC700"}]}, 8, 70),
    ("Риски", "Кто с кем сближается", "conjunction_risk", "pie",
     {"groupby": ["pair_kind"], "metric": metric("COUNT(*)", "Сближения"),
      "donut": True, "show_labels": True, "label_type": "key_percent",
      "labels_outside": True, "row_limit": 20, "color_scheme": "supersetColors"}, 4, 70),
    ("Риски", "Самые «конфликтные» объекты", "conjunction_risk", "echarts_timeseries_bar",
     {"x_axis": "object_name_1", "metrics": [metric("COUNT(*)", "Сближений")],
      "groupby": [], "orientation": "horizontal", "row_limit": 15,
      "x_axis_sort_series": "sum", "x_axis_sort_series_ascending": False,
      "timeseries_limit_metric": metric("COUNT(*)", "Сближений"),
      "adhoc_filters": where("tca_range_km < 1"), "show_legend": False}, 6, 60),
    # One bar per 5 km of disagreement; the ~5 % beyond 100 km (stale or manoeuvring
    # satellites) are left out so that the bulk of the distribution stays readable.
    ("Риски", "Starlink: насколько NORAD (GP) расходится с оператором (SupGP)", "gp_vs_supgp",
     "histogram_v2",
     {"column": "distance_km", "groupby": [], "bins": 20, "normalize": False,
      "cumulative": False, "show_value": True, "show_legend": False,
      "x_axis_title": "расхождение положения, км", "y_axis_title": "спутников Starlink",
      "y_axis_format": ",d", "row_limit": 20000, "color_scheme": "supersetColors",
      "adhoc_filters": where("distance_km < 100")}, 6, 60),
]

# Tabs that embed a page of the `globe` service (globe/*.html) instead of charts.
PAGES = {"Сейчас": "", "Хроника 1957–2026": "chronicle.html"}


def page_markdown(page: str) -> str:
    return (f'<iframe src="{GLOBE_URL}/{page}" width="100%" height="860" style="border:0" '
            'title="3D" allowfullscreen></iframe>')


# --- dashboard layout -----------------------------------------------------------------

def layout(title: str, tabs: list[str], chart_ids: dict[str, int]) -> dict:
    """position_json: [tabs ->] rows (charts packed left to right, 12 columns per row)."""
    top = "TABS-main" if len(tabs) > 1 else "GRID_ID"
    pos = {"DASHBOARD_VERSION_KEY": "v2",
           "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": [top]},
           "HEADER_ID": {"type": "HEADER", "id": "HEADER_ID", "meta": {"text": title}},
           top: {"type": "TABS" if len(tabs) > 1 else "GRID", "id": top, "children": [],
                 "parents": ["ROOT_ID"]}}

    def add(node_id, node_type, parents, meta=None):
        pos[node_id] = {"type": node_type, "id": node_id, "children": [],
                        "parents": parents, "meta": meta or {}}
        pos[parents[-1]]["children"].append(node_id)
        return node_id

    for n, tab in enumerate(tabs):
        parents = ["ROOT_ID", top]
        if len(tabs) > 1:
            parents.append(add(f"TAB-{n}", "TAB", parents, {"text": tab}))
        if tab in PAGES:
            row = add(f"ROW-{n}-0", "ROW", parents, {"background": "BACKGROUND_TRANSPARENT"})
            add(f"MARKDOWN-{n}", "MARKDOWN", [*parents, row],
                {"code": page_markdown(PAGES[tab]), "width": 12, "height": 112})
            continue
        row, used, r = None, 12, 0
        for c_tab, name, _, _, _, width, height in CHARTS:
            if c_tab != tab:
                continue
            if used + width > 12:
                row = add(f"ROW-{n}-{r}", "ROW", parents,
                          {"background": "BACKGROUND_TRANSPARENT"})
                used, r = 0, r + 1
            add(f"CHART-{chart_ids[name]}", "CHART", [*parents, row],
                {"chartId": chart_ids[name], "width": width, "height": height,
                 "sliceName": name, "uuid": str(uuid.uuid5(uuid.NAMESPACE_URL, name))})
            used += width
    return pos


# Dashboard filters: (id, label, column, source dataset of the values, filter type).
# A filter applies to every chart whose dataset has the column; the others are excluded
# from its scope, so selecting "LEO" re-slices the overview and the risk charts.
FILTERS = [
    ("regime", "Режим орбиты", "orbit_regime", "orbit_population", "filter_select"),
    ("type", "Тип объекта", "object_type", "orbit_population", "filter_select"),
    ("owner", "Владелец", "owner", "orbit_population", "filter_select"),
    ("year", "Годы", "year", "launch_activity", "filter_range"),
]


def native_filters(datasets: dict[str, int], columns: dict[str, set[str]],
                   chart_ids: dict[str, int]) -> list[dict]:
    chart_table = {name: table for _, name, table, *_ in CHARTS}
    result = []
    for key, label, column, source, ftype in FILTERS:
        in_scope = [cid for name, cid in chart_ids.items() if column in columns[chart_table[name]]]
        excluded = [cid for cid in chart_ids.values() if cid not in in_scope]
        result.append({
            "id": f"NATIVE_FILTER-{key}", "type": "NATIVE_FILTER", "name": label,
            "filterType": ftype, "description": "",
            "targets": [{"datasetId": datasets[source], "column": {"name": column}}],
            "controlValues": ({"enableEmptyFilter": False, "defaultToFirstItem": False,
                               "multiSelect": True, "searchAllOptions": False,
                               "inverseSelection": False}
                              if ftype == "filter_select" else {"enableEmptyFilter": False}),
            "defaultDataMask": {"extraFormData": {}, "filterState": {}, "ownState": {}},
            "cascadeParentIds": [],
            "scope": {"rootPath": ["ROOT_ID"], "excluded": excluded},
            "chartsInScope": in_scope,
        })
    return result


def main() -> None:
    api = Superset()
    db_id = api.upsert("database", "database_name", DATABASE, {
        "database_name": DATABASE, "sqlalchemy_uri": CH_URI, "expose_in_sqllab": True,
        "allow_run_async": False})
    print(f"database {DATABASE}: {db_id}")

    datasets = {}
    for table in DATASETS:
        existing = api.find("dataset", "table_name", table)
        if existing:
            ds_id = existing["id"]
            api.call("PUT", f"dataset/{ds_id}/refresh")      # pick up new mart columns
        else:
            ds_id = api.call("POST", "dataset/", json={
                "database": db_id, "schema": "mart", "table_name": table})["id"]
        datasets[table] = ds_id
    print(f"datasets: {len(datasets)}")

    dash_ids = {title: api.upsert("dashboard", "slug", slug, {
                    "dashboard_title": title, "slug": slug, "published": True})
                for slug, title, _, _ in DASHBOARDS}
    tab_dash = {tab: dash_ids[title] for _, title, tabs, _ in DASHBOARDS for tab in tabs}

    chart_ids = {}
    for tab, name, table, viz, params, _, _ in CHARTS:
        ds = datasets[table]
        form = {"datasource": f"{ds}__table", "viz_type": viz, **params}
        chart_ids[name] = api.upsert("chart", "slice_name", name, {
            "slice_name": name, "viz_type": viz, "datasource_id": ds,
            "datasource_type": "table", "params": json.dumps(form, ensure_ascii=False),
            "dashboards": [tab_dash[tab]]})
    print(f"charts: {len(chart_ids)}")

    # Charts removed from CHARTS and datasets removed from DATASETS: delete those on our
    # database (charts first, a dataset with charts cannot be deleted).
    q = f"(filters:!((col:database,opr:rel_o_m,value:{db_id})),page_size:100)"
    ours = {d["id"]: d["table_name"] for d in api.call("GET", "dataset/", params={"q": q})["result"]}
    q = "(page_size:100,columns:!(id,slice_name,datasource_id))"
    for c in api.call("GET", "chart/", params={"q": q})["result"]:
        if c["slice_name"] not in chart_ids and c["datasource_id"] in ours:
            api.call("DELETE", f"chart/{c['id']}")
            print(f"deleted chart {c['slice_name']}")
    for ds_id, table in ours.items():
        if table not in datasets:
            api.call("DELETE", f"dataset/{ds_id}")
            print(f"deleted dataset {table}")

    columns = {t: {c["column_name"] for c in api.call("GET", f"dataset/{ds}")["result"]["columns"]}
               for t, ds in datasets.items()}
    for slug, title, tabs, filters in DASHBOARDS:
        own = {name: chart_ids[name] for tab, name, *_ in CHARTS if tab in tabs}
        metadata = {"color_scheme": "supersetColors", "refresh_frequency": 0,
                    "cross_filters_enabled": True,
                    "native_filter_configuration":
                        native_filters(datasets, columns, own) if filters else [],
                    "positions": None}
        api.call("PUT", f"dashboard/{dash_ids[title]}", json={
            "position_json": json.dumps(layout(title, tabs, chart_ids), ensure_ascii=False),
            "json_metadata": json.dumps(metadata), "css": DASHBOARD_CSS})
        print(f"dashboard: {URL}/superset/dashboard/{slug}/")


DASHBOARD_CSS = """
.dashboard-markdown iframe { display: block; }
"""

if __name__ == "__main__":
    try:
        main()
    except Exception as e:      # noqa: BLE001 - show the API error and fail the container
        print(e, file=sys.stderr)
        sys.exit(1)
