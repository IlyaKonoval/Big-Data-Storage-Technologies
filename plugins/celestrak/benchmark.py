"""Storage comparison: the same data, the same workload, three DBMS.

Workload = what the platform will actually do with CelesTrak data:
  load    - bulk load of the ODS CSV export (PostgreSQL COPY / MySQL LOAD DATA / ClickHouse insert)
  query   - point lookup, catalog GROUP BY, time series, JOIN catalog x orbits, top-N, full scan
  storage - bytes on disk incl. indexes (pg_total_relation_size / information_schema / system.parts)
Every query is run once to warm caches, then REPEATS times; the median is reported.
"""
import statistics
import time

from celestrak.targets import Target

REPEATS = 5

_LATEST = "loaded_at = (SELECT max(loaded_at) FROM ods.{t})"

# name -> (description, default SQL, {dbms: SQL override})
QUERIES = {
    "q1_point_lookup": (
        "Последние элементы орбиты МКС (NORAD 25544)",
        "SELECT * FROM ods.gp WHERE norad_cat_id = 25544 ORDER BY epoch DESC LIMIT 1",
        {},
    ),
    "q2_catalog_groupby": (
        "Каталог: число объектов по владельцу и типу (последний снимок)",
        "SELECT owner, object_type, count(*) AS n FROM ods.satcat "
        f"WHERE {_LATEST.format(t='satcat')} GROUP BY owner, object_type ORDER BY n DESC",
        {},
    ),
    "q3_timeseries": (
        "GP: число элементов и средняя mean_motion по дням epoch (вся история)",
        "SELECT date_trunc('day', epoch) AS d, count(*), avg(mean_motion) "
        "FROM ods.gp GROUP BY d ORDER BY d",
        {
            "mysql": "SELECT DATE(epoch) AS d, count(*), avg(mean_motion) "
                     "FROM ods.gp GROUP BY d ORDER BY d",
            "clickhouse": "SELECT toDate(epoch) AS d, count(*), avg(mean_motion) "
                          "FROM ods.gp GROUP BY d ORDER BY d",
        },
    ),
    "q4_join": (
        "JOIN каталог x орбиты: спутники на LEO по владельцам (последние снимки)",
        "SELECT s.owner, count(*) AS n, avg(g.inclination) AS avg_incl "
        "FROM ods.gp g JOIN ods.satcat s ON s.norad_cat_id = g.norad_cat_id "
        "WHERE g.loaded_at = (SELECT max(loaded_at) FROM ods.gp) "
        "AND s.loaded_at = (SELECT max(loaded_at) FROM ods.satcat) "
        "AND g.mean_motion > 11.25 GROUP BY s.owner ORDER BY n DESC LIMIT 20",
        {},
    ),
    "q5_topn": (
        "SOCRATES: топ-100 сближений по вероятности (последний снимок)",
        "SELECT norad_cat_id_1, norad_cat_id_2, tca, tca_range, max_prob FROM ods.socrates "
        f"WHERE {_LATEST.format(t='socrates')} ORDER BY max_prob DESC LIMIT 100",
        {},
    ),
    "q6_full_scan": (
        "SOCRATES: агрегаты по всей истории (полный скан)",
        "SELECT count(*), avg(tca_range), min(tca_range), max(max_prob), "
        "count(DISTINCT norad_cat_id_1) FROM ods.socrates",
        {},
    ),
}


def run_queries(target: Target) -> list[tuple[str, float, int]]:
    """-> [(query_name, median_seconds, rows_returned)]"""
    results = []
    for name, (_, sql, overrides) in QUERIES.items():
        sql = overrides.get(target.name, sql)
        target.query(sql)  # warm-up
        timings, rows = [], 0
        for _ in range(REPEATS):
            started = time.perf_counter()
            rows = len(target.query(sql))
            timings.append(time.perf_counter() - started)
        results.append((name, statistics.median(timings), rows))
    return results
