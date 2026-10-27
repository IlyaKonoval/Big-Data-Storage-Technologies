"""Orbit marts computed in Python: SGP4 propagation of the latest GP / SupGP element sets.

Element sets come from the raw vault (the versions referenced by the latest PIT day, via
bv.object_current), so this is a transformation of the detailed layer, not a new source.

    mart.conjunction_geo       where over the Earth the forecast conjunctions happen (TCA)
    mart.launch_arcs           launch site -> current position of active payloads
    mart.gp_vs_supgp           Starlink: distance between the GP (NORAD) and SupGP
                               (operator) predictions at the same instant
    mart.ground_tracks         90-minute ground tracks of featured objects (ISS, Tiangong,
                               Hubble) and of the newest Starlink launch, as map paths
    mart.orbit_elements        the element sets as two-line elements, for the 3D globe that
                               propagates them in the browser

Positions: SGP4 gives TEME coordinates; they are rotated to Earth-fixed by Greenwich mean
sidereal time (IAU 1982, polar motion ignored) and converted to WGS84 geodetic latitude,
longitude and height. Accuracy is that of GP itself (~1 km at epoch), plenty for maps.
"""
import logging
import math
from datetime import datetime, timedelta, timezone

import numpy as np
from sgp4.api import WGS72, Satrec, SatrecArray, jday
from sgp4.exporter import export_tle

from celestrak import marts, meta

log = logging.getLogger(__name__)

HORIZON_MIN = 90                      # ground tracks: one LEO revolution
SGP4_EPOCH_JD = 2433281.5             # 1949-12-31 00:00 UT, epoch origin of sgp4init
WGS84_A = 6378.137
WGS84_F = 1 / 298.257223563
WGS84_E2 = WGS84_F * (2 - WGS84_F)

# Ground tracks drawn on the map: crewed stations and Hubble (by NORAD ID).
FEATURED = {25544: "ISS", 48274: "Tiangong", 20580: "Hubble"}

ELEMENTS = ["epoch", "mean_motion", "eccentricity", "inclination", "ra_of_asc_node",
            "arg_of_pericenter", "mean_anomaly", "bstar", "mean_motion_dot",
            "mean_motion_ddot"]

OBJECTS_SQL = """
SELECT o.norad_cat_id, ifNull(o.object_name, toString(o.norad_cat_id)) AS name,
       o.object_type, o.owner, ifNull(o.orbit_regime, 'unknown') AS regime, o.is_starlink,
       o.is_active, o.rcs, o.launch_site, toYear(o.launch_date) AS launch_year,
       o.launch_designator, o.launch_date,
       {elements}
FROM bv.object_current AS o
INNER JOIN rv.sat_object_orbit_{source} AS g FINAL
        ON g.hk_object = o.hk_object AND g.load_dts = o.{source}_ldts
WHERE o.in_orbit AND g.mean_motion > 0 AND g.eccentricity >= 0 AND g.eccentricity < 1
"""


def satrec(row: dict) -> Satrec:
    """Satrec from OMM elements (degrees, rev/day) as stored in the vault."""
    epoch: datetime = row["epoch"]
    jd, fr = jday(epoch.year, epoch.month, epoch.day, epoch.hour, epoch.minute,
                  epoch.second + epoch.microsecond / 1e6)
    rad = math.pi / 180
    rev_per_day = 2 * math.pi / 1440          # rev/day -> rad/min
    sat = Satrec()
    sat.sgp4init(
        WGS72, "i", int(row["norad_cat_id"]) % 100000, jd + fr - SGP4_EPOCH_JD,
        row["bstar"] or 0.0,
        (row["mean_motion_dot"] or 0.0) * rev_per_day / 1440,
        (row["mean_motion_ddot"] or 0.0) * rev_per_day / 1440 ** 2,
        row["eccentricity"], row["arg_of_pericenter"] * rad, row["inclination"] * rad,
        row["mean_anomaly"] * rad, row["mean_motion"] * rev_per_day,
        row["ra_of_asc_node"] * rad)
    return sat


def gmst(jd: np.ndarray) -> np.ndarray:
    """Greenwich mean sidereal time [rad] for UT Julian dates (IAU 1982)."""
    t = (jd - 2451545.0) / 36525
    seconds = (67310.54841 + (876600 * 3600 + 8640184.812866) * t
               + 0.093104 * t ** 2 - 6.2e-6 * t ** 3)
    return np.radians((seconds % 86400) / 240)


def teme_to_geodetic(r: np.ndarray, jd: np.ndarray):
    """TEME positions [..., 3] (km) at Julian dates jd (broadcastable to r[..., 0]) ->
    WGS84 latitude, longitude [deg] and height [km]."""
    theta = gmst(jd)
    x = np.cos(theta) * r[..., 0] + np.sin(theta) * r[..., 1]
    y = -np.sin(theta) * r[..., 0] + np.cos(theta) * r[..., 1]
    z = r[..., 2]
    p = np.hypot(x, y)
    # Fixed-point iteration on latitude; forms without 1/cos(lat) stay valid at the poles.
    lat = np.arctan2(z, p * (1 - WGS84_E2))
    for _ in range(6):
        n = WGS84_A / np.sqrt(1 - WGS84_E2 * np.sin(lat) ** 2)
        lat = np.arctan2(z + WGS84_E2 * n * np.sin(lat), p)
    h = (p * np.cos(lat) + z * np.sin(lat)
         - WGS84_A * np.sqrt(1 - WGS84_E2 * np.sin(lat) ** 2))
    return np.degrees(lat), np.degrees(np.arctan2(y, x)), h


def utc(ts: datetime) -> datetime:
    """ClickHouse returns naive UTC datetimes; make them aware so that inserting them back
    does not shift them by the local timezone of the worker."""
    return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)


def julian(ts: list[datetime]) -> tuple[np.ndarray, np.ndarray]:
    parts = [jday(t.year, t.month, t.day, t.hour, t.minute, t.second + t.microsecond / 1e6)
             for t in ts]
    return np.array([p[0] for p in parts]), np.array([p[1] for p in parts])


def load_objects(ch, source: str = "gp") -> list[dict]:
    sql = OBJECTS_SQL.format(source=source, elements=", ".join(f"g.{c} AS {c}" for c in ELEMENTS))
    result = ch.query(sql)
    return [dict(zip(result.column_names, row)) for row in result.result_rows]


def build_orbit_marts(run_id: str | None = None, now: datetime | None = None) -> list[dict]:
    t0 = utc(now or datetime.now(timezone.utc)).replace(second=0, microsecond=0)
    times = [t0 + timedelta(minutes=m) for m in range(HORIZON_MIN + 1)]
    jd, fr = julian(times)
    with meta.client() as ch:
        objects = load_objects(ch, "gp")
        conjunctions = ch.query(
            "SELECT norad_cat_id_1, object_name_1, object_type_1, norad_cat_id_2, "
            "object_name_2, object_type_2, pair_kind, involves_starlink, tca, tca_range_km, "
            "relative_speed_km_s, max_prob FROM mart.conjunction_risk").result_rows
        sites = {r[0]: r[1:] for r in ch.query(
            "SELECT code, name, latitude, longitude FROM rv.ref_launch_site FINAL").result_rows}
        supgp = load_objects(ch, "supgp")
    sats = [satrec(o) for o in objects]
    err, r, _ = SatrecArray(sats).sgp4(jd, fr)
    lat, lon, alt = teme_to_geodetic(r, (jd + fr)[None, :])
    ok = err == 0
    log.info("SGP4: %d objects x %d steps, %d failed points", len(sats), len(times),
             int((~ok).sum()))

    results = [
        _launch_arcs(objects, sites, lat[:, 0], lon[:, 0], alt[:, 0], ok[:, 0], run_id),
        _conjunction_geo(objects, sats, conjunctions, run_id),
        _gp_vs_supgp(objects, sats, supgp, t0, run_id),
        _ground_tracks(objects, times, lat, lon, alt, ok, run_id),
        _orbit_elements(objects, sats, run_id),
    ]
    return results


def _launch_arcs(objects, sites, lat, lon, alt, ok, run_id):
    rows = []
    for i, o in enumerate(objects):
        site = sites.get(o["launch_site"])
        if not ok[i] or site is None or o["object_type"] != "PAY" or not o["is_active"]:
            continue
        rows.append((o["norad_cat_id"], o["name"], o["owner"], o["regime"],
                     bool(o["is_starlink"]), o["launch_year"], o["launch_site"], site[0],
                     site[1], site[2], round(float(lat[i]), 4), round(float(lon[i]), 4),
                     round(float(alt[i]), 1)))
    return marts.replace_table(
        "launch_arcs",
        [("norad_cat_id", "UInt32"), ("object_name", "String"), ("owner", "LowCardinality(String)"),
         ("orbit_regime", "LowCardinality(String)"), ("is_starlink", "Bool"),
         ("launch_year", "Nullable(UInt16)"), ("launch_site", "LowCardinality(String)"),
         ("site_name", "String"), ("site_latitude", "Float64"), ("site_longitude", "Float64"),
         ("latitude", "Float32"), ("longitude", "Float32"), ("altitude_km", "Float32")],
        "(launch_site, norad_cat_id)", rows, run_id)


def _conjunction_geo(objects, sats, conjunctions, run_id):
    """Position of the conjunction at TCA: the propagated object with GP (the two objects
    are within tca_range, a few km, of each other)."""
    by_norad = {o["norad_cat_id"]: s for o, s in zip(objects, sats)}
    rows = []
    for (n1, name1, type1, n2, name2, type2, kind, starlink, tca, rng, speed,
         prob) in conjunctions:
        sat = by_norad.get(n1) or by_norad.get(n2)
        if sat is None:
            continue
        jd, fr = jday(tca.year, tca.month, tca.day, tca.hour, tca.minute,
                      tca.second + tca.microsecond / 1e6)
        e, r, _ = sat.sgp4(jd, fr)
        if e:
            continue
        lat, lon, alt = teme_to_geodetic(np.array(r), np.array(jd + fr))
        rows.append((n1, name1, type1, n2, name2, type2, kind, bool(starlink), utc(tca), rng,
                     speed, prob, round(float(lat), 4), round(float(lon), 4),
                     round(float(alt), 1)))
    return marts.replace_table(
        "conjunction_geo",
        [("norad_cat_id_1", "UInt32"), ("object_name_1", "String"),
         ("object_type_1", "LowCardinality(String)"), ("norad_cat_id_2", "UInt32"),
         ("object_name_2", "String"), ("object_type_2", "LowCardinality(String)"),
         ("pair_kind", "LowCardinality(String)"), ("involves_starlink", "Bool"),
         ("tca", "DateTime64(3)"), ("tca_range_km", "Nullable(Float64)"),
         ("relative_speed_km_s", "Nullable(Float64)"), ("max_prob", "Nullable(Float64)"),
         ("latitude", "Float32"), ("longitude", "Float32"), ("altitude_km", "Float32")],
        "(tca, norad_cat_id_1, norad_cat_id_2)", rows, run_id)


def _gp_vs_supgp(objects, sats, supgp, t0, run_id):
    """Starlink: GP vs operator ephemeris propagated to the same instant (build time)."""
    gp_by_norad = {o["norad_cat_id"]: (o, s) for o, s in zip(objects, sats)}
    jd, fr = jday(t0.year, t0.month, t0.day, t0.hour, t0.minute, 0)
    rows = []
    for o in supgp:
        pair = gp_by_norad.get(o["norad_cat_id"])
        if pair is None:
            continue
        gp_obj, gp_sat = pair
        e1, r1, _ = gp_sat.sgp4(jd, fr)
        e2, r2, _ = satrec(o).sgp4(jd, fr)
        if e1 or e2:
            continue
        distance = math.dist(r1, r2)
        alt_gp = math.hypot(*r1) - WGS84_A
        alt_sup = math.hypot(*r2) - WGS84_A
        epoch_gap_h = (o["epoch"] - gp_obj["epoch"]).total_seconds() / 3600
        rows.append((o["norad_cat_id"], o["name"], utc(gp_obj["epoch"]), utc(o["epoch"]),
                     round(epoch_gap_h, 2), round(distance, 3), round(alt_sup - alt_gp, 3),
                     round(alt_gp, 1), t0))
    return marts.replace_table(
        "gp_vs_supgp",
        [("norad_cat_id", "UInt32"), ("object_name", "String"), ("gp_epoch", "DateTime64(6)"),
         ("supgp_epoch", "DateTime64(6)"), ("epoch_gap_hours", "Float64"),
         ("distance_km", "Float64"), ("radial_diff_km", "Float64"), ("altitude_km", "Float64"),
         ("compared_at", "DateTime")],
        "norad_cat_id", rows, run_id)


def split_at_antimeridian(points: list[tuple[float, float]]) -> list[list[tuple[float, float]]]:
    """Cut a (lon, lat) track where it jumps across +-180 deg, so maps draw no stray lines."""
    segments = [[points[0]]] if points else []
    for prev, cur in zip(points, points[1:]):
        if abs(cur[0] - prev[0]) > 180:
            segments.append([])
        segments[-1].append(cur)
    return [s for s in segments if len(s) > 1]


def _ground_tracks(objects, times, lat, lon, alt, ok, run_id):
    starlink = [o for o in objects if o["is_starlink"] and o["launch_date"]]
    newest = max((o["launch_designator"] for o in starlink), default=None,
                 key=lambda d: max(o["launch_date"] for o in starlink
                                   if o["launch_designator"] == d))
    rows = []
    for i, o in enumerate(objects):
        if o["norad_cat_id"] in FEATURED:
            group = FEATURED[o["norad_cat_id"]]
        elif newest and o["is_starlink"] and o["launch_designator"] == newest:
            group = f"Starlink {newest}"
        else:
            continue
        steps = [t for t in range(len(times)) if ok[i, t]]
        points = [(round(float(lon[i, t]), 4), round(float(lat[i, t]), 4)) for t in steps]
        for n, segment in enumerate(split_at_antimeridian(points)):
            rows.append((o["norad_cat_id"], o["name"], group, n,
                         "[" + ",".join(f"[{x},{y}]" for x, y in segment) + "]",
                         round(float(np.mean(alt[i, steps])), 1)))
    return marts.replace_table(
        "ground_tracks",
        [("norad_cat_id", "UInt32"), ("object_name", "String"),
         ("track_group", "LowCardinality(String)"), ("segment", "UInt8"),
         ("path_json", "String"), ("mean_altitude_km", "Float32")],
        "(track_group, norad_cat_id, segment)", rows, run_id)


def _orbit_elements(objects, sats, run_id):
    rows = []
    for o, sat in zip(objects, sats):
        line1, line2 = export_tle(sat)
        rows.append((o["norad_cat_id"], o["name"], o["object_type"], o["owner"], o["regime"],
                     bool(o["is_starlink"]), utc(o["epoch"]), line1, line2))
    return marts.replace_table(
        "orbit_elements",
        [("norad_cat_id", "UInt32"), ("object_name", "String"),
         ("object_type", "LowCardinality(String)"), ("owner", "LowCardinality(String)"),
         ("orbit_regime", "LowCardinality(String)"), ("is_starlink", "Bool"),
         ("epoch", "DateTime64(6)"), ("tle_line1", "String"), ("tle_line2", "String")],
        "norad_cat_id", rows, run_id)
