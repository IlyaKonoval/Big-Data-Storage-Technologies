"""Mart SQL files and the SGP4 helpers (no database needed)."""
import re
from datetime import datetime, timezone

import numpy as np
import pytest
from sgp4 import omm
from sgp4.api import Satrec, jday

from celestrak import marts, orbits

MARTS = marts.load_marts()

# ISS element set in OMM form, as stored in rv.sat_object_orbit_gp.
ISS = {
    "norad_cat_id": 25544,
    "epoch": datetime(2026, 10, 27, 12, 0, 0),
    "mean_motion": 15.4995,
    "eccentricity": 0.0006703,
    "inclination": 51.6416,
    "ra_of_asc_node": 247.4627,
    "arg_of_pericenter": 130.536,
    "mean_anomaly": 325.0288,
    "bstar": 0.0001027,
    "mean_motion_dot": 0.00016717,
    "mean_motion_ddot": 0.0,
}


def test_marts_found_with_unique_names():
    names = [m.name for m in MARTS]
    assert names and len(names) == len(set(names))


@pytest.mark.parametrize("mart", MARTS, ids=lambda m: m.name)
def test_mart_header_and_single_select(mart):
    assert re.fullmatch(r"[a-z_]+", mart.name)
    assert mart.insight, f"{mart.path.name}: describe the insight"
    assert mart.order_by


@pytest.mark.parametrize("mart", MARTS, ids=lambda m: m.name)
def test_marts_read_the_vault_not_ods(mart):
    """Marts are built from the detailed layer (bv/rv) or technical metadata only."""
    tables = set(re.findall(r"\b(?:FROM|JOIN)\s+([a-z_]+)\.", mart.sql, flags=re.I))
    assert tables <= {"bv", "rv", "meta"}, tables


def test_parse_mart_rejects_missing_header(tmp_path):
    path = tmp_path / "99_bad.sql"
    path.write_text("-- insight: x\nSELECT 1\n", encoding="utf-8")
    with pytest.raises(ValueError, match="mart"):
        marts.parse_mart(path)


def test_parse_mart_rejects_several_statements(tmp_path):
    path = tmp_path / "99_bad.sql"
    path.write_text("-- mart: bad\n-- order_by: x\nSELECT 1;\nSELECT 2;\n", encoding="utf-8")
    with pytest.raises(ValueError, match="one SELECT"):
        marts.parse_mart(path)


def test_satrec_matches_the_sgp4_omm_initializer():
    """Our OMM -> sgp4init conversion gives the same orbit as the library's own parser."""
    reference = Satrec()
    omm.initialize(reference, {
        "OBJECT_NAME": "ISS", "OBJECT_ID": "1998-067A", "EPOCH": "2026-10-27T12:00:00.000000",
        "MEAN_MOTION": str(ISS["mean_motion"]), "ECCENTRICITY": str(ISS["eccentricity"]),
        "INCLINATION": str(ISS["inclination"]), "RA_OF_ASC_NODE": str(ISS["ra_of_asc_node"]),
        "ARG_OF_PERICENTER": str(ISS["arg_of_pericenter"]),
        "MEAN_ANOMALY": str(ISS["mean_anomaly"]), "EPHEMERIS_TYPE": "0",
        "CLASSIFICATION_TYPE": "U", "NORAD_CAT_ID": "25544", "ELEMENT_SET_NO": "999",
        "REV_AT_EPOCH": "1", "BSTAR": str(ISS["bstar"]),
        "MEAN_MOTION_DOT": str(ISS["mean_motion_dot"]), "MEAN_MOTION_DDOT": "0",
    })
    ours = orbits.satrec(ISS)
    jd, fr = jday(2026, 10, 27, 18, 0, 0)
    _, r_ref, _ = reference.sgp4(jd, fr)
    _, r_ours, _ = ours.sgp4(jd, fr)
    assert np.linalg.norm(np.subtract(r_ref, r_ours)) < 0.01   # km


def test_geodetic_conversion_of_iss():
    sat = orbits.satrec(ISS)
    jd, fr = jday(2026, 10, 27, 12, 30, 0)
    _, r, _ = sat.sgp4(jd, fr)
    lat, lon, alt = orbits.teme_to_geodetic(np.array(r), np.array(jd + fr))
    assert abs(lat) <= ISS["inclination"] + 0.1          # ground track bounded by inclination
    assert -180 <= lon <= 180
    assert 380 < alt < 450                               # ISS altitude band, km


def test_geodetic_of_points_on_the_axes():
    """Equator and pole points (Earth rotation only changes longitude): WGS84 geometry."""
    jd = np.array(2461340.5)
    lat, _, h = orbits.teme_to_geodetic(np.array([7000.0, 0.0, 0.0]), jd)
    assert abs(lat) < 1e-6 and abs(h - (7000 - orbits.WGS84_A)) < 1e-6
    lat, _, h = orbits.teme_to_geodetic(np.array([0.0, 0.0, 7000.0]), jd)
    polar_radius = orbits.WGS84_A * (1 - orbits.WGS84_F)
    assert abs(lat - 90) < 1e-6 and abs(h - (7000 - polar_radius)) < 1e-3


def test_utc_makes_naive_datetimes_aware():
    assert orbits.utc(datetime(2026, 1, 1)).tzinfo is timezone.utc


def test_tracks_are_cut_at_the_antimeridian():
    track = [(170.0, 0.0), (179.0, 1.0), (-179.0, 2.0), (-170.0, 3.0), (-160.0, 4.0)]
    assert orbits.split_at_antimeridian(track) == [
        [(170.0, 0.0), (179.0, 1.0)], [(-179.0, 2.0), (-170.0, 3.0), (-160.0, 4.0)]]


def test_exported_tle_reproduces_the_orbit():
    """The globe propagates TLE lines exported from our Satrec: same positions."""
    from sgp4.exporter import export_tle
    sat = orbits.satrec(ISS)
    line1, line2 = export_tle(sat)
    assert line1.startswith("1 25544") and line2.startswith("2 25544")
    again = Satrec.twoline2rv(line1, line2)
    jd, fr = jday(2026, 10, 27, 13, 0, 0)
    assert np.linalg.norm(np.subtract(sat.sgp4(jd, fr)[1], again.sgp4(jd, fr)[1])) < 1.0
