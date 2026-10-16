"""Unit tests for CelesTrak parsing/validation (no network, no databases).

Run: python -m pytest tests  (with plugins/ on PYTHONPATH, see conftest.py)
"""
import json
from datetime import date, datetime

import pytest

from celestrak import client, parsing
from celestrak.sources import GP, SATCAT, SOCRATES, SUPGP

SATCAT_CSV = b"""OBJECT_NAME,OBJECT_ID,NORAD_CAT_ID,OBJECT_TYPE,OPS_STATUS_CODE,OWNER,LAUNCH_DATE,LAUNCH_SITE,DECAY_DATE,PERIOD,INCLINATION,APOGEE,PERIGEE,RCS,DATA_STATUS_CODE,ORBIT_CENTER,ORBIT_TYPE
SL-1 R/B,1957-001A,1,R/B,D,CIS,1957-10-04,TYMSC,1957-12-01,96.19,65.10,938,214,20.4200,,EA,IMP
VANGUARD 1,1958-002B,5,PAY,,US,1958-03-17,AFETR,,132.68,34.24,3819,654,0.1220,,EA,ORB
BROKEN,1958-002C,,PAY,,US,1958-03-17,AFETR,,,,,,,,EA,ORB
"""

GP_JSON = json.dumps([
    {"OBJECT_NAME": "ISS (ZARYA)", "OBJECT_ID": "1998-067A", "EPOCH": "2026-10-16T04:36:59.916096",
     "MEAN_MOTION": 15.5, "ECCENTRICITY": 0.0007, "INCLINATION": 51.64, "RA_OF_ASC_NODE": 257.1,
     "ARG_OF_PERICENTER": 130.5, "MEAN_ANOMALY": 325.0, "EPHEMERIS_TYPE": 0,
     "CLASSIFICATION_TYPE": "U", "NORAD_CAT_ID": 25544, "ELEMENT_SET_NO": 999,
     "REV_AT_EPOCH": 1234, "BSTAR": 0.0002, "MEAN_MOTION_DOT": 1e-5, "MEAN_MOTION_DDOT": 0,
     "UNEXPECTED_FIELD": "ignored"},
]).encode()

SOCRATES_CSV = b"""NORAD_CAT_ID_1,OBJECT_NAME_1,DSE_1,NORAD_CAT_ID_2,OBJECT_NAME_2,DSE_2,TCA,TCA_RANGE,TCA_RELATIVE_SPEED,MAX_PROB,DILUTION
49431,STARLINK-3153 [+],5.441,100275,STARLINK-37750 [+],5.469,2026-10-20 18:32:28.917,0.007,12.094,1.000E+00,0.000
"""


def test_satcat_types_nulls_and_required():
    df = parsing.parse(SATCAT, SATCAT_CSV)
    assert list(df.columns) == list(SATCAT.columns)
    assert len(df) == 2  # row without NORAD_CAT_ID dropped
    first, second = df.iloc[0], df.iloc[1]
    assert first["launch_date"] == date(1957, 10, 4)
    assert first["decay_date"] == date(1957, 12, 1)
    assert first["data_status_code"] is None  # empty string -> NULL
    assert first["rcs"] == pytest.approx(20.42)
    assert second["ops_status_code"] is None


def test_to_rows_produces_plain_python_values():
    df = parsing.parse(SATCAT, SATCAT_CSV)
    tech = {"load_id": "satcat_x", "loaded_at": datetime(2026, 10, 16), "source_file": "s3://raw/x"}
    rows = parsing.to_rows(df, SATCAT, tech)
    assert len(rows[0]) == len(SATCAT.ods_columns)
    row = dict(zip(SATCAT.ods_columns, rows[1]))
    assert row["norad_cat_id"] == 5 and type(row["norad_cat_id"]) is int
    assert row["decay_date"] is None
    assert row["load_id"] == "satcat_x"


def test_gp_json_ignores_unknown_fields_and_parses_epoch():
    df = parsing.parse(GP, GP_JSON)
    assert list(df.columns) == list(GP.columns)
    rows = parsing.to_rows(df, GP, {"load_id": "l", "loaded_at": datetime(2026, 1, 1), "source_file": "f"})
    row = dict(zip(GP.ods_columns, rows[0]))
    assert row["epoch"] == datetime(2026, 10, 16, 4, 36, 59, 916096)
    assert row["norad_cat_id"] == 25544


def test_supgp_missing_optional_columns_become_null():
    df = parsing.parse(SUPGP, GP_JSON)  # GP payload has no RMS / DATA_SOURCE
    assert df["rms"].isna().all()
    assert df["data_source"].iloc[0] is None


def test_socrates_csv():
    df = parsing.parse(SOCRATES, SOCRATES_CSV)
    row = df.iloc[0]
    assert row["norad_cat_id_2"] == 100275
    assert row["max_prob"] == pytest.approx(1.0)
    assert row["tca"].to_pydatetime() == datetime(2026, 10, 20, 18, 32, 28, 917000)


@pytest.mark.parametrize("content,fmt", [
    (b"", "csv"),
    (b"No GP data found", "json"),
    (b"[]", "json"),
    (b"Invalid query", "csv"),
])
def test_validate_rejects_error_pages(content, fmt):
    with pytest.raises(client.BadPayload):
        client.validate(content, fmt)


def test_validate_accepts_datasets():
    client.validate(GP_JSON, "json")
    client.validate(SATCAT_CSV, "csv")
