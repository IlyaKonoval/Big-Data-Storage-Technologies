"""Description of the CelesTrak datasets: URL, raw format and the typed ODS schema.

Column types are logical ("str", "int", "float", "date", "datetime"); parsing.py casts
raw values to them, and the DDL in sql/ods/<dbms>/ mirrors the same column list.
"""
from dataclasses import dataclass, field

OMM_COLUMNS = {
    "object_name": "str",
    "object_id": "str",
    "epoch": "datetime",
    "mean_motion": "float",
    "eccentricity": "float",
    "inclination": "float",
    "ra_of_asc_node": "float",
    "arg_of_pericenter": "float",
    "mean_anomaly": "float",
    "ephemeris_type": "int",
    "classification_type": "str",
    "norad_cat_id": "int",
    "element_set_no": "int",
    "rev_at_epoch": "int",
    "bstar": "float",
    "mean_motion_dot": "float",
    "mean_motion_ddot": "float",
}

# Technical columns appended to every ODS row.
TECH_COLUMNS = ["load_id", "loaded_at", "source_file"]


@dataclass(frozen=True)
class Source:
    name: str            # also the ODS table name
    url: str
    fmt: str             # "csv" | "json"
    columns: dict[str, str] = field(default_factory=dict)
    required: tuple[str, ...] = ()   # rows with NULL here are dropped as unusable
    description: str = ""

    @property
    def table(self) -> str:
        return self.name

    @property
    def ods_columns(self) -> list[str]:
        return list(self.columns) + TECH_COLUMNS


SATCAT = Source(
    name="satcat",
    url="https://celestrak.org/pub/satcat.csv",
    fmt="csv",
    columns={
        "object_name": "str",
        "object_id": "str",
        "norad_cat_id": "int",
        "object_type": "str",
        "ops_status_code": "str",
        "owner": "str",
        "launch_date": "date",
        "launch_site": "str",
        "decay_date": "date",
        "period": "float",
        "inclination": "float",
        "apogee": "float",
        "perigee": "float",
        "rcs": "float",
        "data_status_code": "str",
        "orbit_center": "str",
        "orbit_type": "str",
    },
    required=("norad_cat_id",),
    description="SATCAT: full satellite catalog (all objects ever tracked)",
)

GP = Source(
    name="gp",
    url="https://celestrak.org/NORAD/elements/gp.php?GROUP=active&FORMAT=json",
    fmt="json",
    columns=dict(OMM_COLUMNS),
    required=("norad_cat_id",),
    description="GP: orbital elements (OMM) of all active satellites",
)

SUPGP = Source(
    name="supgp",
    url="https://celestrak.org/NORAD/elements/supplemental/sup-gp.php?FILE=starlink&FORMAT=json",
    fmt="json",
    columns={**OMM_COLUMNS, "rms": "float", "data_source": "str"},
    required=("norad_cat_id",),
    description="SupGP: operator-supplied orbital elements for Starlink",
)

SOCRATES = Source(
    name="socrates",
    url="https://celestrak.org/SOCRATES/sort-minRange.csv",
    fmt="csv",
    columns={
        "norad_cat_id_1": "int",
        "object_name_1": "str",
        "dse_1": "float",
        "norad_cat_id_2": "int",
        "object_name_2": "str",
        "dse_2": "float",
        "tca": "datetime",
        "tca_range": "float",
        "tca_relative_speed": "float",
        "max_prob": "float",
        "dilution": "float",
    },
    required=("norad_cat_id_1", "norad_cat_id_2"),
    description="SOCRATES: predicted conjunctions (close approaches) for the next 7 days",
)

SOURCES = {s.name: s for s in (SATCAT, GP, SUPGP, SOCRATES)}
