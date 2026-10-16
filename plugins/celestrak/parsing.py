"""Turn a raw CelesTrak file into typed rows matching the ODS schema of the source."""
import io
import json
from datetime import date, datetime

import pandas as pd

from celestrak.sources import Source


def parse(source: Source, content: bytes) -> pd.DataFrame:
    """Raw bytes -> DataFrame with exactly source.columns, cast to logical types.

    Unknown source columns are ignored, missing ones become NULL, rows without the
    required keys are dropped (ODS keeps everything else as-is).
    """
    if source.fmt == "csv":
        raw = pd.read_csv(io.BytesIO(content), dtype=str, keep_default_na=False)
    else:
        raw = pd.DataFrame(json.loads(content))
    raw.columns = [c.strip().lower() for c in raw.columns]

    df = pd.DataFrame(index=raw.index)
    for col, kind in source.columns.items():
        values = raw[col] if col in raw.columns else pd.Series([None] * len(raw), index=raw.index)
        df[col] = _cast(values, kind)

    if source.required:
        df = df.dropna(subset=list(source.required))
    return df.reset_index(drop=True)


def _cast(s: pd.Series, kind: str) -> pd.Series:
    if kind == "str":
        s = s.astype(object).where(s.notna(), None)
        return s.map(lambda v: (str(v).strip() or None) if v is not None else None)
    if kind == "int":
        return pd.to_numeric(s, errors="coerce").astype("Int64")
    if kind == "float":
        return pd.to_numeric(s, errors="coerce").astype("float64")
    if kind == "date":
        return pd.to_datetime(s, errors="coerce", format="%Y-%m-%d").dt.date
    if kind == "datetime":
        return pd.to_datetime(s, errors="coerce", format="ISO8601")
    raise ValueError(f"Unknown column kind {kind}")


def to_rows(df: pd.DataFrame, source: Source, tech: dict) -> list[tuple]:
    """DataFrame -> list of tuples of plain Python values (None for NULL).

    `tech` holds the technical columns (load_id, loaded_at, source_file) which are
    the same for the whole file. Order follows source.ods_columns.
    """
    columns = []
    for col, kind in source.columns.items():
        columns.append([_py(v, kind) for v in df[col].tolist()])
    n = len(df)
    for col in ("load_id", "loaded_at", "source_file"):
        columns.append([tech[col]] * n)
    return list(zip(*columns))


def _py(v, kind: str):
    if v is None or v is pd.NA or v is pd.NaT:
        return None
    if isinstance(v, float) and v != v:  # NaN
        return None
    if kind == "int":
        return int(v)
    if kind == "float":
        return float(v)
    if kind == "datetime":
        return v.to_pydatetime() if isinstance(v, pd.Timestamp) else v
    if kind == "date":
        return v if isinstance(v, date) and not isinstance(v, datetime) else None
    return v
