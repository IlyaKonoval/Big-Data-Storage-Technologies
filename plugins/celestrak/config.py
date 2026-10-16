"""Connection settings, taken from environment variables set in docker-compose.yml."""
import os
from dataclasses import dataclass


def _env(name: str, default: str | None = None) -> str:
    value = os.environ.get(name, default)
    if value is None:
        raise RuntimeError(f"Environment variable {name} is not set")
    return value


@dataclass(frozen=True)
class DbSettings:
    host: str
    port: int
    database: str
    user: str
    password: str


def postgres() -> DbSettings:
    return DbSettings(
        host=_env("DWH_PG_HOST", "localhost"),
        port=int(_env("DWH_PG_PORT", "5433")),
        database=_env("DWH_PG_DB", "dwh"),
        user=_env("DWH_PG_USER", "dwh"),
        password=_env("DWH_PG_PASSWORD", "dwh"),
    )


def mysql() -> DbSettings:
    return DbSettings(
        host=_env("MYSQL_HOST", "localhost"),
        port=int(_env("MYSQL_PORT", "3307")),
        database=_env("MYSQL_DB", "ods"),
        user=_env("MYSQL_USER", "dwh"),
        password=_env("MYSQL_PASSWORD", "dwh"),
    )


def clickhouse() -> DbSettings:
    return DbSettings(
        host=_env("CH_HOST", "localhost"),
        port=int(_env("CH_PORT", "8123")),
        database=_env("CH_DB", "ods"),
        user=_env("CH_USER", "dwh"),
        password=_env("CH_PASSWORD", "dwh"),
    )


S3_ENDPOINT_URL = _env("S3_ENDPOINT_URL", "http://localhost:8333")
S3_ACCESS_KEY = _env("S3_ACCESS_KEY", "bdst")
S3_SECRET_KEY = _env("S3_SECRET_KEY", "bdst-secret")
S3_RAW_BUCKET = _env("S3_RAW_BUCKET", "raw")

USER_AGENT = _env("CELESTRAK_USER_AGENT", "bdst-student-project/1.0")
REPORTS_DIR = _env("REPORTS_DIR", "reports")
SQL_DIR = _env("SQL_DIR", "sql")
