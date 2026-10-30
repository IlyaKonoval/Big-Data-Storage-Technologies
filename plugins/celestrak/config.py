"""Settings: endpoints and credentials from Airflow Connections, the rest from environment
variables set in docker-compose.yml."""
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


# Airflow Connections (airflow/config/connections.json).
CLICKHOUSE_CONN = "clickhouse_dwh"
S3_CONN = "s3_raw"
OPENMETADATA_CONN = "openmetadata"
POSTGRES_CONN = "postgres_dwh"
MYSQL_CONN = "mysql_ods"


def connection(conn_id: str):
    """An Airflow Connection; imported here so the package also loads outside Airflow."""
    from airflow.sdk import Connection

    return Connection.get(conn_id)


def _db(conn_id: str) -> DbSettings:
    c = connection(conn_id)
    return DbSettings(host=c.host, port=int(c.port), database=c.schema, user=c.login,
                      password=c.password)


def postgres() -> DbSettings:
    return _db(POSTGRES_CONN)


def mysql() -> DbSettings:
    return _db(MYSQL_CONN)


def clickhouse() -> DbSettings:
    return _db(CLICKHOUSE_CONN)


def openmetadata() -> tuple[str, str, str]:
    """(API URL, admin e-mail, password) of OpenMetadata."""
    c = connection(OPENMETADATA_CONN)
    return f"{c.schema or 'http'}://{c.host}:{c.port}/api", c.login, c.password


S3_RAW_BUCKET = _env("S3_RAW_BUCKET", "raw")

USER_AGENT = _env("CELESTRAK_USER_AGENT", "bdst-student-project/1.0")
REPORTS_DIR = _env("REPORTS_DIR", "reports")
SQL_DIR = _env("SQL_DIR", "sql")
