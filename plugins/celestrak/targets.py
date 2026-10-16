"""The three storages compared in ЛР1 behind one small interface.

PostgreSQL is the main ODS: ingestion DAGs append snapshots with `PostgresTarget.load`.
For the comparison all three load the same CSV export of ODS with their native bulk
path (PostgreSQL COPY, MySQL LOAD DATA, ClickHouse CSVWithNames insert).

CSV convention (produced by export_csv): header row, comma, '"' quoting, NULL = \\N.
"""
import csv
import io
from abc import ABC, abstractmethod

import clickhouse_connect
import psycopg2
import pymysql

from celestrak import config


class Target(ABC):
    name: str

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    @abstractmethod
    def load_csv(self, schema: str, table: str, columns: list[str], path: str) -> None:
        """Replace the table contents with the CSV file at path."""

    @abstractmethod
    def query(self, sql: str) -> list[tuple]: ...

    @abstractmethod
    def execute(self, sql: str) -> None: ...

    @abstractmethod
    def table_bytes(self, schema: str, table: str) -> int: ...

    def count(self, table: str, load_id: str | None = None, schema: str = "ods") -> int:
        where = f" WHERE load_id = '{load_id}'" if load_id else ""
        return int(self.query(f"SELECT count(*) FROM {schema}.{table}{where}")[0][0])

    @abstractmethod
    def close(self) -> None: ...


class PostgresTarget(Target):
    name = "postgres"

    def __init__(self):
        s = config.postgres()
        self.conn = psycopg2.connect(
            host=s.host, port=s.port, dbname=s.database, user=s.user, password=s.password
        )

    def load(self, table: str, columns: list[str], rows: list[tuple], load_id: str) -> int:
        """Append one snapshot to ods.<table>; re-running the same load_id replaces it."""
        buf = io.StringIO()
        csv.writer(buf).writerows(rows)  # None -> empty unquoted field -> NULL
        buf.seek(0)
        with self.conn, self.conn.cursor() as cur:  # one transaction
            cur.execute(f"DELETE FROM ods.{table} WHERE load_id = %s", (load_id,))
            cur.copy_expert(
                f"COPY ods.{table} ({', '.join(columns)}) FROM STDIN WITH (FORMAT csv)", buf
            )
        return len(rows)

    def export_csv(self, table: str, columns: list[str], path: str) -> None:
        sql = (f"COPY (SELECT {', '.join(columns)} FROM ods.{table}) TO STDOUT "
               r"WITH (FORMAT csv, HEADER true, NULL '\N')")
        with open(path, "w", encoding="utf-8", newline="") as f, self.conn.cursor() as cur:
            cur.copy_expert(sql, f)
        self.conn.rollback()

    def load_csv(self, schema, table, columns, path):
        sql = (f"COPY {schema}.{table} ({', '.join(columns)}) FROM STDIN "
               r"WITH (FORMAT csv, HEADER true, NULL '\N')")
        with open(path, encoding="utf-8") as f, self.conn, self.conn.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE {schema}.{table}")
            cur.copy_expert(sql, f)

    def query(self, sql):
        with self.conn.cursor() as cur:
            cur.execute(sql)
            result = cur.fetchall() if cur.description else []
        self.conn.rollback()
        return result

    def execute(self, sql):
        with self.conn, self.conn.cursor() as cur:
            cur.execute(sql)

    def table_bytes(self, schema, table):
        return int(self.query(f"SELECT pg_total_relation_size('{schema}.{table}')")[0][0])

    def close(self):
        self.conn.close()


class MySQLTarget(Target):
    name = "mysql"

    def __init__(self):
        s = config.mysql()
        self.conn = pymysql.connect(
            host=s.host, port=s.port, database=s.database, user=s.user, password=s.password,
            autocommit=False, charset="utf8mb4", local_infile=True,
        )

    def load_csv(self, schema, table, columns, path):
        sql = (
            f"LOAD DATA LOCAL INFILE '{path}' INTO TABLE {schema}.{table} "
            "CHARACTER SET utf8mb4 "
            r"FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' ESCAPED BY '\\' "
            r"LINES TERMINATED BY '\n' IGNORE 1 LINES "
            f"({', '.join(columns)})"
        )
        with self.conn.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE {schema}.{table}")
            cur.execute(sql)
        self.conn.commit()

    def query(self, sql):
        with self.conn.cursor() as cur:
            cur.execute(sql)
            result = cur.fetchall()
        self.conn.commit()
        return list(result)

    def execute(self, sql):
        with self.conn.cursor() as cur:
            cur.execute(sql)
        self.conn.commit()

    def table_bytes(self, schema, table):
        self.query(f"ANALYZE TABLE {schema}.{table}")
        self.execute("SET SESSION information_schema_stats_expiry = 0")
        rows = self.query(
            "SELECT data_length + index_length FROM information_schema.tables "
            f"WHERE table_schema = '{schema}' AND table_name = '{table}'"
        )
        return int(rows[0][0])

    def close(self):
        self.conn.close()


class ClickHouseTarget(Target):
    name = "clickhouse"

    def __init__(self):
        s = config.clickhouse()
        self.client = clickhouse_connect.get_client(
            host=s.host, port=s.port, username=s.user, password=s.password, database=s.database,
            send_receive_timeout=900,
        )

    def load_csv(self, schema, table, columns, path):
        self.client.command(f"TRUNCATE TABLE {schema}.{table}")
        with open(path, "rb") as f:
            self.client.raw_insert(
                f"{schema}.{table}", column_names=columns, insert_block=f, fmt="CSVWithNames",
                settings={"date_time_input_format": "best_effort"},
            )

    def query(self, sql):
        return self.client.query(sql).result_rows

    def execute(self, sql):
        self.client.command(sql)

    def table_bytes(self, schema, table):
        rows = self.query(
            "SELECT sum(bytes_on_disk) FROM system.parts "
            f"WHERE database = '{schema}' AND table = '{table}' AND active"
        )
        return int(rows[0][0] or 0)

    def close(self):
        self.client.close()


TARGETS = {t.name: t for t in (PostgresTarget, MySQLTarget, ClickHouseTarget)}


def connect(name: str) -> Target:
    return TARGETS[name]()
