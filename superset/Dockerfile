FROM apache/superset:6.1.0

# Drivers: ClickHouse (clickhousedb:// dialect for the marts) and PostgreSQL (metadata DB).
USER root
RUN uv pip install --python /app/.venv/bin/python --no-cache \
        clickhouse-connect==0.8.18 psycopg2-binary==2.9.10 requests==2.32.3
COPY superset_config.py /app/pythonpath/superset_config.py
COPY init.sh bootstrap.py /app/bootstrap/
USER superset
