#!/usr/bin/env bash
# One-shot initialisation of Superset: metadata database in airflow-postgres, schema
# migrations, admin user, roles. Idempotent (safe on every `docker compose up`).
set -euo pipefail

python - <<'EOF'
import os
import psycopg2

# The metadata DB lives in the existing airflow-postgres server; create it on first start
# (initdb scripts do not run on an existing volume).
conn = psycopg2.connect(host="airflow-postgres", user="airflow", password="airflow",
                        dbname="postgres")
conn.autocommit = True
with conn.cursor() as cur:
    cur.execute("SELECT 1 FROM pg_database WHERE datname = 'superset'")
    if cur.fetchone() is None:
        cur.execute("CREATE DATABASE superset")
        print("created database superset")
conn.close()
EOF

superset db upgrade
superset fab create-admin \
    --username "${SUPERSET_ADMIN_USER:-admin}" --password "${SUPERSET_ADMIN_PASSWORD:-admin}" \
    --firstname Admin --lastname User --email admin@example.com || true
superset init
