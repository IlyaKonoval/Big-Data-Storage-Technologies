# Big Data Storage Technologies — платформа данных CelesTrak

Система сбора, хранения и анализа открытых данных о космических объектах
[CelesTrak](https://celestrak.org): каталог объектов, орбитальные элементы,
операторские эфемериды Starlink и прогноз опасных сближений.

Отчёты: [ЛР1](reports/lr1/report.md) (сбор, ODS, сравнение СУБД) ·
[ЛР2](reports/lr2/report.md) (Data Vault, SCD2, Data Quality) ·
[ЛР3](reports/lr3/report.md) (витрины, Superset, OpenMetadata, lineage).

## Архитектура

Основная СУБД — **ClickHouse** (с 17.10.2026; до этого ODS жил в PostgreSQL).
UML-диаграмма развёртывания: [`diagrams/architecture.puml`](diagrams/architecture.puml).

![Архитектура](diagrams/architecture.png)

Путь данных:

```
CelesTrak API ─► ingest_* ─► SeaweedFS (S3)   s3://raw/<source>/dt=YYYY-MM-DD/…   сырые файлы
                        └──► ClickHouse ods.*  снимки (append-only)   ── Asset ods_<source>
                                   │
           vault_satcat / vault_orbit / vault_conjunction                ── Asset vault_<name>
                                   ▼
                    stage.* ─► rv.*  Raw Vault: hubs, links, satellites (история = SCD2)
                                   ▼
           vault_business ─► bv.*  Business Vault: орбитальная геометрия, PIT, SCD2-views
                                   ▼
           data_quality   ─► meta.dq_results  ── Asset dq_passed (только без critical-провалов)
                                   ▼
           build_marts    ─► mart.*  14 витрин (SQL + SGP4)                 ── Asset mart_all
                                   ▼
           publish_lineage ─► OpenMetadata: lineage S3 → ods → stage → rv → bv → mart

Superset (дашборды на mart.*) · globe (3D-глобус, nginx) · OpenMetadata (каталог, lineage, качество)

storage_benchmark: ODS ─► CSV ─► PostgreSQL | MySQL | ClickHouse  (архивные СУБД, профиль archive)
```

| DAG | Что делает | Запуск (UTC) |
|---|---|---|
| `ingest_satcat` | `pub/satcat.csv` — каталог всех объектов (~71 000) | 03:00 ежедневно |
| `ingest_gp` | `gp.php?GROUP=active&FORMAT=json` — орбиты активных спутников (~17 000) | каждые 4 ч |
| `ingest_supgp` | `sup-gp.php?FILE=starlink&FORMAT=json` — эфемериды SpaceX (~11 000) | каждые 8 ч |
| `ingest_socrates` | `SOCRATES/sort-minRange.csv` — прогноз сближений (~196 000) | каждые 12 ч |
| `vault_satcat` | ODS → `hub_object`, `hub_launch`, link, сателлиты каталога | Asset `ods_satcat` |
| `vault_orbit` | ODS → сателлиты орбит GP и SupGP | `ods_gp \| ods_supgp` |
| `vault_conjunction` | ODS → `link_conjunction`, сателлиты прогноза | `ods_socrates` |
| `vault_business` | вычисляемый сателлит орбит, PIT | любой `vault_*` |
| `data_quality` | 15 проверок из `sql/dq/`; при успехе публикует `dq_passed` | `vault_business` |
| `build_marts` | витрины `mart.*` из `sql/mart/` и орбитальные витрины на SGP4 | `dq_passed` |
| `publish_lineage` | рёбра lineage пайплайнов в OpenMetadata | `mart_all`; пропускается без профиля `meta` |
| `storage_benchmark` | сравнение СУБД ЛР1 | вручную, нужен профиль `archive` |

Каждый `ingest_*`: `extract` (один HTTP-запрос → файл в S3, регистрация в `meta.raw_files`)
→ `load_ods` (парсинг, типизация, вставка в ClickHouse, запись в `meta.load_log`)
→ `validate` (полнота снимка; публикует Airflow Asset `ods_<source>`, по нему стартует vault).

Политика CelesTrak соблюдается: при ответе 403 («data has not updated… limit requests to once
every 2 hours») запуск помечается *skipped* без повторов; байт-идентичный файл (по sha256) не
загружается повторно. Повторный запуск любого DAG идемпотентен.

Адреса и учётные данные DAG берут из **Airflow Connections** (`clickhouse_dwh`, `s3_raw`,
`openmetadata`, `postgres_dwh`, `mysql_ods`). `airflow-init` импортирует их при старте из шаблона
[`airflow/config/connections.json`](airflow/config/connections.json), подставляя значения из `.env`.

## Запуск

Нужен Docker Desktop: ядро — ≥ 8 ГБ RAM, всё вместе с `bi` и `meta` — ≥ 12 ГБ.

```bash
cp .env.example .env                       # необязательно: значения по умолчанию уже в compose
docker compose up -d --build               # ядро: Airflow + ClickHouse + SeaweedFS
docker compose --profile bi up -d --build  # + Superset и 3D-глобус (ЛР3)
docker compose --profile meta up -d        # + OpenMetadata (ЛР3)
docker compose --profile archive up -d     # + PostgreSQL и MySQL, только для storage_benchmark
```

| Сервис | Профиль | Адрес | Доступ |
|---|---|---|---|
| Airflow | — | http://localhost:8080 | airflow / airflow |
| ClickHouse (ODS, vault, mart, meta) | — | http://localhost:8123/play | dwh / dwh |
| SeaweedFS filer (сырые файлы) | — | http://localhost:8888 | — |
| SeaweedFS S3 API | — | http://localhost:8333 | bdst / bdst-secret |
| Superset | `bi` | http://localhost:8088 | admin / admin |
| 3D-глобус (встроен в Superset) | `bi` | http://localhost:8090 | — |
| OpenMetadata | `meta` | http://localhost:8585 | admin@open-metadata.org / admin |
| PostgreSQL | `archive` | localhost:5433, db `dwh` | dwh / dwh |
| MySQL | `archive` | localhost:3307, db `ods` | dwh / dwh |

DAG создаются на паузе: включите `ingest_*`, `vault_*`, `data_quality`, `build_marts` и
`publish_lineage` в UI. Дальше всё идёт само: загрузка по расписанию, остальное — по событиям
Assets.

- **Superset.** Сервис `superset-bootstrap` при каждом `up` создаёт подключение, датасеты,
  графики и два дашборда из [`superset/bootstrap.py`](superset/bootstrap.py):
  «Околоземное пространство» и «Космос в 3D». Superset читает ClickHouse под пользователем
  `superset` с правами только на чтение `mart`.
- **OpenMetadata.** Первый старт занимает несколько минут. Затем `om-ingest` раз в час собирает
  метаданные ClickHouse, SeaweedFS и Superset, публикует lineage, применяет описания из
  [`openmetadata/catalog.yaml`](openmetadata/catalog.yaml) и прогоняет тесты качества.
  Разовый прогон: `docker compose --profile meta run --rm om-ingest once`.
  Как устроены OpenMetadata, Elasticsearch и lineage — [docs/openmetadata.md](docs/openmetadata.md).

## Структура

```
docker-compose.yml          все сервисы; профили archive, bi, meta
airflow/                    образ Airflow 3.0.6 + зависимости; config/connections.json — Connections
dags/                       ingest_*, vault_*, data_quality, build_marts, publish_lineage,
                            storage_benchmark (тонкие файлы)
plugins/celestrak/          общий код: sources (схемы), client (HTTP+политика), parsing, s3,
                            targets (3 СУБД), meta, ingest, assets, vault, vault_dag, dq,
                            marts, orbits (SGP4), lineage, config, benchmark, report
sql/ods/<dbms>/             DDL ODS для каждой СУБД (+ meta и пользователь superset в ClickHouse)
sql/vault/                  ddl/ (rv, bv), load/<source>/ (stage → hubs → links → satellites), bv/
sql/dq/                     проверки качества, по одной на файл
sql/mart/                   витрины, по одной на файл (шапка mart / insight / order_by)
sql/migrations/             перенос ODS из PostgreSQL в ClickHouse (17.10.2026)
superset/                   образ Superset 6.1, конфиг, bootstrap.py (дашборды как код)
globe/                      3D-глобус: index.html, chronicle.html, common.js, nginx.conf
openmetadata/               om-ingest: workflows/*.yaml, catalog.yaml + catalog.py, quality.py
diagrams/                   PlantUML: архитектура (UML deployment), модель Data Vault
infra/seaweedfs/s3.json     ключи S3
reports/lr1..lr3/           отчёты
tests/                      unit-тесты парсинга, структуры SQL, витрин, lineage и каталога
```

Тесты (без сети и БД): `python -m pytest tests` в окружении с зависимостями
`airflow/requirements.txt`, или в образе Airflow:
`docker run --rm -v "$PWD:/work" -w /work --entrypoint bash bdst-airflow:3.0.6 -c "pip install -q pytest && python -m pytest tests"`
