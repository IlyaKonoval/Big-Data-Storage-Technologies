# Big Data Storage Technologies — платформа данных CelesTrak

Система сбора, хранения и анализа открытых данных о космических объектах
[CelesTrak](https://celestrak.org): каталог объектов, орбитальные элементы,
операторские эфемериды Starlink и прогноз опасных сближений.



## Архитектура

Основная СУБД — **ClickHouse** (с 17.10.2026; до этого ODS жил в PostgreSQL).

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
           data_quality   ─► meta.dq_results  (critical-провал роняет DAG)

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
| `data_quality` | 15 проверок из `sql/dq/` | `vault_business` |
| `storage_benchmark` | сравнение СУБД ЛР1 | вручную, нужен профиль `archive` |

Каждый `ingest_*`: `extract` (один HTTP-запрос → файл в S3, регистрация в `meta.raw_files`)
→ `load_ods` (парсинг, типизация, вставка в ClickHouse, запись в `meta.load_log`)
→ `validate` (полнота снимка; публикует Airflow Asset `ods_<source>`, по нему стартует vault).

Политика CelesTrak соблюдается: при ответе 403 («data has not updated… limit requests to once
every 2 hours») запуск помечается *skipped* без повторов; байт-идентичный файл (по sha256) не
загружается повторно. Повторный запуск любого DAG идемпотентен.

## Запуск

Нужен Docker Desktop (≥ 8 ГБ RAM).

```bash
cp .env.example .env                     # необязательно: значения по умолчанию уже в compose
docker compose up -d --build             # Airflow + ClickHouse + SeaweedFS
docker compose --profile archive up -d   # + PostgreSQL и MySQL, только для storage_benchmark
```

| Сервис | Адрес | Доступ |
|---|---|---|
| Airflow | http://localhost:8080 | airflow / airflow |
| ClickHouse (ODS, vault, meta) | http://localhost:8123/play | dwh / dwh |
| SeaweedFS filer (сырые файлы) | http://localhost:8888 | — |
| SeaweedFS S3 API | http://localhost:8333 | bdst / bdst-secret |
| PostgreSQL *(archive)* | localhost:5433, db `dwh` | dwh / dwh |
| MySQL *(archive)* | localhost:3307, db `ods` | dwh / dwh |

DAG создаются на паузе: включите `ingest_*`, `vault_*` и `data_quality` в UI. Дальше всё идёт
само: загрузка по расписанию, vault и проверки — по событиям Assets.

## Структура

```
docker-compose.yml          все сервисы; PostgreSQL/MySQL в профиле archive
airflow/                    образ Airflow 3.0.6 + зависимости
dags/                       ingest_*, vault_*, data_quality, storage_benchmark (тонкие файлы)
plugins/celestrak/          общий код: sources (схемы), client (HTTP+политика), parsing, s3,
                            targets (3 СУБД), meta, ingest, assets, vault, vault_dag, dq,
                            benchmark, report
sql/ods/<dbms>/             DDL ODS для каждой СУБД (+ meta в ClickHouse)
sql/vault/                  ddl/ (rv, bv), load/<source>/ (stage → hubs → links → satellites), bv/
sql/dq/                     проверки качества, по одной на файл
sql/migrations/             перенос ODS из PostgreSQL в ClickHouse (17.10.2026)
infra/seaweedfs/s3.json     ключи S3
reports/lr1/, reports/lr2/  отчёты
tests/                      unit-тесты парсинга и структуры SQL (без сети и БД)
```

Тесты: `docker run --rm -v "$PWD:/work" -w /work --entrypoint bash bdst-airflow:3.0.6 -c "pip install -q pytest && python -m pytest tests"`
