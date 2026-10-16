# Big Data Storage Technologies — платформа данных CelesTrak

Система сбора, хранения и анализа открытых данных о космических объектах
[CelesTrak](https://celestrak.org): каталог объектов, орбитальные элементы,
операторские эфемериды Starlink и прогноз опасных сближений.

| ЛР | Содержание | Статус |
|---|---|---|
| 1 | Airflow, ETL в слой ODS, сравнение трёх хранилищ | ✅ |
| 2 | DDS (3НФ, SCD2), зависимости DAG, Data Quality | — |
| 3 | Витрины, Superset, OpenMetadata lineage | — |

Предметная область (термины, поля, коды, сущности, особенности данных) —
[docs/domain.md](docs/domain.md). План архитектуры — [docs/architecture-plan.md](docs/architecture-plan.md),
отчёт ЛР1 — [reports/lr1/report.md](reports/lr1/report.md).

## Архитектура (ЛР1)

```
CelesTrak API ──► Airflow ingest_* DAGs ──► SeaweedFS (S3)  s3://raw/<source>/dt=YYYY-MM-DD/…   сырые файлы
                                       └──► PostgreSQL       ods.<source>                         снимки (append-only)
                                                 │
                       storage_benchmark DAG ────┤ CSV-экспорт ODS ─► s3://exports/<run>/
                                                 ├──► MySQL       ods.<source>
                                                 └──► ClickHouse  ods.<source>   + графики в reports/lr1/
```

| DAG | Источник | Расписание (UTC) | Строк за загрузку |
|---|---|---|---|
| `ingest_satcat` | `pub/satcat.csv` — каталог всех объектов | 03:00 ежедневно | ~71 000 |
| `ingest_gp` | `gp.php?GROUP=active&FORMAT=json` — орбиты активных спутников | каждые 4 ч | ~17 000 |
| `ingest_supgp` | `sup-gp.php?FILE=starlink&FORMAT=json` — эфемериды SpaceX | каждые 8 ч | ~10 000 |
| `ingest_socrates` | `SOCRATES/sort-minRange.csv` — прогноз сближений | каждые 12 ч | ~196 000 |
| `storage_benchmark` | ODS → CSV → PostgreSQL / MySQL / ClickHouse | вручную | — |

Каждый `ingest_*`: `extract` (один HTTP-запрос → файл в S3, регистрация в `meta.raw_files`)
→ `load_ods` (парсинг, типизация, `COPY` в PostgreSQL, запись в `meta.load_log`)
→ `validate` (полнота снимка; публикует Airflow Asset `ods_<source>` для DAG слоя DDS).

Политика CelesTrak соблюдается: при ответе 403 («data has not updated… limit requests to once
every 2 hours») запуск помечается *skipped* без повторов; байт-идентичный файл (по sha256) не
загружается повторно. Повторный запуск того же `load_id` идемпотентен.

## Запуск

Нужен Docker Desktop (≥ 8 ГБ RAM для ЛР1).

```bash
cp .env.example .env                     # необязательно: значения по умолчанию уже в compose
docker compose up -d --build             # Airflow + SeaweedFS + PostgreSQL
docker compose --profile compare up -d   # + MySQL и ClickHouse (только для сравнения)
```

| Сервис | Адрес | Доступ |
|---|---|---|
| Airflow | http://localhost:8080 | airflow / airflow |
| SeaweedFS filer (сырые файлы) | http://localhost:8888 | — |
| SeaweedFS S3 API | http://localhost:8333 | bdst / bdst-secret |
| PostgreSQL (ODS, meta) | localhost:5433, db `dwh` | dwh / dwh |
| MySQL | localhost:3307, db `ods` | dwh / dwh |
| ClickHouse | http://localhost:8123/play | dwh / dwh |

DAG создаются на паузе: включите `ingest_*` в UI (первый запуск произойдёт сразу),
затем — при поднятом профиле `compare` — запустите `storage_benchmark`.
Результаты: `reports/lr1/results.md` и PNG-графики рядом.

## Структура

```
docker-compose.yml          все сервисы; MySQL/ClickHouse в профиле compare
airflow/                    образ Airflow 3.0.6 + зависимости
dags/                       ingest_*.py (тонкие), storage_benchmark.py
plugins/celestrak/          общий код: sources (схемы), client (HTTP+политика), parsing,
                            s3, targets (3 СУБД), meta, ingest (фабрика DAG), benchmark, report
sql/ods/<dbms>/             DDL ODS для каждой СУБД (+ meta-схема в PostgreSQL)
infra/seaweedfs/s3.json     ключи S3
reports/lr1/                отчёт и результаты сравнения
tests/                      unit-тесты парсинга (без сети и БД)
```

Тесты: `docker run --rm -v "$PWD:/work" -w /work --entrypoint bash bdst-airflow:3.0.6 -c "pip install -q pytest && python -m pytest tests"`
