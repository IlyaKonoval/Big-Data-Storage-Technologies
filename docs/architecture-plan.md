# План: платформа данных CelesTrak (ЛР1–ЛР3)

## Контекст
По курсу «Технологии хранения больших данных» нужно сделать одну систему, которая собирает, хранит и анализирует данные (сбор → ODS → DDS → витрины → BI + lineage). Источник: CelesTrak, открытое API со спутниковыми данными («экзотический» источник даёт доп. баллы). Репозиторий пока пустой, в нём только `docs/` (задание PDF, `CelesTrak_storage_comparison.md`, `deep-research-report.md`). Всё строим в одном `docker-compose`, профили включают части стека (RAM ≥16 ГБ).

## Схемы
- Архитектура системы: [`diagrams/architecture.puml`](../diagrams/architecture.puml) → ![](../diagrams/architecture.png)
- Модель Data Vault: [`diagrams/data_vault.puml`](../diagrams/data_vault.puml) → ![](../diagrams/data_vault.png)

## Источники → DAG загрузки (ЛР1)
По ТЗ нужно минимум 3 DAG. Берём **4**: каждый отдаёт свою сущность, поэтому DDS получится богатым, а лимиты CelesTrak не нарушаются.

| DAG | Эндпоинт (формат JSON/CSV) | Что это | Расписание |
|---|---|---|---|
| `ingest_satcat` | `celestrak.org/pub/satcat.csv` (или `satcat/records.php`) | Каталог объектов: страна/владелец, запуск, распад, тип, RCS | раз в сутки |
| `ingest_gp` | `NORAD/elements/gp.php?GROUP=active&FORMAT=json` | Орбитальные элементы (OMM), история снимков | каждые 2–4 ч (не чаще обновления источника) |
| `ingest_supgp` | `NORAD/elements/supplemental/sup-gp.php?FILE=starlink&FORMAT=json` | Операторские элементы Starlink (+OneWeb) | каждые 6–8 ч |
| `ingest_socrates` | CSV-выгрузка SOCRATES (top conjunctions) | Сближения пар объектов: TCA, мин. дистанция, вероятность | раз в сутки |

Больше DAG загрузки брать не стоит: каждый новый означает лишние запросы к API и риск получить 403 или бан IP. Дополнительные DAG появятся естественно, это служебные DAG (сравнение, DDS, DQ, витрины, lineage).
**Проверить при реализации:** точные URL и действующую Usage Policy CelesTrak. В deep-research сказано «10 запросов в сутки» и «403 на повтор до обновления»; это нужно сверить с первоисточником.

## Хранилища (ЛР1, п.2 и п.5)
- **Сырые данные:** SeaweedFS (S3 API, Apache 2.0). Бакет `raw/`, ключ `source=<src>/dt=YYYY-MM-DD/<ts>.json|csv`. API вызывается **один раз** за запуск, после этого все загрузчики читают файл из S3.
- **Сравниваем 3 СУБД для ODS:** PostgreSQL 16, MySQL 8 (InnoDB), ClickHouse. Это классический OLTP, второй OLTP и колоночный OLAP. Все три под свободными лицензиями, вывод из `CelesTrak_storage_comparison.md` сохраняется.
- ~~Ожидаемый победитель — PostgreSQL~~. **Решение от 17.10.2026: основная СУБД — ClickHouse** (ODS, `meta`, Data Vault, витрины). Бенчмарк показал, что ClickHouse в 10 раз быстрее на полном скане и хранит данные в 5–8 раз компактнее; история снимков растёт на 10+ млн строк в месяц, а нагрузка аналитическая. Insert-only модель Data Vault ложится на MergeTree естественно. Чего нет (FK, транзакции, UPSERT), компенсируется anti-join загрузкой, ReplacingMergeTree и проверками `data_quality`. Superset и OpenMetadata поддерживают ClickHouse.
- PostgreSQL dwh и MySQL **в архиве**: профиль compose `archive`, тома удалены. Данные перенесены в ClickHouse (`sql/migrations/2026-10-17_postgres_to_clickhouse.sql`), дамп PostgreSQL лежит в `s3://archive/dwh-postgres/dwh_2026-10-17.dump`. Подняв профиль, можно повторить бенчмарк ЛР1 (initdb создаст схемы заново).

### Структура DAG загрузки (одинаковая во всех 4)
`extract (HTTP → S3 raw)` → `load_ods` (только ClickHouse; ODS-таблица `ods.<src>`: поля источника + `load_id`, `loaded_at`, `source_file`; журнал `meta.load_log`) → `validate` → публикация Airflow **Asset** `ods_<src>`.
Общий код вынесен в `plugins/celestrak/` (клиент HTTP с ретраями и учётом 403, S3-хелпер, загрузчики по типу БД), чтобы DAG оставались тонкими.
*Реализовано иначе, чем в первой версии плана:* в регулярной загрузке участвует одна СУБД (с 16.10 — PostgreSQL, с 17.10 — ClickHouse), остальные наполняет `storage_benchmark` из CSV-экспорта ODS. Это экономит ресурсы и гарантирует одинаковые данные во всех трёх СУБД.

### DAG `storage_benchmark`
`export_ods` (ClickHouse → CSV → `s3://exports`) → `bench_postgres` → `bench_mysql` → `bench_clickhouse` (последовательно: bulk-load CSV, 6 запросов, медиана из 5, размер на диске) → `render_report` (PNG + `reports/lr1/results.md`). Сырые замеры в `meta.benchmark_results`.

## DDS (ЛР2), ClickHouse: Data Vault 2.0
*Изменение плана (17.10.2026).* Вместо снежинки DDS строится на **Data Vault 2.0**, по предложению преподавателя («альтернативный трек для экзотических источников» из ТЗ). Звезды нет; витрины ЛР3 строятся прямо из vault через PIT.

Почему DV2 подходит данным CelesTrak:
- естественные бизнес-ключи: NORAD ID (объект) и номер запуска из `object_id` (`1998-067A` → `1998-067`, 6 972 запуска);
- один объект описывают 4 источника → у каждого свой сателлит на общем хабе;
- снимки только дописываются → сателлиты insert-only с `hashdiff` дают SCD2 по построению;
- 319 объектов SOCRATES без записи в SATCAT не ломают модель: для них просто создаётся хаб.

Как закрываются требования ТЗ ЛР2:

| Требование | Решение в DV2 |
|---|---|
| структура, 3НФ | hubs (ключи) / links (связи) / satellites (атрибуты) |
| очистка, типы, обогащение, агрегация | staging (дедуп, нормализация, хэши) + Business Vault (вычисляемые атрибуты, PIT) |
| SCD | сателлиты с `load_dts` + `hashdiff`; view с `valid_from/valid_to/is_current` |
| зависимости | Airflow Assets `ods_*` → `vault_*` → `vault_business` → `data_quality` |
| Data Quality | отдельный DAG, результаты в `meta.dq_results` |

Соглашения: hash key = hex MD5 бизнес-ключа (функция `dv_hash`), составные ключи через `'||'`; `load_dts` = `ods.loaded_at` снимка (повторный прогон идемпотентен); `record_source` = s3-ключ сырого файла.

Особенности ClickHouse: нет FK и уникальных ключей, поэтому хабы и линки пишутся anti-join'ом (`NOT IN`), все таблицы vault — ReplacingMergeTree (страховка от дублей), загрузки разных DAG идут по очереди через pool Airflow `vault` (1 слот), ссылочная целостность проверяется DAG `data_quality`. Сравнение с последней версией сателлита — `argMax(hashdiff, load_dts)`, PIT строится через `ASOF JOIN`.

**Raw Vault (база `rv`; staging — база `stage`)**

| Таблица | Ключ | Источник | Содержимое |
|---|---|---|---|
| `hub_object` | `norad_cat_id` | все 4 | — |
| `hub_launch` | `left(object_id, 8)` | satcat | — |
| `link_object_launch` | объект + запуск | satcat | — |
| `link_conjunction` | пара (min id, max id) | socrates | — |
| `sat_object_satcat_desc` | `hk_object`, `load_dts` | satcat | имя, тип, владелец, orbit_center (меняются редко) |
| `sat_object_satcat_status` | `hk_object`, `load_dts` | satcat | статус, decay_date, rcs, period, apogee, perigee, orbit_type (меняются часто) |
| `sat_launch` | `hk_launch`, `load_dts` | satcat | дата и площадка запуска |
| `sat_object_orbit_gp` / `_supgp` | `hk_object`, `load_dts` | gp / supgp | элементы OMM (+ rms, data_source у supgp) |
| `sat_conjunction` | `hk_conjunction`, `tca`, `load_dts` | socrates | дистанция, скорость, вероятность (multi-active, dependent key `tca`, версия = весь набор TCA пары) |
| `sts_conjunction` | `hk_conjunction`, `load_dts` | socrates | статус пары в прогнозе: `I` появилась / `D` выпала |
| `ref_ops_status`, `ref_object_type` | код | сиды | расшифровка кодов |

**Business Vault (база `bv`)**: computed satellite `sat_object_orbit_derived` (период, высоты перигея/апогея, режим LEO/MEO/GEO/HEO), `pit_object` (по дням: актуальная версия каждого сателлита), SCD2-представления `object_desc_history` / `object_status_history`, текущий прогноз сближений `conjunction_current`.

**DAG** (SQL в `sql/vault/`, общий код `plugins/celestrak/vault.py`):

| DAG | Запуск по Assets | Пишет |
|---|---|---|
| `vault_satcat` | `ods_satcat` | hub_object, hub_launch, link_object_launch, sat_object_satcat_*, sat_launch |
| `vault_orbit` | `ods_gp` или `ods_supgp` | hub_object, sat_object_orbit_gp / _supgp |
| `vault_conjunction` | `ods_socrates` | hub_object, link_conjunction, sat_conjunction |
| `vault_business` | любой `vault_*` | bv.* |
| `data_quality` | `vault_business` | `meta.dq_results` |

Инкрементальность: `meta.vault_log` — какие ODS-снимки уже в vault; новые обрабатываются по порядку `loaded_at`, каждый одной транзакцией.
Проверки DQ: уникальность ключей хабов, линки и сателлиты без хаба, две подряд одинаковые версии сателлита, сверка ODS↔vault, диапазоны (0 ≤ e < 1, inclination 0–180, mean_motion > 0, 0 ≤ max_prob ≤ 1), согласованность запуска (одна дата и площадка), свежесть GP. Critical-провал роняет DAG.

## Витрины и BI (ЛР3), схема `mart`
Подробно — в [отчёте ЛР3](../reports/lr3/report.md). Витрины строятся из Business Vault и Raw Vault (`bv.*`, `rv.*`, `meta.*`), ODS не читают.

| Витрина | Вывод |
|---|---|
| `orbit_population` | что на орбите сейчас по режиму, типу и владельцу; доля мусора и «мёртвых» спутников |
| `catalog_growth` | рост населённости орбиты с 1957 года; разрушения 2007/2009/2021 по году каталогизации |
| `launch_activity`, `launch_sites` | темп запусков, доля Starlink, космодромы |
| `altitude_shells` | плотность объектов по высоте × наклонению (оболочки Starlink, SSO) |
| `conjunction_risk`, `conjunction_geo` | опасные сближения прогноза SOCRATES и их положение над Землёй |
| `gp_vs_supgp` | расхождение NORAD (GP) и оператора (SupGP) по Starlink |
| `chronicle`, `orbit_elements`, `ground_tracks`, `launch_arcs` | данные 3D-глобуса: хроника 1957–2026, TLE, трассы, дуги космодром → спутник |
| `pipeline_health`, `dq_checks` | свежесть источников и итог DQ |

**ETL.** DAG `build_marts` запускается по Asset `dq_passed`: его публикует `data_quality` только без critical-провалов, поэтому витрины строятся из проверенных данных. Каждая витрина пересобирается в `mart.<name>__new` и подменяет живую через `EXCHANGE TABLES`; журнал — `meta.mart_log`. Орбитальные витрины считает Python (SGP4 по элементам GP/SupGP из vault).

**BI.** Superset 6.1 (профиль `bi`, метаданные — база `superset` в `airflow-postgres`); дашборды создаёт `superset/bootstrap.py` через REST API. «Околоземное пространство» — 13 графиков 10 типов на трёх вкладках; «Космос в 3D» — страницы сервиса `globe` (nginx + globe.gl + satellite.js) в iframe.

**OpenMetadata** 1.13.6 (профиль `meta`, Elasticsearch 9.3, метаданные — база `openmetadata_db` в `airflow-postgres`). Контейнер `om-ingest` раз в час: ingestion ClickHouse, SeaweedFS и Superset; lineage пайплайнов; описания, глоссарий, домен, tiers и KPI из `openmetadata/catalog.yaml`; 226 тестов качества на всех 48 таблицах. Lineage `S3 raw → ods → stage → rv → bv → mart → Superset` (99 рёбер) выводится из нашего SQL (`plugins/celestrak/lineage.py`: цели `INSERT INTO` / `CREATE`, источники `FROM` / `JOIN`) и публикуется через REST API DAG-ом `publish_lineage`.

*Изменения относительно плана.*
- `build_marts` запускается по `dq_passed`, а не по `vault_business`: витрины не строятся из непроверенных данных.
- Вместо `mart_decay_watch` и `mart_constellation_growth` сделаны `catalog_growth` (история с 1957 года по SATCAT: собственная история снимков слишком короткая) и витрины 3D-глобуса.
- Lineage не из `system.query_log`, а из SQL-файлов проекта: query_log не знает о шагах Python (S3 → ODS, SGP4) и не привязывает рёбра к DAG. Вместо Python SDK — REST API (`requests`): SDK OpenMetadata конфликтует по зависимостям с Airflow.
- Elasticsearch вместо OpenSearch: OpenMetadata 1.13 поддерживает оба, Elasticsearch — движок по умолчанию в её docker-compose.
- Подключения DAG перенесены из переменных окружения в Airflow Connections (`airflow/config/connections.json`).

## Структура репозитория
```
docker-compose.yml        # без профиля: airflow + clickhouse + seaweedfs; profiles: archive (postgres dwh, mysql), bi (superset, globe), meta (openmetadata + elasticsearch + om-ingest)
.env.example
airflow/{Dockerfile,requirements.txt}
dags/{ingest_*.py, storage_benchmark.py, vault_*.py, data_quality.py, build_marts.py, publish_lineage.py}
plugins/celestrak/{client.py, s3.py, targets.py, ingest.py, vault.py, dq.py, ...}
sql/{ods/<dbms>/, vault/{ddl,load/<source>,bv}/, dq/, mart/, migrations/}   # DDL + трансформации
superset/  globe/  openmetadata/   # дашборды как код, 3D-глобус, каталог и ingestion
reports/{lr1,lr2,lr3}/report.md
```
Версии образов закрепляем точно (Airflow 3.x, Superset, OpenMetadata). Перед стартом проверить совместимость OpenMetadata с выбранной версией Airflow; ingestion OpenMetadata запускаем в его собственном контейнере, а не в нашем Airflow.

## Порядок работ
1. compose `core` + Airflow + SeaweedFS + PG; `plugins/celestrak` (client, s3) → `ingest_satcat` end-to-end.
2. Остальные 3 DAG загрузки; MySQL и ClickHouse с загрузчиками; `storage_benchmark` и отчёт ЛР1.
3. Data Vault 2.0: staging, raw vault, business vault, DAG `vault_*` на Assets, `data_quality` и отчёт ЛР2.
4. Витрины, Superset-дашборд, OpenMetadata + lineage и отчёт ЛР3.

## Проверка
- `docker compose up -d`, в UI Airflow (localhost:8080) все DAG без import errors; ручной trigger `ingest_*` → файл в S3 (`aws s3 ls --endpoint-url http://localhost:8333`), строки в ODS ClickHouse; за ним автоматически `vault_*` → `vault_business` → `data_quality`.
- Повторный запуск идемпотентен: дубли в vault не появляются, сателлит получает новую версию только у изменившихся объектов (проверяем на подменённом снимке SATCAT).
- `data_quality` зелёный на реальных данных и красный на специально испорченной записи.
- Superset: дашборд открывается на `mart.*`. OpenMetadata: lineage показывает полный путь от S3 до витрин.
- pytest для `plugins/celestrak` (мок HTTP, разбор JSON/CSV).
