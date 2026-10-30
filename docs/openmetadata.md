# OpenMetadata в проекте: как устроено, где что хранится, как строится lineage

Гайд описывает именно наш стенд (OpenMetadata 1.13.6, профиль `meta`): какие контейнеры
работают, что хранится в Postgres и что в Elasticsearch, как метаданные попадают в каталог
и как получается граф lineage от S3 до дашбордов Superset. Цифры сняты 30.10.2026.

## 1. Коротко

OpenMetadata — каталог данных. Сам он данные не хранит и не обрабатывает: он хранит
**описания** данных (таблицы, колонки, дашборды, пайплайны), **связи** между ними (lineage,
владельцы, теги) и **результаты проверок** качества. Всё это попадает в каталог извне:
ingestion-воркфлоу читают метаданные из источников, а наш код дописывает lineage, описания
и тесты через REST API.

```
                         ┌───────────────── профиль meta ─────────────────┐
 ClickHouse ─┐           │                                                │
 SeaweedFS  ─┼─ читает ──┤  om-ingest ──REST──► openmetadata-server :8585  │◄── браузер (UI)
 Superset DB ┘           │  (раз в час)          │            │           │
                         │                       ▼            ▼           │
 Airflow: publish_lineage ──────REST───────►  Postgres    Elasticsearch   │
                         │                 openmetadata_db  om-elasticsearch
                         └────────────────────────────────────────────────┘
```

- **Postgres** (`openmetadata_db` в `airflow-postgres`) — источник истины: все сущности и связи.
- **Elasticsearch** (`om-elasticsearch`) — производная копия для поиска, графа lineage в UI
  и аналитики Data Insights. Её можно стереть и перестроить из Postgres.
- **openmetadata-server** — Java-приложение с REST API и веб-интерфейсом. Всё, и UI, и наш
  код, и ingestion, работает только через его API; в базы напрямую никто не пишет.
- **om-ingest** — наш контейнер, который раз в час наполняет каталог.

## 2. Контейнеры и порядок запуска

Все сервисы в профиле `meta` (`docker compose --profile meta up -d`):

| Контейнер | Образ | Что делает | Живёт |
|---|---|---|---|
| `om-db-init` | `postgres:16.4` | создаёт базу `openmetadata_db` в `airflow-postgres`, если её нет | одноразовый |
| `om-elasticsearch` | `elasticsearch:9.3.0` | поисковый движок; single-node, без security, heap 768 МБ | всегда |
| `om-migrate` | `openmetadata/server:1.13.6` | `openmetadata-ops.sh migrate`: создаёт и обновляет схему Postgres (176 таблиц) и индексы ES | одноразовый |
| `openmetadata-server` | `openmetadata/server:1.13.6` | сервер: REST API и UI на :8585, heap 1 ГБ | всегда |
| `om-ingest` | `openmetadata/ingestion-base:1.13.6` | `python ingest.py loop` — наш цикл наполнения каталога | всегда |

Порядок задают `depends_on`: `om-db-init` (завершился) и `om-elasticsearch` (healthy) →
`om-migrate` (завершился) → `openmetadata-server` (healthy) → `om-ingest`. Первый старт
занимает несколько минут: в основном миграции и разогрев JVM.

Настройки сервера — переменные окружения в `docker-compose.yml` (якорь `&om-env`):
- `DB_*`, `OM_DATABASE` — где Postgres;
- `SEARCH_TYPE: elasticsearch`, `ELASTICSEARCH_HOST/PORT` — где поиск;
- `PIPELINE_SERVICE_CLIENT_ENABLED: "false"` — у OpenMetadata обычно есть свой Airflow для
  ingestion. Мы его не поднимаем: ingestion выполняет `om-ingest`, а кнопки «запустить
  ingestion» в UI поэтому не работают — это ожидаемо.

Отдельного Postgres для OpenMetadata нет: используется сервер `airflow-postgres`, база
`openmetadata_db` (рядом с базами `airflow` и `superset`).

## 3. Что хранится в Postgres

База `openmetadata_db`, 176 таблиц. Устроены они одинаково: **одна таблица на тип
сущности**, сама сущность лежит JSON-документом в колонке `json`.

| Таблица | Что в ней |
|---|---|
| `table_entity` | таблицы и представления ClickHouse (48), с колонками, описаниями, тегами |
| `storage_container_entity` | бакет `raw` в SeaweedFS |
| `dashboard_entity`, `chart_entity`, `dashboard_data_model_entity` | дашборды, графики и датасеты Superset |
| `pipeline_entity` | 10 пайплайнов — наши DAG |
| `glossary_term_entity`, `tag_usage` | глоссарий и привязка тегов/терминов к сущностям |
| `test_case`, `data_quality_data_time_series` | тесты качества и история их результатов |
| `kpi_entity` | KPI |
| **`entity_relationship`** | **все связи между сущностями, в том числе lineage** |

`entity_relationship` — граф всего каталога: строка = (`fromid`, `fromentity`, `toid`,
`toentity`, `relation`, `json`). Код `relation` задаёт тип связи:

| `relation` | Смысл | Пример у нас |
|---|---|---|
| 0 | CONTAINS | схема содержит таблицу (48), набор тестов содержит тест (226) |
| 8 | OWNS | команда «Платформа данных» владеет графиком |
| 10 | HAS | домен включает таблицу, дашборд включает график |
| **13** | **UPSTREAM** | **ребро lineage: `fromid` — источник, `toid` — приёмник** |

Посмотреть рёбра lineage прямо в базе:
```sql
-- docker compose exec airflow-postgres psql -U airflow -d openmetadata_db
SELECT fromentity, toentity, count(*), max(json->>'source') AS source
FROM entity_relationship WHERE relation = 13 GROUP BY 1, 2;
```
```
 fromentity         | toentity           | count | source
 table              | table              |    95 | PipelineLineage / ViewLineage
 container          | table              |     4 | PipelineLineage
 table              | dashboardDataModel |     6 | DashboardLineage
 dashboardDataModel | dashboard          |     6 | DashboardLineage
```
95 + 4 = 99 рёбер публикует наш код (§6), 12 — ingestion Superset (§5.3).

## 4. Зачем Elasticsearch и что в нём лежит

Postgres умеет хранить, но не умеет быстро искать по тексту и обходить граф на несколько
шагов с фильтрами. Поэтому сервер при **каждой записи** сущности синхронно обновляет её
документ в Elasticsearch. ES — денормализованная копия, оптимизированная под чтение.

| Индексы | Для чего |
|---|---|
| `table_search_index`, `dashboard_search_index`, `chart_search_index`, `pipeline_search_index`, `container_search_index`, `glossary_term_search_index`, … | строка поиска в UI, фильтры (домен, владелец, tier, теги), страницы Explore |
| `column_search_index` | поиск по колонкам (1 074 документа) |
| `test_case_search_index`, `test_case_result_search_index` | страница Data Quality: список тестов, статусы, история (≈9.7 тыс. результатов) |
| `.ds-di-data-assets-*` (data streams) | ежедневные снимки состояния каталога для Data Insights и KPI |
| алиасы `all`, `dataAsset` | поиск сразу по всем типам |

**Lineage в ES.** Каждое ребро дублируется в документ сущности-приёмника, в поле
`upstreamLineage`: от кого ребро, через какой пайплайн, с каким SQL. Граф в UI сервер
строит именно по этим полям: UI вызывает `GET /api/v1/lineage/getLineage?fqn=…&upstreamDepth=…`,
и сервер обходит граф в ES. Отсюда ограничение: не больше 3 шагов в каждую сторону.

Проверить:
```bash
# индексы
docker compose exec om-elasticsearch curl -s "localhost:9200/_cat/indices?v&s=docs.count:desc"
# входящие рёбра витрины conjunction_risk
docker compose exec om-elasticsearch curl -s \
  "localhost:9200/table_search_index/_search?q=fullyQualifiedName:%22clickhouse.default.mart.conjunction_risk%22&_source=upstreamLineage"
```

**Если ES потерян или рассинхронизирован** (стёрли том `om-es-data`, поиск не находит
сущность), ничего не пропадает: приложение `SearchIndexingApplication` перестраивает все
индексы из Postgres. Оно запускается по расписанию (воскресенье 00:30 UTC) или вручную:
Settings → Applications → Search Indexing → Run.

## 5. Как метаданные попадают в каталог: `om-ingest`

Код: [`openmetadata/ingest.py`](../openmetadata/ingest.py). Режим `loop` повторяет раунд раз в
`OM_INGEST_INTERVAL` секунд (по умолчанию 3600); `once` делает один раунд и выходит
(`docker compose --profile meta run --rm om-ingest once` — перед демонстрацией).

Один раунд:

| # | Шаг | Код | Результат |
|---|---|---|---|
| 0 | вход | `lineage.Client()` | логин admin (`POST /v1/users/login`, пароль в base64) → JWT; затем JWT бота `ingestion-bot` для воркфлоу (`OM_JWT`) |
| 1 | ingestion | `workflows/*.yaml` через `MetadataWorkflow` | сервисы, таблицы, бакет, дашборды, графики, датасеты; lineage Superset |
| 2 | lineage | `celestrak.lineage.publish` | 10 пайплайнов и 99 рёбер (§6) |
| 3 | каталог | `catalog.py` + `catalog.yaml` | описания, владелец, слои, tiers, глоссарий, домен, дата-продукт, KPI |
| 4 | качество | `quality.py` | 226 тестов на 48 таблицах, прогон TestSuite |
| 5 | KPI | `POST /v1/apps/trigger/DataInsightsApplication` | пересчёт Data Insights |

Все шаги идемпотентны: повторный раунд ничего не дублирует, а только приводит каталог в
соответствие с источниками и с `catalog.yaml`.

### 5.1 Ingestion-воркфлоу

Воркфлоу — YAML с тремя частями: `source` (откуда читать), `sink` (`metadata-rest` — отправить
в сервер через REST API) и `workflowConfig` (адрес сервера и JWT). Значения вида `${CH_USER}`
подставляются из окружения контейнера. Пакет `openmetadata-ingestion` (он внутри образа
`ingestion-base`) содержит коннекторы ко всем трём источникам.

| Файл | Источник | Как читает | Что создаёт |
|---|---|---|---|
| `10_clickhouse.yaml` | ClickHouse :8123 | SQLAlchemy-диалект `clickhouse+http`: список баз, таблиц, представлений, колонок и типов | сервис `clickhouse`, схемы `ods`, `stage`, `rv`, `bv`, `mart`, `meta` и их таблицы; `mart.*__new` исключены фильтром; удалённые таблицы помечаются удалёнными |
| `20_s3.yaml` | SeaweedFS :8333 | S3 API (`ListBuckets`), фильтр `^raw$` | сервис `seaweedfs`, контейнер `raw` |
| `30_superset.yaml` | **база Superset в Postgres**, а не его API | таблицы метаданных Superset: дашборды, графики, датасеты | сервис `superset`, 2 дашборда, 13 графиков, 6 датасетов; lineage витрина → датасет → дашборд |

### 5.2 Имена сущностей (FQN)

Каждая сущность адресуется полным именем `сервис.база.схема.таблица`:

| Сущность | FQN |
|---|---|
| таблица ClickHouse | `clickhouse.default.mart.conjunction_risk` — у ClickHouse нет уровня каталога, поэтому база называется `default`, а базы ClickHouse становятся схемами |
| бакет | `seaweedfs.raw` |
| датасет Superset | `superset.model.6` (id датасета в Superset) |
| пайплайн | `airflow.build_marts` |

По FQN наш код находит сущности (`GET /v1/tables/name/<fqn>`), чтобы связать их рёбрами.

### 5.3 Lineage от Superset

Коннектор Superset видит, что датасет построен на таблице `mart.<имя>` базы «ClickHouse
marts», и ищет такую таблицу в каталоге по префиксу `dbServicePrefixes:
["clickhouse.default.mart"]`. Найдя, создаёт рёбра `таблица → датасет` и `датасет → дашборд`
(6 + 6). Графики не становятся узлами графа: они входят в дашборд (связь HAS). Без
префикса коннектор не знает, какому сервису в каталоге соответствует база Superset, и
lineage не появляется.

## 6. Lineage конвейера: как реализован

Код: [`plugins/celestrak/lineage.py`](../plugins/celestrak/lineage.py). Он вызывается в двух
местах: DAG `publish_lineage` (после каждой сборки витрин, Asset `mart_all`) и шаг 2 в
`om-ingest`. Код один и тот же; использует только `requests` и стандартную библиотеку,
поэтому работает и в Airflow, и в контейнере ingestion.

### 6.1 Почему не автоматический lineage из ClickHouse

У OpenMetadata есть коннектор, который строит lineage по журналу запросов `system.query_log`.
Нам он не подходит:
- витрины собираются в `mart.<name>__new` и подменяются `EXCHANGE TABLES`: в журнале видна
  только временная таблица, а не настоящая витрина;
- шаги на Python (загрузка S3 → ODS, витрины SGP4) не оставляют `INSERT … SELECT` в журнале;
- журнал не знает, какой DAG выполнил запрос, а ребро хочется привязать к пайплайну.

Поэтому рёбра выводятся **из нашего кода**: все SQL лежат в репозитории, и из каждого
оператора видно, куда он пишет и откуда читает.

### 6.2 Шаг 1: перечислить шаги конвейера — `all_steps()`

Шаг (`Step`) = цель, источники, DAG, SQL. Список собирается из файлов проекта:

| Откуда | Шаги | DAG |
|---|---|---|
| код (`SOURCES`) | `s3:raw` → `ods.<source>` для gp, supgp, satcat, socrates | `ingest_<source>` |
| `sql/vault/load/<source>/*.sql` | `ods` → `stage` → `rv` | `vault_orbit`, `vault_satcat`, `vault_conjunction` (`VAULT_DAGS`) |
| `sql/vault/ddl/*.sql` | представления `bv.*` (SCD2 и др.) | — (ребро типа ViewLineage) |
| `sql/vault/bv/*.sql` | `rv` → `bv` | `vault_business` |
| `sql/dq/*.sql` | проверяемые таблицы → `meta.dq_results` | `data_quality` |
| `sql/mart/*.sql` | `bv`/`rv`/`meta` → `mart.<имя из шапки -- mart:>` | `build_marts` |
| код (`ORBIT_MARTS`) | витрины SGP4: `bv.object_current`, `rv.sat_object_orbit_gp`, … → `mart.*` | `build_marts` |

### 6.3 Шаг 2: разобрать SQL — `statements()` и `parse()`

1. `statements` удаляет комментарии `-- …` и делит файл на операторы по `;`.
2. `parse` применяет к оператору два регулярных выражения:
   - цель — имя после `INSERT INTO`, `CREATE TABLE [IF NOT EXISTS]` или
     `CREATE [OR REPLACE] VIEW`;
   - источники — все имена `схема.таблица` после `FROM` и `JOIN`, без повторов и без самой цели.
3. Файл витрины — это голый `SELECT` без `INSERT`: цель берётся из шапки `-- mart: <имя>`.

Пример — `sql/vault/load/socrates/30_sat_conjunction.sql`:
```sql
INSERT INTO rv.sat_conjunction (...)          -- цель:      rv.sat_conjunction
SELECT ... FROM stage.socrates AS s ...       -- источник:  stage.socrates
```
→ ребро `stage.socrates → rv.sat_conjunction`, DAG `vault_conjunction`, SQL оператора.

Разбор опирается на соглашение проекта: таблицы всегда пишутся с именем базы
(`stage.socrates`, а не `socrates`). Его проверяют тесты `tests/test_lineage.py`: у каждого
SQL-файла есть цель и источники, цепочка от каждой витрины доходит до `s3:raw`, ни одно
ребро не ведёт к `__new`.

### 6.4 Шаг 3: собрать рёбра — `edges()`

Каждый шаг даёт по ребру на каждый источник. Одинаковые пары (источник, цель) из разных
операторов схлопываются, у ребра остаётся SQL первого. Итого 99 рёбер:

| Слои | Рёбер | Чем создаются |
|---|---:|---|
| `s3:raw` → `ods` | 4 | `ingest_*` |
| `ods` → `stage` | 4 | `vault_*` |
| `stage` → `rv` | 14 | `vault_*` |
| `rv` → `bv` | 7 + 18 | `vault_business` + представления |
| `bv` → `bv` | 2 | представления |
| `rv`/`bv`/`meta`/`mart` → `mart` | 28 | `build_marts` |
| `ods`/`stage`/`rv`/`bv`/`meta` → `meta.dq_results` | 22 | `data_quality` |

### 6.5 Шаг 4: опубликовать — `publish()`

1. `ensure_pipelines` создаёт (или обновляет — `PUT` идемпотентен) сервис пайплайнов
   `airflow` и по пайплайну на каждый DAG, с русским названием, описанием и ссылкой на DAG
   в Airflow UI (`PIPELINES`).
2. Для каждого ребра по FQN находятся обе сущности (`ref()`, с кешем): таблица
   `clickhouse.default.<схема>.<таблица>` или контейнер `seaweedfs.raw`.
3. Отправляется
   ```
   PUT /api/v1/lineage
   {"edge": {"fromEntity": {"id": …, "type": "table"},
             "toEntity":   {"id": …, "type": "table"},
             "lineageDetails": {"source": "PipelineLineage",      // или ViewLineage
                                "sqlQuery": "INSERT INTO …",
                                "pipeline": {"id": …, "type": "pipeline"}}}}
   ```
   Сервер пишет строку `relation = 13` в `entity_relationship` и обновляет `upstreamLineage`
   в документе приёмника в ES. Повторный `PUT` того же ребра обновляет его, а не дублирует.
4. Если сущности ещё нет в каталоге (таблица создана, но ingestion ClickHouse её ещё не
   видел), ребро пропускается и имя попадает в `missing`. Следующий раунд `om-ingest` сначала
   занесёт таблицу, потом опубликует ребро.

### 6.6 Что видно в UI

Таблица → вкладка **Lineage**. На ребре — значок пайплайна; клик по ребру показывает DAG и
SQL, которым получена таблица. Глубина просмотра — 3 шага в каждую сторону (настраивается в
`catalog.yaml`, `lineage_depth`; больше сервер не разрешает). Чтобы увидеть всю цепочку
сразу, откройте таблицу из середины, например `rv.sat_conjunction`:

```
seaweedfs.raw → ods.socrates → stage.socrates → rv.sat_conjunction → bv.conjunction_current
             → mart.conjunction_risk → датасет Superset → дашборд
```

Lineage у нас **табличный**: связи колонка → колонка не строятся (для этого нужен полный
SQL-парсер, регулярных выражений недостаточно).

## 7. Каталог как код, качество и KPI

**`catalog.yaml` + `catalog.py`.** Всё, что в каталоге написано руками, лежит в YAML:
описания 48 таблиц и 98 колонок, сервисов, схем, дашбордов, графиков; команда-владелец
«Платформа данных»; классификация `Layer` (слой по схеме) и Tier; глоссарий `SpaceObjects`
(12 терминов, привязаны к колонкам); домен «Околоземное пространство» и дата-продукт; KPI.
`catalog.py` читает сущность, сравнивает с YAML и отправляет JSON Patch только с отличиями
(описания сравниваются после `html.unescape`: сервер хранит их экранированными). Повторный
запуск — 0 правок. Править описание — значит править YAML, а не UI.

**Тесты качества — `quality.py`.** Тест в OpenMetadata — `testCase` типа из библиотеки
(`tableRowCountToBeBetween`, `columnValuesToBeNotNull`, …) с параметрами. `quality.py`
создаёт их и запускает воркфлоу TestSuite (`orm-test-runner`), который выполняет проверки
**в ClickHouse** и отправляет результаты на сервер. 15 проверок `sql/dq` стали тестами
`tableCustomSQLQuery`: SQL возвращает строки-нарушения, тест падает, если их больше 0.
Результаты: история в `data_quality_data_time_series` (Postgres), страница Data Quality — из ES.

**KPI и Data Insights.** KPI — цель на долю ресурсов с описанием (95 %) и с владельцем
(100 %). Считает их приложение `DataInsightsApplication`: обходит каталог, пишет ежедневные
снимки в data streams `di-data-assets-*` в ES, и по ним считается процент. По расписанию оно
работает раз в сутки (03:00 UTC); `om-ingest` запускает его после каждого раунда, чтобы KPI
сразу отражали правки.

Встроенные приложения сервера (Settings → Applications):

| Приложение | Расписание (UTC) | Что делает |
|---|---|---|
| `SearchIndexingApplication` | вс 00:30 | перестраивает индексы ES из Postgres |
| `DataInsightsApplication` | ежедневно 03:00 | снимки каталога для Data Insights и KPI |
| `DataRetentionApplication` | вс 00:00 | чистит старые события и результаты |
| `DataContractValidationApplication` | ежедневно 00:00 | проверка контрактов данных (у нас их нет) |

## 8. Частые вопросы и проблемы

| Ситуация | Причина и что делать |
|---|---|
| `openmetadata-server` не стартует | смотреть `docker compose logs om-migrate om-elasticsearch`; сервер ждёт healthy ES и завершённых миграций |
| новая таблица или витрина не видна | её ещё не видел ingestion ClickHouse: `docker compose --profile meta run --rm om-ingest once` |
| на новой таблице нет рёбер lineage | ребро пропущено как `missing` до появления таблицы; тот же разовый прогон |
| поиск не находит то, что есть | рассинхрон ES: Search Indexing → Run |
| граф обрезан | глубина 3 шага — предел сервера; открыть узел из середины цепочки |
| кнопка «Run ingestion» в UI не работает | так задумано: `PIPELINE_SERVICE_CLIENT_ENABLED=false`, ingestion выполняет `om-ingest` |
| KPI не обновились | Data Insights считает с задержкой; Insights → KPIs через пару минут после раунда |
| начать каталог с нуля | `docker compose --profile meta down`, удалить том `om-es-data` и базу `openmetadata_db`, поднять снова — `om-migrate` создаст схему, `om-ingest` наполнит всё заново |

Логи раунда: `docker compose logs -f om-ingest` (строки `om-ingest`, `om-catalog`,
`om-quality`, `celestrak.lineage`: сколько рёбер опубликовано, что пропущено, сколько тестов
упало).
