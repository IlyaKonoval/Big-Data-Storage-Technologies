# Как устроены DAG и разбор API CelesTrak

Документ описывает реализацию: как устроены DAG, как скачивается и разбирается ответ API,
как данные попадают в ODS (ЛР1) и дальше в Data Vault (ЛР2). С 17.10.2026 основная СУБД —
**ClickHouse** (ODS, `meta`, vault); PostgreSQL и MySQL в архиве и нужны только бенчмарку. Предметная область (поля, коды, сущности) —
в [domain.md](domain.md), архитектура и план — в [architecture-plan.md](architecture-plan.md).

## Общая идея

DAG-файлы почти пустые. Вся логика лежит в пакете `plugins/celestrak/`, а каждый DAG —
это вызов **фабрики** с параметрами своего источника. Четыре DAG загрузки устроены
одинаково и различаются только описанием источника.

```
dags/ingest_gp.py ──► build_ingest_dag(GP, schedule="15 */4 * * *")
                            │
plugins/celestrak/
  sources.py   ЧТО грузим: URL, формат, колонки и их типы
  client.py    КАК скачиваем: HTTP, 403, повторы, проверка ответа
  s3.py        КУДА кладём сырой файл (SeaweedFS)
  parsing.py   КАК разбираем: CSV/JSON → типизированные строки
  targets.py   КАК пишем в СУБД (ClickHouse; PostgreSQL / MySQL — для бенчмарка)
  meta.py      ЖУРНАЛЫ: meta.raw_files, meta.load_log, meta.benchmark_results
  ingest.py    ФАБРИКА DAG: собирает всё в extract → load_ods → validate
  assets.py    Airflow Assets, связывающие слои: ods_* → vault_* → vault_business
  vault.py     ЗАГРУЗКА DATA VAULT: ODS-снимок → stage → hubs/links/satellites, business vault
  vault_dag.py ФАБРИКА DAG vault_satcat / vault_orbit / vault_conjunction
  dq.py        ПРОВЕРКИ КАЧЕСТВА: sql/dq/*.sql → meta.dq_results
  benchmark.py, report.py   сравнение СУБД и отчёт
  config.py    параметры подключений из переменных окружения docker-compose
```

Airflow добавляет папку `plugins/` в `sys.path`, поэтому DAG импортируют пакет как
`from celestrak import ...`.

## 1. DAG-файл: только параметры

`dags/ingest_gp.py`:
```python
build_ingest_dag(GP, schedule="15 */4 * * *", doc_md="...")
```
Airflow импортирует файл, фабрика возвращает объект DAG, и он появляется в UI. Текст
`doc_md` виден в интерфейсе Airflow на странице DAG.

Первая строка каждого файла — комментарий `# airflow dag: ...`. Он нужен для *safe mode*
Airflow: файлы, в которых нет слов «airflow» и «dag», Airflow не разбирает.

**Добавить новый источник** = описать `Source` в `sources.py`, создать DDL в
`sql/ods/clickhouse/` и такой же DAG-файл из нескольких строк.

## 2. Описание источника: `sources.py`

Класс `Source` — декларативная схема набора:
```python
GP = Source(
    name="gp",                        # имя таблицы ods.gp
    url="https://celestrak.org/NORAD/elements/gp.php?GROUP=active&FORMAT=json",
    fmt="json",
    columns={"object_name": "str", "epoch": "datetime",
             "mean_motion": "float", "norad_cat_id": "int", ...},
    required=("norad_cat_id",),       # без этого поля строка бесполезна
)
```
Схема хранится в одном месте, и её используют парсер, загрузчик, экспорт в CSV для
сравнения СУБД и тесты. Наборы: `SATCAT`, `GP`, `SUPGP`, `SOCRATES`. Свойство
`ods_columns` = колонки источника + технические `load_id`, `loaded_at`, `source_file`.

## 3. Фабрика DAG: `ingest.py`

`build_ingest_dag` использует TaskFlow API Airflow 3 (`airflow.sdk.dag`, `airflow.sdk.task`):

```python
@dag(dag_id=f"ingest_{source.name}", schedule=schedule, catchup=False,
     max_active_runs=1, default_args={"retries": 2, "retry_delay": timedelta(minutes=5)})
def ingest():
    raw = extract()
    validate(raw, load_ods(raw))
```
- `catchup=False` — не догонять пропущенные интервалы, иначе при включении DAG полетела бы
  пачка запросов к API.
- `max_active_runs=1` — два запуска одного DAG не идут параллельно.
- `retries=2` — повтор упавшей задачи через 5 минут. 403 от CelesTrak ретраем не считается,
  см. ниже.
- Задачи передают друг другу через XCom только **маленький словарь** (`load_id`, путь в S3,
  время), а не сами данные. Данные идут через S3.

### Задача `extract`
```python
content = client.fetch(source.url, source.fmt)      # 1 HTTP-запрос
sha = hashlib.sha256(content).hexdigest()
if meta.last_sha256(source.name) == sha:            # файл не изменился
    raise AirflowSkipException(...)
load_id = f"gp_{fetched_at:%Y%m%dT%H%M%S}"          # gp_20261016T201839
key = f"gp/dt=2026-10-16/{load_id}.json"
s3.put(key, content, ...)                           # сырой файл как есть
meta.register_raw_file(...)                         # запись в meta.raw_files
return {"load_id": ..., "s3_key": ..., "fetched_at": ...}
```
`AirflowSkipException` помечает задачу как *skipped*, и следующие задачи тоже пропускаются.
Это не ошибка.

`meta.last_sha256` возвращает хэш последнего файла, который загрузился **успешно**. Если
прошлая загрузка упала, тот же файл будет загружен заново.

Ключ в S3 разбит по дате (`dt=YYYY-MM-DD`), как разделы в озере данных: файлы за день
удобно читать маской, например `s3://raw/gp/dt=2026-10-16/*.json`.

### Задача `load_ods`
Читает файл **из S3**, а не из API, разбирает его и пишет в ClickHouse:
```python
df = parsing.parse(source, s3.get(raw["s3_key"]))
rows = parsing.to_rows(df, source, {"load_id": ..., "loaded_at": ..., "source_file": ...})
target.load(source.table, source.ods_columns, rows, raw["load_id"])
meta.log_load(...)   # строк, секунд, success/failed
```
Неудачная загрузка тоже записывается в `meta.load_log` со статусом `failed` и текстом ошибки.

### Задача `validate`
Сверяет `count(*)` по этому `load_id` в базе с числом разобранных строк. Параметр
`outlets=[Asset("ods_gp")]` сообщает Airflow, что данные обновились. На это событие
подписаны DAG слоя Data Vault (data-aware scheduling, раздел 8).

## 4. Как скачивается API: `client.py`

Функция `fetch`:

| Ответ | Реакция |
|---|---|
| 200 | проверка содержимого (`validate`) |
| **403** | исключение `SourceNotUpdated`, задача пропускается, **без повтора**: повторы ведут к бану IP |
| 5xx или сетевая ошибка | до 3 попыток с паузой 10 и 20 с |

Запрос подписывается заголовком `User-Agent` (переменная `CELESTRAK_USER_AGENT`), чтобы
CelesTrak видел, кто его делает.

Пример ответа 403, полученного на практике:
```
SupGP data has not updated since last successful FILE=starlink download at 2026-10-16 20:06:49 UTC
Please limit requests to once every 2 hours
```

Проверка `validate` нужна потому, что на ошибочный запрос CelesTrak отвечает
**200 с текстом** вроде «No GP data found». Для JSON проверяется, что пришёл непустой массив,
для CSV — что первая строка похожа на заголовок с запятыми.

## 5. Как парсится ответ: `parsing.py`

`parse` превращает байты в `DataFrame` строго по схеме источника.

**Шаг 1. Чтение формата:**
```python
if source.fmt == "csv":
    raw = pd.read_csv(io.BytesIO(content), dtype=str, keep_default_na=False)
else:
    raw = pd.DataFrame(json.loads(content))     # JSON-массив объектов → таблица
raw.columns = [c.strip().lower() for c in raw.columns]   # NORAD_CAT_ID → norad_cat_id
```
CSV читается целиком как строки (`dtype=str`), чтобы pandas ничего не угадывал сам.
Например, `"1957-001A"` не должен стать числом, а `"NA"` не должен превратиться в NULL.

**Шаг 2. Отбор колонок и приведение типов** (`_cast`):
```python
for col, kind in source.columns.items():
    values = raw[col] if col in raw.columns else <все NULL>
    df[col] = _cast(values, kind)
```

| Тип в схеме | Как приводится | Пример |
|---|---|---|
| `str` | обрезка пробелов, `""` → NULL | `" ISS (ZARYA) "` → `ISS (ZARYA)` |
| `int` | `to_numeric(errors="coerce")` → `Int64` (целое с поддержкой NULL) | `"25544"` → 25544 |
| `float` | `to_numeric(errors="coerce")` | `"1.000E+00"` → 1.0 |
| `date` | формат `%Y-%m-%d` | `"1957-10-04"` → date |
| `datetime` | ISO 8601 | `"2026-10-16T04:36:59.916096"` → datetime |

`errors="coerce"` означает, что мусорное значение превращается в NULL, а не роняет загрузку.

Из этого следует:
- **лишние поля игнорируются**: если CelesTrak добавит новое поле, загрузка не сломается;
- **недостающие становятся NULL**: у GP нет `RMS`, но та же схема работает для SupGP;
- строки без обязательных полей (`required`) отбрасываются.

**Шаг 3. Преобразование в строки для базы** (`to_rows`). Типы pandas и numpy заменяются
обычными типами Python (`int`, `float`, `datetime`, `None`), потому что драйверы СУБД не
понимают `numpy.int64` и `pd.NaT`. К каждой строке добавляются три технических поля:

| Поле | Назначение |
|---|---|
| `load_id` | идентификатор загрузки (`gp_20261016T201839`); все строки одного файла имеют один `load_id` |
| `loaded_at` | момент скачивания файла (UTC); по нему отличают снимки |
| `source_file` | путь к сырому файлу в S3: строку можно отследить до исходника |

## 6. Запись в базу: `targets.py`

`ClickHouseTarget.load`:
```python
DELETE FROM ods.gp WHERE load_id = {load_id:String}   # lightweight delete прошлой попытки
client.insert("ods.gp", rows, column_names=...)        # один INSERT-блок
```
В ClickHouse нет транзакций, но один INSERT-блок в MergeTree атомарен: снимок (до ~1 млн
строк) либо виден целиком, либо отсутствует. `DELETE` того же `load_id` перед вставкой нужен
на случай ретрая: если задача упала после вставки и Airflow её перезапустил, дублей не
будет (проверено: двойная загрузка того же `load_id` даёт ровно одну копию снимка).

ODS — **append-only**: каждая загрузка добавляет полный снимок, старые не изменяются.
Последний снимок выбирается по `loaded_at = (SELECT max(loaded_at) ...)`.

Классы `PostgresTarget` и `MySQLTarget` реализуют тот же интерфейс `Target`:
`load_csv`, `analyze`, `query`, `execute`, `count`, `table_bytes`. Их использует только
`storage_benchmark`.

## 7. DAG сравнения: `dags/storage_benchmark.py`

Его логика написана прямо в файле, без фабрики:
```python
exported >> pg >> my >> ch >> render_report(exported)
```
Требует архивные СУБД: `docker compose --profile archive up -d`.
- `export_ods` выгружает все таблицы ODS из ClickHouse в CSV в `s3://exports/<run_id>/`.
  Формат CSV общий для всех трёх СУБД: заголовок, запятая, кавычки `"`, NULL = `\N`.
- `bench` — **одна функция**, вызванная три раза через `.override(task_id=...)`:
  - загрузка CSV родным bulk-методом СУБД: PostgreSQL `COPY`, MySQL `LOAD DATA LOCAL INFILE`,
    ClickHouse `CSVWithNames` (в базу `bench`, чтобы не трогать ODS);
  - `ANALYZE` (вне замера времени) и сверка числа строк с экспортом;
  - 6 запросов из `benchmark.py` (прогрев + медиана из 5);
  - размер таблиц на диске.
- Задачи идут **последовательно**, чтобы СУБД не мешали друг другу.
- `render_report` строит графики и `reports/lr1/results.md`. Сырые замеры лежат в
  `meta.benchmark_results`.

После этого DAG в PostgreSQL и MySQL лежит та же копия ODS, что и в ClickHouse.

## 8. Data Vault: `vault.py`, `vault_dag.py`, `sql/vault/`

Модель (hubs, links, satellites, business vault) описана в
[architecture-plan.md](architecture-plan.md) и [отчёте ЛР2](../reports/lr2/report.md).
Здесь — как она загружается.

```
ingest_satcat ──(ods_satcat)──────────► vault_satcat ──────┐
ingest_gp, ingest_supgp ──(ods_gp | ods_supgp)► vault_orbit ┼─(vault_*)─► vault_business ─(vault_business)─► data_quality
ingest_socrates ──(ods_socrates)──────► vault_conjunction ─┘
```

Задачи DAG raw vault: `ensure_schema` → `load_<source>` → `publish` (outlet `vault_<name>`).
- `ensure_schema` применяет DDL (`sql/ods/clickhouse/`, `sql/vault/ddl/`): всё через
  `IF NOT EXISTS` / `OR REPLACE`, повторять можно. Initdb ClickHouse срабатывает только на
  пустом томе, поэтому схема поддерживается так.
- `load_<source>` берёт снимки из `meta.load_log`, которых ещё нет в `meta.vault_log`, и по
  одному, от старых к новым, прогоняет SQL-файлы `sql/vault/load/<source>/` по порядку имён:

| Файл | Что делает |
|---|---|
| `00_stage.sql` | `stage.<source>` из одного снимка: дедуп, нормализация ключей, hash keys, hashdiff |
| `1x_hub_*.sql` | новые бизнес-ключи (anti-join `NOT IN`) |
| `2x_link_*.sql` | новые связи |
| `3x_sat_*.sql` | новые версии атрибутов: только где hashdiff ≠ последней версии на момент снимка |
| `4x_sts_*.sql` | статус пар SOCRATES: появилась `I` / выпала из прогноза `D` |

  Число записанных строк по каждому шагу пишется в `meta.vault_log.steps` (JSON). Затем
  `reconcile` проверяет, что все ключи staging дошли до хабов и линков.
- **Идемпотентность.** Каждый шаг вставляет только отсутствующее, `load_dts` берётся из
  снимка, а не из текущего времени. Повторная загрузка того же снимка пишет 0 строк.
- **Pool `vault` на 1 слот.** Все DAG пишут в общий `hub_object`, а в ClickHouse нет
  уникальных ключей, которые разрешили бы гонку параллельных вставок. Поэтому задачи загрузки
  идут по очереди (ReplacingMergeTree и проверка `duplicate_keys` — вторая линия защиты).
- `vault_business` выполняет `sql/vault/bv/*.sql`: вычисляемый сателлит орбит и PIT
  (`ASOF JOIN` выбирает версию, действовавшую на конец дня).

## 9. Data Quality: `dq.py`, `sql/dq/`

Одна проверка = один SQL-файл с заголовком:
```sql
-- check: orbit_element_ranges
-- severity: critical
-- description: Orbital elements are physically possible: 0 <= e < 1, ...
SELECT ... FROM rv.sat_object_orbit_gp WHERE NOT (...)   -- строки = нарушения
```
`run_checks` оборачивает запрос в `SELECT count(), groupArray(5)(...)`, пишет число нарушений
и 5 примеров в `meta.dq_results`. Задача `gate` роняет DAG, если провалилась хотя бы одна
`critical`-проверка; `warning` только фиксируются. Новая проверка = новый файл, код не меняется.

## Служебные таблицы (`meta`, ClickHouse)

| Таблица | Строка = | Зачем |
|---|---|---|
| `meta.raw_files` | один скачанный файл | история обращений к API: URL, размер, sha256, путь в S3, запуск DAG |
| `meta.load_log` | загрузка файла в одну СУБД | число строк, длительность, статус, ошибка |
| `meta.benchmark_results` | одно измерение бенчмарка | исходные данные для графиков сравнения |
| `meta.vault_log` | снимок ODS, загруженный в vault | строк в ODS и staging, строк по шагам, длительность; водяной знак инкремента |
| `meta.dq_results` | одна проверка в одном запуске DQ | число нарушений, примеры, пройдена ли |

`load_log` и `vault_log` — ReplacingMergeTree (повтор заменяет строку), читаются с `FINAL`.

```sql
-- история запросов к API
SELECT source, url, bytes, fetched_at FROM meta.raw_files FINAL ORDER BY fetched_at;
-- откуда взялась строка
SELECT norad_cat_id, epoch, load_id, source_file FROM ods.gp WHERE norad_cat_id = 25544;
-- что дал каждый снимок в vault
SELECT load_id, ods_rows, steps FROM meta.vault_log FINAL ORDER BY finished_at;
-- последний прогон проверок качества
SELECT check_name, severity, violations, passed FROM meta.dq_results
WHERE run_id = (SELECT argMax(run_id, checked_at) FROM meta.dq_results);
```

## Где посмотреть вживую
- **Airflow → DAG → Graph:** схема задач. Клик по задаче → **Logs**: лог запроса, размер
  файла, путь в S3, число строк и время, а у пропущенных запусков — текст ответа 403.
- **Тесты:** `tests/test_parsing.py` (что подаётся на вход парсера и что получается) и
  `tests/test_vault.py` (структура SQL-файлов vault и DQ). Запуск:
  `.venv\Scripts\python -m pytest tests`.
