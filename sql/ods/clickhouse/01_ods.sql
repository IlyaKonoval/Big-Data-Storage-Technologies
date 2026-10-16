-- ODS copy in ClickHouse for the storage comparison (filled by storage_benchmark from
-- the CSV export of PostgreSQL ODS), columnar MergeTree storage.
-- Sorting key = typical access path (object + time); partitions by load month.
CREATE DATABASE IF NOT EXISTS ods;

CREATE TABLE IF NOT EXISTS ods.satcat
(
    object_name       Nullable(String),
    object_id         Nullable(String),
    norad_cat_id      UInt32,
    object_type       LowCardinality(Nullable(String)),
    ops_status_code   LowCardinality(Nullable(String)),
    owner             LowCardinality(Nullable(String)),
    launch_date       Nullable(Date32),
    launch_site       LowCardinality(Nullable(String)),
    decay_date        Nullable(Date32),
    period            Nullable(Float64),
    inclination       Nullable(Float64),
    apogee            Nullable(Float64),
    perigee           Nullable(Float64),
    rcs               Nullable(Float64),
    data_status_code  LowCardinality(Nullable(String)),
    orbit_center      LowCardinality(Nullable(String)),
    orbit_type        LowCardinality(Nullable(String)),
    load_id           String,
    loaded_at         DateTime64(6),
    source_file       String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(loaded_at)
ORDER BY (loaded_at, norad_cat_id);

CREATE TABLE IF NOT EXISTS ods.gp
(
    object_name          Nullable(String),
    object_id            Nullable(String),
    epoch                Nullable(DateTime64(6)),
    mean_motion          Nullable(Float64),
    eccentricity         Nullable(Float64),
    inclination          Nullable(Float64),
    ra_of_asc_node       Nullable(Float64),
    arg_of_pericenter    Nullable(Float64),
    mean_anomaly         Nullable(Float64),
    ephemeris_type       Nullable(Int32),
    classification_type  LowCardinality(Nullable(String)),
    norad_cat_id         UInt32,
    element_set_no       Nullable(Int32),
    rev_at_epoch         Nullable(Int32),
    bstar                Nullable(Float64),
    mean_motion_dot      Nullable(Float64),
    mean_motion_ddot     Nullable(Float64),
    load_id              String,
    loaded_at            DateTime64(6),
    source_file          String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(loaded_at)
ORDER BY (norad_cat_id, loaded_at);

CREATE TABLE IF NOT EXISTS ods.supgp
(
    object_name          Nullable(String),
    object_id            Nullable(String),
    epoch                Nullable(DateTime64(6)),
    mean_motion          Nullable(Float64),
    eccentricity         Nullable(Float64),
    inclination          Nullable(Float64),
    ra_of_asc_node       Nullable(Float64),
    arg_of_pericenter    Nullable(Float64),
    mean_anomaly         Nullable(Float64),
    ephemeris_type       Nullable(Int32),
    classification_type  LowCardinality(Nullable(String)),
    norad_cat_id         UInt32,
    element_set_no       Nullable(Int32),
    rev_at_epoch         Nullable(Int32),
    bstar                Nullable(Float64),
    mean_motion_dot      Nullable(Float64),
    mean_motion_ddot     Nullable(Float64),
    rms                  Nullable(Float64),
    data_source          LowCardinality(Nullable(String)),
    load_id              String,
    loaded_at            DateTime64(6),
    source_file          String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(loaded_at)
ORDER BY (norad_cat_id, loaded_at);

CREATE TABLE IF NOT EXISTS ods.socrates
(
    norad_cat_id_1      UInt32,
    object_name_1       Nullable(String),
    dse_1               Nullable(Float64),
    norad_cat_id_2      UInt32,
    object_name_2       Nullable(String),
    dse_2               Nullable(Float64),
    tca                 Nullable(DateTime64(3)),
    tca_range           Nullable(Float64),
    tca_relative_speed  Nullable(Float64),
    max_prob            Nullable(Float64),
    dilution            Nullable(Float64),
    load_id             String,
    loaded_at           DateTime64(6),
    source_file         String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(loaded_at)
ORDER BY (loaded_at, norad_cat_id_1, norad_cat_id_2);
