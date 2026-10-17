-- Raw Vault (Data Vault 2.0) in ClickHouse: hubs = business keys, links = relationships,
-- satellites = descriptive attributes with full history (insert-only, hashdiff deltas).
--
-- Conventions
--   hash key / hashdiff = lower hex MD5 of the parts joined by '||' (NULL -> '')
--   load_dts            = ODS loaded_at of the snapshot (re-runs are idempotent)
--   record_source       = S3 key of the raw file the row came from
-- ClickHouse has no foreign keys or unique constraints: loaders insert only keys that are
-- absent (anti-join), ReplacingMergeTree collapses accidental duplicates of the sorting key,
-- and referential integrity is verified by the data_quality DAG.

CREATE DATABASE IF NOT EXISTS rv;
CREATE DATABASE IF NOT EXISTS stage;

CREATE FUNCTION IF NOT EXISTS dv_hash AS (s) -> lower(hex(MD5(s)));

-- Hubs --------------------------------------------------------------------------------

-- Space object, business key = NORAD catalog number. Fed by all four sources.
CREATE TABLE IF NOT EXISTS rv.hub_object
(
    hk_object      String,
    norad_cat_id   UInt32,
    load_dts       DateTime64(6),
    record_source  String
)
ENGINE = ReplacingMergeTree
ORDER BY hk_object;

-- Launch, business key = international designator without the piece letter (1998-067).
CREATE TABLE IF NOT EXISTS rv.hub_launch
(
    hk_launch          String,
    launch_designator  String,
    load_dts           DateTime64(6),
    record_source      String
)
ENGINE = ReplacingMergeTree
ORDER BY hk_launch;

-- Links -------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS rv.link_object_launch
(
    hk_object_launch  String,
    hk_object         String,
    hk_launch         String,
    load_dts          DateTime64(6),
    record_source     String
)
ENGINE = ReplacingMergeTree
ORDER BY hk_object_launch;

-- Pair of objects in a predicted close approach; ordered pair (smaller NORAD first).
CREATE TABLE IF NOT EXISTS rv.link_conjunction
(
    hk_conjunction  String,
    hk_object_1     String,
    hk_object_2     String,
    load_dts        DateTime64(6),
    record_source   String
)
ENGINE = ReplacingMergeTree
ORDER BY hk_conjunction;

-- Satellites --------------------------------------------------------------------------
-- SATCAT attributes are split by rate of change: description (rare) and status (frequent).

CREATE TABLE IF NOT EXISTS rv.sat_object_satcat_desc
(
    hk_object      String,
    load_dts       DateTime64(6),
    hashdiff       String,
    record_source  String,
    object_name    Nullable(String),
    object_type    LowCardinality(Nullable(String)),
    owner          LowCardinality(Nullable(String)),
    orbit_center   LowCardinality(Nullable(String))
)
ENGINE = ReplacingMergeTree
ORDER BY (hk_object, load_dts);

CREATE TABLE IF NOT EXISTS rv.sat_object_satcat_status
(
    hk_object         String,
    load_dts          DateTime64(6),
    hashdiff          String,
    record_source     String,
    ops_status_code   LowCardinality(Nullable(String)),
    decay_date        Nullable(Date32),
    data_status_code  LowCardinality(Nullable(String)),
    orbit_type        LowCardinality(Nullable(String)),
    rcs               Nullable(Float64),
    period            Nullable(Float64),
    inclination       Nullable(Float64),
    apogee            Nullable(Float64),
    perigee           Nullable(Float64)
)
ENGINE = ReplacingMergeTree
ORDER BY (hk_object, load_dts);

CREATE TABLE IF NOT EXISTS rv.sat_launch
(
    hk_launch      String,
    load_dts       DateTime64(6),
    hashdiff       String,
    record_source  String,
    launch_date    Nullable(Date32),
    launch_site    LowCardinality(Nullable(String))
)
ENGINE = ReplacingMergeTree
ORDER BY (hk_launch, load_dts);

-- Orbital elements: one satellite per source system (GP = 18 SDS radar, SupGP = operator).
CREATE TABLE IF NOT EXISTS rv.sat_object_orbit_gp
(
    hk_object            String,
    load_dts             DateTime64(6),
    hashdiff             String,
    record_source        String,
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
    element_set_no       Nullable(Int32),
    rev_at_epoch         Nullable(Int32),
    bstar                Nullable(Float64),
    mean_motion_dot      Nullable(Float64),
    mean_motion_ddot     Nullable(Float64)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(load_dts)
ORDER BY (hk_object, load_dts);

CREATE TABLE IF NOT EXISTS rv.sat_object_orbit_supgp
(
    hk_object            String,
    load_dts             DateTime64(6),
    hashdiff             String,
    record_source        String,
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
    element_set_no       Nullable(Int32),
    rev_at_epoch         Nullable(Int32),
    bstar                Nullable(Float64),
    mean_motion_dot      Nullable(Float64),
    mean_motion_ddot     Nullable(Float64),
    rms                  Nullable(Float64),
    data_source          LowCardinality(Nullable(String))
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(load_dts)
ORDER BY (hk_object, load_dts);

-- Conjunction forecast: multi-active satellite (several TCAs per pair in one forecast).
-- The whole set of a pair is versioned together: set_hashdiff covers all its rows.
CREATE TABLE IF NOT EXISTS rv.sat_conjunction
(
    hk_conjunction      String,
    load_dts            DateTime64(6),
    tca                 DateTime64(3),
    hashdiff            String,
    set_hashdiff        String,
    record_source       String,
    dse_1               Nullable(Float64),
    dse_2               Nullable(Float64),
    tca_range           Nullable(Float64),
    tca_relative_speed  Nullable(Float64),
    max_prob            Nullable(Float64),
    dilution            Nullable(Float64)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(load_dts)
ORDER BY (hk_conjunction, load_dts, tca);

-- Status tracking satellite: when a pair appears in (I) or drops out of (D) the forecast.
CREATE TABLE IF NOT EXISTS rv.sts_conjunction
(
    hk_conjunction  String,
    load_dts        DateTime64(6),
    status          LowCardinality(String),
    record_source   String,
    CONSTRAINT status_values CHECK status IN ('I', 'D')
)
ENGINE = ReplacingMergeTree
ORDER BY (hk_conjunction, load_dts);

-- Reference tables (code lists, see docs/domain.md) -----------------------------------

CREATE TABLE IF NOT EXISTS rv.ref_ops_status
(
    code         String,
    description  String,
    is_active    Bool
)
ENGINE = ReplacingMergeTree
ORDER BY code;

INSERT INTO rv.ref_ops_status
SELECT * FROM values('code String, description String, is_active Bool',
    ('+', 'Operational', true),
    ('P', 'Partially operational', true),
    ('B', 'Backup/standby', true),
    ('S', 'Spare', true),
    ('X', 'Extended mission', true),
    ('-', 'Nonoperational', false),
    ('D', 'Decayed', false),
    ('?', 'Unknown', false))
WHERE code NOT IN (SELECT code FROM rv.ref_ops_status);

CREATE TABLE IF NOT EXISTS rv.ref_object_type
(
    code         String,
    description  String
)
ENGINE = ReplacingMergeTree
ORDER BY code;

INSERT INTO rv.ref_object_type
SELECT * FROM values('code String, description String',
    ('PAY', 'Payload'),
    ('R/B', 'Rocket body'),
    ('DEB', 'Debris'),
    ('UNK', 'Unknown'))
WHERE code NOT IN (SELECT code FROM rv.ref_object_type);
