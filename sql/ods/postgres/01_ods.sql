-- ODS layer in PostgreSQL: raw CelesTrak snapshots, append-only.
-- Every load appends a full snapshot tagged with load_id / loaded_at.
CREATE SCHEMA IF NOT EXISTS ods;

-- SATCAT: satellite catalog (https://celestrak.org/pub/satcat.csv)
CREATE TABLE IF NOT EXISTS ods.satcat (
    object_name       TEXT,
    object_id         TEXT,
    norad_cat_id      INTEGER NOT NULL,
    object_type       TEXT,
    ops_status_code   TEXT,
    owner             TEXT,
    launch_date       DATE,
    launch_site       TEXT,
    decay_date        DATE,
    period            DOUBLE PRECISION,
    inclination       DOUBLE PRECISION,
    apogee            DOUBLE PRECISION,
    perigee           DOUBLE PRECISION,
    rcs               DOUBLE PRECISION,
    data_status_code  TEXT,
    orbit_center      TEXT,
    orbit_type        TEXT,
    load_id           TEXT        NOT NULL,
    loaded_at         TIMESTAMP   NOT NULL,
    source_file       TEXT        NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_satcat_loaded_at ON ods.satcat (loaded_at);
CREATE INDEX IF NOT EXISTS ix_satcat_norad ON ods.satcat (norad_cat_id);

-- GP: general perturbations orbital elements (OMM JSON), GROUP=active
CREATE TABLE IF NOT EXISTS ods.gp (
    object_name          TEXT,
    object_id            TEXT,
    epoch                TIMESTAMP,
    mean_motion          DOUBLE PRECISION,
    eccentricity         DOUBLE PRECISION,
    inclination          DOUBLE PRECISION,
    ra_of_asc_node       DOUBLE PRECISION,
    arg_of_pericenter    DOUBLE PRECISION,
    mean_anomaly         DOUBLE PRECISION,
    ephemeris_type       INTEGER,
    classification_type  TEXT,
    norad_cat_id         INTEGER NOT NULL,
    element_set_no       INTEGER,
    rev_at_epoch         INTEGER,
    bstar                DOUBLE PRECISION,
    mean_motion_dot      DOUBLE PRECISION,
    mean_motion_ddot     DOUBLE PRECISION,
    load_id              TEXT        NOT NULL,
    loaded_at            TIMESTAMP   NOT NULL,
    source_file          TEXT        NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_gp_loaded_at ON ods.gp (loaded_at);
CREATE INDEX IF NOT EXISTS ix_gp_norad_epoch ON ods.gp (norad_cat_id, epoch);

-- SupGP: operator-supplied orbital elements (Starlink), same OMM + rms/data_source
CREATE TABLE IF NOT EXISTS ods.supgp (
    object_name          TEXT,
    object_id            TEXT,
    epoch                TIMESTAMP,
    mean_motion          DOUBLE PRECISION,
    eccentricity         DOUBLE PRECISION,
    inclination          DOUBLE PRECISION,
    ra_of_asc_node       DOUBLE PRECISION,
    arg_of_pericenter    DOUBLE PRECISION,
    mean_anomaly         DOUBLE PRECISION,
    ephemeris_type       INTEGER,
    classification_type  TEXT,
    norad_cat_id         INTEGER NOT NULL,
    element_set_no       INTEGER,
    rev_at_epoch         INTEGER,
    bstar                DOUBLE PRECISION,
    mean_motion_dot      DOUBLE PRECISION,
    mean_motion_ddot     DOUBLE PRECISION,
    rms                  DOUBLE PRECISION,
    data_source          TEXT,
    load_id              TEXT        NOT NULL,
    loaded_at            TIMESTAMP   NOT NULL,
    source_file          TEXT        NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_supgp_loaded_at ON ods.supgp (loaded_at);
CREATE INDEX IF NOT EXISTS ix_supgp_norad_epoch ON ods.supgp (norad_cat_id, epoch);

-- SOCRATES: predicted close approaches between pairs of objects
CREATE TABLE IF NOT EXISTS ods.socrates (
    norad_cat_id_1      INTEGER NOT NULL,
    object_name_1       TEXT,
    dse_1               DOUBLE PRECISION,
    norad_cat_id_2      INTEGER NOT NULL,
    object_name_2       TEXT,
    dse_2               DOUBLE PRECISION,
    tca                 TIMESTAMP,
    tca_range           DOUBLE PRECISION,
    tca_relative_speed  DOUBLE PRECISION,
    max_prob            DOUBLE PRECISION,
    dilution            DOUBLE PRECISION,
    load_id             TEXT        NOT NULL,
    loaded_at           TIMESTAMP   NOT NULL,
    source_file         TEXT        NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_socrates_loaded_at ON ods.socrates (loaded_at);
CREATE INDEX IF NOT EXISTS ix_socrates_pair ON ods.socrates (norad_cat_id_1, norad_cat_id_2);

-- Scratch copies for the bulk-load benchmark: the ODS CSV export is loaded here, so
-- PostgreSQL is measured on the same input as MySQL/ClickHouse without touching ODS.
CREATE SCHEMA IF NOT EXISTS bench;
CREATE TABLE IF NOT EXISTS bench.satcat   (LIKE ods.satcat   INCLUDING ALL);
CREATE TABLE IF NOT EXISTS bench.gp       (LIKE ods.gp       INCLUDING ALL);
CREATE TABLE IF NOT EXISTS bench.supgp    (LIKE ods.supgp    INCLUDING ALL);
CREATE TABLE IF NOT EXISTS bench.socrates (LIKE ods.socrates INCLUDING ALL);
