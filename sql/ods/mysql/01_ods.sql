-- ODS copy in MySQL 8 (InnoDB) for the storage comparison: filled from the CSV export
-- of PostgreSQL ODS by the storage_benchmark DAG. `id` is a surrogate clustered key.
-- Executed by the mysql image on first start inside database `ods`.
USE ods;

CREATE TABLE IF NOT EXISTS satcat (
    id                BIGINT AUTO_INCREMENT PRIMARY KEY,
    object_name       VARCHAR(128),
    object_id         VARCHAR(16),
    norad_cat_id      INT NOT NULL,
    object_type       VARCHAR(8),
    ops_status_code   VARCHAR(4),
    owner             VARCHAR(16),
    launch_date       DATE,
    launch_site       VARCHAR(16),
    decay_date        DATE,
    period            DOUBLE,
    inclination       DOUBLE,
    apogee            DOUBLE,
    perigee           DOUBLE,
    rcs               DOUBLE,
    data_status_code  VARCHAR(8),
    orbit_center      VARCHAR(8),
    orbit_type        VARCHAR(8),
    load_id           VARCHAR(64)  NOT NULL,
    loaded_at         DATETIME(6)  NOT NULL,
    source_file       VARCHAR(255) NOT NULL,
    KEY ix_satcat_loaded_at (loaded_at),
    KEY ix_satcat_norad (norad_cat_id)
) ENGINE = InnoDB;

CREATE TABLE IF NOT EXISTS gp (
    id                   BIGINT AUTO_INCREMENT PRIMARY KEY,
    object_name          VARCHAR(128),
    object_id            VARCHAR(16),
    epoch                DATETIME(6),
    mean_motion          DOUBLE,
    eccentricity         DOUBLE,
    inclination          DOUBLE,
    ra_of_asc_node       DOUBLE,
    arg_of_pericenter    DOUBLE,
    mean_anomaly         DOUBLE,
    ephemeris_type       INT,
    classification_type  VARCHAR(4),
    norad_cat_id         INT NOT NULL,
    element_set_no       INT,
    rev_at_epoch         INT,
    bstar                DOUBLE,
    mean_motion_dot      DOUBLE,
    mean_motion_ddot     DOUBLE,
    load_id              VARCHAR(64)  NOT NULL,
    loaded_at            DATETIME(6)  NOT NULL,
    source_file          VARCHAR(255) NOT NULL,
    KEY ix_gp_loaded_at (loaded_at),
    KEY ix_gp_norad_epoch (norad_cat_id, epoch)
) ENGINE = InnoDB;

CREATE TABLE IF NOT EXISTS supgp (
    id                   BIGINT AUTO_INCREMENT PRIMARY KEY,
    object_name          VARCHAR(128),
    object_id            VARCHAR(16),
    epoch                DATETIME(6),
    mean_motion          DOUBLE,
    eccentricity         DOUBLE,
    inclination          DOUBLE,
    ra_of_asc_node       DOUBLE,
    arg_of_pericenter    DOUBLE,
    mean_anomaly         DOUBLE,
    ephemeris_type       INT,
    classification_type  VARCHAR(4),
    norad_cat_id         INT NOT NULL,
    element_set_no       INT,
    rev_at_epoch         INT,
    bstar                DOUBLE,
    mean_motion_dot      DOUBLE,
    mean_motion_ddot     DOUBLE,
    rms                  DOUBLE,
    data_source          VARCHAR(32),
    load_id              VARCHAR(64)  NOT NULL,
    loaded_at            DATETIME(6)  NOT NULL,
    source_file          VARCHAR(255) NOT NULL,
    KEY ix_supgp_loaded_at (loaded_at),
    KEY ix_supgp_norad_epoch (norad_cat_id, epoch)
) ENGINE = InnoDB;

CREATE TABLE IF NOT EXISTS socrates (
    id                  BIGINT AUTO_INCREMENT PRIMARY KEY,
    norad_cat_id_1      INT NOT NULL,
    object_name_1       VARCHAR(128),
    dse_1               DOUBLE,
    norad_cat_id_2      INT NOT NULL,
    object_name_2       VARCHAR(128),
    dse_2               DOUBLE,
    tca                 DATETIME(3),
    tca_range           DOUBLE,
    tca_relative_speed  DOUBLE,
    max_prob            DOUBLE,
    dilution            DOUBLE,
    load_id             VARCHAR(64)  NOT NULL,
    loaded_at           DATETIME(6)  NOT NULL,
    source_file         VARCHAR(255) NOT NULL,
    KEY ix_socrates_loaded_at (loaded_at),
    KEY ix_socrates_pair (norad_cat_id_1, norad_cat_id_2)
) ENGINE = InnoDB;
