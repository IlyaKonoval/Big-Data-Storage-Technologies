-- Business Vault: derived data on top of the raw vault. Business rules live here; the raw
-- vault keeps source data as delivered.
CREATE DATABASE IF NOT EXISTS bv;

-- Computed satellite: orbit geometry derived from GP / SupGP elements.
CREATE TABLE IF NOT EXISTS bv.sat_object_orbit_derived
(
    hk_object           String,
    source              LowCardinality(String),   -- gp | supgp
    load_dts            DateTime64(6),
    epoch               Nullable(DateTime64(6)),
    period_min          Float64,
    semi_major_axis_km  Float64,
    perigee_km          Float64,
    apogee_km           Float64,
    orbit_regime        LowCardinality(String)    -- LEO | MEO | GEO | HEO
)
ENGINE = ReplacingMergeTree
ORDER BY (hk_object, source, load_dts);

-- Point-in-time table: for every object and day, load_dts of the satellite versions valid
-- at the end of that day (NULL = no version yet). Marts join satellites through it.
-- The current day is rebuilt on every run; the newest built_at wins (read with FINAL).
CREATE TABLE IF NOT EXISTS bv.pit_object
(
    snapshot_date  Date,
    hk_object      String,
    desc_ldts      Nullable(DateTime64(6)),
    status_ldts    Nullable(DateTime64(6)),
    gp_ldts        Nullable(DateTime64(6)),
    supgp_ldts     Nullable(DateTime64(6)),
    built_at       DateTime64(6)
)
ENGINE = ReplacingMergeTree(built_at)
ORDER BY (snapshot_date, hk_object);

-- SCD2 views over the SATCAT satellites: valid_from / valid_to / is_current.
CREATE OR REPLACE VIEW bv.object_desc_history AS
SELECT *, valid_to IS NULL AS is_current
FROM
(
    SELECT h.norad_cat_id,
           s.object_name,
           s.object_type,
           s.owner,
           s.orbit_center,
           s.load_dts AS valid_from,
           leadInFrame(toNullable(s.load_dts)) OVER w AS valid_to,
           s.record_source
    FROM rv.sat_object_satcat_desc AS s FINAL
    INNER JOIN (SELECT hk_object, any(norad_cat_id) AS norad_cat_id
                FROM rv.hub_object GROUP BY hk_object) AS h ON h.hk_object = s.hk_object
    WINDOW w AS (PARTITION BY s.hk_object ORDER BY s.load_dts
                 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
);

CREATE OR REPLACE VIEW bv.object_status_history AS
SELECT *, valid_to IS NULL AS is_current
FROM
(
    SELECT h.norad_cat_id,
           s.ops_status_code,
           r.description AS ops_status,
           ifNull(r.is_active, false) AS is_active,
           s.decay_date,
           s.data_status_code,
           s.orbit_type,
           s.rcs,
           s.period,
           s.inclination,
           s.apogee,
           s.perigee,
           s.load_dts AS valid_from,
           leadInFrame(toNullable(s.load_dts)) OVER w AS valid_to,
           s.record_source
    FROM rv.sat_object_satcat_status AS s FINAL
    INNER JOIN (SELECT hk_object, any(norad_cat_id) AS norad_cat_id
                FROM rv.hub_object GROUP BY hk_object) AS h ON h.hk_object = s.hk_object
    LEFT JOIN rv.ref_ops_status AS r ON r.code = s.ops_status_code
    WINDOW w AS (PARTITION BY s.hk_object ORDER BY s.load_dts
                 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
)
SETTINGS join_use_nulls = 1;

-- Conjunctions of the latest forecast: pairs whose last status is 'I', with the latest
-- version of their forecast set.
CREATE OR REPLACE VIEW bv.conjunction_current AS
SELECT k.norad_cat_id_1,
       k.norad_cat_id_2,
       s.tca,
       s.tca_range,
       s.tca_relative_speed,
       s.max_prob,
       s.dilution,
       s.dse_1,
       s.dse_2,
       s.load_dts AS load_dts   -- explicit: l.load_dts would otherwise make the name ambiguous
FROM rv.sat_conjunction AS s FINAL
INNER JOIN (SELECT hk_conjunction, max(load_dts) AS load_dts
            FROM rv.sat_conjunction GROUP BY hk_conjunction) AS l
        ON l.hk_conjunction = s.hk_conjunction AND l.load_dts = s.load_dts
INNER JOIN (SELECT c.hk_conjunction,
                   h1.norad_cat_id AS norad_cat_id_1,
                   h2.norad_cat_id AS norad_cat_id_2
            FROM rv.link_conjunction AS c FINAL
            INNER JOIN rv.hub_object AS h1 FINAL ON h1.hk_object = c.hk_object_1
            INNER JOIN rv.hub_object AS h2 FINAL ON h2.hk_object = c.hk_object_2) AS k
        ON k.hk_conjunction = s.hk_conjunction
WHERE s.hk_conjunction IN (SELECT hk_conjunction FROM rv.sts_conjunction
                           GROUP BY hk_conjunction HAVING argMax(status, load_dts) = 'I');

-- Current state of every object: the satellite versions referenced by the latest PIT day,
-- plus its launch. Orbit geometry comes from the GP computed satellite; objects without GP
-- (debris, rocket bodies, inactive payloads) get the regime from SATCAT period/apogee/perigee
-- by the same rules. The marts read objects through this view.
CREATE OR REPLACE VIEW bv.object_current AS
SELECT p.hk_object AS hk_object,
       h.norad_cat_id AS norad_cat_id,
       d.object_name AS object_name,
       ifNull(d.object_type, 'UNK') AS object_type,
       ifNull(t.description, 'Unknown') AS object_type_name,
       ifNull(d.owner, 'TBD') AS owner,
       d.orbit_center AS orbit_center,
       s.ops_status_code AS ops_status_code,
       ifNull(r.is_active, false) AS is_active,
       s.decay_date AS decay_date,
       p.status_ldts IS NOT NULL AND s.decay_date IS NULL
           AND ifNull(d.orbit_center, 'EA') = 'EA' AS in_orbit,   -- catalogued, not decayed
       s.rcs AS rcs,
       ifNull(g.inclination, s.inclination) AS inclination,
       ifNull(o.perigee_km, s.perigee) AS perigee_km,
       ifNull(o.apogee_km, s.apogee) AS apogee_km,
       ifNull(o.period_min, s.period) AS period_min,
       ifNull(o.orbit_regime,
              multiIf(s.period IS NULL OR s.period <= 0 OR s.apogee IS NULL OR s.perigee IS NULL, NULL,
                      (s.apogee - s.perigee) / (s.apogee + s.perigee + 2 * 6378.137) >= 0.25, 'HEO',
                      1440 / s.period BETWEEN 0.99 AND 1.01
                          AND (s.apogee - s.perigee) / (s.apogee + s.perigee + 2 * 6378.137) < 0.01, 'GEO',
                      1440 / s.period > 11.25, 'LEO',
                      'MEO')) AS orbit_regime,
       g.epoch AS gp_epoch,
       p.gp_ldts IS NOT NULL AS has_gp,
       p.supgp_ldts IS NOT NULL AS has_supgp,
       p.gp_ldts AS gp_ldts,
       p.supgp_ldts AS supgp_ldts,
       ifNull(d.object_name, '') LIKE 'STARLINK%' AS is_starlink,
       l.launch_designator AS launch_designator,
       l.launch_date AS launch_date,
       l.launch_site AS launch_site,
       p.snapshot_date AS snapshot_date
FROM (SELECT * FROM bv.pit_object FINAL
      WHERE snapshot_date = (SELECT max(snapshot_date) FROM bv.pit_object)) AS p
INNER JOIN (SELECT hk_object, any(norad_cat_id) AS norad_cat_id
            FROM rv.hub_object GROUP BY hk_object) AS h ON h.hk_object = p.hk_object
LEFT JOIN rv.sat_object_satcat_desc AS d FINAL
       ON d.hk_object = p.hk_object AND d.load_dts = p.desc_ldts
LEFT JOIN rv.sat_object_satcat_status AS s FINAL
       ON s.hk_object = p.hk_object AND s.load_dts = p.status_ldts
LEFT JOIN rv.sat_object_orbit_gp AS g FINAL
       ON g.hk_object = p.hk_object AND g.load_dts = p.gp_ldts
LEFT JOIN (SELECT * FROM bv.sat_object_orbit_derived FINAL WHERE source = 'gp') AS o
       ON o.hk_object = p.hk_object AND o.load_dts = p.gp_ldts
LEFT JOIN rv.ref_ops_status AS r ON r.code = s.ops_status_code
LEFT JOIN rv.ref_object_type AS t ON t.code = d.object_type
LEFT JOIN (SELECT lo.hk_object AS hk_object,
                  any(hl.launch_designator) AS launch_designator,
                  any(sl.launch_date) AS launch_date,
                  any(sl.launch_site) AS launch_site
           FROM rv.link_object_launch AS lo FINAL
           INNER JOIN rv.hub_launch AS hl FINAL ON hl.hk_launch = lo.hk_launch
           INNER JOIN (SELECT hk_launch,
                              argMax(launch_date, load_dts) AS launch_date,
                              argMax(launch_site, load_dts) AS launch_site
                       FROM rv.sat_launch GROUP BY hk_launch) AS sl
                   ON sl.hk_launch = lo.hk_launch
           GROUP BY lo.hk_object) AS l ON l.hk_object = p.hk_object
SETTINGS join_use_nulls = 1;
