-- Computed satellite (business rules from docs/domain.md) for every GP / SupGP version that
-- does not have it yet:
--   period      = 1440 / mean_motion                       [min]
--   a           = (mu / n^2)^(1/3), n in rad/s, mu = 398600.4418 km^3/s^2
--   perigee     = a * (1 - e) - 6378.137,  apogee = a * (1 + e) - 6378.137   [km]
--   regime      HEO: e >= 0.25; GEO: 0.99 <= mm <= 1.01 and e < 0.01; LEO: mm > 11.25; else MEO
-- Versions with impossible elements (mm <= 0, e outside [0, 1)) are skipped here and
-- reported by the data_quality check orbit_element_ranges.
INSERT INTO bv.sat_object_orbit_derived
    (hk_object, source, load_dts, epoch, period_min, semi_major_axis_km, perigee_km,
     apogee_km, orbit_regime)
SELECT hk_object, source, load_dts, epoch,
       1440 / mm AS period_min,
       pow(398600.4418 / pow(mm * 2 * pi() / 86400, 2), 1 / 3) AS semi_major_axis_km,
       semi_major_axis_km * (1 - e) - 6378.137 AS perigee_km,
       semi_major_axis_km * (1 + e) - 6378.137 AS apogee_km,
       multiIf(e >= 0.25, 'HEO',
               mm BETWEEN 0.99 AND 1.01 AND e < 0.01, 'GEO',
               mm > 11.25, 'LEO',
               'MEO') AS orbit_regime
FROM
(
    SELECT hk_object, 'gp' AS source, load_dts, epoch,
           assumeNotNull(mean_motion) AS mm, assumeNotNull(eccentricity) AS e
    FROM rv.sat_object_orbit_gp
    WHERE mean_motion > 0 AND eccentricity >= 0 AND eccentricity < 1
    UNION ALL
    SELECT hk_object, 'supgp' AS source, load_dts, epoch,
           assumeNotNull(mean_motion) AS mm, assumeNotNull(eccentricity) AS e
    FROM rv.sat_object_orbit_supgp
    WHERE mean_motion > 0 AND eccentricity >= 0 AND eccentricity < 1
) AS o
WHERE (hk_object, source, load_dts) NOT IN
      (SELECT hk_object, source, load_dts FROM bv.sat_object_orbit_derived);
