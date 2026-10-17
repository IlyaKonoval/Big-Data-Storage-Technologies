-- check: orbit_element_ranges
-- severity: critical
-- description: Orbital elements are physically possible: 0 <= e < 1, 0 <= inclination <= 180, mean_motion > 0, epoch present
SELECT 'gp' AS source, hk_object, load_dts, eccentricity, inclination, mean_motion
FROM rv.sat_object_orbit_gp
WHERE NOT ifNull(eccentricity >= 0 AND eccentricity < 1 AND inclination BETWEEN 0 AND 180
                 AND mean_motion > 0 AND epoch IS NOT NULL, false)
UNION ALL
SELECT 'supgp', hk_object, load_dts, eccentricity, inclination, mean_motion
FROM rv.sat_object_orbit_supgp
WHERE NOT ifNull(eccentricity >= 0 AND eccentricity < 1 AND inclination BETWEEN 0 AND 180
                 AND mean_motion > 0 AND epoch IS NOT NULL, false)
