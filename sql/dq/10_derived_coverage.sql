-- check: derived_coverage
-- severity: critical
-- description: Every valid GP / SupGP version has its computed business vault row
SELECT 'gp' AS source, hk_object, load_dts
FROM rv.sat_object_orbit_gp
WHERE mean_motion > 0 AND eccentricity >= 0 AND eccentricity < 1
  AND (hk_object, 'gp', load_dts) NOT IN
      (SELECT hk_object, source, load_dts FROM bv.sat_object_orbit_derived)
UNION ALL
SELECT 'supgp', hk_object, load_dts
FROM rv.sat_object_orbit_supgp
WHERE mean_motion > 0 AND eccentricity >= 0 AND eccentricity < 1
  AND (hk_object, 'supgp', load_dts) NOT IN
      (SELECT hk_object, source, load_dts FROM bv.sat_object_orbit_derived)
