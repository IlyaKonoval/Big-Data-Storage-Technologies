-- check: orphan_satellites
-- severity: critical
-- description: Every satellite row hangs on an existing hub or link key
SELECT 'sat_object_satcat_desc' AS satellite, hk_object AS hk FROM rv.sat_object_satcat_desc
WHERE hk_object NOT IN (SELECT hk_object FROM rv.hub_object)
UNION ALL
SELECT 'sat_object_satcat_status', hk_object FROM rv.sat_object_satcat_status
WHERE hk_object NOT IN (SELECT hk_object FROM rv.hub_object)
UNION ALL
SELECT 'sat_object_orbit_gp', hk_object FROM rv.sat_object_orbit_gp
WHERE hk_object NOT IN (SELECT hk_object FROM rv.hub_object)
UNION ALL
SELECT 'sat_object_orbit_supgp', hk_object FROM rv.sat_object_orbit_supgp
WHERE hk_object NOT IN (SELECT hk_object FROM rv.hub_object)
UNION ALL
SELECT 'sat_launch', hk_launch FROM rv.sat_launch
WHERE hk_launch NOT IN (SELECT hk_launch FROM rv.hub_launch)
UNION ALL
SELECT 'sat_conjunction', hk_conjunction FROM rv.sat_conjunction
WHERE hk_conjunction NOT IN (SELECT hk_conjunction FROM rv.link_conjunction)
UNION ALL
SELECT 'sts_conjunction', hk_conjunction FROM rv.sts_conjunction
WHERE hk_conjunction NOT IN (SELECT hk_conjunction FROM rv.link_conjunction)
UNION ALL
SELECT 'bv.sat_object_orbit_derived', hk_object FROM bv.sat_object_orbit_derived
WHERE hk_object NOT IN (SELECT hk_object FROM rv.hub_object)
