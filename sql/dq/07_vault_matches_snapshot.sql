-- check: vault_matches_snapshot
-- severity: critical
-- description: Для каждого объекта последнего снимка версия сателлита, действующая на тот момент, совпадает со снимком (ничего не потеряно и не устарело)
SELECT 'sat_object_satcat_desc' AS satellite, s.hk_object AS hk
FROM stage.satcat AS s
LEFT JOIN
(
    SELECT hk_object, argMax(hashdiff, load_dts) AS hd FROM rv.sat_object_satcat_desc
    WHERE load_dts <= (SELECT max(load_dts) FROM stage.satcat) GROUP BY hk_object
) AS v ON v.hk_object = s.hk_object
WHERE v.hd != s.hd_desc
UNION ALL
SELECT 'sat_object_satcat_status', s.hk_object
FROM stage.satcat AS s
LEFT JOIN
(
    SELECT hk_object, argMax(hashdiff, load_dts) AS hd FROM rv.sat_object_satcat_status
    WHERE load_dts <= (SELECT max(load_dts) FROM stage.satcat) GROUP BY hk_object
) AS v ON v.hk_object = s.hk_object
WHERE v.hd != s.hd_status
UNION ALL
SELECT 'sat_object_orbit_gp', s.hk_object
FROM stage.gp AS s
LEFT JOIN
(
    SELECT hk_object, argMax(hashdiff, load_dts) AS hd FROM rv.sat_object_orbit_gp
    WHERE load_dts <= (SELECT max(load_dts) FROM stage.gp) GROUP BY hk_object
) AS v ON v.hk_object = s.hk_object
WHERE v.hd != s.hashdiff
UNION ALL
SELECT 'sat_object_orbit_supgp', s.hk_object
FROM stage.supgp AS s
LEFT JOIN
(
    SELECT hk_object, argMax(hashdiff, load_dts) AS hd FROM rv.sat_object_orbit_supgp
    WHERE load_dts <= (SELECT max(load_dts) FROM stage.supgp) GROUP BY hk_object
) AS v ON v.hk_object = s.hk_object
WHERE v.hd != s.hashdiff
