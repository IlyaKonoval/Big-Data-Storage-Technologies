-- check: satellite_redundant_versions
-- severity: critical
-- description: No two consecutive satellite versions of a key are identical (delta load works, every SCD2 interval is a real change)
SELECT satellite, hk, load_dts
FROM
(
    SELECT 'sat_object_satcat_desc' AS satellite, hk_object AS hk, load_dts, hashdiff,
           lagInFrame(hashdiff) OVER w AS prev
    FROM rv.sat_object_satcat_desc FINAL
    WINDOW w AS (PARTITION BY hk_object ORDER BY load_dts
                 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
    UNION ALL
    SELECT 'sat_object_satcat_status', hk_object, load_dts, hashdiff, lagInFrame(hashdiff) OVER w
    FROM rv.sat_object_satcat_status FINAL
    WINDOW w AS (PARTITION BY hk_object ORDER BY load_dts
                 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
    UNION ALL
    SELECT 'sat_launch', hk_launch, load_dts, hashdiff, lagInFrame(hashdiff) OVER w
    FROM rv.sat_launch FINAL
    WINDOW w AS (PARTITION BY hk_launch ORDER BY load_dts
                 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
    UNION ALL
    SELECT 'sat_object_orbit_gp', hk_object, load_dts, hashdiff, lagInFrame(hashdiff) OVER w
    FROM rv.sat_object_orbit_gp FINAL
    WINDOW w AS (PARTITION BY hk_object ORDER BY load_dts
                 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
    UNION ALL
    SELECT 'sat_object_orbit_supgp', hk_object, load_dts, hashdiff, lagInFrame(hashdiff) OVER w
    FROM rv.sat_object_orbit_supgp FINAL
    WINDOW w AS (PARTITION BY hk_object ORDER BY load_dts
                 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
    UNION ALL
    SELECT 'sat_conjunction (set)', hk_conjunction, load_dts, set_hashdiff,
           lagInFrame(set_hashdiff) OVER w
    FROM (SELECT DISTINCT hk_conjunction, load_dts, set_hashdiff FROM rv.sat_conjunction)
    WINDOW w AS (PARTITION BY hk_conjunction ORDER BY load_dts
                 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
)
WHERE prev = hashdiff
