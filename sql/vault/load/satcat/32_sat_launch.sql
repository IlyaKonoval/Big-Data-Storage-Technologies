-- Launch date and site are repeated on every object of the launch; the primary object
-- (smallest NORAD number, usually piece A) is taken as the source of truth.
-- Disagreements between objects are reported by the data_quality check launch_consistency.
INSERT INTO rv.sat_launch (hk_launch, load_dts, hashdiff, record_source, launch_date, launch_site)
SELECT s.hk_launch, s.load_dts, s.hd_launch, s.record_source, s.launch_date, s.launch_site
FROM
(
    SELECT assumeNotNull(hk_launch) AS hk_launch, load_dts, hd_launch, record_source,
           launch_date, launch_site
    FROM stage.satcat
    WHERE hk_launch IS NOT NULL
    ORDER BY hk_launch, norad_cat_id
    LIMIT 1 BY hk_launch
) AS s
LEFT JOIN
(
    SELECT hk_launch, argMax(hashdiff, load_dts) AS hashdiff
    FROM rv.sat_launch
    WHERE load_dts <= (SELECT max(load_dts) FROM stage.satcat)
    GROUP BY hk_launch
) AS prev ON prev.hk_launch = s.hk_launch
WHERE prev.hashdiff != s.hd_launch;
