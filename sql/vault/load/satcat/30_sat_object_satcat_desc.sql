-- Delta load: a new version only when the attributes differ from the latest version
-- known at this snapshot's load_dts. Unchanged objects add nothing; re-running the same
-- snapshot compares with its own rows and adds nothing either (idempotent).
-- An object without a previous version gets '' from the LEFT JOIN, so it is inserted.
INSERT INTO rv.sat_object_satcat_desc
    (hk_object, load_dts, hashdiff, record_source, object_name, object_type, owner, orbit_center)
SELECT s.hk_object, s.load_dts, s.hd_desc, s.record_source,
       s.object_name, s.object_type, s.owner, s.orbit_center
FROM stage.satcat AS s
LEFT JOIN
(
    SELECT hk_object, argMax(hashdiff, load_dts) AS hashdiff
    FROM rv.sat_object_satcat_desc
    WHERE load_dts <= (SELECT max(load_dts) FROM stage.satcat)
    GROUP BY hk_object
) AS prev ON prev.hk_object = s.hk_object
WHERE prev.hashdiff != s.hd_desc;
