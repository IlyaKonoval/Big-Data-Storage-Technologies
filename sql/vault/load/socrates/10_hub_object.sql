-- Both objects of every pair. Objects missing from SATCAT (decayed long ago, analyst
-- objects) simply get a hub row here: in Data Vault no source waits for another.
INSERT INTO rv.hub_object (hk_object, norad_cat_id, load_dts, record_source)
SELECT hk_object, any(norad_cat_id), any(load_dts), any(record_source)
FROM
(
    SELECT hk_object_1 AS hk_object, norad_cat_id_1 AS norad_cat_id, load_dts, record_source
    FROM stage.socrates
    UNION ALL
    SELECT hk_object_2, norad_cat_id_2, load_dts, record_source
    FROM stage.socrates
)
WHERE hk_object NOT IN (SELECT hk_object FROM rv.hub_object)
GROUP BY hk_object;
